#include "../stream_vs_message_server/benchmark_config.hpp"
#include <gflags/gflags.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <signal.h>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <chrono>
#include <algorithm>
#include <random>
#include <iomanip>
#include <sys/eventfd.h>
#include <dlfcn.h>

#include "tux.h"

// TUX function pointer types
typedef ssize_t (*libtux_send_tux_msg_t)(int fd, const struct msghdr *msg);
typedef ssize_t (*libtux_recv_tux_msg_t)(int fd, struct msghdr *msg);

// Global function pointers
libtux_send_tux_msg_t g_libtux_send_tux_msg = nullptr;
libtux_recv_tux_msg_t g_libtux_recv_tux_msg = nullptr;

// Initialize TUX functions
bool initialize_tux_functions() {
    void* handle = RTLD_DEFAULT;
    g_libtux_send_tux_msg = (libtux_send_tux_msg_t)dlsym(handle, "libtux_send_tux_msg");
    g_libtux_recv_tux_msg = (libtux_recv_tux_msg_t)dlsym(handle, "libtux_recv_tux_msg");
    
    if (!g_libtux_send_tux_msg || !g_libtux_recv_tux_msg) {
        handle = dlopen("libtux.so", RTLD_LAZY);
        if (!handle) return false;
        
        g_libtux_send_tux_msg = (libtux_send_tux_msg_t)dlsym(handle, "libtux_send_tux_msg");
        g_libtux_recv_tux_msg = (libtux_recv_tux_msg_t)dlsym(handle, "libtux_recv_tux_msg");
    }
    
    return g_libtux_send_tux_msg && g_libtux_recv_tux_msg;
}

// Command line flags
DEFINE_string(server, "127.0.0.1", "Server address");
DEFINE_uint32(port, 12345, "Server port");
DEFINE_uint32(threads, 4, "Number of client threads");
DEFINE_uint32(connections_per_thread, 8, "Connections per thread");
DEFINE_uint32(runtime, 30, "Runtime in seconds");
DEFINE_bool(measure_latency, false, "Track and report request latencies");
DEFINE_uint32(tuple_size, 1024, "Size of the tuple payload in bytes");
DEFINE_string(tux_mode, "none", "TUX mode: 'none', 'message', or 'stream'");
DEFINE_uint32(qps_per_thread, 0, "Target queries per second per thread (0 = max speed)");
DEFINE_bool(debug, false, "Enable debug output");
DEFINE_uint32(batch_size, 1, "Number of tuples to send in a batch");
DEFINE_bool(wait_response, false, "Wait for server response after each batch");

// Global variables
std::atomic<bool> should_exit(false);
std::atomic<uint64_t> total_tuples_sent(0);
std::atomic<uint64_t> total_bytes_sent(0);
std::atomic<uint64_t> total_tux_calls(0);

// Latency tracking
std::vector<double> send_latencies;
std::mutex latency_mutex;

// Signal handler for CTRL+C
void handle_signal(int) {
    should_exit.store(true);
    std::cout << "Shutting down gracefully..." << std::endl;
}

// Return timestamp in microseconds
uint64_t get_timestamp_us() {
    auto now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()
    ).count();
}

// Helper function to create a socket and connect to the server
int connect_to_server(const char* server_ip, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Failed to create socket");
        return -1;
    }
    
    // Set TCP_NODELAY to disable Nagle's algorithm
    int flag = 1;
    if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int)) < 0) {
        perror("Failed to set TCP_NODELAY");
        close(sock);
        return -1;
    }
    
    // Set non-blocking
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
    
    // Connect to server
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, server_ip, &server_addr.sin_addr);
    
    // Non-blocking connect
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        if (errno != EINPROGRESS) {
            perror("Failed to connect");
            close(sock);
            return -1;
        }
        
        // Wait for connection to complete
        fd_set wfds;
        struct timeval tv;
        FD_ZERO(&wfds);
        FD_SET(sock, &wfds);
        tv.tv_sec = 5;
        tv.tv_usec = 0;
        
        if (select(sock + 1, NULL, &wfds, NULL, &tv) <= 0) {
            perror("Connection timeout");
            close(sock);
            return -1;
        }
        
        // Check if connection was successful
        int error = 0;
        socklen_t len = sizeof(error);
        if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error) {
            perror("Connection failed");
            close(sock);
            return -1;
        }
    }
    
    return sock;
}

// Client connection class
class ClientConnection {
private:
    int socket_fd;
    bool uses_tux;
    TransportMode transport_mode;
    uint32_t next_tuple_id;
    std::mt19937 rng;
    
    // For batched sending
    std::vector<char> batch_buffer;
    uint32_t current_batch_size;
    uint32_t current_batch_bytes;
    char response_buffer[256];
    
public:
    ClientConnection(int fd, TransportMode mode)
        : socket_fd(fd), uses_tux(mode != NONE), transport_mode(mode),
          next_tuple_id(0), current_batch_size(0), current_batch_bytes(0) {
        
        // Initialize random number generator
        std::random_device rd;
        rng.seed(rd());
        
        if (uses_tux && (!g_libtux_send_tux_msg || !g_libtux_recv_tux_msg)) {
            std::cerr << "TUX functions not initialized" << std::endl;
            uses_tux = false;
        }
        
        // Initialize batch buffer if batch_size > 1
        if (FLAGS_batch_size > 1) {
            batch_buffer.resize(MAX_TUPLE_SIZE * FLAGS_batch_size);
        }
    }
    
    ~ClientConnection() {
        // Flush any pending batched tuples before closing
        if (current_batch_size > 0) {
            flush_batch();
        }
        
        if (socket_fd >= 0) {
            close(socket_fd);
        }
    }
    
    // Send a tuple using the configured transport mode
    bool send_tuple(uint32_t size) {
        auto tuple_start_time = get_timestamp_us();
        uint32_t tuple_id = next_tuple_id++;
        
        // Create the tuple using stack allocation
        BenchmarkTuple tuple;
        BenchmarkTuple::create(tuple, tuple_id, size);
        // Set the timestamp for latency measurement
        tuple.timestamp = std::chrono::high_resolution_clock::now().time_since_epoch().count();
        size_t tuple_size = sizeof(BenchmarkHeader) + tuple.payload_size;
        
        bool success = false;
        
        // Check if we need to handle batching
        if (FLAGS_batch_size > 1) {
            // Add the tuple to the batch buffer
            if (current_batch_size >= FLAGS_batch_size || 
                current_batch_bytes + tuple_size > batch_buffer.size()) {
                // Batch is full, flush it first
                if (!flush_batch()) {
                    return false;
                }
            }
            
            // Add tuple to batch buffer
            memcpy(batch_buffer.data() + current_batch_bytes, &tuple, tuple_size);
            current_batch_bytes += tuple_size;
            current_batch_size++;
            
            // Check if batch is now full and should be sent
            if (current_batch_size >= FLAGS_batch_size) {
                success = flush_batch();
            } else {
                // Not sending yet, but consider it successful for now
                success = true;
            }
        } else if (uses_tux && g_libtux_send_tux_msg) {
            // Use TUX for sending with g_libtux_send_tux_msg
            // Set up the iovec array and msghdr for TUX
            struct iovec iov;
            iov.iov_base = &tuple;
            iov.iov_len = tuple_size;
            
            // Set up the message header
            struct msghdr msg;
            memset(&msg, 0, sizeof(msg));
            msg.msg_iov = &iov;
            msg.msg_iovlen = 1;
            
            // Set order flag in TUX header based on transport mode
            if (transport_mode == STREAM) {
                // Stream interface - in-order delivery
                // This is handled by the TUX library when using libtux_send_tux_msg
            }
            
            // Send the message using TUX
            ssize_t bytes_sent = g_libtux_send_tux_msg(socket_fd, &msg);
            success = (bytes_sent == static_cast<ssize_t>(tuple_size));
            
            if (success) {
                total_tux_calls.fetch_add(1);
            }
        } else {
            // Fallback to standard socket send
            ssize_t sent = send(socket_fd, &tuple, tuple_size, 0);
            success = (sent == static_cast<ssize_t>(tuple_size));
        }
        
        if (success && !FLAGS_batch_size > 1) {
            total_tuples_sent.fetch_add(1);
            total_bytes_sent.fetch_add(tuple_size);
            
            // Track latency if enabled
            if (FLAGS_measure_latency) {
                auto tuple_end_time = get_timestamp_us();
                double latency_ms = (tuple_end_time - tuple_start_time) / 1000.0;
                
                std::lock_guard<std::mutex> lock(latency_mutex);
                send_latencies.push_back(latency_ms);
            }
        }
        
        return success;
    }
    
    // Flush the current batch of tuples to the server
    bool flush_batch() {
        if (current_batch_size == 0 || current_batch_bytes == 0) {
            return true;
        }
        
        bool success = false;
        
        // Send the batched data
        if (uses_tux && g_libtux_send_tux_msg) {
            struct iovec iov;
            iov.iov_base = batch_buffer.data();
            iov.iov_len = current_batch_bytes;
            
            struct msghdr msg;
            memset(&msg, 0, sizeof(msg));
            msg.msg_iov = &iov;
            msg.msg_iovlen = 1;
            
            ssize_t bytes_sent = g_libtux_send_tux_msg(socket_fd, &msg);
            success = (bytes_sent == static_cast<ssize_t>(current_batch_bytes));
            
            if (success) {
                total_tux_calls.fetch_add(1);
            }
        } else {
            ssize_t bytes_sent = send(socket_fd, batch_buffer.data(), current_batch_bytes, 0);
            success = (bytes_sent == static_cast<ssize_t>(current_batch_bytes));
        }
        
        if (success) {
            // Add batch stats to global counters
            total_tuples_sent.fetch_add(current_batch_size);
            total_bytes_sent.fetch_add(current_batch_bytes);
            
            // Wait for response from server if requested
            if (FLAGS_wait_response) {
                // Set a short timeout for reading response
                struct timeval tv;
                tv.tv_sec = 0;
                tv.tv_usec = 100000; // 100ms timeout
                
                fd_set read_fds;
                FD_ZERO(&read_fds);
                FD_SET(socket_fd, &read_fds);
                
                if (select(socket_fd + 1, &read_fds, NULL, NULL, &tv) > 0) {
                    ssize_t recv_size = recv(socket_fd, response_buffer, sizeof(response_buffer), 0);
                    if (recv_size <= 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                        if (FLAGS_debug) {
                            std::cerr << "Error receiving response: " << strerror(errno) << std::endl;
                        }
                    }
                }
            }
            
            // Reset batch counters
            current_batch_size = 0;
            current_batch_bytes = 0;
        }
        
        return success;
    }
    
    int get_socket_fd() const { return socket_fd; }
    bool is_tux_enabled() const { return uses_tux; }
};

// Calculate percentile from a sorted vector of values
double calculate_percentile(const std::vector<double>& sorted_values, double percentile) {
    if (sorted_values.empty()) {
        return 0.0;
    }
    
    double index = percentile * (sorted_values.size() - 1);
    size_t lower_index = static_cast<size_t>(index);
    double weight = index - lower_index;
    
    if (lower_index + 1 >= sorted_values.size()) {
        return sorted_values[lower_index];
    }
    
    return sorted_values[lower_index] * (1 - weight) + sorted_values[lower_index + 1] * weight;
}

// Worker thread function
void worker_thread(int thread_id) {
    std::vector<std::unique_ptr<ClientConnection>> connections;
    TransportMode mode = parseTransportMode(FLAGS_tux_mode);
    
    // Create connections
    for (uint32_t i = 0; i < FLAGS_connections_per_thread; i++) {
        int sock = connect_to_server(FLAGS_server.c_str(), FLAGS_port);
        if (sock < 0) {
            std::cerr << "Thread " << thread_id << " failed to create connection " << i << std::endl;
            continue;
        }
        
        connections.push_back(std::make_unique<ClientConnection>(sock, mode));
        if (FLAGS_debug) {
            std::cout << "Thread " << thread_id << " created connection " << i
                      << " with fd " << sock << std::endl;
        }
    }
    
    if (connections.empty()) {
        std::cerr << "Thread " << thread_id << " couldn't establish any connections" << std::endl;
        return;
    }
    
    // Set up epoll for connection management
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("Failed to create epoll instance");
        return;
    }
    
    // Create eventfd for rate limiting timing
    int timer_fd = eventfd(0, EFD_NONBLOCK);
    if (timer_fd < 0) {
        perror("Failed to create eventfd");
        close(epoll_fd);
        return;
    }
    
    // Register the timer with epoll
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = timer_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, timer_fd, &ev) < 0) {
        perror("Failed to add eventfd to epoll");
        close(timer_fd);
        close(epoll_fd);
        return;
    }
    
    // Register all connections with epoll
    for (const auto& conn : connections) {
        int fd = conn->get_socket_fd();
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLOUT;
        ev.data.fd = fd;
        
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            perror("Failed to add socket to epoll");
        }
    }
    
    // Setup rate limiting if needed
    bool rate_limiting = (FLAGS_qps_per_thread > 0);
    uint64_t send_interval_us = rate_limiting ? 1000000 / FLAGS_qps_per_thread : 0;
    uint64_t last_send_time = 0;
    
    // Main processing loop
    const int MAX_EVENTS = 64;
    struct epoll_event events[MAX_EVENTS];
    
    while (!should_exit.load()) {
        // Determine timeout for epoll_wait
        int timeout = rate_limiting ? 1 : 10; // ms
        
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, timeout);
        
        uint64_t now = get_timestamp_us();
        
        // Check if it's time to send a tuple (when rate limiting)
        if (rate_limiting) {
            if (now - last_send_time >= send_interval_us) {
                // Find a connection and send a tuple
                for (auto& conn : connections) {
                    if (conn->send_tuple(FLAGS_tuple_size)) {
                        last_send_time = now;
                        break;
                    }
                }
            }
        } else {
            // No rate limiting - send as fast as possible on all connections
            for (auto& conn : connections) {
                conn->send_tuple(FLAGS_tuple_size);
            }
        }
        
        // Process epoll events if any
        for (int i = 0; i < num_events; i++) {
            // Handle socket events if needed (currently we're just sending, not receiving)
        }
    }
    
    // Cleanup
    close(timer_fd);
    close(epoll_fd);
}

// Print benchmark statistics
void print_statistics(double run_time_seconds) {
    uint64_t tuples = total_tuples_sent.load();
    uint64_t bytes = total_bytes_sent.load();
    uint64_t tux_calls = total_tux_calls.load();
    
    double tuples_per_sec = tuples / run_time_seconds;
    double mb_per_sec = (bytes / (1024.0 * 1024.0)) / run_time_seconds;
    
    std::cout << "=== Benchmark Results ===" << std::endl;
    std::cout << "Transport Mode: " << FLAGS_tux_mode << std::endl;
    std::cout << "Runtime: " << run_time_seconds << " seconds" << std::endl;
    std::cout << "Batch Size: " << FLAGS_batch_size << std::endl;
    std::cout << "Wait for Response: " << (FLAGS_wait_response ? "Yes" : "No") << std::endl;
    std::cout << "Total Tuples Sent: " << tuples << std::endl;
    std::cout << "Estimated Batches Sent: " << (FLAGS_batch_size > 1 ? (tuples / FLAGS_batch_size) : tuples) << std::endl;
    std::cout << "Total Data Sent: " << bytes << " bytes (" 
              << (bytes / (1024.0 * 1024.0)) << " MB)" << std::endl;
    std::cout << "TUX Calls: " << tux_calls << std::endl;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2) 
              << tuples_per_sec << " tuples/sec, " 
              << mb_per_sec << " MB/sec" << std::endl;
    
    if (FLAGS_measure_latency) {
        std::vector<double> sorted_latencies;
        {
            std::lock_guard<std::mutex> lock(latency_mutex);
            sorted_latencies = send_latencies;
        }
        
        if (!sorted_latencies.empty()) {
            std::sort(sorted_latencies.begin(), sorted_latencies.end());
            
            double avg_latency = 0.0;
            for (double lat : sorted_latencies) {
                avg_latency += lat;
            }
            avg_latency /= sorted_latencies.size();
            
            double p50 = calculate_percentile(sorted_latencies, 0.5);
            double p90 = calculate_percentile(sorted_latencies, 0.9);
            double p99 = calculate_percentile(sorted_latencies, 0.99);
            double p999 = calculate_percentile(sorted_latencies, 0.999);
            
            std::cout << "Latency (ms):" << std::endl;
            std::cout << "  Average: " << std::fixed << std::setprecision(3) << avg_latency << std::endl;
            std::cout << "  p50: " << p50 << std::endl;
            std::cout << "  p90: " << p90 << std::endl;
            std::cout << "  p99: " << p99 << std::endl;
            std::cout << "  p99.9: " << p999 << std::endl;
        }
    }
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    
    // Set up signal handler
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    
    // Initialize TUX functions
    if (!initialize_tux_functions()) {
        std::cerr << "Failed to initialize TUX functions" << std::endl;
        return 1;
    }
    
    // Start client threads
    std::vector<std::thread> threads;
    for (uint32_t i = 0; i < FLAGS_threads; i++) {
        threads.emplace_back(worker_thread, i);
    }
    
    // Print progress during the benchmark
    auto start_time = std::chrono::high_resolution_clock::now();
    uint64_t prev_tuples = 0;
    
    std::cout << "Running benchmark with " << FLAGS_threads << " threads, "
              << FLAGS_connections_per_thread << " connections per thread, "
              << "tuple size: " << FLAGS_tuple_size << " bytes, "
              << "transport mode: " << FLAGS_tux_mode << std::endl;
    
    // Main loop with periodic status updates
    for (uint32_t i = 0; i < FLAGS_runtime && !should_exit.load(); i++) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        uint64_t current_tuples = total_tuples_sent.load();
        uint64_t delta = current_tuples - prev_tuples;
        prev_tuples = current_tuples;
        
        std::cout << "[" << (i+1) << "/" << FLAGS_runtime << "] "
                  << delta << " tuples/sec, "
                  << (delta * FLAGS_tuple_size / (1024.0 * 1024.0)) << " MB/sec"
                  << std::endl;
    }
    
    // Signal threads to exit
    should_exit.store(true);
    
    auto end_time = std::chrono::high_resolution_clock::now();
    double run_time_seconds = std::chrono::duration<double>(end_time - start_time).count();
    
    // Wait for all threads to finish
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    
    // Print final statistics
    print_statistics(run_time_seconds);
    
    return 0;
}
