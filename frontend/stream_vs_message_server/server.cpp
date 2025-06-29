#include "benchmark_config.hpp"
#include <gflags/gflags.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <signal.h>
#include <netinet/tcp.h>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <cstring>
#include <mutex>
#include <memory>
#include <chrono>
#include <random>
#include <dlfcn.h>

#include "tux.h"

// -------------------------------------------------------------------------------------
// Command line flags
DEFINE_uint32(port, 12345, "Server port");
DEFINE_uint32(epoll_timeout, 10, "Epoll timeout in ms");
DEFINE_uint32(max_connections, 1024, "Maximum number of connections");
DEFINE_bool(debug, false, "Enable debug output");
DEFINE_uint32(worker_threads, 4, "Number of worker threads");
DEFINE_uint32(dispatcher_threads, 1, "Number of dispatcher threads");
DEFINE_string(tux_mode, "none", "TUX mode: 'none', 'message', or 'stream'");
DEFINE_bool(selective_pushdown, true, "Enable selective UDF pushdown");
DEFINE_bool(measure_latency, true, "Track and report processing latencies");
DEFINE_uint32(runtime, 30, "Runtime in seconds");
DEFINE_bool(verify_data, true, "Verify data integrity");
DEFINE_bool(send_responses, true, "Send responses after processing tuples");

// -------------------------------------------------------------------------------------
// TUX-related function declarations and typedefs
typedef bool (*libtux_register_input_message_handler_t)(int fd, 
                                                      tux_input_message_handler handler, 
                                                      void* user_state,
                                                      tux_user_state_context_switch_out_handler ctx_out,
                                                      tux_user_state_context_switch_in_handler ctx_in);

typedef bool (*libtux_register_input_stream_handler_t)(int fd, 
                                                     tux_input_stream_handler handler, 
                                                     void* user_state,
                                                     tux_user_state_context_switch_out_handler ctx_out,
                                                     tux_user_state_context_switch_in_handler ctx_in);

typedef void (*libtux_pushdown_pause_t)(int fd);
typedef void (*libtux_pushdown_resume_t)(int fd);

// Forward declarations for functions
bool process_benchmark_tuple(const void* data, size_t len);
int stream_process_tuple(void* user_state, const void* data, size_t len);
int message_process_tuple(void* user_state, const void* data, size_t len);
bool process_batched_tuples(const void* data, size_t len, int& num_tuples_processed);

// Function pointers
libtux_register_input_message_handler_t g_libtux_register_input_message_handler = nullptr;
libtux_register_input_stream_handler_t g_libtux_register_input_stream_handler = nullptr;
libtux_pushdown_pause_t g_libtux_pushdown_pause = nullptr;
libtux_pushdown_resume_t g_libtux_pushdown_resume = nullptr;

// Global variables
std::atomic<bool> should_exit(false);
std::atomic<uint64_t> total_tuples_processed(0);
std::atomic<uint64_t> total_bytes_processed(0);
std::atomic<uint64_t> total_udf_calls(0);
std::atomic<uint64_t> num_errors(0);
std::atomic<uint64_t> total_batches_processed(0);
std::atomic<uint64_t> total_responses_sent(0);

// Latency tracking
std::vector<double> processing_latencies;
std::vector<double> batch_processing_latencies;
std::mutex latency_mutex;

// Helper function for context switching (required for TUX handlers)
void leanstore_ctx_out(void* user_state __attribute__((unused)), 
                      char* user_tl_buffer __attribute__((unused)), 
                      size_t user_tl_buffer_size __attribute__((unused))) {
    // No state to save in our benchmark
}

void leanstore_ctx_in(void* user_state __attribute__((unused)), 
                     char* user_tl_buffer __attribute__((unused)), 
                     size_t user_tl_buffer_size __attribute__((unused))) {
    // No state to restore in our benchmark
}

// Initialize TUX functions
bool initialize_tux_functions() {
    void* handle = RTLD_DEFAULT;
    
    g_libtux_register_input_message_handler = 
        (libtux_register_input_message_handler_t)dlsym(handle, "libtux_register_input_message_handler");
    
    g_libtux_register_input_stream_handler = 
        (libtux_register_input_stream_handler_t)dlsym(handle, "libtux_register_input_stream_handler");
    
    g_libtux_pushdown_pause = (libtux_pushdown_pause_t)dlsym(handle, "libtux_pushdown_pause");
    g_libtux_pushdown_resume = (libtux_pushdown_resume_t)dlsym(handle, "libtux_pushdown_resume");
    
    if (!g_libtux_register_input_message_handler || !g_libtux_register_input_stream_handler) {
        handle = dlopen("libtux.so", RTLD_LAZY);
        if (!handle) {
            std::cerr << "Failed to load libtux.so" << std::endl;
            return false;
        }
        
        g_libtux_register_input_message_handler = 
            (libtux_register_input_message_handler_t)dlsym(handle, "libtux_register_input_message_handler");
        
        g_libtux_register_input_stream_handler = 
            (libtux_register_input_stream_handler_t)dlsym(handle, "libtux_register_input_stream_handler");
        
        g_libtux_pushdown_pause = (libtux_pushdown_pause_t)dlsym(handle, "libtux_pushdown_pause");
        g_libtux_pushdown_resume = (libtux_pushdown_resume_t)dlsym(handle, "libtux_pushdown_resume");
    }
    
    if (!g_libtux_register_input_message_handler) {
        std::cerr << "Failed to find libtux_register_input_message_handler" << std::endl;
        return false;
    }
    
    if (!g_libtux_register_input_stream_handler) {
        std::cerr << "Failed to find libtux_register_input_stream_handler" << std::endl;
        return false;
    }
    
    std::cout << "TUX functions initialized successfully" << std::endl;
    return true;
}

// Signal handler for CTRL+C
void handle_signal(int) {
    should_exit.store(true);
    std::cout << "Shutting down gracefully..." << std::endl;
}

// Helper structure for connections
struct ClientConnection {
    int socket_fd;
    bool uses_tux;
    std::string buffer;
    void* tux_ctx;  // TUX context for this connection
    
    // For batch processing
    std::vector<char> response_buffer;
    
    ClientConnection(int fd) : socket_fd(fd), uses_tux(false), tux_ctx(nullptr) {
        // Initialize response buffer
        if (FLAGS_send_responses) {
            response_buffer.resize(64); // Simple response buffer
        }
    }
    
    ~ClientConnection() {
        if (socket_fd >= 0) {
            close(socket_fd);
        }
    }
    
    // Send a response after processing tuples
    bool send_response(int num_tuples_processed) {
        if (!FLAGS_send_responses) {
            return true;
        }
        
        // Create a simple response message
        char* buf = response_buffer.data();
        std::string response = "PROCESSED " + std::to_string(num_tuples_processed) + " TUPLES";
        size_t response_len = response.length();
        memcpy(buf, response.c_str(), response_len);
        
        ssize_t sent = send(socket_fd, buf, response_len, 0);
        bool success = (sent == static_cast<ssize_t>(response_len));
        
        if (success) {
            total_responses_sent.fetch_add(1);
        }
        
        return success;
    }
};

// Process a benchmark tuple
bool process_benchmark_tuple(const void *data, size_t len) {
    if (!data || len < sizeof(BenchmarkTuple)) {
        return false;
    }
    
    const BenchmarkTuple* tuple = static_cast<const BenchmarkTuple*>(data);
    
    // Data verification (simple checksum)
    if (FLAGS_verify_data) {
        uint32_t checksum = 0;
        for (uint32_t i = 0; i < tuple->payload_size; i++) {
            checksum += tuple->payload[i];
        }
        
        if (checksum != tuple->checksum) {
            num_errors.fetch_add(1);
            return false;
        }
    }
    
    // Count the tuple
    total_tuples_processed.fetch_add(1);
    total_bytes_processed.fetch_add(len);
    
    // Track latency if enabled
    if (FLAGS_measure_latency) {
        auto now = std::chrono::high_resolution_clock::now();
        auto tuple_time = std::chrono::nanoseconds(tuple->timestamp);
        auto client_time = std::chrono::time_point<std::chrono::high_resolution_clock>(tuple_time);
        double latency_ms = std::chrono::duration<double, std::milli>(now - client_time).count();
        
        // Only record if the timestamp is valid and latency is reasonable
        if (latency_ms > 0 && latency_ms < 10000) {  // Ignore clearly wrong values
            std::lock_guard<std::mutex> lock(latency_mutex);
            processing_latencies.push_back(latency_ms);
        }
    }
    
    return true;
}

// Process a batch of tuples
bool process_batched_tuples(const void* data, size_t len, int& num_tuples_processed) {
    num_tuples_processed = 0;
    if (!data || len == 0) {
        return false;
    }
    
    auto start_time = std::chrono::high_resolution_clock::now();
    const char* buffer = static_cast<const char*>(data);
    size_t offset = 0;
    
    while (offset + sizeof(BenchmarkHeader) <= len) {
        const BenchmarkTuple* tuple = reinterpret_cast<const BenchmarkTuple*>(buffer + offset);
        size_t tuple_size = sizeof(BenchmarkHeader) + tuple->payload_size;
        
        if (offset + tuple_size > len) {
            // Incomplete tuple, can't process the rest of the batch
            break;
        }
        
        // Process the tuple
        if (process_benchmark_tuple(tuple, tuple_size)) {
            num_tuples_processed++;
        }
        
        // Move to the next tuple
        offset += tuple_size;
    }
    
    if (num_tuples_processed > 0) {
        auto end_time = std::chrono::high_resolution_clock::now();
        double batch_latency = std::chrono::duration<double, std::milli>(end_time - start_time).count();
        
        // Track batch processing latency
        {
            std::lock_guard<std::mutex> lock(latency_mutex);
            batch_processing_latencies.push_back(batch_latency);
        }
        
        // Increment the batch counter
        total_batches_processed.fetch_add(1);
    }
    
    return num_tuples_processed > 0;
}

// Implementation of UDF functions
int stream_process_tuple(void* user_state __attribute__((unused)), const void* data, size_t len) {
    // Process the tuple using the shared function
    return process_benchmark_tuple(data, len) ? 0 : -1;
}

int message_process_tuple(void* user_state __attribute__((unused)), const void* data, size_t len) {
    // Process the tuple using the shared function
    return process_benchmark_tuple(data, len) ? 0 : -1;
}

// TUX stream handler (ordered processing)
void tux_stream_handler(int fd, struct tux_user_context* ctx __attribute__((unused)), void* user_state) {
    // Get the connection data
    auto* conn = reinterpret_cast<ClientConnection*>(user_state);
    if (!conn) {
        std::cerr << "Invalid connection state in stream handler" << std::endl;
        return;
    }
    
    // Note: TUX guarantees that the stream handler will be called with ordered data
    // The handler should read data from the socket using recv() or other methods
    char buffer[MAX_TUPLE_SIZE * 64]; // Increased buffer size to handle batches
    ssize_t bytes_read = recv(fd, buffer, sizeof(buffer), 0);
    
    if (bytes_read <= 0) {
        if (bytes_read < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            std::cerr << "Error reading in stream handler: " << strerror(errno) << std::endl;
        }
        return;
    }
    
    // Add received data to the buffer
    conn->buffer.append(buffer, bytes_read);
    
    // Try to process as a batch first
    if (conn->buffer.size() >= sizeof(BenchmarkHeader)) {
        int num_tuples_processed = 0;
        bool batch_processed = process_batched_tuples(conn->buffer.data(), conn->buffer.size(), num_tuples_processed);
        
        if (batch_processed) {
            // Send response if needed
            if (FLAGS_send_responses) {
                conn->send_response(num_tuples_processed);
            }
            
            // Clear the entire buffer since we've processed all data
            conn->buffer.clear();
            return;
        }
    }
    
    // Fall back to processing individual tuples
    int total_tuples_processed = 0;
    while (conn->buffer.size() >= sizeof(BenchmarkHeader)) {
        const BenchmarkTuple* tuple = reinterpret_cast<const BenchmarkTuple*>(conn->buffer.data());
        size_t tuple_size = sizeof(BenchmarkHeader) + tuple->payload_size;
        
        if (conn->buffer.size() < tuple_size) {
            break; // Incomplete tuple, wait for more data
        }
        
        // Process the complete tuple
        if (process_benchmark_tuple(tuple, tuple_size)) {
            total_tuples_processed++;
        }
        
        // Remove processed data from the buffer
        conn->buffer.erase(0, tuple_size);
    }
    
    // Send response if needed
    if (FLAGS_send_responses && total_tuples_processed > 0) {
        conn->send_response(total_tuples_processed);
    }
}

// TUX message handler (unordered processing)
int tux_message_handler(int fd __attribute__((unused)), 
                       struct tux_user_context* user_ctx __attribute__((unused)), 
                       const struct tux_user_message_group* msg_group, void* user_state) {
    auto* conn = reinterpret_cast<ClientConnection*>(user_state);
    int total_tuples_processed = 0;
    
    // Process each message in the group
    for (int i = 0; i < msg_group->n_messages; i++) {
        const struct tux_user_message* msg = &msg_group->messages[i];
        assert(msg->n_packets == 1);
        // Process each packet in the message
        for (int j = 0; j < msg->n_packets; j++) {
            const void* data = msg->packets[j].iov_base;
            size_t len = msg->packets[j].iov_len;
            
            // Try to process as a batch first
            int batch_tuples_processed = 0;
            if (process_batched_tuples(data, len, batch_tuples_processed)) {
                total_tuples_processed += batch_tuples_processed;
                total_batches_processed.fetch_add(1);
            } else if (process_benchmark_tuple(data, len)) {
                // Fall back to processing as a single tuple
                total_tuples_processed++;
                total_udf_calls.fetch_add(1);
            }
        }
    }
    
    // Send response if needed
    if (FLAGS_send_responses && total_tuples_processed > 0) {
        conn->send_response(total_tuples_processed);
    }
    
    // Return DONE to indicate we've consumed the message
    return TUX_MESSAGE_DONE;
}

// Register TUX handlers for a connection
bool register_tux_handlers(ClientConnection* conn) {
    if (!conn || !conn->uses_tux) {
        return false;
    }
    
    bool result = false;
    TransportMode mode = parseTransportMode(FLAGS_tux_mode);
    
    if (mode == MESSAGE) {
        if (!g_libtux_register_input_message_handler) {
            std::cerr << "TUX message handler function not initialized" << std::endl;
            return false;
        }
        
        result = g_libtux_register_input_message_handler(
            conn->socket_fd,
            tux_message_handler,
            conn,               // Pass the connection as user state
            leanstore_ctx_out,
            leanstore_ctx_in
        );
        
        if (result) {
            if (FLAGS_debug) {
                std::cout << "Registered TUX message handler for fd " << conn->socket_fd << std::endl;
            }
        } else {
            std::cerr << "Failed to register TUX message handler for fd " << conn->socket_fd << std::endl;
        }
    } else if (mode == STREAM) {
        if (!g_libtux_register_input_stream_handler) {
            std::cerr << "TUX stream handler function not initialized" << std::endl;
            return false;
        }
        
        result = g_libtux_register_input_stream_handler(
            conn->socket_fd,
            tux_stream_handler,
            conn,               // Pass the connection as user state
            leanstore_ctx_out,
            leanstore_ctx_in
        );
        
        if (result) {
            if (FLAGS_debug) {
                std::cout << "Registered TUX stream handler for fd " << conn->socket_fd << std::endl;
            }
        } else {
            std::cerr << "Failed to register TUX stream handler for fd " << conn->socket_fd << std::endl;
        }
    }
    
    return result;
}

// Worker thread function
void worker_thread(int thread_id __attribute__((unused)), int epoll_fd) {
    const int MAX_EVENTS = 64;
    struct epoll_event events[MAX_EVENTS];
    
    while (!should_exit.load()) {
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, FLAGS_epoll_timeout);
        
        for (int i = 0; i < num_events; i++) {
            if (should_exit.load()) break;
            
            auto* conn = static_cast<ClientConnection*>(events[i].data.ptr);
            
            // Handle events
            if (events[i].events & EPOLLIN) {
                if (conn->uses_tux) {
                    // For TUX, receive processing is handled by the registered handlers
                    // We just need to check if there are errors on the socket
                    char buf[1];
                    int ret = recv(conn->socket_fd, buf, 1, MSG_PEEK | MSG_DONTWAIT);
                    if (ret <= 0 && (ret == 0 || (errno != EAGAIN && errno != EWOULDBLOCK))) {
                        // Connection closed or error
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->socket_fd, nullptr);
                        delete conn;
                    }
                } else {
                    // Traditional socket processing
                    char buffer[MAX_TUPLE_SIZE * 64]; // Increased buffer size to handle batches
                    ssize_t bytes_read = recv(conn->socket_fd, buffer, sizeof(buffer), 0);
                    
                    if (bytes_read <= 0) {
                        // Connection closed or error
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->socket_fd, nullptr);
                        delete conn;
                        continue;
                    }
                    
                    // Process received data
                    conn->buffer.append(buffer, bytes_read);
                    
                    // First try to process as a batch
                    if (conn->buffer.size() >= sizeof(BenchmarkHeader)) {
                        int num_tuples_processed = 0;
                        bool batch_processed = process_batched_tuples(conn->buffer.data(), conn->buffer.size(), num_tuples_processed);
                        
                        if (batch_processed) {
                            // Send response for the batch if needed
                            if (FLAGS_send_responses) {
                                conn->send_response(num_tuples_processed);
                            }
                            
                            // Clear the entire buffer since we've processed all data
                            conn->buffer.clear();
                        } else {
                            // Fall back to processing individual tuples
                            // Process complete tuples
                            while (conn->buffer.size() >= sizeof(BenchmarkHeader)) {
                                const BenchmarkTuple* tuple = reinterpret_cast<const BenchmarkTuple*>(conn->buffer.data());
                                size_t tuple_size = sizeof(BenchmarkHeader) + tuple->payload_size;
                                
                                if (conn->buffer.size() < tuple_size) {
                                    // Incomplete tuple, wait for more data
                                    break;
                                }
                                
                                auto start_time = std::chrono::high_resolution_clock::now();
                                
                                // Process the tuple
                                if (FLAGS_verify_data) {
                                    uint32_t checksum = 0;
                                    for (uint32_t i = 0; i < tuple->payload_size; i++) {
                                        checksum += tuple->payload[i];
                                    }
                                    
                                    if (checksum != tuple->checksum) {
                                        num_errors.fetch_add(1);
                                    }
                                }
                                
                                auto end_time = std::chrono::high_resolution_clock::now();
                                double latency = std::chrono::duration<double, std::milli>(end_time - start_time).count();
                                
                                if (FLAGS_measure_latency) {
                                    std::lock_guard<std::mutex> guard(latency_mutex);
                                    processing_latencies.push_back(latency);
                                }
                                
                                total_tuples_processed.fetch_add(1);
                                total_bytes_processed.fetch_add(tuple_size);
                                
                                // Remove processed tuple from buffer
                                conn->buffer.erase(0, tuple_size);
                                
                                // Send response after processing if needed
                                if (FLAGS_send_responses) {
                                    conn->send_response(1);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    
    // Register signal handler
    signal(SIGINT, handle_signal);
    
    // Initialize TUX if needed
    TransportMode mode = parseTransportMode(FLAGS_tux_mode);
    bool use_tux = (mode == STREAM || mode == MESSAGE);
    
    if (use_tux) {
        if (!initialize_tux_functions()) {
            std::cerr << "Failed to initialize TUX functions, falling back to standard socket mode" << std::endl;
            FLAGS_tux_mode = "none";
        } else {
            std::cout << "TUX initialized successfully in " << FLAGS_tux_mode << " mode" << std::endl;
        }
    }
    
    // Create server socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket" << std::endl;
        return 1;
    }
    
    // Set socket options
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "Failed to set socket options" << std::endl;
        close(server_fd);
        return 1;
    }
    
    // Disable Nagle's algorithm
    if (setsockopt(server_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
        std::cerr << "Failed to disable Nagle's algorithm" << std::endl;
    }
    
    // Bind to port
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(FLAGS_port);
    
    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Failed to bind to port " << FLAGS_port << std::endl;
        close(server_fd);
        return 1;
    }
    
    // Listen for connections
    if (listen(server_fd, 128) < 0) {
        std::cerr << "Failed to listen for connections" << std::endl;
        close(server_fd);
        return 1;
    }
    
    // Make server socket non-blocking
    int flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);
    
    // Create epoll instance
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        std::cerr << "Failed to create epoll instance" << std::endl;
        close(server_fd);
        return 1;
    }
    
    // Add server socket to epoll
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) < 0) {
        std::cerr << "Failed to add server socket to epoll" << std::endl;
        close(server_fd);
        close(epoll_fd);
        return 1;
    }
    
    std::cout << "Server started on port " << FLAGS_port << std::endl;
    std::cout << "TUX mode: " << FLAGS_tux_mode << std::endl;
    
    // Start worker threads
    std::vector<std::thread> worker_threads;
    for (uint32_t i = 0; i < FLAGS_worker_threads; i++) {
        worker_threads.emplace_back(worker_thread, i, epoll_fd);
    }
    
    // Register UDFs
    int (*stream_udf)(void*, const void*, size_t) = &stream_process_tuple;
    int (*message_udf)(void*, const void*, size_t) = &message_process_tuple;
    
    // Main event loop
    struct epoll_event events[64];
    auto start_time = std::chrono::steady_clock::now();
    
    while (!should_exit.load()) {
        // Check if runtime has been exceeded
        auto current_time = std::chrono::steady_clock::now();
        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time).count();
        if (FLAGS_runtime > 0 && elapsed_seconds >= FLAGS_runtime) {
            should_exit.store(true);
            break;
        }
        
        // Accept new connections
        int num_events = epoll_wait(epoll_fd, events, 64, FLAGS_epoll_timeout);
        
        for (int i = 0; i < num_events; i++) {
            if (events[i].data.fd == server_fd) {
                // New connection
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
                
                if (client_fd < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    }
                    std::cerr << "Failed to accept connection: " << strerror(errno) << std::endl;
                    continue;
                }
                
                // Make socket non-blocking
                int flags = fcntl(client_fd, F_GETFL, 0);
                fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
                
                // Disable Nagle's algorithm
                int flag = 1;
                setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
                
                // Create connection object
                auto* conn = new ClientConnection(client_fd);
                
                // Set up TUX if enabled
                TransportMode mode = parseTransportMode(FLAGS_tux_mode);
                if (mode != NONE) {
                    conn->uses_tux = true;
                    
                    // Register TUX handlers for this connection
                    if (!register_tux_handlers(conn)) {
                        std::cerr << "Failed to register TUX handlers" << std::endl;
                        delete conn;
                        close(client_fd);
                        continue;
                    }
                    
                    // For TUX mode, we don't need to register UDFs here
                    // The handlers are already registered with the connection
                    // This is handled by the register_tux_handlers function
                }
                
                // Add to epoll
                struct epoll_event ev;
                ev.events = EPOLLIN;
                ev.data.ptr = conn;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
                    std::cerr << "Failed to add client to epoll" << std::endl;
                    delete conn;
                    continue;
                }
                
                char ip_str[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &client_addr.sin_addr, ip_str, INET_ADDRSTRLEN);
                if (FLAGS_debug) {
                    std::cout << "New connection from " << ip_str << ":" << ntohs(client_addr.sin_port) << std::endl;
                }
            } else {
                // Handle in worker threads
            }
        }
        
        // Print progress every second
        static auto last_report_time = start_time;
        static uint64_t last_tuples = 0;
        static uint64_t last_bytes = 0;
        
        auto now = std::chrono::steady_clock::now();
        auto time_since_report = std::chrono::duration_cast<std::chrono::seconds>(now - last_report_time).count();
        
        if (time_since_report >= 1) {
            uint64_t current_tuples = total_tuples_processed.load();
            uint64_t current_bytes = total_bytes_processed.load();
            
            uint64_t tuples_since_last = current_tuples - last_tuples;
            uint64_t bytes_since_last = current_bytes - last_bytes;
            
            double tuples_per_sec = tuples_since_last / time_since_report;
            double mb_per_sec = (bytes_since_last / (1024.0 * 1024.0)) / time_since_report;
            
            std::cout << "Processed: " << tuples_per_sec << " tuples/sec, " 
                      << mb_per_sec << " MB/sec, " 
                      << "Total: " << current_tuples << " tuples, "
                      << (current_bytes / (1024 * 1024)) << " MB" 
                      << std::endl;
            
            last_report_time = now;
            last_tuples = current_tuples;
            last_bytes = current_bytes;
        }
    }
    
    // Wait for worker threads to finish
    for (auto& t : worker_threads) {
        t.join();
    }
    
    // Calculate and print statistics
    uint64_t total_tuples = total_tuples_processed.load();
    uint64_t total_bytes = total_bytes_processed.load();
    uint64_t total_udfs = total_udf_calls.load();
    auto end_time = std::chrono::steady_clock::now();
    double runtime_seconds = std::chrono::duration<double>(end_time - start_time).count();
    
    std::cout << "\n=== Final Results ===" << std::endl;
    std::cout << "Runtime: " << runtime_seconds << " seconds" << std::endl;
    std::cout << "TUX mode: " << FLAGS_tux_mode << std::endl;
    std::cout << "Total tuples processed: " << total_tuples << std::endl;
    std::cout << "Total data processed: " << (total_bytes / (1024.0 * 1024.0)) << " MB" << std::endl;
    std::cout << "Total UDF calls: " << total_udfs << std::endl;
    std::cout << "Total batches processed: " << total_batches_processed.load() << std::endl;
    std::cout << "Total responses sent: " << total_responses_sent.load() << std::endl;
    std::cout << "Throughput: " << (total_tuples / runtime_seconds) << " tuples/sec" << std::endl;
    std::cout << "Data rate: " << (total_bytes / runtime_seconds / (1024.0 * 1024.0)) << " MB/sec" << std::endl;
    std::cout << "Errors: " << num_errors.load() << std::endl;
    
    if (FLAGS_measure_latency && !processing_latencies.empty()) {
        // Calculate latency statistics
        std::sort(processing_latencies.begin(), processing_latencies.end());
        
        double p50 = processing_latencies[processing_latencies.size() * 0.5];
        double p90 = processing_latencies[processing_latencies.size() * 0.9];
        double p99 = processing_latencies[processing_latencies.size() * 0.99];
        double p999 = processing_latencies[processing_latencies.size() * 0.999];
        
        std::cout << "\n=== Tuple Processing Latency Statistics (ms) ===" << std::endl;
        std::cout << "p50: " << p50 << std::endl;
        std::cout << "p90: " << p90 << std::endl;
        std::cout << "p99: " << p99 << std::endl;
        std::cout << "p999: " << p999 << std::endl;
    }
    
    if (FLAGS_measure_latency && !batch_processing_latencies.empty()) {
        // Calculate batch latency statistics
        std::sort(batch_processing_latencies.begin(), batch_processing_latencies.end());
        
        double p50 = batch_processing_latencies[batch_processing_latencies.size() * 0.5];
        double p90 = batch_processing_latencies[batch_processing_latencies.size() * 0.9];
        double p99 = batch_processing_latencies[batch_processing_latencies.size() * 0.99];
        double p999 = batch_processing_latencies[batch_processing_latencies.size() * 0.999];
        
        std::cout << "\n=== Batch Processing Latency Statistics (ms) ===" << std::endl;
        std::cout << "p50: " << p50 << std::endl;
        std::cout << "p90: " << p90 << std::endl;
        std::cout << "p99: " << p99 << std::endl;
        std::cout << "p999: " << p999 << std::endl;
        std::cout << "Average tuples per batch: " << 
            (total_batches_processed.load() > 0 ? 
             static_cast<double>(total_tuples_processed.load()) / total_batches_processed.load() : 0.0) 
            << std::endl;
    }
    
    // Clean up
    close(server_fd);
    close(epoll_fd);
    
    return 0;
}
