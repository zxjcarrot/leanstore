#include "Units.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ScrambledZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <iostream>
#include <iomanip>
#include <string>
#include <mutex>
#include <memory>
#include <queue>
#include <unordered_map>
#include <netinet/tcp.h>
#include <dlfcn.h>

#include <memory.h>
#include <random>

#include "tux.h"
// -------------------------------------------------------------------------------------
DEFINE_string(server, "127.0.0.1", "Server address");
DEFINE_uint32(port, 12345, "Server port");
DEFINE_uint32(threads, 4, "Number of client threads");
DEFINE_uint32(connections_per_thread, 8, "Connections per thread");
DEFINE_uint32(runtime, 10, "Runtime in seconds");
DEFINE_uint64(requests, 0, "Total number of requests (0 = unlimited)");
DEFINE_uint32(key_range, 1000000, "Range of keys to use");
DEFINE_uint32(value_size, 1024, "Size of values for PUT operations");
DEFINE_uint32(read_ratio, 80, "Read ratio (0-100)");
DEFINE_bool(zipf, true, "Use zipfian distribution for keys");
DEFINE_double(zipf_factor, 0.99, "Zipf factor (higher = more skew)");
DEFINE_uint32(report_interval, 1, "Reporting interval in seconds");
DEFINE_bool(debug, false, "Enable debug output");
DEFINE_uint32(max_inflight_per_connection, 16, "Maximum in-flight requests per connection");
DEFINE_uint64(throttle_rate, 0, "Max requests per second per thread (0 = no limit)");
DEFINE_bool(measure_latency, true, "Track and report request latencies");
DEFINE_bool(tux, false, "Use TUX message interface instead of POSIX send/recv");

typedef ssize_t (*libtux_send_tux_msg_t)(int fd, const struct msghdr *msg);
typedef ssize_t (*libtux_recv_tux_msg_t)(int fd, struct msghdr *msg);

// Global function pointers
libtux_send_tux_msg_t g_libtux_send_tux_msg = nullptr;
libtux_recv_tux_msg_t g_libtux_recv_tux_msg = nullptr;

// Function to initialize TUX interfaces
bool initialize_tux_functions() {
    // Try to load from the already loaded libraries first
    void* handle = RTLD_DEFAULT;
    
    // Load functions
    g_libtux_send_tux_msg = (libtux_send_tux_msg_t)dlsym(handle, "libtux_send_tux_msg");
    g_libtux_recv_tux_msg = (libtux_recv_tux_msg_t)dlsym(handle, "libtux_recv_tux_msg");
    
    // If either function is not found, try explicitly loading the library
    if (!g_libtux_send_tux_msg || !g_libtux_recv_tux_msg) {
        handle = dlopen("libtux.so", RTLD_LAZY);
        if (!handle) {
            std::cerr << "Failed to load libtux.so: " << dlerror() << std::endl;
            return false;
        }
        
        if (!g_libtux_send_tux_msg) {
            g_libtux_send_tux_msg = (libtux_send_tux_msg_t)dlsym(handle, "libtux_send_tux_msg");
            if (!g_libtux_send_tux_msg) {
                std::cerr << "Failed to find libtux_send_tux_msg: " << dlerror() << std::endl;
                return false;
            }
        }
        
        if (!g_libtux_recv_tux_msg) {
            g_libtux_recv_tux_msg = (libtux_recv_tux_msg_t)dlsym(handle, "libtux_recv_tux_msg");
            if (!g_libtux_recv_tux_msg) {
                std::cerr << "Failed to find libtux_recv_tux_msg: " << dlerror() << std::endl;
                return false;
            }
        }
    }
    
    return g_libtux_send_tux_msg && g_libtux_recv_tux_msg;
}

// Message types - must match server definitions
enum MessageType {
  GET_REQUEST = 1,
  GET_RESPONSE = 2,
  PUT_REQUEST = 3,
  PUT_RESPONSE = 4,
  ERROR_RESPONSE = 5
};

// Message header with request ID
struct MessageHeader {
  uint8_t type;
  uint8_t reserved[3];
  uint32_t request_id;
  uint32_t payload_size;
} __attribute__((packed));

struct GetRequest {
  uint64_t key;
} __attribute__((packed));

struct PutRequest {
  uint64_t key;
  // Value follows as variable-length data
} __attribute__((packed));

struct PutResponse {
  uint8_t success;
} __attribute__((packed));

struct ErrorResponse {
  uint32_t error_code;
  // Error message follows as variable-length data
} __attribute__((packed));

// Modified Statistics class with fine-grained latency tracking
class Statistics {
private:
  // Existing atomic counters remain unchanged
  std::atomic<uint64_t> total_requests{0};
  std::atomic<uint64_t> successful_requests{0};
  std::atomic<uint64_t> failed_requests{0};
  std::atomic<uint64_t> get_requests{0};
  std::atomic<uint64_t> put_requests{0};
  std::atomic<uint64_t> get_success{0};
  std::atomic<uint64_t> put_success{0};
  std::atomic<uint64_t> total_bytes_sent{0};
  std::atomic<uint64_t> total_bytes_received{0};
  
  // For throughput calculation
  std::mutex time_mutex;
  std::chrono::high_resolution_clock::time_point start_time;
  std::chrono::high_resolution_clock::time_point last_report_time;
  uint64_t last_interval_total{0};
  
  // Thread-local latency collection buffers
  struct LatencyBuffer {
    std::vector<double> latencies;
    std::vector<double> get_latencies;
    std::vector<double> put_latencies;
    
    void clear() {
      latencies.clear();
      get_latencies.clear();
      put_latencies.clear();
    }
  };
  
  // Thread-local latency buffers
  static thread_local LatencyBuffer tl_latency_buffer;
  
  // Global latency storage (protected by mutex)
  std::mutex global_latency_mutex;
  std::vector<double> global_latencies;
  std::vector<double> global_get_latencies;
  std::vector<double> global_put_latencies;
  
  // Sampling rate to reduce memory usage
  double latency_sampling_rate = 1.0;  // Sample all by default
  std::atomic<bool> collecting_final_stats{false};
  
public:
  Statistics() {
    reset();
    
    // Adjust sampling rate based on expected load
    // Lower sampling for high throughput benchmarks (still statistically valid)
    if (FLAGS_threads * FLAGS_connections_per_thread > 50) {
      latency_sampling_rate = 0.1;  // Sample 10% of requests
    }
  }
  
  void reset() {
    // Reset atomic counters
    total_requests = 0;
    successful_requests = 0;
    failed_requests = 0;
    get_requests = 0;
    put_requests = 0;
    get_success = 0;
    put_success = 0;
    total_bytes_sent = 0;
    total_bytes_received = 0;
    
    // Reset timing
    start_time = std::chrono::high_resolution_clock::now();
    last_report_time = start_time;
    last_interval_total = 0;
    
    // Reset global latency vectors
    std::lock_guard<std::mutex> lock(global_latency_mutex);
    global_latencies.clear();
    global_get_latencies.clear();
    global_put_latencies.clear();
    
    // Each thread will reset its thread-local buffer when needed
  }
  
  // Base record methods without latency tracking (unchanged)
  void record_get(bool success, uint64_t bytes_sent, uint64_t bytes_received) {
    total_requests.fetch_add(1, std::memory_order_relaxed);
    get_requests.fetch_add(1, std::memory_order_relaxed);
    
    if (success) {
      successful_requests.fetch_add(1, std::memory_order_relaxed);
      get_success.fetch_add(1, std::memory_order_relaxed);
    } else {
      failed_requests.fetch_add(1, std::memory_order_relaxed);
    }
    
    total_bytes_sent.fetch_add(bytes_sent, std::memory_order_relaxed);
    total_bytes_received.fetch_add(bytes_received, std::memory_order_relaxed);
  }

  void record_put(bool success, uint64_t bytes_sent, uint64_t bytes_received) {
    total_requests.fetch_add(1, std::memory_order_relaxed);
    put_requests.fetch_add(1, std::memory_order_relaxed);
    
    if (success) {
      successful_requests.fetch_add(1, std::memory_order_relaxed);
      put_success.fetch_add(1, std::memory_order_relaxed);
    } else {
      failed_requests.fetch_add(1, std::memory_order_relaxed);
    }
    
    total_bytes_sent.fetch_add(bytes_sent, std::memory_order_relaxed);
    total_bytes_received.fetch_add(bytes_received, std::memory_order_relaxed);
  }

  // Record methods with latency tracking (optimized to use thread-local storage)
  void record_get(bool success, uint64_t bytes_sent, uint64_t bytes_received, 
                 const std::chrono::high_resolution_clock::time_point& start_time) {
    // Record base stats
    record_get(success, bytes_sent, bytes_received);
    
    // Record latency if enabled and selected by sampling
    if (FLAGS_measure_latency && should_sample()) {
      auto now = std::chrono::high_resolution_clock::now();
      double latency_ms = std::chrono::duration<double, std::milli>(now - start_time).count();
      
      // Store in thread-local buffer
      tl_latency_buffer.latencies.push_back(latency_ms);
      tl_latency_buffer.get_latencies.push_back(latency_ms);
      
      // If the buffer gets too large, flush to global storage
      maybe_flush_latencies();
      
      // Ensure capturing all latencies when finalizing
      if (collecting_final_stats.load(std::memory_order_relaxed)) {
        flush_latencies();
      }
    }
  }
  
  void record_put(bool success, uint64_t bytes_sent, uint64_t bytes_received,
                 const std::chrono::high_resolution_clock::time_point& start_time) {
    // Record base stats
    record_put(success, bytes_sent, bytes_received);
    
    // Record latency if enabled and selected by sampling
    if (FLAGS_measure_latency && should_sample()) {
      auto now = std::chrono::high_resolution_clock::now();
      double latency_ms = std::chrono::duration<double, std::milli>(now - start_time).count();
      
      // Store in thread-local buffer
      tl_latency_buffer.latencies.push_back(latency_ms);
      tl_latency_buffer.put_latencies.push_back(latency_ms);
      
      // If the buffer gets too large, flush to global storage
      maybe_flush_latencies();
      
      // Ensure capturing all latencies when finalizing
      if (collecting_final_stats.load(std::memory_order_relaxed)) {
        flush_latencies();
      }
    }
  }
  
  // Standard print_report method (unchanged)
  void print_report(bool final = false) {
    // Notify threads to flush their latency buffers for final report
    if (final) {
      collecting_final_stats.store(true, std::memory_order_relaxed);
      
      // Sleep briefly to allow threads to flush their latency data
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    
    // Rest of print_report implementation unchanged
    std::lock_guard<std::mutex> lock(time_mutex);
    auto now = std::chrono::high_resolution_clock::now();
    double elapsed_total = std::chrono::duration<double>(now - start_time).count();
    double elapsed_since_last = std::chrono::duration<double>(now - last_report_time).count();

    uint64_t total = total_requests.load(std::memory_order_relaxed);
    uint64_t success = successful_requests.load(std::memory_order_relaxed);
    uint64_t failed = failed_requests.load(std::memory_order_relaxed);
    uint64_t gets = get_requests.load(std::memory_order_relaxed);
    uint64_t puts = put_requests.load(std::memory_order_relaxed);
    uint64_t get_ok = get_success.load(std::memory_order_relaxed);
    uint64_t put_ok = put_success.load(std::memory_order_relaxed);
    uint64_t bytes_sent = total_bytes_sent.load(std::memory_order_relaxed);
    uint64_t bytes_received = total_bytes_received.load(std::memory_order_relaxed);

    // Calculate throughput - protect against division by zero
    double total_throughput = (elapsed_total > 0) ? (total / elapsed_total) : 0;
    double interval_throughput = (elapsed_since_last > 0) ? 
                               ((total - last_interval_total) / elapsed_since_last) : 0;
    
    double get_success_rate = (gets > 0) ? (100.0 * get_ok / gets) : 0;
    double put_success_rate = (puts > 0) ? (100.0 * put_ok / puts) : 0;
    double overall_success_rate = (total > 0) ? (100.0 * success / total) : 0;

    if (final) {
      std::cout << "\n========== FINAL STATISTICS ==========\n";
    } else {
      std::cout << "----- Progress Report -----\n";
    }

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Runtime: " << elapsed_total << "s\n";
    std::cout << "Throughput: " << total_throughput << " ops/sec";
    if (!final) {
      std::cout << " (last " << elapsed_since_last << "s: " << interval_throughput << " ops/sec)";
    }
    std::cout << "\n";
    
    std::cout << "Total requests: " << total 
              << " (GET: " << gets << ", PUT: " << puts << ")\n";
    std::cout << "Success rate: " << overall_success_rate << "% "
              << "(GET: " << get_success_rate << "%, PUT: " << put_success_rate << "%)\n";
    
    double mb_sent = bytes_sent / (1024.0 * 1024.0);
    double mb_received = bytes_received / (1024.0 * 1024.0);
    std::cout << "Network: " << mb_sent << " MB sent, " 
              << mb_received << " MB received\n";

    if (!final) {
      last_interval_total = total;
      last_report_time = now;
      std::cout << "---------------------------\n";
    } else {
      std::cout << "======================================\n";
      if (final && FLAGS_measure_latency) {
        print_latency_stats();
      }
    }
  }
  
  uint64_t get_total_requests() const {
    return total_requests.load(std::memory_order_relaxed);
  }
  
private:
  // Determine if we should sample this request for latency tracking
  bool should_sample() {
    // If collecting final stats, sample everything
    if (collecting_final_stats.load(std::memory_order_relaxed)) {
      return true;
    }
    
    // Otherwise use sampling rate
    if (latency_sampling_rate >= 1.0) {
      return true;  // Sample everything
    } else {
      // Fast thread-local random sampling
      static thread_local std::random_device rd;
      static thread_local std::mt19937 gen(rd());
      static thread_local std::uniform_real_distribution<> dis(0.0, 1.0);
      
      return dis(gen) < latency_sampling_rate;
    }
  }
  
  // Check if we should flush thread-local buffers to global storage
  void maybe_flush_latencies() {
    // Flush if any buffer gets too large
    const size_t FLUSH_THRESHOLD = 1000;
    
    if (tl_latency_buffer.latencies.size() > FLUSH_THRESHOLD ||
        tl_latency_buffer.get_latencies.size() > FLUSH_THRESHOLD ||
        tl_latency_buffer.put_latencies.size() > FLUSH_THRESHOLD) {
      flush_latencies();
    }
  }
  
  // Flush thread-local latency buffers to global storage
  void flush_latencies() {
    // Only lock if we actually have data to flush
    if (tl_latency_buffer.latencies.empty() &&
        tl_latency_buffer.get_latencies.empty() &&
        tl_latency_buffer.put_latencies.empty()) {
      return;
    }
    
    // Acquire lock and move data to global buffers
    {
      std::lock_guard<std::mutex> lock(global_latency_mutex);
      
      // Move latencies to global buffers (more efficient than copying)
      global_latencies.insert(global_latencies.end(), 
                             tl_latency_buffer.latencies.begin(),
                             tl_latency_buffer.latencies.end());
                             
      global_get_latencies.insert(global_get_latencies.end(), 
                                 tl_latency_buffer.get_latencies.begin(),
                                 tl_latency_buffer.get_latencies.end());
                                 
      global_put_latencies.insert(global_put_latencies.end(), 
                                 tl_latency_buffer.put_latencies.begin(),
                                 tl_latency_buffer.put_latencies.end());
    }
    
    // Clear thread-local buffers
    tl_latency_buffer.clear();
  }
  
  // Calculate percentile from sorted data
  double calculate_percentile(const std::vector<double>& sorted_data, double percentile) {
    if (sorted_data.empty()) return 0.0;
    
    double index = percentile * (sorted_data.size() - 1);
    size_t lower_index = static_cast<size_t>(index);
    size_t upper_index = std::min(lower_index + 1, sorted_data.size() - 1);
    double weight = index - lower_index;
    
    return sorted_data[lower_index] * (1 - weight) + sorted_data[upper_index] * weight;
  }
  
  // Helper method to print percentiles
  void print_percentiles(const std::vector<double>& sorted_data) {
    if (sorted_data.empty()) {
      std::cout << "  No latency data available." << std::endl;
      return;
    }
    
    double min = sorted_data.front();
    double max = sorted_data.back();
    double sum = std::accumulate(sorted_data.begin(), sorted_data.end(), 0.0);
    double avg = sum / sorted_data.size();
    
    double p50 = calculate_percentile(sorted_data, 0.5);
    double p90 = calculate_percentile(sorted_data, 0.9);
    double p95 = calculate_percentile(sorted_data, 0.95);
    double p99 = calculate_percentile(sorted_data, 0.99);
    double p999 = calculate_percentile(sorted_data, 0.999);
    
    std::cout << "  Samples: " << sorted_data.size();
    if (latency_sampling_rate < 1.0) {
      std::cout << " (sampled at " << (latency_sampling_rate * 100) << "%)";
    }
    std::cout << std::endl;
    
    std::cout << "  Average: " << avg << std::endl;
    std::cout << "  Min: " << min << std::endl;
    std::cout << "  Max: " << max << std::endl;
    std::cout << "  p50: " << p50 << std::endl;
    std::cout << "  p90: " << p90 << std::endl;
    std::cout << "  p95: " << p95 << std::endl;
    std::cout << "  p99: " << p99 << std::endl;
    std::cout << "  p99.9: " << p999 << std::endl;
  }
  
  // Print latency statistics
  void print_latency_stats() {
    // Ensure all thread-local buffers are flushed
    collecting_final_stats.store(true, std::memory_order_relaxed);
    
    // Make a copy of the global latency vectors to avoid holding the lock during sorting
    std::vector<double> all_latencies;
    std::vector<double> all_get_latencies;
    std::vector<double> all_put_latencies;
    
    {
      std::lock_guard<std::mutex> lock(global_latency_mutex);
      all_latencies = global_latencies;
      all_get_latencies = global_get_latencies;
      all_put_latencies = global_put_latencies;
    }
    
    // Sort the copies for percentile calculation
    std::sort(all_latencies.begin(), all_latencies.end());
    std::sort(all_get_latencies.begin(), all_get_latencies.end());
    std::sort(all_put_latencies.begin(), all_put_latencies.end());
    
    std::cout << "\n========== LATENCY STATISTICS ==========" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    
    // Print GET latency stats
    std::cout << "GET Latency (ms):" << std::endl;
    print_percentiles(all_get_latencies);
    
    std::cout << std::endl;
    
    // Print PUT latency stats
    std::cout << "PUT Latency (ms):" << std::endl;
    print_percentiles(all_put_latencies);
    
    // Print combined latency stats
    std::cout << "\nAll Operations Latency (ms):" << std::endl;
    print_percentiles(all_latencies);
    
    std::cout << "=======================================" << std::endl;
  }
};

// Initialize thread_local buffer
thread_local Statistics::LatencyBuffer Statistics::tl_latency_buffer;

// Structure to store a pending request
struct PendingRequest {
  uint32_t request_id;
  uint8_t request_type;
  uint64_t key;
  std::chrono::high_resolution_clock::time_point start_time;
  size_t bytes_sent;
  size_t bytes_received;
  
  PendingRequest(uint32_t id, uint8_t type, uint64_t k)
    : request_id(id), request_type(type), key(k),
      start_time(std::chrono::high_resolution_clock::now()),
      bytes_sent(0), bytes_received(0) {}
};

// A simple operation to be performed
struct Operation {
  bool is_get;
  uint64_t key;
  std::string value;  // For PUT operations
};

// Connection class for pipelined operations
class Connection {
private:
  int fd;
  std::string server;
  uint16_t port;
  bool connected;
  
  // Buffer for sending and receiving
  std::vector<char> send_buffer;
  size_t send_offset;
  
  std::vector<char> recv_buffer;
  size_t recv_offset;
  size_t recv_needed;
  
  // Current operation being received
  MessageHeader current_header;
  bool header_received;
  
  // Request tracking
  std::unordered_map<uint32_t, PendingRequest>& pending_requests;
  std::atomic<size_t>& active_requests;
  
  // TUX mode flag
  bool use_tux;

public:
  Connection(const std::string& server_addr, uint16_t server_port,
            std::unordered_map<uint32_t, PendingRequest>& requests,
            std::atomic<size_t>& active_count)
    : fd(-1), server(server_addr), port(server_port), connected(false),
      send_buffer(4096), send_offset(0),
      recv_buffer(4096), recv_offset(0), recv_needed(0),
      header_received(false),
      pending_requests(requests), active_requests(active_count),
      use_tux(FLAGS_tux && g_libtux_send_tux_msg && g_libtux_recv_tux_msg) {}
  
  ~Connection() {
    close_connection();
  }

  int get_fd() const { return fd; }
  
  bool is_connected() const { return connected; }
  
  bool has_capacity() const { 
    return active_requests.load() < FLAGS_max_inflight_per_connection; 
  }
  
  // Check if we have any data to send
  bool has_pending_send() const { 
    return send_offset > 0; 
  }
  
  // Establish connection with blocking IO
  bool connect_to_server() {
    close_connection();
    
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      if (FLAGS_debug) perror("socket");
      return false;
    }
    
    // Set TCP_NODELAY
    int flag = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int)) < 0) {
      if (FLAGS_debug) perror("setsockopt TCP_NODELAY");
    }
    
    // Prepare server address
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, server.c_str(), &server_addr.sin_addr) <= 0) {
      if (FLAGS_debug) perror("inet_pton");
      close(fd);
      fd = -1;
      return false;
    }
    
    // Blocking connect
    if (::connect(fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
      if (FLAGS_debug) perror("connect");
      close(fd);
      fd = -1;
      return false;
    }

    printf("Connected to server %s:%u\n", server.c_str(), port);
    
    // Set non-blocking mode for subsequent operations
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      if (FLAGS_debug) perror("fcntl");
      close(fd);
      fd = -1;
      return false;
    }
    
    connected = true;
    return true;
  }
  
  void close_connection() {
    if (fd >= 0) {
      close(fd);
      fd = -1;
    }
    connected = false;
    send_offset = 0;
    recv_offset = 0;
    recv_needed = 0;
    header_received = false;
  }
  
  // Prepare a request to be sent
  bool prepare_request(uint8_t type, uint32_t request_id, const void* payload, size_t payload_size) {
    // Create header
    MessageHeader header;
    header.type = type;
    header.request_id = request_id;
    header.payload_size = payload_size;
    
    // Calculate total size
    size_t total_size = sizeof(header) + payload_size;
    
    // Ensure buffer is large enough
    if (send_buffer.size() - send_offset < total_size) {
      send_buffer.resize(send_offset + total_size);
    }
    
    // Copy data into buffer
    memcpy(&send_buffer[send_offset], &header, sizeof(header));
    if (payload && payload_size > 0) {
      memcpy(&send_buffer[send_offset + sizeof(header)], payload, payload_size);
    }
    
    // Update active requests count
    active_requests.fetch_add(1);
    
    // Increment offset for next time
    send_offset += total_size;

    //printf("Prepared request %u of type %u with payload size %zu\n", request_id, type, payload_size);
    
    return true;
  }
  
  // Try to send data
  bool handle_write() {
    if (!connected) return true;
    // print buffer state
    //printf("send_offset = %zu, sned_buffer.size() = %zu\n", send_offset,  send_buffer.size());

    // If we have no data to send, return success
    if (send_buffer.empty() || send_offset == 0) {
      return true;
    }
    
    ssize_t sent = 0;
        
    if (use_tux && g_libtux_send_tux_msg) {
        // Use TUX message interface
        struct iovec iov;
        iov.iov_base = send_buffer.data();
        iov.iov_len = send_offset;
        
        struct msghdr msg;
        memset(&msg, 0, sizeof(msg));
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;
        
        sent = g_libtux_send_tux_msg(fd, &msg);
    } else {
        // Use standard POSIX send
        sent = send(fd, send_buffer.data(), send_offset, 0);
    }
    
    if (sent < 0) {
      if (errno == EAGAIN || EWOULDBLOCK) {
        return true;  // Try again later
      }
      
      // Error occurred
      if (FLAGS_debug) perror(use_tux ? "libtux_send_tux_msg" : "send");
      return false;
    }
    
    // If we sent some but not all data
    if (sent > 0 && static_cast<size_t>(sent) < send_offset) {
      // Move remaining data to beginning of buffer
      memmove(send_buffer.data(), send_buffer.data() + sent, send_offset - sent);
      send_offset -= sent;
    } 
    // If we sent all data
    else if (static_cast<size_t>(sent) == send_offset) {
      send_offset = 0;
    }
    
    //printf("Sent %zd bytes\n", sent);
    return true;
  }
    
  // Try to receive data
  bool handle_read(Statistics& stats) {
    if (!connected) return false;
    
    // Continue reading until we'd block or finish processing a message
    while (true) {
      if (!header_received) {
        // We need to receive a header
        if (recv_offset < sizeof(MessageHeader)) {
          // Make sure buffer is large enough
          if (recv_buffer.size() < sizeof(MessageHeader)) {
            recv_buffer.resize(sizeof(MessageHeader));
          }
          
          // Try to receive more header bytes
          ssize_t bytes_to_read = sizeof(MessageHeader) - recv_offset;
          ssize_t received = 0;
                    
          if (use_tux && g_libtux_recv_tux_msg) {
              // Use TUX message interface for receiving
              struct iovec iov;
              iov.iov_base = recv_buffer.data() + recv_offset;
              iov.iov_len = bytes_to_read;
              
              struct msghdr msg;
              memset(&msg, 0, sizeof(msg));
              msg.msg_iov = &iov;
              msg.msg_iovlen = 1;
              
              received = g_libtux_recv_tux_msg(fd, &msg);
          } else {
              // Use standard POSIX recv
              received = recv(fd, recv_buffer.data() + recv_offset, bytes_to_read, 0);
          }
          
          if (received <= 0) {
            if (received < 0 && (errno == EAGAIN || EWOULDBLOCK)) {
              return true;  // No more data available right now
            }
            
            // Connection closed or error
            if (received < 0) {
              if (FLAGS_debug) perror(use_tux ? "libtux_recv_tux_msg" : "recv header");
            } else {
              if (FLAGS_debug) std::cerr << "Connection closed by server" << std::endl;
            }
            return false;
          }
          
          // Update received count
          recv_offset += received;
          
          // If we still don't have a complete header, return and try again later
          if (recv_offset < sizeof(MessageHeader)) {
            return true;
          }
        }
        
        // We have a complete header now
        memcpy(&current_header, recv_buffer.data(), sizeof(MessageHeader));
        
        if (FLAGS_debug) {
          printf("Received header - type: %u, request_id: %u, payload_size: %u\n", 
                current_header.type, current_header.request_id, current_header.payload_size);
        }
        
        // Set state for payload reception
        header_received = true;
        recv_offset = 0;  // Reset for payload
        
        // Handle the payload part
        if (current_header.payload_size > 0) {
          // Resize buffer if needed for the payload
          if (recv_buffer.size() < current_header.payload_size) {
            recv_buffer.resize(current_header.payload_size);
          }
          recv_needed = current_header.payload_size;
        } else {
          // No payload, process the response now
          process_response(stats);
          header_received = false;  // Reset for next message
          return true;
        }
      } else {
        // We're receiving a payload
        if (recv_offset < recv_needed) {
          // Try to receive more payload bytes
          ssize_t bytes_to_read = recv_needed - recv_offset;
          ssize_t received = 0;
                    
          if (use_tux && g_libtux_recv_tux_msg) {
              // Use TUX message interface for receiving
              struct iovec iov;
              iov.iov_base = recv_buffer.data() + recv_offset;
              iov.iov_len = bytes_to_read;
              
              struct msghdr msg;
              memset(&msg, 0, sizeof(msg));
              msg.msg_iov = &iov;
              msg.msg_iovlen = 1;
              
              received = g_libtux_recv_tux_msg(fd, &msg);
          } else {
              // Use standard POSIX recv
              received = recv(fd, recv_buffer.data() + recv_offset, bytes_to_read, 0);
          }
          
          if (received <= 0) {
            if (received < 0 && (errno == EAGAIN || EWOULDBLOCK)) {
              return true;  // No more data available right now
            }
            
            // Connection closed or error
            if (received < 0) {
              if (FLAGS_debug) perror(use_tux ? "libtux_recv_tux_msg" : "recv payload");
            } else {
              if (FLAGS_debug) std::cerr << "Connection closed by server" << std::endl;
            }
            return false;
          }
          
          // Update received count
          recv_offset += received;
          
          // If we still don't have the complete payload, return and try again later
          if (recv_offset < recv_needed) {
            return true;
          }
        }
        
        // We have the complete payload now
        if (FLAGS_debug) {
          printf("Received complete payload of %zu bytes for request %u\n", 
                recv_offset, current_header.request_id);
        }
        
        // Process the complete response
        process_response(stats);
        
        // Reset state for next message
        header_received = false;
        recv_offset = 0;
      }
    }
  }

  
private:
  void process_response(Statistics& stats) {
  // Find the pending request
  auto it = pending_requests.find(current_header.request_id);
  if (it == pending_requests.end()) {
    if (FLAGS_debug) {
      std::cerr << "Received response for unknown request ID: " 
               << current_header.request_id << std::endl;
    }
    return;
  }
  
  // Track bytes received for this request
  it->second.bytes_received = sizeof(MessageHeader) + recv_needed;
  
  bool success = false;
  
  // Process based on message type
  if (current_header.type == GET_RESPONSE) {
    // GET response - just need to verify the type matches
    if (it->second.request_type == GET_REQUEST) {
      success = true;
    }
  } else if (current_header.type == PUT_RESPONSE) {
    // PUT response - check the success flag
    if (it->second.request_type == PUT_REQUEST && recv_needed >= sizeof(PutResponse)) {
      PutResponse* response = reinterpret_cast<PutResponse*>(recv_buffer.data());
      success = (response->success != 0);
    }
  } else if (current_header.type == ERROR_RESPONSE) {
    // Error response handling (unchanged)...
    success = false;
  }
  
  if (FLAGS_debug) {
    printf("Processing response for request %u (type %u): %s\n", 
          current_header.request_id, it->second.request_type, 
          success ? "SUCCESS" : "FAILURE");
  }
  
  // Record stats with latency
  if (it->second.request_type == GET_REQUEST) {
    stats.record_get(success, it->second.bytes_sent, it->second.bytes_received, 
                    it->second.start_time);
  } else {
    stats.record_put(success, it->second.bytes_sent, it->second.bytes_received, 
                    it->second.start_time);
  }
  
  // Remove from pending requests and decrement counter
  pending_requests.erase(it);
  active_requests.fetch_sub(1);
}
};

// Worker thread implementation using epoll and connection pipelining
class WorkerThread {
private:
  uint32_t thread_id;
  std::atomic<bool>& running;
  std::atomic<uint64_t>& request_counter;
  Statistics& stats;
  
  // For request generation
  std::unique_ptr<leanstore::utils::ScrambledZipfGenerator> zipf_generator;
  std::string value_data;
  
  // For managing connections
  int epoll_fd;
  std::vector<std::unique_ptr<Connection>> connections;
  std::unordered_map<uint32_t, PendingRequest> pending_requests;
  std::atomic<uint32_t> next_request_id;
  
  // For throttling
  uint64_t throttle_rate;
  std::chrono::steady_clock::time_point last_throttle_check;
  uint64_t throttle_counter;
  
public:
  WorkerThread(uint32_t id, std::atomic<bool>& run_flag, 
              std::atomic<uint64_t>& req_counter, Statistics& stats_ref)
    : thread_id(id), running(run_flag), request_counter(req_counter), stats(stats_ref),
      next_request_id(id * 10000000), throttle_rate(FLAGS_throttle_rate),
      last_throttle_check(std::chrono::steady_clock::now()), throttle_counter(0) {
    
    // Initialize epoll
    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
      throw std::runtime_error("Failed to create epoll instance");
    }
    
    // Create zipf generator if needed
    if (FLAGS_zipf) {
      zipf_generator = std::make_unique<leanstore::utils::ScrambledZipfGenerator>(
        0, FLAGS_key_range, FLAGS_zipf_factor);
    }
    
    // Pre-allocate value data
    value_data.resize(FLAGS_value_size, 'A');
    
    // Create and connect connections
    for (uint32_t i = 0; i < FLAGS_connections_per_thread; i++) {
      // For each connection, create an atomic counter for active requests
      std::atomic<size_t>* counter = new std::atomic<size_t>(0);
      
      auto conn = std::make_unique<Connection>(FLAGS_server, FLAGS_port, 
                                              pending_requests, *counter);
      
      if (conn->connect_to_server()) {
        // Add to epoll for monitoring
        add_to_epoll(conn.get());
        connections.push_back(std::move(conn));
      }
    }
    
    if (connections.empty()) {
      throw std::runtime_error("Failed to establish any connections");
    }
  }
  
  ~WorkerThread() {
    if (epoll_fd >= 0) {
      close(epoll_fd);
    }
  }
  
  // Add a connection to epoll
  void add_to_epoll(Connection* conn) {
    struct epoll_event ev;
    ev.events = EPOLLIN;  // Always monitor for readable
    ev.events |= EPOLLOUT;  // Monitor for writability if we have data to send
    ev.data.ptr = conn;
    
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn->get_fd(), &ev) < 0) {
      throw std::runtime_error("Failed to add connection to epoll");
    }
  }
  
  // Update epoll monitoring for a connection
  void update_epoll(Connection* conn) {
    struct epoll_event ev;
    ev.events = EPOLLIN;  // Always monitor for readable
    ev.events |= EPOLLOUT;  // Monitor for writability if we have data to send
    
    ev.data.ptr = conn;
    
    if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, conn->get_fd(), &ev) < 0) {
      throw std::runtime_error("Failed to update connection in epoll");
    }
  }
  
  // Generate a new request ID
  uint32_t generate_request_id() {
    return next_request_id.fetch_add(1, std::memory_order_relaxed);
  }
  
  // Check throttling
  bool check_throttling() {
    if (throttle_rate == 0) return true;  // No throttling
    
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_throttle_check).count();
    
    if (elapsed >= 1000) {  // 1 second elapsed
      last_throttle_check = now;
      throttle_counter = 0;
      return true;
    }
    
    return (throttle_counter++ < throttle_rate);
  }
  
  // Main worker loop
  void run() {
    const int MAX_EVENTS = 64;
    struct epoll_event events[MAX_EVENTS];
    
    while (running.load(std::memory_order_relaxed) && 
          (FLAGS_requests == 0 || request_counter.load(std::memory_order_relaxed) < FLAGS_requests)) {
      
      // Try to send new requests on connections with capacity
      if (check_throttling()) {
        for (auto& conn : connections) {
          while (conn->has_capacity()) {
            send_new_request(*conn);
          }
        }
      }
      
      // Wait for events with a short timeout
      int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, 1);
      
      if (num_events < 0) {
        if (errno == EINTR) continue;
        perror("epoll_wait");
        break;
      }
      
      // Process events
      for (int i = 0; i < num_events; i++) {
        Connection* conn = static_cast<Connection*>(events[i].data.ptr);
        
        bool conn_ok = true;
        
        // Handle errors first
        if (events[i].events & (EPOLLERR | EPOLLHUP)) {
          conn_ok = false;
        }
        
        // Handle write events (sending requests)
        if (conn_ok && (events[i].events & EPOLLOUT)) {
          conn_ok = conn->handle_write();
        }
        
        // Handle read events (receiving responses)
        if (conn_ok && (events[i].events & EPOLLIN)) {
          conn_ok = conn->handle_read(stats);
        }
        
        // If connection failed, reconnect
        if (!conn_ok) {
          // Remove from epoll
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->get_fd(), nullptr);
          
          // Reconnect
          if (conn->connect_to_server()) {
            add_to_epoll(conn);
          }
        } else {
          // Update epoll registration if needed
          update_epoll(conn);
        }
      }
    }
  }
  
  // Prepare and send a new request
  void send_new_request(Connection& conn) {
    if (FLAGS_requests > 0 && 
        request_counter.load(std::memory_order_relaxed) >= FLAGS_requests) {
      return;
    }
    
    // Generate key
    uint64_t key;
    if (FLAGS_zipf) {
      key = zipf_generator->rand();
    } else {
      key = leanstore::utils::RandomGenerator::getRandU64(0, FLAGS_key_range - 1);
    }
    
    // Determine operation type
    bool is_get = leanstore::utils::RandomGenerator::getRandU64(0, 99) < FLAGS_read_ratio;
    
    // Generate request ID
    uint32_t request_id = generate_request_id();
    
    // Create pending request
    PendingRequest pending(request_id, is_get ? GET_REQUEST : PUT_REQUEST, key);
    
    if (is_get) {
      // Prepare GET request
      GetRequest request;
      request.key = key;
      
      // Calculate bytes sent for this request
      pending.bytes_sent = sizeof(MessageHeader) + sizeof(request);
      
      // Add to pending requests
      pending_requests.emplace(request_id, pending);
      
      // Prepare connection buffer
      conn.prepare_request(GET_REQUEST, request_id, &request, sizeof(request));
    } else {
      // For PUT, fill value with some data that includes the key
      std::string key_str = std::to_string(key);
      std::copy(key_str.begin(), key_str.end(), value_data.begin());
      
      // Prepare PUT request
      PutRequest request;
      request.key = key;
      
      // Create a temporary buffer for the PUT request
      std::vector<char> put_buffer(sizeof(request) + value_data.size());
      memcpy(put_buffer.data(), &request, sizeof(request));
      memcpy(put_buffer.data() + sizeof(request), value_data.data(), value_data.size());
      
      // Calculate bytes sent for this request
      pending.bytes_sent = sizeof(MessageHeader) + sizeof(request) + value_data.size();
      
      // Add to pending requests
      pending_requests.emplace(request_id, pending);
      
      // Prepare connection buffer
      conn.prepare_request(PUT_REQUEST, request_id, put_buffer.data(), 
                          sizeof(request) + value_data.size());
    }
    
    // Increment request counter
    if (FLAGS_requests > 0) {
      request_counter.fetch_add(1, std::memory_order_relaxed);
    }
  }
};

// Main worker thread function
void worker_thread(uint32_t thread_id, Statistics& stats,
                  std::atomic<bool>& running, std::atomic<uint64_t>& request_counter) {
  try {
    WorkerThread worker(thread_id, running, request_counter, stats);
    worker.run();
  } catch (const std::exception& e) {
    std::cerr << "Thread " << thread_id << " error: " << e.what() << std::endl;
  }
}

// Stats reporting thread
void report_thread(Statistics& stats, std::atomic<bool>& running) {
  while (running.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_report_interval));
    stats.print_report();
  }
}

// Add new command line flags for loading phase
DEFINE_bool(load, false, "Run loading phase before benchmark");
DEFINE_bool(load_verbose, false, "Print progress during loading phase");
DEFINE_uint32(load_batch_size, 1000, "Number of keys to load in each batch");

// Function to load the database sequentially using existing parameters
bool run_loading_phase() {
  auto start_time = std::chrono::high_resolution_clock::now();
  
  std::cout << "=== Starting Loading Phase ===" << std::endl;
  std::cout << "Keys to load: " << FLAGS_key_range << std::endl;
  std::cout << "Value size: " << FLAGS_value_size << " bytes" << std::endl;
  if (FLAGS_tux && g_libtux_send_tux_msg && g_libtux_recv_tux_msg) {
    std::cout << "Using TUX message interface" << std::endl;
  }
  
  // Create a socket connection to the server
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    perror("socket");
    return false;
  }
  
  // Set TCP_NODELAY
  int flag = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int)) < 0) {
    perror("setsockopt TCP_NODELAY");
  }
  
  // Prepare server address
  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(FLAGS_port);
  
  if (inet_pton(AF_INET, FLAGS_server.c_str(), &server_addr.sin_addr) <= 0) {
    perror("inet_pton");
    close(fd);
    return false;
  }
  
  // Connect to server
  if (connect(fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    perror("connect");
    close(fd);
    return false;
  }
  
  std::cout << "Connected to server " << FLAGS_server << ":" << FLAGS_port << std::endl;
  
  // Prepare buffers for request/response handling
  std::vector<char> send_buffer(sizeof(MessageHeader) + sizeof(PutRequest) + FLAGS_value_size);
  std::vector<char> recv_buffer(sizeof(MessageHeader) + sizeof(PutResponse));
  std::vector<char> value_data(FLAGS_value_size);
  
  // Statistics
  uint64_t successful_loads = 0;
  uint64_t failed_loads = 0;
  
  // Calculate batches for progress reporting
  uint64_t num_batches = (FLAGS_key_range + FLAGS_load_batch_size - 1) / FLAGS_load_batch_size;
  uint64_t batch_counter = 0;
  
  // Load all keys sequentially
  for (uint64_t key = 0; key < FLAGS_key_range; key++) {
    // Update value data to include the key for traceability
    snprintf(value_data.data(), FLAGS_value_size, "KEY-%016lx", key);
    // Fill rest with recognizable pattern
    for (size_t i = strlen(value_data.data()); i < FLAGS_value_size; i++) {
      value_data[i] = 'A' + (i % 26);
    }
    
    // Create PUT request
    MessageHeader header;
    header.type = PUT_REQUEST;
    header.request_id = key;  // Use key as request ID
    header.payload_size = sizeof(PutRequest) + FLAGS_value_size;
    
    // Copy header to send buffer
    memcpy(send_buffer.data(), &header, sizeof(header));
    
    // Copy key to request structure
    PutRequest* request = reinterpret_cast<PutRequest*>(send_buffer.data() + sizeof(header));
    request->key = key;
    
    // Copy value data
    memcpy(send_buffer.data() + sizeof(header) + sizeof(PutRequest), 
           value_data.data(), FLAGS_value_size);
    
    // Send request using either TUX or standard send
    size_t total_size = sizeof(header) + header.payload_size;
    
    if (FLAGS_tux && g_libtux_send_tux_msg) {
      // Use TUX message interface
      struct iovec iov;
      iov.iov_base = send_buffer.data();
      iov.iov_len = total_size;
      
      struct msghdr msg;
      memset(&msg, 0, sizeof(msg));
      msg.msg_iov = &iov;
      msg.msg_iovlen = 1;
      
      if (g_libtux_send_tux_msg(fd, &msg) != static_cast<ssize_t>(total_size)) {
        perror("libtux_send_tux_msg");
        close(fd);
        return false;
      }
    } else {
      // Use standard POSIX send
      if (send(fd, send_buffer.data(), total_size, MSG_NOSIGNAL) != static_cast<ssize_t>(total_size)) {
        perror("send");
        close(fd);
        return false;
      }
    }
    
    // Receive response header
    if (FLAGS_tux && g_libtux_recv_tux_msg) {
      // Use TUX message interface
      struct iovec iov;
      iov.iov_base = recv_buffer.data();
      iov.iov_len = sizeof(MessageHeader);
      
      struct msghdr msg;
      memset(&msg, 0, sizeof(msg));
      msg.msg_iov = &iov;
      msg.msg_iovlen = 1;
      
      if (g_libtux_recv_tux_msg(fd, &msg) != sizeof(MessageHeader)) {
        perror("libtux_recv_tux_msg header");
        close(fd);
        return false;
      }
    } else {
      // Use standard POSIX recv
      if (recv(fd, recv_buffer.data(), sizeof(MessageHeader), MSG_WAITALL) != sizeof(MessageHeader)) {
        perror("recv header");
        close(fd);
        return false;
      }
    }
    
    // Parse response header
    MessageHeader* response_header = reinterpret_cast<MessageHeader*>(recv_buffer.data());
    
    // Verify request ID matches
    if (response_header->request_id != key) {
      std::cerr << "Error: Response ID " << response_header->request_id 
                << " does not match request ID " << key << std::endl;
      close(fd);
      return false;
    }
    
    // Receive response payload if any
    if (response_header->payload_size > 0) {
      if (FLAGS_tux && g_libtux_recv_tux_msg) {
        // Use TUX message interface
        struct iovec iov;
        iov.iov_base = recv_buffer.data() + sizeof(MessageHeader);
        iov.iov_len = response_header->payload_size;
        
        struct msghdr msg;
        memset(&msg, 0, sizeof(msg));
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;
        
        if (g_libtux_recv_tux_msg(fd, &msg) != static_cast<ssize_t>(response_header->payload_size)) {
          perror("libtux_recv_tux_msg payload");
          close(fd);
          return false;
        }
      } else {
        // Use standard POSIX recv
        if (recv(fd, recv_buffer.data() + sizeof(MessageHeader), 
                 response_header->payload_size, MSG_WAITALL) != 
                static_cast<ssize_t>(response_header->payload_size)) {
          perror("recv payload");
          close(fd);
          return false;
        }
      }
      
      // Check response type and status
      if (response_header->type == PUT_RESPONSE) {
        PutResponse* response = reinterpret_cast<PutResponse*>(
            recv_buffer.data() + sizeof(MessageHeader));
        if (response->success) {
          successful_loads++;
        } else {
          failed_loads++;
          if (FLAGS_load_verbose) {
            std::cerr << "Failed to put key " << key << std::endl;
          }
        }
      } else if (response_header->type == ERROR_RESPONSE) {
        failed_loads++;
        if (FLAGS_load_verbose) {
          ErrorResponse* error = reinterpret_cast<ErrorResponse*>(
              recv_buffer.data() + sizeof(MessageHeader));
          std::string error_msg;
          if (response_header->payload_size > sizeof(ErrorResponse)) {
            error_msg = std::string(
                recv_buffer.data() + sizeof(MessageHeader) + sizeof(ErrorResponse),
                response_header->payload_size - sizeof(ErrorResponse));
          }
          std::cerr << "Error loading key " << key << ": code=" << error->error_code
                    << ", message=" << error_msg << std::endl;
        }
      }
    }
    
    // Report progress for batches
    if (++batch_counter % FLAGS_load_batch_size == 0 || key == FLAGS_key_range - 1) {
      uint64_t batch_num = batch_counter / FLAGS_load_batch_size;
      double percent_complete = 100.0 * key / (FLAGS_key_range - 1);
      
      if (FLAGS_load_verbose || batch_num % 10 == 0 || key == FLAGS_key_range - 1) {
        std::cout << "Loading progress: " << key + 1 << "/" << FLAGS_key_range
                  << " keys (" << std::fixed << std::setprecision(1) 
                  << percent_complete << "% complete)" << std::endl;
      }
    }
  }
  
  // Close connection
  close(fd);
  
  // Calculate statistics
  auto end_time = std::chrono::high_resolution_clock::now();
  double elapsed = std::chrono::duration<double>(end_time - start_time).count();
  double throughput = FLAGS_key_range / elapsed;
  
  std::cout << "=== Loading Phase Complete ===" << std::endl;
  std::cout << "Total time: " << elapsed << " seconds" << std::endl;
  std::cout << "Successful loads: " << successful_loads << std::endl;
  std::cout << "Failed loads: " << failed_loads << std::endl;
  std::cout << "Throughput: " << throughput << " ops/sec" << std::endl;
  std::cout << "==============================" << std::endl << std::endl;
  
  return successful_loads > 0;
}

// Main function (unchanged from your code)
int main(int argc, char** argv) {
  gflags::SetUsageMessage("LeanStore RPC Client Benchmark");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  
  // Validate arguments
  if (FLAGS_read_ratio > 100) {
    std::cerr << "Error: read_ratio must be between 0 and 100" << std::endl;
    return 1;
  }
  
  // Print configuration
  std::cout << "=== RPC Benchmark Configuration ===" << std::endl;
  std::cout << "Server: " << FLAGS_server << ":" << FLAGS_port << std::endl;
  std::cout << "Threads: " << FLAGS_threads << std::endl;
  std::cout << "Connections per thread: " << FLAGS_connections_per_thread << std::endl;
  std::cout << "Max in-flight requests per connection: " << FLAGS_max_inflight_per_connection << std::endl;
  std::cout << "Key range: " << FLAGS_key_range << std::endl;
  std::cout << "Value size: " << FLAGS_value_size << " bytes" << std::endl;
  std::cout << "Read/write ratio: " << FLAGS_read_ratio << "/" << (100 - FLAGS_read_ratio) << std::endl;
  std::cout << "Key distribution: " << (FLAGS_zipf ? "Zipfian" : "Uniform") << std::endl;
  if (FLAGS_zipf) {
    std::cout << "Zipf factor: " << FLAGS_zipf_factor << std::endl;
  }
  if (FLAGS_throttle_rate > 0) {
    std::cout << "Throttle rate: " << FLAGS_throttle_rate << " ops/sec per thread" << std::endl;
  }
  std::cout << "Runtime: " << (FLAGS_requests > 0 ? "Until " + std::to_string(FLAGS_requests) + " requests" : 
                             std::to_string(FLAGS_runtime) + " seconds") << std::endl;
  std::cout << "===================================" << std::endl;
  
  // Initialize TUX functions if needed
  bool tux_available = false;
  if (FLAGS_tux) {
      std::cout << "TUX mode: ";
      tux_available = initialize_tux_functions();
      if (tux_available) {
          std::cout << "enabled (successfully loaded TUX functions)" << std::endl;
      } else {
          std::cout << "DISABLED (failed to load TUX functions)" << std::endl;
      }
  }

  // In your main function, after printing configuration but before starting the benchmark:
  if (FLAGS_load) {
    std::cout << "Loading phase enabled: will load " << FLAGS_key_range << " keys" << std::endl;
    std::cout << "===================================" << std::endl;
    
    if (!run_loading_phase()) {
      std::cerr << "Loading phase failed or had errors." << std::endl;
      if (!FLAGS_load_verbose) {
        std::cerr << "Try running with --load_verbose for more details." << std::endl;
      }
      // Continue with benchmark despite loading issues
    }
  }

  // Initialize statistics
  Statistics stats;
  std::atomic<bool> running(true);
  std::atomic<uint64_t> request_counter(0);
  
  // Start worker threads
  std::vector<std::thread> worker_threads;
  for (uint32_t i = 0; i < FLAGS_threads; i++) {
    worker_threads.emplace_back(worker_thread, i, std::ref(stats), std::ref(running), std::ref(request_counter));
  }
  
  // Start reporting thread
  std::thread stats_thread(report_thread, std::ref(stats), std::ref(running));
  
  // Wait for runtime or max requests
  if (FLAGS_requests > 0) {
    while (request_counter.load() < FLAGS_requests && running.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  } else {
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_runtime));
  }
  
  // Stop all threads
  running.store(false, std::memory_order_relaxed);
  
  // Wait for worker threads to complete
  for (auto& thread : worker_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  
  // Wait for stats thread
  if (stats_thread.joinable()) {
    stats_thread.join();
  }
  
  // Print final statistics
  stats.print_report(true);
  
  return 0;
}