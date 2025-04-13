#include "../shared/LeanStoreAdapter.hpp"
#include "../shared/GenericSchema.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
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

// TPCC specific command line flags
DEFINE_string(server, "127.0.0.1", "Server address");
DEFINE_uint32(port, 12345, "Server port");
DEFINE_uint32(threads, 4, "Number of client threads");
DEFINE_uint32(connections_per_thread, 8, "Connections per thread");
DEFINE_uint32(runtime, 10, "Runtime in seconds");
DEFINE_bool(measure_latency, true, "Track and report request latencies");
DEFINE_bool(tux, false, "Use TUX message interface instead of POSIX send/recv");
DEFINE_uint32(warehouses, 10, "Number of warehouses in TPCC benchmark");
DEFINE_bool(warehouse_affinity, false, "Whether threads have affinity to specific warehouses");
DEFINE_uint32(max_inflight, 1, "Maximum number of in-flight transactions per thread (0 = unlimited)");
DEFINE_uint32(tx_rate, 0, "Target transactions per second per thread (0 = unlimited)");
DEFINE_bool(kv_mode, false, "Run in key-value mode instead of TPC-C mode");
DEFINE_uint32(kv_get_percent, 100, "Percentage of GET operations in key-value mode (0-100)");
DEFINE_uint32(key_range, 1000000, "Range of keys to use for key-value operations");
DEFINE_uint32(value_size, 1000, "Size of values for PUT operations in bytes");

DEFINE_bool(load_data, false, "Run in data loading mode before benchmark");
DEFINE_uint64(load_keys, 1000000, "Number of keys to load in data loading mode");
DEFINE_uint32(load_pipeline, 128, "Maximum pipeline depth during data loading");
DEFINE_uint32(load_report_interval, 10000, "Report loading progress every N keys");
// TUX function pointers
typedef ssize_t (*libtux_send_tux_msg_t)(int fd, const struct msghdr *msg);
typedef ssize_t (*libtux_recv_tux_msg_t)(int fd, struct msghdr *msg);

libtux_send_tux_msg_t g_libtux_send_tux_msg = nullptr;
libtux_recv_tux_msg_t g_libtux_recv_tux_msg = nullptr;

// Define our key type
using BinaryKey = u64;
using BinaryPayload = BytesPayload<1024>; // Support up to 1KB values

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

// Message types matching server definitions
enum MessageType {
    // Original KV RPC types
    GET_REQUEST = 1,
    GET_RESPONSE = 2,
    PUT_REQUEST = 3,
    PUT_RESPONSE = 4,
    ERROR_RESPONSE = 5,
    
    // TPC-C RPC types
    NEW_ORDER_REQUEST = 10,
    NEW_ORDER_RESPONSE = 11,
    PAYMENT_BY_ID_REQUEST = 12,
    PAYMENT_BY_ID_RESPONSE = 13,
    PAYMENT_BY_NAME_REQUEST = 14,
    PAYMENT_BY_NAME_RESPONSE = 15,
    DELIVERY_REQUEST = 16,
    DELIVERY_RESPONSE = 17,
    STOCK_LEVEL_REQUEST = 18,
    STOCK_LEVEL_RESPONSE = 19,
    ORDER_STATUS_ID_REQUEST = 20,
    ORDER_STATUS_ID_RESPONSE = 21,
    ORDER_STATUS_NAME_REQUEST = 22,
    ORDER_STATUS_NAME_RESPONSE = 23
};

// Message header matching server format
struct MessageHeader {
    uint8_t type;
    uint8_t reserved[3];
    uint32_t request_id;
    uint32_t payload_size;
} __attribute__((packed));


// ---------------------- Key-Value Request/Response Structures ------------------------

// Get request format: header + key
struct GetRequest {
    BinaryKey key;
} __attribute__((packed));

// Put request format: header + key + value
struct PutRequest {
BinaryKey key;
// Value follows as variable-length payload
} __attribute__((packed));

// Put response format: header + success flag
struct PutResponse {
uint8_t success;
} __attribute__((packed));

  
// TPC-C Request/Response Structures
struct NewOrderRequest {
    Integer w_id;
    Integer d_id;
    Integer c_id;
    Integer ol_cnt;  // Number of order lines
    // followed by arrays of lineNumbers, supwares, itemids, qtys
} __attribute__((packed));

struct NewOrderResponse {
    uint8_t success;
    double total_amount;
} __attribute__((packed));

struct PaymentByIdRequest {
    Integer w_id;
    Integer d_id;
    Integer c_w_id;
    Integer c_d_id;
    Integer c_id;
    Numeric h_amount;
} __attribute__((packed));

struct PaymentByIdResponse {
    uint8_t success;
} __attribute__((packed));

struct PaymentByNameRequest {
    Integer w_id;
    Integer d_id;
    Integer c_w_id;
    Integer c_d_id;
    char c_last[16];
    Numeric h_amount;
} __attribute__((packed));

struct PaymentByNameResponse {
    uint8_t success;
} __attribute__((packed));

struct DeliveryRequest {
    Integer w_id;
    Integer carrier_id;
} __attribute__((packed));

struct DeliveryResponse {
    uint8_t success;
} __attribute__((packed));

struct StockLevelRequest {
    Integer w_id;
    Integer d_id;
    Integer threshold;
} __attribute__((packed));

struct StockLevelResponse {
    uint8_t success;
    Integer low_stock_count;
} __attribute__((packed));

struct OrderStatusIdRequest {
    Integer w_id;
    Integer d_id;
    Integer c_id;
} __attribute__((packed));

struct OrderStatusIdResponse {
    uint8_t success;
    Integer o_id;
} __attribute__((packed));

struct OrderStatusNameRequest {
    Integer w_id;
    Integer d_id;
    char c_last[16];
} __attribute__((packed));

struct OrderStatusNameResponse {
    uint8_t success;
    Integer o_id;
} __attribute__((packed));

struct ErrorResponse {
    uint32_t error_code;
} __attribute__((packed));

// TPC-C Transaction Types - must match TPCCWorkload::tx() distribution
enum TPCCTxType {
    PAYMENT = 0,
    ORDER_STATUS = 1,
    DELIVERY = 2,
    STOCK_LEVEL = 3,
    NEW_ORDER = 4
};

// Add KV transaction types in the enum section
enum KVTxType {
    KV_GET = 5,
    KV_PUT = 6
};

// Structure to track a pending transaction
struct PendingTransaction {
    uint32_t request_id;
    TPCCTxType tx_type;
    std::chrono::high_resolution_clock::time_point start_time;
    size_t request_size;  // Store request size for later reporting
    
    PendingTransaction(uint32_t id, TPCCTxType type, size_t req_size = 0)
        : request_id(id), tx_type(type), 
          start_time(std::chrono::high_resolution_clock::now()),
          request_size(req_size) {}
};

// Statistics class for TPCC
class TPCCStatistics {
private:
    std::atomic<uint64_t> total_tx{0};
    std::atomic<uint64_t> successful_tx{0};
    std::atomic<uint64_t> failed_tx{0};
    std::atomic<uint64_t> tx_counts[7]{0}; // Count by transaction type
    std::atomic<uint64_t> inflight_tx{0};  // Currently in-flight transactions
    
    std::mutex time_mutex;
    std::chrono::high_resolution_clock::time_point start_time;
    std::chrono::high_resolution_clock::time_point last_report_time;
    uint64_t last_report_total{0};
    
    struct LatencyStats {
        std::mutex mutex;
        std::vector<double> latencies;
    };
    
    std::array<LatencyStats, 7> tx_latencies; // One for each transaction type

    std::atomic<uint64_t> total_request_bytes{0};
    std::atomic<uint64_t> total_response_bytes{0};
    std::atomic<uint64_t> request_bytes_by_type[7]{0}; // Size by transaction type
    std::atomic<uint64_t> response_bytes_by_type[7]{0}; // Size by transaction type

public:
    TPCCStatistics() {
        reset();
    }
    
    // Add this method to the TPCCStatistics class
    void record_message_sizes(TPCCTxType tx_type, size_t request_size, size_t response_size) {
        total_request_bytes.fetch_add(request_size, std::memory_order_relaxed);
        total_response_bytes.fetch_add(response_size, std::memory_order_relaxed);
        request_bytes_by_type[tx_type].fetch_add(request_size, std::memory_order_relaxed);
        response_bytes_by_type[tx_type].fetch_add(response_size, std::memory_order_relaxed);
    }

    void reset() {
        total_tx = 0;
        successful_tx = 0;
        failed_tx = 0;
        total_request_bytes = 0;
        total_response_bytes = 0;
        
        for (int i = 0; i < 7; i++) {
            tx_counts[i] = 0;
            request_bytes_by_type[i] = 0;
            response_bytes_by_type[i] = 0;
            std::lock_guard<std::mutex> lock(tx_latencies[i].mutex);
            tx_latencies[i].latencies.clear();
        }
        
        start_time = std::chrono::high_resolution_clock::now();
        last_report_time = start_time;
        last_report_total = 0;
    }
    
    void record_transaction(TPCCTxType tx_type, bool success, 
                           const std::chrono::high_resolution_clock::time_point& start_time) {
        total_tx.fetch_add(1, std::memory_order_relaxed);
        tx_counts[tx_type].fetch_add(1, std::memory_order_relaxed);
        
        if (success) {
            successful_tx.fetch_add(1, std::memory_order_relaxed);
        } else {
            failed_tx.fetch_add(1, std::memory_order_relaxed);
        }
        
        // Record latency if enabled
        if (FLAGS_measure_latency) {
            auto now = std::chrono::high_resolution_clock::now();
            double latency_ms = std::chrono::duration<double, std::milli>(now - start_time).count();
            
            std::lock_guard<std::mutex> lock(tx_latencies[tx_type].mutex);
            tx_latencies[tx_type].latencies.push_back(latency_ms);
        }
    }
    
    void set_inflight_count(uint64_t count) {
        inflight_tx.store(count, std::memory_order_relaxed);
    }
    
    void print_report(bool final = false) {
        std::lock_guard<std::mutex> lock(time_mutex);
        auto now = std::chrono::high_resolution_clock::now();
        double elapsed_total = std::chrono::duration<double>(now - start_time).count();
        double elapsed_since_last = std::chrono::duration<double>(now - last_report_time).count();
        
        uint64_t total = total_tx.load();
        uint64_t success = successful_tx.load();
        uint64_t failed = failed_tx.load();
        uint64_t inflight = inflight_tx.load();
        
        double throughput = (elapsed_total > 0) ? (total / elapsed_total) : 0;
        double interval_throughput = (elapsed_since_last > 0) ? 
                                  ((total - last_report_total) / elapsed_since_last) : 0;
        
        if (final) {
            std::cout << "\n========== FINAL TPCC STATISTICS ==========\n";
        } else {
            std::cout << "----- TPCC Progress Report -----\n";
        }
        
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Runtime: " << elapsed_total << "s\n";
        std::cout << "Throughput: " << throughput << " txn/sec";
        if (!final) {
            std::cout << " (last " << elapsed_since_last << "s: " << interval_throughput << " txn/sec)";
        }
        std::cout << "\n";
        
        std::cout << "Total transactions: " << total;
        if (!final) {
            std::cout << " (In-flight: " << inflight << ")";
        }
        std::cout << "\n";
        
        std::cout << "Success rate: " << (total > 0 ? (100.0 * success / total) : 0) << "%\n";
        
        const char* tx_names[] = {"Payment", "Order-Status", "Delivery", "Stock-Level", "New-Order", "KV-GET", "KV-PUT"};
        std::cout << "Transaction mix:\n";
        
        if (total > 0) {
            for (int i = 0; i < 7; i++) {
                uint64_t count = tx_counts[i].load();
                std::cout << "  " << tx_names[i] << ": " << count 
                          << " (" << (100.0 * count / total) << "%)\n";
            }
        }
        
        if (final && FLAGS_measure_latency) {
            print_latency_stats();
        }
        
        uint64_t req_bytes = total_request_bytes.load();
        uint64_t resp_bytes = total_response_bytes.load();
        uint64_t total_bytes = req_bytes + resp_bytes;
        
        double req_mb = req_bytes / (1024.0 * 1024.0);
        double resp_mb = resp_bytes / (1024.0 * 1024.0);
        double total_mb = total_bytes / (1024.0 * 1024.0);
        
        double bandwidth_mbps = (elapsed_total > 0) ? (total_mb / elapsed_total) : 0;
        
        std::cout << "\nNetwork Statistics:\n";
        std::cout << "  Total sent: " << std::fixed << std::setprecision(2) << req_mb << " MB\n";
        std::cout << "  Total received: " << resp_mb << " MB\n";
        std::cout << "  Total traffic: " << total_mb << " MB\n";
        std::cout << "  Average bandwidth: " << bandwidth_mbps << " MB/sec\n";
        
        if (final) {
            std::cout << "\nDetailed Message Size Statistics by Transaction Type:\n";
            const char* tx_names[] = {"Payment", "Order-Status", "Delivery", "Stock-Level", "New-Order", "KV-GET", "KV-PUT"};
            
            for (int i = 0; i < 7; i++) {
                uint64_t count = tx_counts[i].load();
                uint64_t req_bytes_type = request_bytes_by_type[i].load();
                uint64_t resp_bytes_type = response_bytes_by_type[i].load();
                
                if (count > 0) {
                    double avg_req_bytes = static_cast<double>(req_bytes_type) / count;
                    double avg_resp_bytes = static_cast<double>(resp_bytes_type) / count;
                    
                    std::cout << "  " << tx_names[i] << ":\n";
                    std::cout << "    Avg request size: " << avg_req_bytes << " bytes\n";
                    std::cout << "    Avg response size: " << avg_resp_bytes << " bytes\n";
                    std::cout << "    Total request data: " << (req_bytes_type / 1024.0 / 1024.0) << " MB\n";
                    std::cout << "    Total response data: " << (resp_bytes_type / 1024.0 / 1024.0) << " MB\n";
                }
            }
        }
        
        if (!final) {
            last_report_total = total;
            last_report_time = now;
        }
    }
    
private:
    double calculate_percentile(const std::vector<double>& sorted_data, double percentile) {
        if (sorted_data.empty()) return 0.0;
        
        double index = percentile * (sorted_data.size() - 1);
        size_t lower_idx = static_cast<size_t>(index);
        size_t upper_idx = std::min(lower_idx + 1, sorted_data.size() - 1);
        double weight = index - lower_idx;
        
        return sorted_data[lower_idx] * (1 - weight) + sorted_data[upper_idx] * weight;
    }
    
    void print_latency_stats() {
        const char* tx_names[] = {"Payment", "Order-Status", "Delivery", "Stock-Level", "New-Order", "KV-GET", "KV-PUT"};
        std::cout << "\n==== LATENCY STATISTICS ====\n";
        
        for (int i = 0; i < 7; i++) {
            std::vector<double> latencies;
            {
                std::lock_guard<std::mutex> lock(tx_latencies[i].mutex);
                latencies = tx_latencies[i].latencies; // make a copy
            }
            
            if (latencies.empty()) continue;
            
            std::sort(latencies.begin(), latencies.end());
            
            double min = latencies.front();
            double max = latencies.back();
            double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
            double avg = sum / latencies.size();
            double p50 = calculate_percentile(latencies, 0.5);
            double p95 = calculate_percentile(latencies, 0.95);
            double p99 = calculate_percentile(latencies, 0.99);
            
            std::cout << tx_names[i] << " Latency (ms, " << latencies.size() << " samples):\n";
            std::cout << "  Min: " << min << ", Avg: " << avg << ", Max: " << max << "\n";
            std::cout << "  p50: " << p50 << ", p95: " << p95 << ", p99: " << p99 << "\n\n";
        }
    }
};

// TPCC random generator matching TPCCWorkload::tx() distribution
class TPCCRandomGenerator {
private:
    std::mt19937 gen;
    
public:
    TPCCRandomGenerator() : gen(std::random_device{}()) {}
    
    int urand(int low, int high) {
        return std::uniform_int_distribution<>(low, high)(gen);
    }
    
    TPCCTxType getRandomTransaction() {
        int rnd = urand(0, 9999);
        
        if (rnd < 4300) {
            return PAYMENT;
        }
        rnd -= 4300;
        
        if (rnd < 400) {
            return ORDER_STATUS;
        }
        rnd -= 400;
        
        if (rnd < 400) {
            return DELIVERY;
        }
        rnd -= 400;
        
        if (rnd < 400) {
            return STOCK_LEVEL;
        }
        
        return NEW_ORDER;
    }
    
    int selectWarehouse(int num_warehouses, bool warehouse_affinity, int thread_id) {
        if (warehouse_affinity) {
            int warehouses_per_thread = std::max(1, (int)(num_warehouses / FLAGS_threads));
            int start_wid = (thread_id * warehouses_per_thread) % num_warehouses + 1;
            int end_wid = std::min(start_wid + warehouses_per_thread - 1, num_warehouses);
            return urand(start_wid, end_wid);
        } else {
            return urand(1, num_warehouses);
        }
    }
    
    // Helper functions to generate random TPC-C data
    int urandexcept(int low, int high, int except) {
        if (high <= low) return low;
        int r = urand(low, high - 1);
        if (r >= except) r++;
        return r;
    }
    
    int nurand(int A, int x, int y) {
        return ((urand(0, A) | urand(x, y)) % (y - x + 1)) + x;
    }
    
    int generateCustomerId() {
        return nurand(1023, 1, 3000);
    }
    
    int generateItemId() {
        return nurand(8191, 1, 100000);
    }

    // Generate random last name according to TPC-C specification
    std::string generateLastName(int num) {
        static const char* const syllables[] = {
            "BAR", "OUGHT", "ABLE", "PRI", "PRES", 
            "ESE", "ANTI", "CALLY", "ATION", "EING"
        };

        std::string name;
        name.reserve(16);
        
        name += syllables[num / 100];
        name += syllables[(num / 10) % 10];
        name += syllables[num % 10];
        
        return name;
    }

    // Generate a non-uniform random last name according to TPC-C 2.1.6
    std::string generateRandomLastName() {
        int random = nurand(255, 0, 999);
        return generateLastName(random);
    }
};

// Key-Value workload generator
class KVWorkloadGenerator {
private:
    std::mt19937 gen;
    std::uniform_int_distribution<BinaryKey> key_dist;
    std::vector<char> random_value_data;
    
public:
    KVWorkloadGenerator() : 
        gen(std::random_device{}()),
        key_dist(1, FLAGS_key_range) {
        
        // Pre-generate random value data for PUTs
        random_value_data.resize(FLAGS_value_size);
        std::uniform_int_distribution<unsigned char> char_dist(0, 255);
        for (size_t i = 0; i < FLAGS_value_size; i++) {
            random_value_data[i] = static_cast<char>(char_dist(gen));
        }
    }
    
    bool isGetOperation() {
        return std::uniform_int_distribution<>(1, 100)(gen) <= FLAGS_kv_get_percent;
    }
    
    BinaryKey getRandomKey() {
        return key_dist(gen);
    }
    
    const char* getRandomValue() const {
        return random_value_data.data();
    }
    
    size_t getValueSize() const {
        return FLAGS_value_size;
    }
};

// Connection class for TPCC client
class TPCCConnection {
private:
    int fd;
    bool connected;
    bool use_tux;
    std::vector<char> send_buffer;
    std::vector<char> recv_buffer;
    std::unordered_map<uint32_t, PendingTransaction>& pending_transactions;
    
    // Add these tracking variables for partial message processing
    bool header_read;                // Whether we've read a complete header
    size_t payload_bytes_read;       // How many bytes of the payload we've read so far
    MessageHeader current_header;    // Store the current header being processed

public:
    TPCCConnection(const std::string& server, uint16_t port,
                  std::unordered_map<uint32_t, PendingTransaction>& transactions)
        : fd(-1), connected(false), 
          use_tux(FLAGS_tux && g_libtux_send_tux_msg && g_libtux_recv_tux_msg),
          send_buffer(4096), recv_buffer(4096), pending_transactions(transactions),
          header_read(false), payload_bytes_read(0) {
        
        connect_to_server(server, port);
    }
    
    ~TPCCConnection() {
        close_connection();
    }
    
    int get_fd() const { return fd; }
    bool is_connected() const { return connected; }
    
    bool connect_to_server(const std::string& server, uint16_t port) {
        close_connection();
        
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return false;
        
        // Set TCP_NODELAY
        int flag = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
        
        struct sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        
        if (inet_pton(AF_INET, server.c_str(), &server_addr.sin_addr) <= 0) {
            close(fd);
            fd = -1;
            return false;
        }
        
        if (::connect(fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            close(fd);
            fd = -1;
            return false;
        }
        
        printf("Connected to server %s:%d\n", server.c_str(), port);
        // Set non-blocking mode
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        
        connected = true;
        return true;
    }
    
    void close_connection() {
        if (fd >= 0) {
            close(fd);
            fd = -1;
        }
        connected = false;
    }
    
    // Generic function to send a request with header and payload
    template <typename T>
    bool send_request(uint8_t msg_type, uint32_t request_id, const T& payload) {
        return send_request_with_data(msg_type, request_id, &payload, sizeof(T));
    }
    
    // Send request with arbitrary data (for variable length payloads)
    bool send_request_with_data(uint8_t msg_type, uint32_t request_id, const void* payload, size_t payload_size) {
        if (!connected) return false;
        
        // Prepare message header
        MessageHeader header;
        header.type = msg_type;
        header.request_id = request_id;
        header.payload_size = payload_size;
        memset(header.reserved, 0, sizeof(header.reserved));
        
        // Calculate total size for tracking
        size_t total_size = sizeof(header) + payload_size;
        
        // Ensure buffer is large enough
        if (send_buffer.size() < total_size) {
            send_buffer.resize(total_size);
        }
        
        // Copy data to buffer
        memcpy(send_buffer.data(), &header, sizeof(header));
        if (payload_size > 0 && payload != nullptr) {
            memcpy(send_buffer.data() + sizeof(header), payload, payload_size);
        }
        
        // Send the request
        ssize_t bytes_sent;
        if (use_tux) {
            // TUX sending code...
        } else {
            bytes_sent = send(fd, send_buffer.data(), total_size, 0);
        }
        
        // Return true only if we sent the entire message
        return (bytes_sent == total_size);
    }
    
    // Send a payment transaction
    bool send_payment(uint32_t request_id, Integer w_id, TPCCRandomGenerator& random_gen) {
        // According to TPC-C spec, 60% lookup by ID, 40% by name
        bool by_name = (random_gen.urand(1, 100) <= 40);
        
        if (by_name) {
            return send_payment_by_name(request_id, w_id, random_gen);
        } else {
            PaymentByIdRequest req;
            req.w_id = w_id;
            req.d_id = random_gen.urand(1, 10);
            req.c_w_id = req.w_id; // For simplicity, customer is from same warehouse
            req.c_d_id = req.d_id; // For simplicity, customer is from same district
            req.c_id = random_gen.generateCustomerId();
            req.h_amount = random_gen.urand(100, 5000) / 100.0; // Random amount between 1.00 and 50.00
            
            if (send_request(PAYMENT_BY_ID_REQUEST, request_id, req)) {
                pending_transactions.emplace(request_id, PendingTransaction(request_id, PAYMENT, sizeof(req)));
                return true;
            }
            return false;
        }
    }
    
    // Send an order status transaction
    bool send_order_status(uint32_t request_id, Integer w_id, TPCCRandomGenerator& random_gen) {
        // According to TPC-C spec, 60% lookup by ID, 40% by name
        bool by_name = (random_gen.urand(1, 100) <= 40);
        
        if (by_name) {
            return send_order_status_by_name(request_id, w_id, random_gen);
        } else {
            OrderStatusIdRequest req;
            req.w_id = w_id;
            req.d_id = random_gen.urand(1, 10);
            req.c_id = random_gen.generateCustomerId();
            
            if (send_request(ORDER_STATUS_ID_REQUEST, request_id, req)) {
                pending_transactions.emplace(request_id, PendingTransaction(request_id, ORDER_STATUS, sizeof(req)));
                return true;
            }
            return false;
        }
    }
    
    // Send a delivery transaction
    bool send_delivery(uint32_t request_id, Integer w_id, TPCCRandomGenerator& random_gen) {
        DeliveryRequest req;
        req.w_id = w_id;
        req.carrier_id = random_gen.urand(1, 10);
        
        if (send_request(DELIVERY_REQUEST, request_id, req)) {
            pending_transactions.emplace(request_id, PendingTransaction(request_id, DELIVERY, sizeof(req)));
            return true;
        }
        return false;
    }
    
    // Send a stock level transaction
    bool send_stock_level(uint32_t request_id, Integer w_id, TPCCRandomGenerator& random_gen) {
        StockLevelRequest req;
        req.w_id = w_id;
        req.d_id = random_gen.urand(1, 10);
        req.threshold = random_gen.urand(10, 20);
        
        if (send_request(STOCK_LEVEL_REQUEST, request_id, req)) {
            pending_transactions.emplace(request_id, PendingTransaction(request_id, STOCK_LEVEL, sizeof(req)));
            return true;
        }
        return false;
    }
    
    // Send a new order transaction
    bool send_new_order(uint32_t request_id, Integer w_id, TPCCRandomGenerator& random_gen) {
        // Generate random parameters for the new order
        Integer d_id = random_gen.urand(1, 10);
        Integer c_id = random_gen.generateCustomerId();
        Integer ol_cnt = random_gen.urand(5, 15);  // Between 5 and 15 order lines
        
        // Calculate the exact size needed for the payload
        size_t base_offset = sizeof(NewOrderRequest);
        size_t array_size = ol_cnt * sizeof(Integer);
        size_t total_array_size = 4 * array_size;  // 4 arrays (lineNumbers, supwares, itemids, qtys)
        size_t payload_size = base_offset + total_array_size;
        
        // Create a buffer for the entire payload
        std::vector<char> payload(payload_size, 0);  // Initialize with zeros
        
        // Fill in the fixed part
        NewOrderRequest* req = reinterpret_cast<NewOrderRequest*>(payload.data());
        req->w_id = w_id;
        req->d_id = d_id;
        req->c_id = c_id;
        req->ol_cnt = ol_cnt;
        
        // Get pointers to the array sections - carefully aligned
        Integer* lineNumbers = reinterpret_cast<Integer*>(payload.data() + base_offset);
        Integer* supwares = reinterpret_cast<Integer*>(payload.data() + base_offset + array_size);
        Integer* itemids = reinterpret_cast<Integer*>(payload.data() + base_offset + 2 * array_size);
        Integer* qtys = reinterpret_cast<Integer*>(payload.data() + base_offset + 3 * array_size);
        
        // Fill arrays with appropriate data
        for (Integer i = 0; i < ol_cnt; i++) {
            lineNumbers[i] = i + 1;  // Line numbers are 1-based
            supwares[i] = w_id;      // Usually from home warehouse
            itemids[i] = random_gen.generateItemId();  // Random item ID
            qtys[i] = random_gen.urand(1, 10);        // Random quantity
        }
        
        // // Debug info to verify payload size
        // std::cout << "NEW_ORDER: ol_cnt=" << ol_cnt 
        //           << ", payload_size=" << payload_size
        //           << ", expected_size=" << (base_offset + 4 * array_size)
        //           << std::endl;
        
        // Send the request using our existing helper
        if (send_request_with_data(NEW_ORDER_REQUEST, request_id, payload.data(), payload_size)) {
            pending_transactions.emplace(request_id, PendingTransaction(request_id, NEW_ORDER, payload_size));
            return true;
        }
        return false;
    }
    
    // Send a transaction based on the type
    bool send_transaction(uint32_t request_id, TPCCTxType tx_type, Integer w_id, TPCCRandomGenerator& random_gen) {
        switch (tx_type) {
            case PAYMENT:
                return send_payment(request_id, w_id, random_gen);
            case ORDER_STATUS:
                return send_order_status(request_id, w_id, random_gen);
            case DELIVERY:
                return send_delivery(request_id, w_id, random_gen);
            case STOCK_LEVEL:
                return send_stock_level(request_id, w_id, random_gen);
            case NEW_ORDER:
                return send_new_order(request_id, w_id, random_gen);
            default:
                return false;
        }
    }
    
    // Try to receive and process responses
    bool receive_responses(TPCCStatistics& stats) {
        if (!connected) return false;
        
        if (use_tux) {
            // TUX implementation remains unchanged as it reads into the buffer directly
            // Ensure receive buffer is adequately sized
            if (recv_buffer.size() < 4096) {
                recv_buffer.resize(4096);
            }
            
            struct iovec iov;
            iov.iov_base = recv_buffer.data();
            iov.iov_len = recv_buffer.size();
            
            struct msghdr msg;
            memset(&msg, 0, sizeof(msg));
            msg.msg_iov = &iov;
            msg.msg_iovlen = 1;
            
            ssize_t bytes_read = g_libtux_recv_tux_msg(fd, &msg);
            
            if (bytes_read <= 0) {
                return (bytes_read < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
            }
            
            // We must have at least a header
            if (bytes_read < sizeof(MessageHeader)) {
                return true; // Incomplete message, try again later
            }
            
            // Extract header
            const MessageHeader* header = reinterpret_cast<const MessageHeader*>(recv_buffer.data());
            
            // Process the complete response
            process_response(*header, recv_buffer.data() + sizeof(MessageHeader), stats);
        } else {
            // Standard socket receive - read header first if needed
            if (!header_read) {
                ssize_t bytes_read = recv(fd, &current_header, sizeof(current_header), 0);
                
                if (bytes_read < 0) {
                    return (errno == EAGAIN || errno == EWOULDBLOCK);
                }
                
                if (bytes_read == 0) {
                    // Connection closed
                    return false;
                }
                
                if (bytes_read < sizeof(MessageHeader)) {
                    // Partial header received, need to read more later
                    return true;
                }
                
                // Full header received
                header_read = true;
                payload_bytes_read = 0;
            }
            
            // At this point, we have a complete header in current_header
            // If there's a payload, read it
            if (current_header.payload_size > 0) {
                // Make sure buffer is large enough for the complete payload
                if (recv_buffer.size() < current_header.payload_size) {
                    recv_buffer.resize(current_header.payload_size);
                }
                
                // Read the remaining payload bytes
                ssize_t bytes_read = recv(
                    fd, 
                    recv_buffer.data() + payload_bytes_read,
                    current_header.payload_size - payload_bytes_read, 
                    0);
                
                if (bytes_read < 0) {
                    return (errno == EAGAIN || errno == EWOULDBLOCK);
                }
                
                if (bytes_read == 0) {
                    // Connection closed
                    return false;
                }
                
                payload_bytes_read += bytes_read;
                
                if (payload_bytes_read < current_header.payload_size) {
                    // Need to read more payload bytes
                    return true;
                }
            }
            
            // At this point we have the complete message (header + payload if any)
            // Process the response
            process_response(current_header, recv_buffer.data(), stats);
            
            // Reset state for next message
            header_read = false;
            payload_bytes_read = 0;
        }
        
        return true;
    }
    
    void process_response(const MessageHeader& header, const void* payload, TPCCStatistics& stats) {
        // Find the pending transaction
        auto it = pending_transactions.find(header.request_id);
        if (it == pending_transactions.end()) {
            return; // Unknown transaction ID
        }
        
        bool success = false;
        
        // Check response type based on request type
        switch (header.type) {
            case PAYMENT_BY_ID_RESPONSE:
            case PAYMENT_BY_NAME_RESPONSE:
                if (it->second.tx_type == PAYMENT) {
                    const PaymentByIdResponse* response = reinterpret_cast<const PaymentByIdResponse*>(payload);
                    success = response->success != 0;
                }
                break;
                
            case ORDER_STATUS_ID_RESPONSE:
            case ORDER_STATUS_NAME_RESPONSE:
                if (it->second.tx_type == ORDER_STATUS) {
                    const OrderStatusIdResponse* response = reinterpret_cast<const OrderStatusIdResponse*>(payload);
                    success = response->success != 0;
                }
                break;
                
            case DELIVERY_RESPONSE:
                if (it->second.tx_type == DELIVERY) {
                    const DeliveryResponse* response = reinterpret_cast<const DeliveryResponse*>(payload);
                    success = response->success != 0;
                }
                break;
                
            case STOCK_LEVEL_RESPONSE:
                if (it->second.tx_type == STOCK_LEVEL) {
                    const StockLevelResponse* response = reinterpret_cast<const StockLevelResponse*>(payload);
                    success = response->success != 0;
                }
                break;
                
            case NEW_ORDER_RESPONSE:
                if (it->second.tx_type == NEW_ORDER) {
                    const NewOrderResponse* response = reinterpret_cast<const NewOrderResponse*>(payload);
                    success = response->success != 0;
                }
                break;
                
            case GET_RESPONSE:
                if (it->second.tx_type == static_cast<TPCCTxType>(KV_GET)) {
                    // For KV operations, we consider any response successful
                    success = true;
                }
                break;
                
            case PUT_RESPONSE:
                if (it->second.tx_type == static_cast<TPCCTxType>(KV_PUT)) {
                    const PutResponse* response = reinterpret_cast<const PutResponse*>(payload);
                    success = response->success != 0;
                }
                break;
                
            case ERROR_RESPONSE:
                success = false;
                break;
                
            default:
                success = false;
                break;
        }
        
        // Record the transaction result
        stats.record_transaction(it->second.tx_type, success, it->second.start_time);
        
        // Record message sizes - find request size from transaction's stored size
        size_t request_size = it->second.request_size;
        size_t response_size = sizeof(MessageHeader) + header.payload_size;
        stats.record_message_sizes(it->second.tx_type, request_size, response_size);
        
        // Remove from pending transactions
        pending_transactions.erase(it);
        //printf("Transaction %u completed: %s\n", header.request_id, success ? "Success" : "Failure");
    }

    // Send a payment transaction with customer lookup by name
    bool send_payment_by_name(uint32_t request_id, Integer w_id, TPCCRandomGenerator& random_gen) {
        PaymentByNameRequest req;
        req.w_id = w_id;
        req.d_id = random_gen.urand(1, 10);
        req.c_w_id = req.w_id; // For simplicity, customer is from same warehouse
        req.c_d_id = req.d_id; // For simplicity, customer is from same district
        
        // Generate a last name according to TPC-C spec
        std::string last_name = random_gen.generateRandomLastName();
        strncpy(req.c_last, last_name.c_str(), sizeof(req.c_last) - 1);
        req.c_last[sizeof(req.c_last) - 1] = '\0'; // Ensure null-termination
        
        req.h_amount = random_gen.urand(100, 5000) / 100.0; // Random amount between 1.00 and 50.00
        
        if (send_request(PAYMENT_BY_NAME_REQUEST, request_id, req)) {
            pending_transactions.emplace(request_id, PendingTransaction(request_id, PAYMENT, sizeof(req)));
            return true;
        }
        return false;
    }

    // Send order status transaction with customer lookup by name
    bool send_order_status_by_name(uint32_t request_id, Integer w_id, TPCCRandomGenerator& random_gen) {
        OrderStatusNameRequest req;
        req.w_id = w_id;
        req.d_id = random_gen.urand(1, 10);
        
        // Generate a last name according to TPC-C spec
        std::string last_name = random_gen.generateRandomLastName();
        strncpy(req.c_last, last_name.c_str(), sizeof(req.c_last) - 1);
        req.c_last[sizeof(req.c_last) - 1] = '\0'; // Ensure null-termination
        
        if (send_request(ORDER_STATUS_NAME_REQUEST, request_id, req)) {
            pending_transactions.emplace(request_id, PendingTransaction(request_id, ORDER_STATUS, sizeof(req)));
            return true;
        }
        return false;
    }

    // Send a Get request
    bool send_get(uint32_t request_id, BinaryKey key) {
        GetRequest req;
        req.key = key;
        
        // Calculate total message size for tracking
        size_t total_request_size = sizeof(MessageHeader) + sizeof(GetRequest);
        
        if (send_request(GET_REQUEST, request_id, req)) {
            pending_transactions.emplace(request_id, 
                PendingTransaction(request_id, static_cast<TPCCTxType>(KV_GET), total_request_size));
            return true;
        }
        return false;
    }

    // Send a Put request
    bool send_put(uint32_t request_id, BinaryKey key, const char* value, size_t value_size) {
        // Calculate total size needed for the payload
        size_t payload_size = sizeof(PutRequest) + value_size;
        
        // Create a buffer for the entire payload
        std::vector<char> payload(payload_size);
        
        // Fill in the key
        PutRequest* req = reinterpret_cast<PutRequest*>(payload.data());
        req->key = key;
        
        // Copy the value after the key
        if (value && value_size > 0) {
            memcpy(payload.data() + sizeof(PutRequest), value, value_size);
        }
        
        // Send the request with the complete payload
        if (send_request_with_data(PUT_REQUEST, request_id, payload.data(), payload_size)) {
            //printf("Sent PUT request with key %u and value size %zu\n", key, value_size);
            pending_transactions.emplace(request_id, PendingTransaction(request_id, static_cast<TPCCTxType>(KV_PUT), payload_size));
            return true;
        }
        return false;
    }
};

// Worker thread implementation
class TPCCWorkerThread {
private:
    uint32_t thread_id;
    std::atomic<bool>& running;
    TPCCStatistics& stats;
    TPCCRandomGenerator random_gen;
    KVWorkloadGenerator kv_generator;
    std::atomic<uint32_t> next_request_id;
    
    std::vector<std::unique_ptr<TPCCConnection>> connections;
    std::unordered_map<uint32_t, PendingTransaction> pending_transactions;
    
    int epoll_fd;
    
    // Rate limiting members
    std::chrono::high_resolution_clock::time_point last_tx_time;
    std::chrono::nanoseconds tx_interval;

public:
    TPCCWorkerThread(uint32_t id, std::atomic<bool>& run_flag, TPCCStatistics& stats_ref)
        : thread_id(id), running(run_flag), stats(stats_ref), next_request_id(id * 1000000) {
        
        // Initialize rate limiting
        last_tx_time = std::chrono::high_resolution_clock::now();
        if (FLAGS_tx_rate > 0) {
            // Calculate interval between transactions to achieve target rate
            tx_interval = std::chrono::nanoseconds(1000000000ULL / FLAGS_tx_rate);
        } else {
            tx_interval = std::chrono::nanoseconds(0);
        }
        
        // Initialize epoll
        epoll_fd = epoll_create1(0);
        if (epoll_fd < 0) {
            throw std::runtime_error("Failed to create epoll instance");
        }
        
        // Create connections
        for (uint32_t i = 0; i < FLAGS_connections_per_thread; i++) {
            auto conn = std::make_unique<TPCCConnection>(FLAGS_server, FLAGS_port, pending_transactions);
            
            if (conn->is_connected()) {
                // Add to epoll
                struct epoll_event ev;
                ev.events = EPOLLIN;
                ev.data.ptr = conn.get();
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn->get_fd(), &ev);
                
                connections.push_back(std::move(conn));
            }
        }
        
        if (connections.empty()) {
            throw std::runtime_error("Failed to establish any connections");
        }
    }
    
    ~TPCCWorkerThread() {
        if (epoll_fd >= 0) {
            close(epoll_fd);
        }
    }
    
    void run() {
        const int MAX_EVENTS = 32;
        struct epoll_event events[MAX_EVENTS];
        
        size_t conn_index = 0;
        
        while (running.load()) {
            // Check rate limit for transactions per second
            bool can_send = true;
            if (FLAGS_tx_rate > 0) {
                auto now = std::chrono::high_resolution_clock::now();
                auto elapsed = now - last_tx_time;
                
                if (elapsed < tx_interval) {
                    // Not enough time has passed since last transaction
                    can_send = false;
                    
                }
            }
            
            // Check inflight transaction limit
            if (can_send && FLAGS_max_inflight > 0 && pending_transactions.size() >= FLAGS_max_inflight) {
                can_send = false;
            }
            
            // Generate and send new transactions if allowed by rate limit
            if (can_send) {
                for (size_t i = 0; i < connections.size() && 
                     (FLAGS_max_inflight == 0 || pending_transactions.size() < FLAGS_max_inflight); i++) {
                    auto& conn = connections[conn_index];
                    conn_index = (conn_index + 1) % connections.size();
                    
                    if (conn->is_connected()) {
                        uint32_t request_id = next_request_id++;
                        bool success = false;
                        
                        if (FLAGS_kv_mode) {
                            // Key-value workload
                            BinaryKey key = kv_generator.getRandomKey();
                            
                            if (kv_generator.isGetOperation()) {
                                // Send a GET request
                                success = conn->send_get(request_id, key);
                            } else {
                                // Send a PUT request
                                success = conn->send_put(request_id, key, 
                                                      kv_generator.getRandomValue(),
                                                      kv_generator.getValueSize());
                            }
                        } else {
                            // Original TPC-C workload
                            TPCCTxType tx_type = random_gen.getRandomTransaction();
                            int w_id = random_gen.selectWarehouse(FLAGS_warehouses, FLAGS_warehouse_affinity, thread_id);
                            success = conn->send_transaction(request_id, tx_type, w_id, random_gen);
                        }
                        
                        // Update last transaction time for rate limiting
                        if (success && FLAGS_tx_rate > 0) {
                            last_tx_time = std::chrono::high_resolution_clock::now();
                            break; // Only send one transaction at a time when rate limiting
                        }
                    }
                }
            }
            
            // Check for responses
            int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
            
            for (int i = 0; i < num_events; i++) {
                TPCCConnection* conn = static_cast<TPCCConnection*>(events[i].data.ptr);
                
                if (events[i].events & EPOLLIN) {
                    if (!conn->receive_responses(stats)) {
                        // Handle connection error - reconnect
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->get_fd(), nullptr);
                        
                        // if (conn->connect_to_server(FLAGS_server, FLAGS_port)) {
                        //     struct epoll_event ev;
                        //     ev.events = EPOLLIN;
                        //     ev.data.ptr = conn;
                        //     epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn->get_fd(), &ev);
                        // }
                    }
                }
            }
            
            // Add a small sleep to prevent CPU spin if we have nothing to do
            // if (!can_send && num_events == 0) {
            //     std::this_thread::sleep_for(std::chrono::milliseconds(1));
            // }
            
            // Update in-flight transaction count in statistics
            stats.set_inflight_count(pending_transactions.size());
        }
    }
    
    // Add a method to get current in-flight transaction count
    size_t get_inflight_count() const {
        return pending_transactions.size();
    }
};

// Worker thread function
void tpcc_worker_thread(uint32_t thread_id, TPCCStatistics& stats, std::atomic<bool>& running,
                        std::vector<TPCCWorkerThread*>& worker_thread_ptrs) {
    try {
        auto worker = std::make_unique<TPCCWorkerThread>(thread_id, running, stats);
        worker_thread_ptrs[thread_id] = worker.get();
        worker->run();
    } catch (const std::exception& e) {
        std::cerr << "Thread " << thread_id << " error: " << e.what() << std::endl;
    }
}

// Report thread function
void tpcc_report_thread(TPCCStatistics& stats, std::atomic<bool>& running,
                       const std::vector<TPCCWorkerThread*>& worker_thread_ptrs) {
    while (running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        // Gather in-flight transaction counts
        uint64_t total_inflight = 0;
        for (auto worker : worker_thread_ptrs) {
            if (worker) {
                total_inflight += worker->get_inflight_count();
            }
        }
        
        stats.set_inflight_count(total_inflight);
        stats.print_report(false);
    }
}



// Function to load data phase for the client with one connection and no epoll
void load_data_phase() {
    std::cout << "=== Starting Data Loading Phase ===" << std::endl;
    std::cout << "Loading " << FLAGS_load_keys << " keys with pipeline depth of " << FLAGS_load_pipeline << std::endl;
    
    // Initialize TUX if needed
    if (FLAGS_tux && !g_libtux_send_tux_msg && !g_libtux_recv_tux_msg) {
        initialize_tux_functions();
    }
    
    // Create a single connection
    std::unordered_map<uint32_t, PendingTransaction> pending_transactions;
    auto conn = std::make_unique<TPCCConnection>(FLAGS_server, FLAGS_port, pending_transactions);
    
    if (!conn->is_connected()) {
        std::cerr << "Failed to establish connection for data loading." << std::endl;
        return;
    }
    
    std::cout << "Connection established for loading." << std::endl;
    
    // Generate random data for values
    KVWorkloadGenerator gen;
    const char* value_data = gen.getRandomValue();
    size_t value_size = gen.getValueSize();
    
    // Statistics for loading
    uint64_t keys_loaded = 0;
    uint64_t keys_sent = 0;
    uint64_t last_report = 0;
    TPCCStatistics stats;
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Load data
    while (keys_loaded < FLAGS_load_keys) {
        // Process responses if we've hit our pipeline limit
        while (pending_transactions.size() >= FLAGS_load_pipeline || 
               (keys_sent >= FLAGS_load_keys && keys_loaded < keys_sent)) {
            if (!conn->receive_responses(stats)) {
                std::cerr << "Connection error during data loading." << std::endl;
                return;
            }
            
            // Calculate how many responses we've received
            keys_loaded = keys_sent - pending_transactions.size();
            
            // Break if we can send more requests
            if (pending_transactions.size() < FLAGS_load_pipeline && keys_sent < FLAGS_load_keys) {
                break;
            }
            
            // // Small delay to prevent busy waiting
            // if (!pending_transactions.empty()) {
            //     std::this_thread::sleep_for(std::chrono::milliseconds(1));
            // }
        }
        
        // Send more keys if we haven't sent them all and have space in our pipeline
        while (keys_sent < FLAGS_load_keys && pending_transactions.size() < FLAGS_load_pipeline) {
            uint32_t request_id = keys_sent + 1;  // Use key number as request ID
            BinaryKey key = keys_sent + 1;        // Keys start at 1
            
            // Send the PUT request
            if (conn->send_put(request_id, key, value_data, value_size)) {
                keys_sent++;
            } else {
                std::cerr << "Failed to send PUT request during data loading." << std::endl;
                break;
            }
        }
        
        // Report progress periodically
        if (keys_loaded >= last_report + FLAGS_load_report_interval) {
            auto now = std::chrono::high_resolution_clock::now();
            double elapsed = std::chrono::duration<double>(now - start_time).count();
            double rate = keys_loaded / elapsed;
            
            std::cout << "Loaded " << keys_loaded << " of " << FLAGS_load_keys << " keys (" 
                      << std::fixed << std::setprecision(2) << (100.0 * keys_loaded / FLAGS_load_keys) << "%), "
                      << rate << " keys/sec, "
                      << "pipeline: " << pending_transactions.size() << std::endl;
                      
            last_report = keys_loaded;
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    double total_time = std::chrono::duration<double>(end_time - start_time).count();
    
    std::cout << "=== Data Loading Complete ===" << std::endl;
    std::cout << "Loaded " << keys_loaded << " keys in " << total_time << " seconds ("
              << (keys_loaded / total_time) << " keys/sec)" << std::endl;
}

int main(int argc, char** argv) {
    gflags::SetUsageMessage("TPC-C Client Benchmark");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Initialize TUX if needed
    if (FLAGS_tux) {
        if (initialize_tux_functions()) {
            std::cout << "TUX mode enabled" << std::endl;
        } else {
            std::cout << "TUX mode requested but not available" << std::endl;
            FLAGS_tux = false;
        }
    }
    
    // Print configuration
    std::cout << "=== Benchmark Configuration ===" << std::endl;
    std::cout << "Server: " << FLAGS_server << ":" << FLAGS_port << std::endl;
    std::cout << "Threads: " << FLAGS_threads << std::endl;
    std::cout << "Connections per thread: " << FLAGS_connections_per_thread << std::endl;
    std::cout << "Mode: " << (FLAGS_kv_mode ? "Key-Value" : "TPC-C") << std::endl;

    if (FLAGS_kv_mode) {
        std::cout << "KV Workload: " << FLAGS_kv_get_percent << "% GETs, " 
                  << (100 - FLAGS_kv_get_percent) << "% PUTs" << std::endl;
        std::cout << "Key range: " << FLAGS_key_range << std::endl;
        std::cout << "Value size: " << FLAGS_value_size << " bytes" << std::endl;
            // Run data loading phase if requested
        if (FLAGS_load_data) {
            load_data_phase();
            
            // If we're only loading data, exit after loading
            if (FLAGS_runtime == 0) {
                return 0;
            }
            
            std::cout << "Loading complete, starting benchmark..." << std::endl;
        }
    } else {
        std::cout << "Warehouses: " << FLAGS_warehouses << std::endl;
        std::cout << "Warehouse affinity: " << (FLAGS_warehouse_affinity ? "enabled" : "disabled") << std::endl;
    }
    
    std::cout << "Runtime: " << FLAGS_runtime << " seconds" << std::endl;
    
    if (FLAGS_max_inflight > 0) {
        std::cout << "Max in-flight transactions: " << FLAGS_max_inflight << " per thread" << std::endl;
    } else {
        std::cout << "Max in-flight transactions: unlimited" << std::endl;
    }
    
    if (FLAGS_tx_rate > 0) {
        std::cout << "Target transaction rate: " << FLAGS_tx_rate << " tx/sec per thread" << std::endl;
    } else {
        std::cout << "Target transaction rate: unlimited" << std::endl;
    }
    
    std::cout << "===================================" << std::endl;
    
    // Initialize statistics
    TPCCStatistics stats;
    std::atomic<bool> running(true);
    
    // Create vector to store worker thread pointers
    std::vector<TPCCWorkerThread*> worker_thread_ptrs(FLAGS_threads, nullptr);
    
    // Start worker threads
    std::vector<std::thread> worker_threads;
    for (uint32_t i = 0; i < FLAGS_threads; i++) {
        worker_threads.emplace_back(tpcc_worker_thread, i, std::ref(stats), 
                                   std::ref(running), std::ref(worker_thread_ptrs));
    }
    
    // Start reporting thread
    std::thread stats_thread(tpcc_report_thread, std::ref(stats), 
                            std::ref(running), std::cref(worker_thread_ptrs));
    
    // Wait for configured runtime
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_runtime));
    
    // Stop all threads
    running.store(false);
    
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