#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

// Maximum tuple size for the benchmark
constexpr size_t MAX_TUPLE_SIZE = 3000;
constexpr size_t MAX_PAYLOAD_SIZE = MAX_TUPLE_SIZE - 16; // Account for header size

// Header for benchmark tuples
struct BenchmarkHeader {
    uint32_t tuple_id;        // Unique identifier for the tuple
    uint32_t payload_size;    // Size of the payload in bytes
    uint32_t checksum;        // Checksum for data verification
    uint32_t flags;           // Additional flags for future use
    uint64_t timestamp;       // Timestamp for latency measurement
};

// Complete benchmark tuple with payload
struct BenchmarkTuple {
    uint32_t tuple_id;        // Unique identifier for the tuple
    uint32_t payload_size;    // Size of the payload in bytes
    uint32_t checksum;        // Checksum for data verification
    uint32_t flags;           // Additional flags for future use
    uint64_t timestamp;       // Timestamp for latency measurement
    char payload[MAX_PAYLOAD_SIZE]; // Variable-length payload

    // Helper to create a tuple with specified payload size using stack allocation
    static void create(BenchmarkTuple& tuple, uint32_t id, uint32_t size) {
        if (size > MAX_PAYLOAD_SIZE) {
            size = MAX_PAYLOAD_SIZE;
        }
        
        tuple.tuple_id = id;
        tuple.payload_size = size;
        tuple.flags = 0;
        tuple.timestamp = 0;  // Will be set right before sending
        
        // Fill payload with a deterministic pattern based on ID
        for (uint32_t i = 0; i < size; i++) {
            tuple.payload[i] = static_cast<char>((id + i) % 256);
        }
        
        // Calculate checksum
        tuple.checksum = 0;
        for (uint32_t i = 0; i < size; i++) {
            tuple.checksum += static_cast<uint8_t>(tuple.payload[i]);
        }
    }
    
    // Verify the tuple's integrity
    bool verify() const {
        uint32_t computed_checksum = 0;
        for (uint32_t i = 0; i < payload_size; i++) {
            computed_checksum += static_cast<uint8_t>(payload[i]);
        }
        return computed_checksum == checksum;
    }
    
    // Get total size of the tuple including header and payload
    size_t total_size() const {
        return sizeof(BenchmarkHeader) + payload_size;
    }
};

// Benchmark statistics structure
struct BenchmarkStats {
    uint64_t tuples_processed;
    uint64_t bytes_processed;
    uint64_t errors;
    double throughput_tuples_per_sec;
    double throughput_mb_per_sec;
    double avg_latency_ms;
    double p50_latency_ms;
    double p90_latency_ms;
    double p99_latency_ms;
    double p999_latency_ms;
};

// Enum for transport modes
enum TransportMode {
    NONE,     // No TUX, standard socket send/recv
    STREAM,   // TUX stream interface (ordered delivery)
    MESSAGE   // TUX message interface (out-of-order delivery)
};

// Convert string transport mode to enum
inline TransportMode parseTransportMode(const std::string& mode) {
    if (mode == "none") return NONE;
    if (mode == "stream") return STREAM;
    if (mode == "message") return MESSAGE;
    return NONE; // Default
}
