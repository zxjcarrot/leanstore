#include "../shared/LeanStoreAdapter.hpp"
#include "../shared/GenericSchema.hpp"
#include "Units.hpp"
#include "TPCCWorkload.hpp"
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
#include <signal.h>
// -------------------------------------------------------------------------------------
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <cstring>
#include <mutex>
#include <dlfcn.h>

#include "tux.h"

// -------------------------------------------------------------------------------------
DEFINE_uint32(port, 12345, "Server port");
DEFINE_uint32(epoll_timeout, 100, "Epoll timeout in ms");
DEFINE_uint32(max_connections, 1024, "Maximum number of connections");
DEFINE_bool(debug, false, "Enable debug output");
DEFINE_uint32(warehouse_count, 10, "Number of warehouses");
DEFINE_bool(warehouse_affinity, true, "Whether to enforce warehouse affinity");
DEFINE_bool(tpcc_remove, true, "Whether to remove processed entries");
DEFINE_bool(order_wdc_index, true, "Whether to use order_wdc index");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// Define our key type
using BinaryKey = u64;
using BinaryPayload = BytesPayload<1024>; // Support up to 1KB values

// Now define our table type
using KVTable = Relation<BinaryKey, BinaryPayload>;

// Message types for TPC-C
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

// Message header (12 bytes)
struct MessageHeader {
  uint8_t type;
  uint8_t reserved[3];
  uint32_t request_id;
  uint32_t payload_size;
} __attribute__((packed));

// Message structure to hold complete request or response
struct Message {
  MessageHeader header;
  std::vector<char> payload;
  
  Message() {}
  
  Message(uint8_t type, uint32_t request_id, size_t payload_size) {
    header.type = type;
    header.request_id = request_id;
    header.payload_size = payload_size;
    memset(header.reserved, 0, sizeof(header.reserved));
    if (payload_size > 0) {
      payload.resize(payload_size);
    }
  }
  
  size_t total_size() const {
    return sizeof(header) + payload.size();
  }
};

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

// ---------------------- TPC-C Request/Response Structures ------------------------

// NEW_ORDER
struct NewOrderRequest {
  Integer w_id;
  Integer d_id;
  Integer c_id;
  Integer ol_cnt;  // Number of order lines
  // followed by arrays of lineNumbers, supwares, itemids, qtys
} __attribute__((packed));

struct NewOrderResponse {
  uint8_t success;
  double total_amount;  // Optional: total amount of the order
} __attribute__((packed));

// PAYMENT_BY_ID
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

// PAYMENT_BY_NAME
struct PaymentByNameRequest {
  Integer w_id;
  Integer d_id;
  Integer c_w_id;
  Integer c_d_id;
  char c_last[16];  // Using fixed size for simplicity
  Numeric h_amount;
} __attribute__((packed));

struct PaymentByNameResponse {
  uint8_t success;
} __attribute__((packed));

// DELIVERY
struct DeliveryRequest {
  Integer w_id;
  Integer carrier_id;
} __attribute__((packed));

struct DeliveryResponse {
  uint8_t success;
} __attribute__((packed));

// STOCK_LEVEL
struct StockLevelRequest {
  Integer w_id;
  Integer d_id;
  Integer threshold;
} __attribute__((packed));

struct StockLevelResponse {
  uint8_t success;
  Integer low_stock_count;  // Number of items below threshold
} __attribute__((packed));

// ORDER_STATUS_ID
struct OrderStatusIdRequest {
  Integer w_id;
  Integer d_id;
  Integer c_id;
} __attribute__((packed));

struct OrderStatusIdResponse {
  uint8_t success;
  Integer o_id;  // Order ID
} __attribute__((packed));

// ORDER_STATUS_NAME
struct OrderStatusNameRequest {
  Integer w_id;
  Integer d_id;
  char c_last[16];  // Using fixed size for simplicity
} __attribute__((packed));

struct OrderStatusNameResponse {
  uint8_t success;
  Integer o_id;  // Order ID
} __attribute__((packed));

// Error response format: header + error code + message
struct ErrorResponse {
  uint32_t error_code;
  // Error message follows as variable-length payload
} __attribute__((packed));

// Connection context
struct ConnectionContext {
  int fd;
  uint32_t worker_id;
  
  // Read buffers
  std::vector<char> read_buffer;    // Buffer for reading data
  size_t bytes_read;                // How many bytes have been read into read_buffer
  size_t current_message_size;      // Size of the message currently being read
  
  // Request queue - parsed complete messages ready to be processed
  std::queue<Message> request_queue;
  
  // Response queue - completed responses ready to be sent
  std::queue<Message> response_queue;
  
  // Write buffers
  std::vector<char> write_buffer;   // Current aggregated data to write
  size_t bytes_written;             // How many bytes have been written from write_buffer
  
  // TUX-specific state
  bool received_via_tux;
  
  ConnectionContext(int fd, uint32_t worker_id) 
    : fd(fd), worker_id(worker_id),
      bytes_read(0), current_message_size(0), 
      bytes_written(0), received_via_tux(false) {
    // Start with enough capacity for a few standard messages
    read_buffer.reserve(4096);
    write_buffer.reserve(4096);
  }

  ConnectionContext() 
    : fd(-1), worker_id(0),
      bytes_read(0), current_message_size(0),
      bytes_written(0), received_via_tux(false) {
    read_buffer.reserve(4096);
    write_buffer.reserve(4096);
  }
  
  // Process incoming data and extract complete messages
  void process_incoming_data() {
    // We need at least a header to determine message size
    while (bytes_read >= sizeof(MessageHeader)) {
      // If we haven't determined the current message size yet
      if (current_message_size == 0) {
        MessageHeader* header = reinterpret_cast<MessageHeader*>(read_buffer.data());
        
        // Validate the message type
        if (header->type < GET_REQUEST || header->type > ORDER_STATUS_NAME_REQUEST) {
          // Invalid message type, clear the buffer and return
          bytes_read = 0;
          return;
        }
        
        // Calculate total message size
        current_message_size = sizeof(MessageHeader) + header->payload_size;
      }
      
      // Check if we have a complete message
      if (bytes_read >= current_message_size) {
        // Extract the message
        MessageHeader* header = reinterpret_cast<MessageHeader*>(read_buffer.data());
        
        Message msg;
        msg.header = *header;
        
        // Copy payload if present
        if (header->payload_size > 0) {
          msg.payload.resize(header->payload_size);
          memcpy(msg.payload.data(), read_buffer.data() + sizeof(MessageHeader), header->payload_size);
        }
        
        // Add to request queue
        request_queue.push(std::move(msg));
        
        // Remove the processed message from the read buffer
        if (bytes_read > current_message_size) {
          // Move remaining data to the beginning of the buffer
          memmove(read_buffer.data(), read_buffer.data() + current_message_size, bytes_read - current_message_size);
        }
        bytes_read -= current_message_size;
        current_message_size = 0;
      } else {
        // Don't have a complete message yet, need more data
        break;
      }
    }
  }
  
  // Prepare the next batch of data to write
  void prepare_write_data() {
    // If we've written everything in the write buffer, prepare more data
    if (bytes_written >= write_buffer.size()) {
      write_buffer.clear();
      bytes_written = 0;
      
      // Check if we have responses to send
      if (response_queue.empty()) {
        return;
      }
      
      // Get up to 5 responses and batch them for writing
      size_t total_size = 0;
      std::vector<Message> batch;
      
      while (!response_queue.empty()) {
        batch.push_back(std::move(response_queue.front()));
        total_size += batch.back().total_size();
        response_queue.pop();
      }
      
      // Prepare the write buffer with all responses
      write_buffer.resize(total_size);
      size_t offset = 0;
      
      for (const auto& msg : batch) {
        // Copy header
        memcpy(write_buffer.data() + offset, &msg.header, sizeof(MessageHeader));
        offset += sizeof(MessageHeader);
        
        // Copy payload if any
        if (!msg.payload.empty()) {
          memcpy(write_buffer.data() + offset, msg.payload.data(), msg.payload.size());
          offset += msg.payload.size();
        }
      }
    }
  }
  
  // Add a response to the queue
  void queue_response(Message&& response) {
    response_queue.push(std::move(response));
  }
  
  // Create and queue an error response
  void send_error_response(uint32_t request_id, uint32_t error_code, const std::string& message) {
    // Create error response
    Message response(ERROR_RESPONSE, request_id, sizeof(ErrorResponse) + message.size());
    
    // Set error code
    ErrorResponse* err = reinterpret_cast<ErrorResponse*>(response.payload.data());
    err->error_code = error_code;
    
    // Copy error message
    memcpy(response.payload.data() + sizeof(ErrorResponse), message.c_str(), message.size());
    
    // Queue the response
    queue_response(std::move(response));
  }
};

void process_all_requests(ConnectionContext* ctx);
void process_single_request(ConnectionContext* ctx, Message&& request);
void process_new_order_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size);
void process_payment_by_id_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size);
void process_payment_by_name_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size);
void process_delivery_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size);
void process_stock_level_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size);
void process_order_status_id_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size);
void process_order_status_name_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size);
void process_get_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size);
void process_put_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size);

// Global variables
std::atomic<bool> g_running(true);
std::vector<int> g_epoll_fds;
std::vector<std::thread> g_worker_threads;

// Fixed-size array for connection contexts, with initialization flag
struct ConnectionPool {
  ConnectionContext connections[1024]; // Fixed size to match FLAGS_max_connections
  std::unique_ptr<std::mutex[]> mutexes; // Array of mutexes
  bool active[1024]; // Whether the connection slot is active

  ConnectionPool() : mutexes(new std::mutex[1024]) {
    memset(active, false, sizeof(active));
  }

  bool add_connection(int fd, uint32_t worker_id) {
    if (fd >= 1024) { // Match with max size
      printf("Invalid file descriptor: %d\n", fd);
      return false;
    }
    
    std::lock_guard<std::mutex> lock(mutexes[fd]);
    if (active[fd]) {
      printf("Connection %d already in use\n", fd);
      return false; // Already in use
    }
    
    connections[fd] = ConnectionContext(fd, worker_id);
    active[fd] = true;
    return true;
  }

  bool remove_connection(int fd) {
    if (fd >= 1024) { // Match with max size
      return false;
    }
    
    std::lock_guard<std::mutex> lock(mutexes[fd]);
    if (!active[fd]) {
      return false; // Not active
    }
    
    active[fd] = false;
    return true;
  }

  bool is_active(int fd) {
    if (fd >= 1024) { // Match with max size
      return false;
    }
    
    std::lock_guard<std::mutex> lock(mutexes[fd]);
    return active[fd];
  }
  
  // Get connection without locking
  ConnectionContext* get_connection_unsafe(int fd) {
    if (fd >= 1024 || !active[fd]) { // Match with max size
      return nullptr;
    }
    return &connections[fd];
  }

  // Get connection with locking
  ConnectionContext* get_connection(int fd) {
    if (fd >= 1024) { // Match with max size
      return nullptr;
    }
    
    std::lock_guard<std::mutex> lock(mutexes[fd]);
    if (!active[fd]) {
      return nullptr;
    }
    return &connections[fd];
  }
  
  // Get the mutex for a specific fd
  std::mutex& get_mutex(int fd) {
    return mutexes[fd];
  }
};

ConnectionPool g_connections;
LeanStore* g_db;

LeanStoreAdapter<KVTable>* g_table = nullptr;

// TPCC adapter and related table types
typedef LeanStoreAdapter<warehouse_t> warehouse_adapter_t;
typedef LeanStoreAdapter<district_t> district_adapter_t;
typedef LeanStoreAdapter<customer_t> customer_adapter_t;
typedef LeanStoreAdapter<customer_wdl_t> customer_wdl_adapter_t;
typedef LeanStoreAdapter<history_t> history_adapter_t;
typedef LeanStoreAdapter<neworder_t> neworder_adapter_t;
typedef LeanStoreAdapter<order_t> order_adapter_t;
typedef LeanStoreAdapter<order_wdc_t> order_wdc_adapter_t;
typedef LeanStoreAdapter<orderline_t> orderline_adapter_t;
typedef LeanStoreAdapter<item_t> item_adapter_t;
typedef LeanStoreAdapter<stock_t> stock_adapter_t;

// TPCC table pointers
warehouse_adapter_t* g_warehouse;
district_adapter_t* g_district;
customer_adapter_t* g_customer;
customer_wdl_adapter_t* g_customer_wdl;
history_adapter_t* g_history;
neworder_adapter_t* g_neworder;
order_adapter_t* g_order;
order_wdc_adapter_t* g_order_wdc;
orderline_adapter_t* g_orderline;
item_adapter_t* g_item;
stock_adapter_t* g_stock;

// TPCC workload pointer
TPCCWorkload<LeanStoreAdapter>* g_tpcc_workload;

// Function to make a socket non-blocking
int set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1) {
    perror("fcntl F_GETFL");
    return -1;
  }
  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    perror("fcntl F_SETFL O_NONBLOCK");
    return -1;
  }
  return 0;
}

// TUX-related function declarations
int tux_message_handler(int fd, struct tux_user_context* user_ctx, 
  const struct tux_user_message* msg, 
  void* user_state);

typedef ssize_t (*libtux_send_tux_msg_t)(int fd, const struct msghdr *msg);
libtux_send_tux_msg_t g_libtux_send_tux_msg = nullptr;

typedef bool (*libtux_register_input_message_handler_t)(int fd, 
                                                       tux_input_message_handler handler, 
                                                       void* user_state,
                                                       tux_user_state_context_switch_out_handler ctx_out,
                                                       tux_user_state_context_switch_in_handler ctx_in);
libtux_register_input_message_handler_t g_libtux_register_input_message_handler = nullptr;

typedef ssize_t (*libtux_recv_tux_msg_t)(int fd, struct msghdr *msg);
libtux_recv_tux_msg_t g_libtux_recv_tux_msg = nullptr;

bool initialize_tux_functions() {
  void* handle = RTLD_DEFAULT;
  
  g_libtux_send_tux_msg = (libtux_send_tux_msg_t)dlsym(handle, "libtux_send_tux_msg");
  g_libtux_register_input_message_handler = 
      (libtux_register_input_message_handler_t)dlsym(handle, "libtux_register_input_message_handler");
  g_libtux_recv_tux_msg = (libtux_recv_tux_msg_t)dlsym(handle, "libtux_recv_tux_msg");
  
  if (!g_libtux_send_tux_msg || !g_libtux_register_input_message_handler || !g_libtux_recv_tux_msg) {
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
      
      if (!g_libtux_register_input_message_handler) {
          g_libtux_register_input_message_handler = 
              (libtux_register_input_message_handler_t)dlsym(handle, "libtux_register_input_message_handler");
          if (!g_libtux_register_input_message_handler) {
              std::cerr << "Failed to find libtux_register_input_message_handler: " << dlerror() << std::endl;
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
  return true;
}

void leanstore_ctx_out(void* user_state, char * user_tl_state_buffer, size_t buffer_size) {
  jumpmu::saveThreadLocalState(user_tl_state_buffer, buffer_size);
}

void leanstore_ctx_in(void* user_state, char * user_tl_state_buffer, size_t buffer_size) {
  jumpmu::restoreThreadLocalstate(user_tl_state_buffer, buffer_size);
}

bool register_tux_handler(int fd) {
    if (!g_libtux_register_input_message_handler) {
        std::cerr << "TUX register handler function not initialized" << std::endl;
        return false;
    }
    
    bool res = g_libtux_register_input_message_handler(
        fd,
        tux_message_handler,
        nullptr,
        leanstore_ctx_out,
        leanstore_ctx_in
    );

    if (!res) {
        std::cerr << "Failed to register TUX message handler" << std::endl;
        return false;
    }
    std::cout << "Registered TUX message handler for fd " << fd << std::endl;
    return true;
}
// Update the send_error_response function to use the new Message-based approach
void send_error_response(ConnectionContext* ctx, uint32_t request_id, uint32_t error_code, const std::string& message) {
  printf("Sending error response: %s to %d\n", message.c_str(), ctx->fd);
  
  // Create error response message
  Message response(ERROR_RESPONSE, request_id, sizeof(ErrorResponse) + message.size());
  
  // Set error code
  ErrorResponse* err = reinterpret_cast<ErrorResponse*>(response.payload.data());
  err->error_code = error_code;
  
  // Copy error message
  memcpy(response.payload.data() + sizeof(ErrorResponse), message.c_str(), message.size());
  
  // Queue the response
  ctx->queue_response(std::move(response));
}

// Process a NEW_ORDER request
void process_new_order_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size) {
  
  if (payload_size < sizeof(NewOrderRequest)) {
    send_error_response(ctx, request_id, 400, "Invalid NEW_ORDER request");
    return;
  }
  
  const NewOrderRequest* request = reinterpret_cast<const NewOrderRequest*>(payload);
  
  // Extract arrays of lineNumbers, supwares, itemids, qtys
  size_t base_offset = sizeof(NewOrderRequest);
  size_t array_size = request->ol_cnt * sizeof(Integer);
  
  if (payload_size < base_offset + 4 * array_size) {
    send_error_response(ctx, request_id, 400, "Invalid NEW_ORDER request: missing order line data");
    return;
  }
  
  // Extract arrays from the payload
  const Integer* lineNumbers = reinterpret_cast<const Integer*>(reinterpret_cast<const char*>(payload) + base_offset);
  const Integer* supwares = reinterpret_cast<const Integer*>(reinterpret_cast<const char*>(payload) + base_offset + array_size);
  const Integer* itemids = reinterpret_cast<const Integer*>(reinterpret_cast<const char*>(payload) + base_offset + 2 * array_size);
  const Integer* qtys = reinterpret_cast<const Integer*>(reinterpret_cast<const char*>(payload) + base_offset + 3 * array_size);
  
  // Convert to vectors for TPCCWorkload
  std::vector<Integer> lineNumbers_vec(lineNumbers, lineNumbers + request->ol_cnt);
  std::vector<Integer> supwares_vec(supwares, supwares + request->ol_cnt);
  std::vector<Integer> itemids_vec(itemids, itemids + request->ol_cnt);
  std::vector<Integer> qtys_vec(qtys, qtys + request->ol_cnt);
  
  // Execute the request with transaction scope
  try {
    bool success = false;
    
    jumpmuTry() {
      cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, true);
      
      // Call TPCCWorkload's newOrder method
      g_tpcc_workload->newOrder(
        request->w_id,
        request->d_id,
        request->c_id,
        lineNumbers_vec,
        supwares_vec,
        itemids_vec,
        qtys_vec,
        // Use current time as timestamp
        static_cast<Timestamp>(std::time(nullptr))
      );
      
      cr::Worker::my().commitTX();
      success = true;
    } jumpmuCatch() {
      send_error_response(ctx, request_id, 500, "Transaction aborted");
      return;
    }
    
    // Prepare response
    if (success) {
      // Create response message
      Message response(NEW_ORDER_RESPONSE, request_id, sizeof(NewOrderResponse));
      
      // Fill response payload
      NewOrderResponse* resp = reinterpret_cast<NewOrderResponse*>(response.payload.data());
      resp->success = 1;
      resp->total_amount = 0.0; // We could calculate this if needed
      
      // Queue the response
      ctx->queue_response(std::move(response));
    } else {
      send_error_response(ctx, request_id, 500, "Operation failed");
    }
  } catch (const std::exception& e) {
    send_error_response(ctx, request_id, 500, e.what());
  }
}

// Process a PAYMENT_BY_ID request
void process_payment_by_id_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size) {
  
  if (payload_size < sizeof(PaymentByIdRequest)) {
    send_error_response(ctx, request_id, 400, "Invalid PAYMENT_BY_ID request");
    return;
  }
  
  const PaymentByIdRequest* request = reinterpret_cast<const PaymentByIdRequest*>(payload);
  
  // Execute the request with transaction scope
  try {
    bool success = false;
    
    jumpmuTry() {
      cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, false);
      
      // Call TPCCWorkload's paymentById method
      g_tpcc_workload->paymentById(
        request->w_id,
        request->d_id,
        request->c_w_id,
        request->c_d_id,
        request->c_id,
        static_cast<Timestamp>(std::time(nullptr)),
        request->h_amount,
        static_cast<Timestamp>(std::time(nullptr))
      );
      
      cr::Worker::my().commitTX();
      success = true;
    } jumpmuCatch() {
      send_error_response(ctx, request_id, 500, "Transaction aborted");
      return;
    }
    
    // Prepare response
    if (success) {
      // Create response message
      Message response(PAYMENT_BY_ID_RESPONSE, request_id, sizeof(PaymentByIdResponse));
      
      // Fill response payload
      PaymentByIdResponse* resp = reinterpret_cast<PaymentByIdResponse*>(response.payload.data());
      resp->success = 1;
      
      // Queue the response
      ctx->queue_response(std::move(response));
    } else {
      send_error_response(ctx, request_id, 500, "Operation failed");
    }
  } catch (const std::exception& e) {
    send_error_response(ctx, request_id, 500, e.what());
  }
}

// Process a PAYMENT_BY_NAME request
void process_payment_by_name_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size) {
  
  if (payload_size < sizeof(PaymentByNameRequest)) {
    send_error_response(ctx, request_id, 400, "Invalid PAYMENT_BY_NAME request");
    return;
  }
  
  const PaymentByNameRequest* request = reinterpret_cast<const PaymentByNameRequest*>(payload);
  
  // Convert c_last to Varchar<16>
  size_t c_last_len = strnlen(request->c_last, 16);
  Varchar<16> c_last(request->c_last, c_last_len);

  // Execute the request with transaction scope
  try {
    bool success = false;
    
    jumpmuTry() {
      cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, false);
      
      // Call TPCCWorkload's paymentByName method
      g_tpcc_workload->paymentByName(
        request->w_id,
        request->d_id,
        request->c_w_id,
        request->c_d_id,
        c_last,
        static_cast<Timestamp>(std::time(nullptr)),
        request->h_amount,
        static_cast<Timestamp>(std::time(nullptr))
      );
      
      cr::Worker::my().commitTX();
      success = true;
    } jumpmuCatch() {
      send_error_response(ctx, request_id, 500, "Transaction aborted");
      return;
    }
    
    // Prepare response
    if (success) {
      // Create response message
      Message response(PAYMENT_BY_NAME_RESPONSE, request_id, sizeof(PaymentByNameResponse));
      
      // Fill response payload
      PaymentByNameResponse* resp = reinterpret_cast<PaymentByNameResponse*>(response.payload.data());
      resp->success = 1;
      
      // Queue the response
      ctx->queue_response(std::move(response));
    } else {
      send_error_response(ctx, request_id, 500, "Operation failed");
    }
  } catch (const std::exception& e) {
    send_error_response(ctx, request_id, 500, e.what());
  }
}

// Process a DELIVERY request
void process_delivery_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size) {
  
  if (payload_size < sizeof(DeliveryRequest)) {
    send_error_response(ctx, request_id, 400, "Invalid DELIVERY request");
    return;
  }
  
  const DeliveryRequest* request = reinterpret_cast<const DeliveryRequest*>(payload);
  
  // Execute the request with transaction scope
  try {
    bool success = false;
    
    jumpmuTry() {
      cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, false);
      
      // Call TPCCWorkload's delivery method
      g_tpcc_workload->delivery(
        request->w_id,
        request->carrier_id,
        static_cast<Timestamp>(std::time(nullptr))
      );
      
      cr::Worker::my().commitTX();
      success = true;
    } jumpmuCatch() {
      send_error_response(ctx, request_id, 500, "Transaction aborted");
      return;
    }
    
    // Prepare response
    if (success) {
      // Create response message
      Message response(DELIVERY_RESPONSE, request_id, sizeof(DeliveryResponse));
      
      // Fill response payload
      DeliveryResponse* resp = reinterpret_cast<DeliveryResponse*>(response.payload.data());
      resp->success = 1;
      
      // Queue the response
      ctx->queue_response(std::move(response));
    } else {
      send_error_response(ctx, request_id, 500, "Operation failed");
    }
  } catch (const std::exception& e) {
    send_error_response(ctx, request_id, 500, e.what());
  }
}

// Process a STOCK_LEVEL request
void process_stock_level_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size) {
  
  if (payload_size < sizeof(StockLevelRequest)) {
    send_error_response(ctx, request_id, 400, "Invalid STOCK_LEVEL request");
    return;
  }
  
  const StockLevelRequest* request = reinterpret_cast<const StockLevelRequest*>(payload);
  
  // Execute the request with transaction scope
  try {
    bool success = false;
    Integer low_stock_count = 0;
    
    jumpmuTry() {
      cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, false);
      
      // Call TPCCWorkload's stockLevel method
      g_tpcc_workload->stockLevel(
        request->w_id,
        request->d_id,
        request->threshold
      );
      
      cr::Worker::my().commitTX();
      success = true;
    } jumpmuCatch() {
      send_error_response(ctx, request_id, 500, "Transaction aborted");
      return;
    }
    
    // Prepare response
    if (success) {
      // Create response message
      Message response(STOCK_LEVEL_RESPONSE, request_id, sizeof(StockLevelResponse));
      
      // Fill response payload
      StockLevelResponse* resp = reinterpret_cast<StockLevelResponse*>(response.payload.data());
      resp->success = 1;
      resp->low_stock_count = low_stock_count;
      
      // Queue the response
      ctx->queue_response(std::move(response));
    } else {
      send_error_response(ctx, request_id, 500, "Operation failed");
    }
  } catch (const std::exception& e) {
    send_error_response(ctx, request_id, 500, e.what());
  }
}

// Process an ORDER_STATUS_ID request
void process_order_status_id_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size) {
  
  if (payload_size < sizeof(OrderStatusIdRequest)) {
    send_error_response(ctx, request_id, 400, "Invalid ORDER_STATUS_ID request");
    return;
  }
  
  const OrderStatusIdRequest* request = reinterpret_cast<const OrderStatusIdRequest*>(payload);
  
  // Execute the request with transaction scope
  try {
    bool success = false;
    Integer o_id = 0;
    
    jumpmuTry() {
      cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, false);
      
      // Call TPCCWorkload's orderStatusId method
      g_tpcc_workload->orderStatusId(
        request->w_id,
        request->d_id,
        request->c_id
      );
      
      cr::Worker::my().commitTX();
      success = true;
    } jumpmuCatch() {
      send_error_response(ctx, request_id, 500, "Transaction aborted");
      return;
    }
    
    // Prepare response
    if (success) {
      // Create response message
      Message response(ORDER_STATUS_ID_RESPONSE, request_id, sizeof(OrderStatusIdResponse));
      
      // Fill response payload
      OrderStatusIdResponse* resp = reinterpret_cast<OrderStatusIdResponse*>(response.payload.data());
      resp->success = 1;
      resp->o_id = o_id;
      
      // Queue the response
      ctx->queue_response(std::move(response));
    } else {
      send_error_response(ctx, request_id, 500, "Operation failed");
    }
  } catch (const std::exception& e) {
    send_error_response(ctx, request_id, 500, e.what());
  }
}

// Process an ORDER_STATUS_NAME request
void process_order_status_name_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size) {
  
  if (payload_size < sizeof(OrderStatusNameRequest)) {
    send_error_response(ctx, request_id, 400, "Invalid ORDER_STATUS_NAME request");
    return;
  }
  
  const OrderStatusNameRequest* request = reinterpret_cast<const OrderStatusNameRequest*>(payload);
  
  // Convert c_last to Varchar<16>
  size_t c_last_len = strnlen(request->c_last, 16);
  Varchar<16> c_last(request->c_last, c_last_len);
  
  // Execute the request with transaction scope
  try {
    bool success = false;
    Integer o_id = 0;
    
    jumpmuTry() {
      cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, false);
      
      // Call TPCCWorkload's orderStatusName method
      g_tpcc_workload->orderStatusName(
        request->w_id,
        request->d_id,
        c_last
      );
      
      cr::Worker::my().commitTX();
      success = true;
    } jumpmuCatch() {
      send_error_response(ctx, request_id, 500, "Transaction aborted");
      return;
    }
    
    // Prepare response
    if (success) {
      // Create response message
      Message response(ORDER_STATUS_NAME_RESPONSE, request_id, sizeof(OrderStatusNameResponse));
      
      // Fill response payload
      OrderStatusNameResponse* resp = reinterpret_cast<OrderStatusNameResponse*>(response.payload.data());
      resp->success = 1;
      resp->o_id = o_id;
      
      // Queue the response
      ctx->queue_response(std::move(response));
    } else {
      send_error_response(ctx, request_id, 500, "Operation failed");
    }
  } catch (const std::exception& e) {
    send_error_response(ctx, request_id, 500, e.what());
  }
}

// Process a GET request
void process_get_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size) {
  if (payload_size < sizeof(BinaryKey)) {
    send_error_response(ctx, request_id, 400, "Invalid GET request: missing key");
    return;
  }
  
  // Extract key from payload
  BinaryKey key = *reinterpret_cast<const BinaryKey*>(payload);
  
  // Try to get value from database
  bool found = false;
  BinaryPayload found_payload;
  
  jumpmuTry() {
    cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, false);
    
    // Use lookup1 with a lambda to check if the key exists and extract the payload
    typename KVTable::Key k_key;
    k_key.my_key = key;  // Using my_key from the schema
    
    g_table->lookup1(k_key, [&](const KVTable& record) {
      // Copy data from record to our value
      found_payload = record.my_payload;  // Using my_payload from schema
      found = true;
    });
    
    cr::Worker::my().commitTX();
  } jumpmuCatch() {
    found = false;
  }
  
  // Prepare response
  if (found) {
    // Create GET_RESPONSE message
    Message response(GET_RESPONSE, request_id, sizeof(found_payload));
    
    // Copy found payload to response payload
    memcpy(response.payload.data(), &found_payload, sizeof(found_payload));
    
    // Queue the response
    ctx->queue_response(std::move(response));
  } else {
    // Key not found
    send_error_response(ctx, request_id, 404, "Key not found");
  }
}

// Process a PUT request
void process_put_request(ConnectionContext* ctx, uint32_t request_id, const void* payload, size_t payload_size) {
  // A PUT request needs at least a key and some value data
  if (payload_size <= sizeof(BinaryKey)) {
    send_error_response(ctx, request_id, 400, "Invalid PUT request: missing value data");
    return;
  }
  
  // Extract key from the beginning of payload
  BinaryKey key = *reinterpret_cast<const BinaryKey*>(payload);
  
  // Value follows the key in the payload
  size_t value_size = payload_size - sizeof(BinaryKey);
  const void* value_data = reinterpret_cast<const char*>(payload) + sizeof(BinaryKey);
  
  if (FLAGS_debug) {
    std::cout << "Processing PUT request for key=" << key 
              << " value_size=" << value_size << std::endl;
  }
  
  // Check if the value size is valid
  if (value_size > sizeof(BinaryPayload::value)) {
    send_error_response(ctx, request_id, 413, 
                       "Value too large, max size: " + std::to_string(sizeof(BinaryPayload::value)));
    return;
  }
  
  bool success = false;
  
  // START TRANSACTION
  jumpmuTry() {
    cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, false);
    
    // Check if key exists
    bool exists = false;
    typename KVTable::Key k_key;
    k_key.my_key = key;  // Using my_key from the schema
    
    g_table->lookup1(k_key, [&](const KVTable& record) {
      exists = true;
    });
    
    if (exists) {
      UpdateDescriptorGenerator1(tabular_update_descriptor, KVTable, my_payload);
      // Key exists, use update
      g_table->update1(k_key, [&](KVTable& record) {
        // Copy the value data into the record
        memcpy(record.my_payload.value, value_data, value_size);
        // Set the actual size used
      }, tabular_update_descriptor);
    } else {
      // Key doesn't exist, insert new record
      KVTable record;
      // Copy the value data
      memcpy(record.my_payload.value, value_data, value_size);
      
      g_table->insert(k_key, record);
    }
    
    cr::Worker::my().commitTX();
    success = true;
    
  } jumpmuCatch() {
    if (FLAGS_debug) {
      std::cout << "Transaction aborted during PUT" << std::endl;
    }
    success = false;
  }
  
  // Prepare response
  if (success) {
    // Create PUT_RESPONSE message (just a header with no payload)
    Message response(PUT_RESPONSE, request_id, 0);
    
    // Queue the response
    ctx->queue_response(std::move(response));
  } else {
    send_error_response(ctx, request_id, 500, "Failed to store value");
  }
}

// Handle read event for a connection
bool handle_read(ConnectionContext* ctx) {
  std::lock_guard<std::mutex> lock(g_connections.get_mutex(ctx->fd));
  
  // First, try to receive a TUX message if the function is available
  if (g_libtux_recv_tux_msg) {
    // Setup buffer for receiving TUX message
    char tux_buf[8192]; // Buffer for TUX message
    struct iovec iov;
    iov.iov_base = tux_buf;
    iov.iov_len = sizeof(tux_buf);
    
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    
    // Try to receive a TUX message
    ssize_t recv_result = g_libtux_recv_tux_msg(ctx->fd, &msg);
    
    if (recv_result > 0) {
      // We received a TUX message - process it directly
      ctx->received_via_tux = true;
      
      // Check if we have at least a header
      if (recv_result < sizeof(MessageHeader)) {
        ctx->send_error_response(0, 400, "Invalid message format");
        return true;
      }
      
      // Extract header
      MessageHeader* header = reinterpret_cast<MessageHeader*>(tux_buf);
      
      // Validate header
      if (header->type < GET_REQUEST || header->type > ORDER_STATUS_NAME_REQUEST) {
        ctx->send_error_response(header->request_id, 400, "Invalid request type");
        return true;
      }
      
      // Check payload size
      if (header->payload_size > 1024 * 1024) {  // 1MB max payload
        ctx->send_error_response(header->request_id, 400, "Payload too large");
        return true;
      }
      
      // Check if we have the full message
      if (recv_result < sizeof(MessageHeader) + header->payload_size) {
        ctx->send_error_response(header->request_id, 400, "Incomplete message");
        return true;
      }
      
      // Create message and add to request queue
      Message request;
      request.header = *header;
      
      if (header->payload_size > 0) {
        request.payload.resize(header->payload_size);
        memcpy(request.payload.data(), tux_buf + sizeof(MessageHeader), header->payload_size);
      }
      
      ctx->request_queue.push(std::move(request));
      
      // Process all pending requests
      process_all_requests(ctx);
      return true;
    }
  }
  
  // Standard socket read
  // Try to read as much data as possible
  size_t capacity = ctx->read_buffer.capacity();
  size_t current_size = ctx->read_buffer.size();

  // Ensure the buffer has enough space for new data
  if (current_size < ctx->bytes_read + 4096) {  // Always ensure at least 4KB space available
      // Resize the buffer to fit more data while preserving existing content
      size_t new_size = std::max(capacity * 2, ctx->bytes_read + 4096);
      new_size = std::min(new_size, size_t(1024 * 1024)); // 1MB max
      ctx->read_buffer.resize(new_size);
  }

  // Read directly into the buffer at the current position
  ssize_t bytes_read = read(
      ctx->fd,
      ctx->read_buffer.data() + ctx->bytes_read,
      ctx->read_buffer.size() - ctx->bytes_read);  // Use available space in the buffer

  if (bytes_read <= 0) {
      if (bytes_read < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
          return true;  // No more data available right now
      }
      printf("Read with buffer size %d bytes_read %d error or connection closed on fd %d: %s\n", ctx->read_buffer.size() - ctx->bytes_read, bytes_read, ctx->fd, strerror(errno));
      return false;  // Error or connection closed
  }

  //printf("Read %zd bytes from fd %d\n", bytes_read, ctx->fd);

  ctx->bytes_read += bytes_read;

  // Process the received data
  ctx->process_incoming_data();

  // Process all pending requests
  process_all_requests(ctx);

  return true;
}

// Handle write event for a connection
bool handle_write(ConnectionContext* ctx) {
  std::lock_guard<std::mutex> lock(g_connections.get_mutex(ctx->fd));
  
  // Prepare data to write if needed
  ctx->prepare_write_data();
  
  // If there's nothing to write, we're done
  if (ctx->write_buffer.empty()) {
    return true;
  }
  
  // Use TUX if available and this connection received via TUX
  if (ctx->received_via_tux && g_libtux_send_tux_msg) {
    struct iovec iov;
    iov.iov_base = ctx->write_buffer.data() + ctx->bytes_written;
    iov.iov_len = ctx->write_buffer.size() - ctx->bytes_written;
    
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    
    ssize_t bytes_written = g_libtux_send_tux_msg(ctx->fd, &msg);
    
    if (bytes_written <= 0) {
      if (bytes_written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return true;  // Would block, try again later
      }
      return false;  // Error
    }
    
    ctx->bytes_written += bytes_written;
    
    // Prepare more data if we've written everything
    if (ctx->bytes_written >= ctx->write_buffer.size()) {
      ctx->prepare_write_data();
    }
    
    return true;
  }
  
  // Standard socket write
  ssize_t bytes_written = write(
      ctx->fd,
      ctx->write_buffer.data() + ctx->bytes_written,
      ctx->write_buffer.size() - ctx->bytes_written);
  
  if (bytes_written <= 0) {
    if (bytes_written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return true;  // Would block, try again later
    }
    return false;  // Error
  }
  
  ctx->bytes_written += bytes_written;
  
  // Prepare more data if we've written everything
  if (ctx->bytes_written >= ctx->write_buffer.size()) {
    ctx->prepare_write_data();
  }
  
  return true;
}

// Process all pending requests in the queue
void process_all_requests(ConnectionContext* ctx) {
  while (!ctx->request_queue.empty()) {
    // Get the next request
    Message request = std::move(ctx->request_queue.front());
    ctx->request_queue.pop();
    
    // Process the request
    process_single_request(ctx, std::move(request));
  }
}

// Process a single request message
void process_single_request(ConnectionContext* ctx, Message&& request) {
  // Extract payload pointer for convenience
  const void* payload_ptr = request.payload.empty() ? nullptr : request.payload.data();
  
  switch (request.header.type) {
    case GET_REQUEST:
      process_get_request(ctx, request.header.request_id, payload_ptr, request.payload.size());
      break;
      
    case PUT_REQUEST:
      process_put_request(ctx, request.header.request_id, payload_ptr, request.payload.size());
      break;
      
    case NEW_ORDER_REQUEST:
      process_new_order_request(ctx, request.header.request_id, payload_ptr, request.payload.size());
      break;
      
    case PAYMENT_BY_ID_REQUEST:
      process_payment_by_id_request(ctx, request.header.request_id, payload_ptr, request.payload.size());
      break;
      
    case PAYMENT_BY_NAME_REQUEST:
      process_payment_by_name_request(ctx, request.header.request_id, payload_ptr, request.payload.size());
      break;
      
    case DELIVERY_REQUEST:
      process_delivery_request(ctx, request.header.request_id, payload_ptr, request.payload.size());
      break;
      
    case STOCK_LEVEL_REQUEST:
      process_stock_level_request(ctx, request.header.request_id, payload_ptr, request.payload.size());
      break;
      
    case ORDER_STATUS_ID_REQUEST:
      process_order_status_id_request(ctx, request.header.request_id, payload_ptr, request.payload.size());
      break;
      
    case ORDER_STATUS_NAME_REQUEST:
      process_order_status_name_request(ctx, request.header.request_id, payload_ptr, request.payload.size());
      break;
      
    default:
      ctx->send_error_response(request.header.request_id, 400, "Invalid request type");
      break;
  }
}

// TUX message handler
int tux_message_handler(int fd, struct tux_user_context* user_ctx, 
                        const struct tux_user_message* msg, 
                        void* user_state) {
  // Setup a connection context for this request
  if (!g_connections.is_active(fd)) {
    g_connections.add_connection(fd, 0);  // Worker ID doesn't matter for TUX
  }
  
  ConnectionContext* ctx = g_connections.get_connection(fd);
  if (!ctx) {
    return -1;  // Failed to get connection context
  }
  
  ctx->fd = fd;
  ctx->received_via_tux = true;
  
  // Prepare buffer for the incoming message
  size_t buffer_size = 0;
  std::unique_ptr<char[]> buffer_ptr;
  const char* buffer = nullptr;
  
  // If msg->n_packets == 1, point buffer to the first packet
  // otherwise, allocate a buffer for the entire message
  if (msg->n_packets == 1) {
    buffer = static_cast<const char*>(msg->packets[0].iov_base);
    buffer_size = msg->packets[0].iov_len;
  } else {
    for (uint32_t i = 0; i < msg->n_packets; ++i) {
      buffer_size += msg->packets[i].iov_len;
    }
    
    buffer_ptr = std::make_unique<char[]>(buffer_size);
    buffer = buffer_ptr.get();
    
    if (!buffer) {
      ctx->send_error_response(0, 500, "Memory allocation failed");
      return -1;
    }
    
    size_t offset = 0;
    for (uint32_t i = 0; i < msg->n_packets; ++i) {
      memcpy(buffer_ptr.get() + offset, 
             msg->packets[i].iov_base, 
             msg->packets[i].iov_len);
      offset += msg->packets[i].iov_len;
    }
  }
  
  // Process the message similarly to handle_read
  if (buffer_size < sizeof(MessageHeader)) {
    ctx->send_error_response(0, 400, "Invalid message format");
    return 0;
  }
  
  const MessageHeader* header = reinterpret_cast<const MessageHeader*>(buffer);
  
  // Validate header
  if (header->type < GET_REQUEST || header->type > ORDER_STATUS_NAME_REQUEST) {
    ctx->send_error_response(header->request_id, 400, "Invalid request type");
    return 0;
  }
  
  // Check payload size
  if (header->payload_size > 1024 * 1024) {  // 1MB max payload
    ctx->send_error_response(header->request_id, 400, "Payload too large");
    return 0;
  }
  
  // Check if we have the full message
  if (buffer_size < sizeof(MessageHeader) + header->payload_size) {
    ctx->send_error_response(header->request_id, 400, "Incomplete message");
    return 0;
  }
  
  // Create message and add to request queue
  Message request;
  request.header = *header;
  
  if (header->payload_size > 0) {
    request.payload.resize(header->payload_size);
    memcpy(request.payload.data(), buffer + sizeof(MessageHeader), header->payload_size);
  }
  
  ctx->request_queue.push(std::move(request));
  
  // Process all pending requests
  process_all_requests(ctx);
  
  // Try to write responses immediately
  handle_write(ctx);
  
  return 0;
}

cr::CRManager * g_crm = nullptr;
// Worker thread function
void worker_thread(uint32_t worker_id) {
  // Set thread name
  char thread_name[16];
  snprintf(thread_name, sizeof(thread_name), "worker-%u", worker_id);
  pthread_setname_np(pthread_self(), thread_name);
  
  // Create epoll instance
  int epoll_fd = epoll_create1(0);
  if (epoll_fd == -1) {
    perror("epoll_create1");
    return;
  }
  
  g_epoll_fds[worker_id] = epoll_fd;
  printf("Worker %u: epoll fd %d\n", worker_id, epoll_fd);
  // Event processing loop
  struct epoll_event events[64];
  while (g_running) {
    int num_events = epoll_wait(epoll_fd, events, 64, FLAGS_epoll_timeout);
    
    if (num_events == -1) {
      if (errno == EINTR) {
        continue;  // Interrupted by signal
      }
      perror("epoll_wait");
      break;
    }
    
    for (int i = 0; i < num_events; i++) {
      int fd = events[i].data.fd;
      ConnectionContext* ctx = g_connections.get_connection_unsafe(fd);
      
      if (!ctx) {
        std::cerr << "Error: Missing connection context for fd " << fd << std::endl;
        continue;
      }
      
      bool keep_connection = true;
      
      // Handle read events
      if (events[i].events & EPOLLIN) {
        keep_connection = handle_read(ctx) && keep_connection;
      }
      
      handle_write(ctx);
      
      // Handle error events
      if (events[i].events & (EPOLLERR | EPOLLHUP)) {
        std::cerr << "Error on fd " << fd << ": " << strerror(errno) << std::endl;
        keep_connection = false;
      }
      
      if (!keep_connection) {
        // Close the connection on error
        printf("Closing connection %d on worker %u\n", fd, worker_id);
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        g_connections.remove_connection(fd);
      }
    }
  }
  
  close(epoll_fd);
}

// Accept incoming connections and distribute them to worker threads
void accept_connections(int listen_fd) {
  uint32_t next_worker = 0;
  
  while (g_running) {
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    
    int client_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_addr_len);
    if (client_fd == -1) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;  // Interrupted or would block
      }
      perror("accept");
      break;
    }
    
    // Make socket non-blocking
    if (set_nonblocking(client_fd) == -1) {
      close(client_fd);
      continue;
    }
    
    // Try to register TUX handler for this connection
    register_tux_handler(client_fd);
    
    // Add connection to pool
    if (!g_connections.add_connection(client_fd, next_worker)) {
      std::cerr << "Error: Failed to add connection " << client_fd << " to pool" << std::endl;
      close(client_fd);
      continue;
    }

    printf("Accepted connection %d from %s:%d, assigning to worker %d epoll_fd %d\n", client_fd, inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port), next_worker, g_epoll_fds[next_worker]);
    
    // Add to epoll for the selected worker
    struct epoll_event ev;
    ev.events = EPOLLIN;  // Edge-triggered
    ev.data.fd = client_fd;
    
    if (epoll_ctl(g_epoll_fds[next_worker], EPOLL_CTL_ADD, client_fd, &ev) == -1) {
      printf("epoll_ctl failed for fd %d on epoll_fd %d: %s\n", client_fd, g_epoll_fds[next_worker], strerror(errno));
      perror("epoll_ctl");
      g_connections.remove_connection(client_fd);
      close(client_fd);
      continue;
    }
    
    // Round-robin worker selection
    next_worker = (next_worker + 1) % FLAGS_worker_threads;
  }
}

// Signal handler
void signal_handler(int sig) {
  g_running = false;
  exit(-1);
}

// Initialize and run the server
int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  
  // Initialize signal handlers
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  
  // Try to load TUX functions
  // if (!initialize_tux_functions()) {
  //   std::cout << "TUX functions not available, using standard sockets only." << std::endl;
  // }

  std::cout << "Starting TPC-C server with " << FLAGS_worker_threads << " worker threads." << std::endl;

  // Initialize LeanStore
  g_db = new LeanStore();

  auto& crm = g_db->getCRManager();
  g_crm = &crm;

  // Create table
  LeanStoreAdapter<KVTable>* table_ptr = new LeanStoreAdapter<KVTable>();
  
  crm.scheduleJobSync(0, [&]() {
    
    *table_ptr = LeanStoreAdapter<KVTable>(*g_db, "KVStore"); 
    // Initialize TPCC tables
    g_warehouse = new warehouse_adapter_t(*g_db, "warehouse");
    g_district = new district_adapter_t(*g_db, "district");
    g_customer = new customer_adapter_t(*g_db, "customer");
    g_customer_wdl = new customer_wdl_adapter_t(*g_db, "customer_wdl");
    g_history = new history_adapter_t(*g_db, "history");
    g_neworder = new neworder_adapter_t(*g_db, "neworder");
    g_order = new order_adapter_t(*g_db, "order");
    g_order_wdc = new order_wdc_adapter_t(*g_db, "order_wdc");
    g_orderline = new orderline_adapter_t(*g_db, "orderline");
    g_item = new item_adapter_t(*g_db, "item");
    g_stock = new stock_adapter_t(*g_db, "stock");
  });

  g_table = table_ptr;
  
  // Initialize TPCC workload
  g_tpcc_workload = new TPCCWorkload<LeanStoreAdapter>(
      *g_warehouse, *g_district, *g_customer, *g_customer_wdl,
      *g_history, *g_neworder, *g_order, *g_order_wdc,
      *g_orderline, *g_item, *g_stock,
      FLAGS_order_wdc_index,
      FLAGS_warehouse_count,
      FLAGS_tpcc_remove,
      false,  // manually_handle_isolation_anomalies
      FLAGS_warehouse_affinity
  );

  cout << "Loading TPC-C" << endl;
  crm.scheduleJobSync(0, [&]() {
      cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      g_tpcc_workload->loadItem();
      g_tpcc_workload->loadWarehouse();
      cr::Worker::my().commitTX();
  });
  std::atomic<u32> g_w_id = 1;
  for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      crm.scheduleJobAsync(t_i, [&]() {
        while (true) {
            u32 w_id = g_w_id++;
            if (w_id > FLAGS_warehouse_count) {
              return;
            }
            cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
            g_tpcc_workload->loadStock(w_id);
            g_tpcc_workload->loadDistrinct(w_id);
            for (Integer d_id = 1; d_id <= 10; d_id++) {
              g_tpcc_workload->loadCustomer(w_id, d_id);
              g_tpcc_workload->loadOrders(w_id, d_id);
            }
            cr::Worker::my().commitTX();
        }
      });
  }
  crm.joinAll();
  // -------------------------------------------------------------------------------------
  

  // Create listening socket
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd == -1) {
    perror("socket");
    return 1;
  }
  
  // Set socket options
  int opt = 1;
  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
    perror("setsockopt");
    close(listen_fd);
    return 1;
  }
  
  // Bind to port
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(FLAGS_port);
  
  if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
    perror("bind");
    close(listen_fd);
    return 1;
  }
  
  // Listen for connections
  if (listen(listen_fd, SOMAXCONN) == -1) {
    perror("listen");
    close(listen_fd);
    return 1;
  }

  std::cout << "Server listening on port " << FLAGS_port << std::endl;
  
  // Initialize epoll fd vector
  g_epoll_fds.resize(FLAGS_worker_threads);
  
  // Start worker threads
  g_worker_threads.resize(FLAGS_worker_threads);
  for (uint32_t i = 0; i < FLAGS_worker_threads; i++) {
    crm.scheduleJobAsync(i, [i]() {
      worker_thread(i);
    });
  }
  
  // Accept connections in the main thread
  accept_connections(listen_fd);
  
  // Cleanup
  g_running = false;
  // for (auto& thread : g_worker_threads) {
  //   thread.join();
  // }
  
  crm.joinAll();
  close(listen_fd);
  
  delete g_tpcc_workload;
  delete g_warehouse;
  delete g_district;
  delete g_customer;
  delete g_customer_wdl;
  delete g_history;
  delete g_neworder;
  delete g_order;
  delete g_order_wdc;
  delete g_orderline;
  delete g_item;
  delete g_stock;
  
  return 0;
}