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
#include <signal.h>
// -------------------------------------------------------------------------------------
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <cstring>
#include <mutex>
// -------------------------------------------------------------------------------------
DEFINE_uint32(port, 12345, "Server port");
DEFINE_uint32(worker_count, 4, "Number of worker threads");
DEFINE_uint32(epoll_timeout, 100, "Epoll timeout in ms");
DEFINE_uint32(max_connections, 1024, "Maximum number of connections");
DEFINE_bool(debug, false, "Enable debug output");
// -------------------------------------------------------------------------------------
using namespace leanstore;

// Define our key type
using BinaryKey = u64;
using BinaryPayload = BytesPayload<1024>; // Support up to 1KB values

// Now define our table type
using KVTable = Relation<BinaryKey, BinaryPayload>;

// Message types
enum MessageType {
  GET_REQUEST = 1,
  GET_RESPONSE = 2,
  PUT_REQUEST = 3,
  PUT_RESPONSE = 4,
  ERROR_RESPONSE = 5
};

// Message header (12 bytes) - added request_id
struct MessageHeader {
  uint8_t type;
  uint8_t reserved[3];
  uint32_t request_id;  // Added to track request/response pairs
  uint32_t payload_size;
} __attribute__((packed));

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

// Error response format: header + error code + message
struct ErrorResponse {
  uint32_t error_code;
  // Error message follows as variable-length payload
} __attribute__((packed));

// Connection state
enum ConnectionState {
  READING_HEADER,
  READING_PAYLOAD,
  PROCESSING,
  WRITING_RESPONSE
};

// Connection context
struct ConnectionContext {
  int fd;
  ConnectionState state;
  uint32_t worker_id;
  
  // Read buffer
  char header_buf[sizeof(MessageHeader)];
  uint32_t header_bytes_read;
  MessageHeader header;
  
  std::vector<char> payload_buf;
  uint32_t payload_bytes_read;
  
  // Write buffer
  std::vector<char> response_buf;
  uint32_t response_bytes_written;
  
  ConnectionContext(int fd, uint32_t worker_id) 
    : fd(fd), state(READING_HEADER), worker_id(worker_id),
      header_bytes_read(0), payload_bytes_read(0), response_bytes_written(0) {}

  ConnectionContext() : fd(-1), state(READING_HEADER), worker_id(0),
      header_bytes_read(0), payload_bytes_read(0), response_bytes_written(0) {}
};

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
    memset(active, 0, sizeof(active));
  }

  bool add_connection(int fd, uint32_t worker_id) {
    if (fd >= 1024) { // Match with max size
      return false;
    }
    
    std::lock_guard<std::mutex> lock(mutexes[fd]);
    if (active[fd]) {
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
LeanStoreAdapter<KVTable>* g_table;

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

// Send an error response - updated to preserve request ID
void send_error_response(ConnectionContext* ctx, uint32_t error_code, const std::string& message) {
  size_t total_size = sizeof(MessageHeader) + sizeof(ErrorResponse) + message.size();
  ctx->response_buf.resize(total_size);
  
  MessageHeader* header = reinterpret_cast<MessageHeader*>(ctx->response_buf.data());
  header->type = ERROR_RESPONSE;
  header->request_id = ctx->header.request_id;  // Preserve the request ID
  header->payload_size = sizeof(ErrorResponse) + message.size();
  
  ErrorResponse* response = reinterpret_cast<ErrorResponse*>(ctx->response_buf.data() + sizeof(MessageHeader));
  response->error_code = error_code;
  
  memcpy(ctx->response_buf.data() + sizeof(MessageHeader) + sizeof(ErrorResponse), 
         message.c_str(), message.size());
         
  ctx->response_bytes_written = 0;
  ctx->state = WRITING_RESPONSE;
}

// Handle read event for a connection
bool handle_read(ConnectionContext* ctx) {
  std::lock_guard<std::mutex> lock(g_connections.get_mutex(ctx->fd));
  
  if (ctx->state == READING_HEADER) {
    // Read the header
    ssize_t bytes_read = read(ctx->fd, 
                             ctx->header_buf + ctx->header_bytes_read, 
                             sizeof(MessageHeader) - ctx->header_bytes_read);
    
    if (bytes_read <= 0) {
      if (bytes_read == 0 || errno != EAGAIN) {
        // Connection closed or error
        return false;
      }
      return true;
    }
    
    ctx->header_bytes_read += bytes_read;
    
    if (ctx->header_bytes_read == sizeof(MessageHeader)) {
      // Header complete, prepare for payload
      memcpy(&ctx->header, ctx->header_buf, sizeof(MessageHeader));
      
      // Validate header
      if (ctx->header.type != GET_REQUEST && ctx->header.type != PUT_REQUEST) {
        send_error_response(ctx, 400, "Invalid request type");
        return true;
      }
      
      // Check payload size
      if (ctx->header.payload_size > 1024 * 1024) {  // 1MB max payload
        send_error_response(ctx, 400, "Payload too large");
        return true;
      }
      
      // Prepare payload buffer
      ctx->payload_buf.resize(ctx->header.payload_size);
      ctx->payload_bytes_read = 0;
      ctx->state = READING_PAYLOAD;
    }
  }
  
  if (ctx->state == READING_PAYLOAD) {
    // Read the payload
    ssize_t bytes_read = read(ctx->fd, 
                             ctx->payload_buf.data() + ctx->payload_bytes_read, 
                             ctx->header.payload_size - ctx->payload_bytes_read);
    
    if (bytes_read <= 0) {
      if (bytes_read == 0 || errno != EAGAIN) {
        // Connection closed or error
        return false;
      }
      return true;
    }
    
    ctx->payload_bytes_read += bytes_read;
    
    if (ctx->payload_bytes_read == ctx->header.payload_size) {
      // Payload complete, start processing
      ctx->state = PROCESSING;
    }
  }
  
  return true;
}

// Handle write event for a connection
bool handle_write(ConnectionContext* ctx) {
  std::lock_guard<std::mutex> lock(g_connections.get_mutex(ctx->fd));
  
  if (ctx->state == WRITING_RESPONSE) {
    ssize_t bytes_written = write(ctx->fd, 
                                 ctx->response_buf.data() + ctx->response_bytes_written,
                                 ctx->response_buf.size() - ctx->response_bytes_written);
                                 
    if (bytes_written <= 0) {
      if (bytes_written == 0 || errno != EAGAIN) {
        // Connection closed or error
        return false;
      }
      return true;
    }
    
    ctx->response_bytes_written += bytes_written;
    
    if (ctx->response_bytes_written == ctx->response_buf.size()) {
      // Response complete, prepare for next message
      ctx->header_bytes_read = 0;
      ctx->state = READING_HEADER;
    }
  }
  
  return true;
}

// Process a GET request - updated to preserve request ID
void process_get_request(ConnectionContext* ctx) {
  std::lock_guard<std::mutex> lock(g_connections.get_mutex(ctx->fd));
  
  if (ctx->header.payload_size < sizeof(GetRequest)) {
    send_error_response(ctx, 400, "Invalid GET request");
    return;
  }
  
  GetRequest* request = reinterpret_cast<GetRequest*>(ctx->payload_buf.data());
  BinaryKey key = request->key;
  
  // Execute the request with optimized transaction scope
  try {
    bool found = false;
    BinaryPayload result;
    
    // START TRANSACTION - Only wrapping the database operation
    jumpmuTry() {
      cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, true);
      
      g_table->lookup1({key}, [&](const KVTable& record) {
        result = record.my_payload;
        found = true;
      });
      
      cr::Worker::my().commitTX();
    } jumpmuCatch() {
      send_error_response(ctx, 500, "Transaction aborted");
      return;
    }
    
    // Prepare response outside transaction
    if (found) {
      // Prepare response - use the full size since we don't track actual size
      size_t total_size = sizeof(MessageHeader) + sizeof(result.value);
      ctx->response_buf.resize(total_size);
      
      MessageHeader* resp_header = reinterpret_cast<MessageHeader*>(ctx->response_buf.data());
      resp_header->type = GET_RESPONSE;
      resp_header->request_id = ctx->header.request_id;  // Preserve the request ID
      resp_header->payload_size = sizeof(result.value);
      
      // Copy value
      memcpy(ctx->response_buf.data() + sizeof(MessageHeader), result.value, sizeof(result.value));
    } else {
      send_error_response(ctx, 404, "Key not found");
      return;
    }
  } catch (const std::exception& e) {
    send_error_response(ctx, 500, e.what());
    return;
  }
  
  ctx->response_bytes_written = 0;
  ctx->state = WRITING_RESPONSE;
}

// Process a PUT request - updated to preserve request ID
void process_put_request(ConnectionContext* ctx) {
  std::lock_guard<std::mutex> lock(g_connections.get_mutex(ctx->fd));
  
  if (ctx->header.payload_size < sizeof(PutRequest)) {
    send_error_response(ctx, 400, "Invalid PUT request");
    return;
  }
  
  PutRequest* request = reinterpret_cast<PutRequest*>(ctx->payload_buf.data());
  BinaryKey key = request->key;
  
  // Value follows the key
  char* value_data = ctx->payload_buf.data() + sizeof(PutRequest);
  uint32_t value_size = ctx->header.payload_size - sizeof(PutRequest);
  
  if (value_size > sizeof(BinaryPayload::value)) {
    send_error_response(ctx, 400, "Value too large");
    return;
  }
  
  // Execute the request with optimized transaction scope
  try {
    bool success = false;
    
    // START TRANSACTION - Only wrapping the database operation
    jumpmuTry() {
      cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, false);
      
      bool exists = false;
      g_table->lookup1({key}, [&](const KVTable&) {
        exists = true;
      });
      
      if (exists) {
        UpdateDescriptorGenerator1(tabular_update_descriptor, KVTable, my_payload);
        
        g_table->update1({key}, [&](KVTable& record) {
          std::memcpy(record.my_payload.value, value_data, value_size);
        }, tabular_update_descriptor);
      } else {
        BinaryPayload payload;
        std::memcpy(payload.value, value_data, value_size);
        
        g_table->insert({key}, {payload});
      }
      
      cr::Worker::my().commitTX();
      success = true;
    } jumpmuCatch() {
      send_error_response(ctx, 500, "Transaction aborted");
      return;
    }
    
    // Prepare response outside transaction
    if (success) {
      // Prepare success response
      size_t total_size = sizeof(MessageHeader) + sizeof(PutResponse);
      ctx->response_buf.resize(total_size);
      
      MessageHeader* resp_header = reinterpret_cast<MessageHeader*>(ctx->response_buf.data());
      resp_header->type = PUT_RESPONSE;
      resp_header->request_id = ctx->header.request_id;  // Preserve the request ID
      resp_header->payload_size = sizeof(PutResponse);
      
      PutResponse* response = reinterpret_cast<PutResponse*>(ctx->response_buf.data() + sizeof(MessageHeader));
      response->success = 1;
    } else {
      send_error_response(ctx, 500, "Operation failed");
      return;
    }
  } catch (const std::exception& e) {
    send_error_response(ctx, 500, e.what());
    return;
  }
  
  ctx->response_bytes_written = 0;
  ctx->state = WRITING_RESPONSE;
}

// Process a message based on type
void process_message(ConnectionContext* ctx) {
  switch (ctx->header.type) {
    case GET_REQUEST:
      process_get_request(ctx);
      break;
    case PUT_REQUEST:
      process_put_request(ctx);
      break;
    default:
      send_error_response(ctx, 400, "Unknown request type");
      break;
  }
}

// Worker thread function
void worker_thread_func(uint32_t worker_id) {
  // Run the worker thread as a LeanStore job
  g_db->getCRManager().scheduleJobSync(worker_id, [worker_id]() {
    int epoll_fd = g_epoll_fds[worker_id];
    struct epoll_event events[64]; // Process up to 64 events at once
    
    while (g_running) {
      // Process events using epoll
      int num_events = epoll_wait(epoll_fd, events, 64, FLAGS_epoll_timeout);
      
      // For each event, process the connection
      for (int i = 0; i < num_events; i++) {
        int fd = events[i].data.fd;
        uint32_t event_mask = events[i].events;
        
        // Get connection (no lock needed for this check)
        ConnectionContext* ctx = g_connections.get_connection_unsafe(fd);
        if (!ctx) {
          continue;
        }
        
        bool keep_connection = true;
        if (event_mask & EPOLLERR || event_mask & EPOLLHUP) {
          // Error condition
          keep_connection = false;
        }

        // Begin transaction for any data operation
        jumpmuTry() {
          //cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION);
          
          if (keep_connection && event_mask & EPOLLIN) {
            keep_connection = handle_read(ctx);
          }
          
          if (keep_connection && ctx->state == PROCESSING) {
            process_message(ctx);
          }
          
          if (keep_connection && (event_mask & EPOLLOUT) && ctx->state == WRITING_RESPONSE) {
            keep_connection = handle_write(ctx);
          }
          
          //cr::Worker::my().commitTX();
        } jumpmuCatch() {
          // Transaction failed, but we can continue with the connection
          if (ctx->state == PROCESSING) {
            send_error_response(ctx, 500, "Transaction aborted");
          }
        }
        
        // Update epoll events based on current state
        if (keep_connection) {
          // struct epoll_event ev;
          // ev.data.fd = fd;
          // ev.events = EPOLLIN;  // Always monitor read events
          
          // {
          //   std::lock_guard<std::mutex> lock(g_connections.get_mutex(fd));
          //   if (ctx->state == WRITING_RESPONSE) {
          //     ev.events |= EPOLLOUT;  // Add write events if we have data to send
          //   }
          // }
          
          // epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
        } else {
          // Remove from epoll and close connection
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
          g_connections.remove_connection(fd);
          close(fd);
        }
      }
    }
  });
}

// Acceptor thread function
void acceptor_thread_func(int server_fd) {
  // Create epoll instance for the acceptor
  int acceptor_epoll = epoll_create1(0);
  if (acceptor_epoll < 0) {
    perror("epoll_create1 acceptor");
    return;
  }
  
  // Add server socket to epoll
  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = server_fd;
  if (epoll_ctl(acceptor_epoll, EPOLL_CTL_ADD, server_fd, &ev) < 0) {
    perror("epoll_ctl server_fd");
    close(acceptor_epoll);
    return;
  }
  
  // Accept loop
  struct epoll_event events[1];  // Just one for the server socket
  struct sockaddr_in client_addr;
  socklen_t client_len = sizeof(client_addr);
  
  while (g_running) {
    int num_events = epoll_wait(acceptor_epoll, events, 1, FLAGS_epoll_timeout);
    
    for (int i = 0; i < num_events; i++) {
      if (events[i].data.fd == server_fd) {
        // Accept new connection
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
          if (errno != EAGAIN && errno != EWOULDBLOCK) {
            perror("accept");
          }
          continue;
        }
        
        // Check if fd is within our limits
        if (client_fd >= 1024) {
          close(client_fd);
          std::cerr << "Connection rejected: fd " << client_fd << " exceeds limit" << std::endl;
          continue;
        }
        
        // Set non-blocking mode
        if (set_nonblocking(client_fd) < 0) {
          close(client_fd);
          continue;
        }
        
        // Distribute connections round-robin to worker threads
        static uint32_t next_worker = 0;
        uint32_t worker_id = next_worker % FLAGS_worker_count;
        next_worker++;
        
        // Add connection to our pool
        if (!g_connections.add_connection(client_fd, worker_id)) {
          close(client_fd);
          continue;
        }
        
        // Add to epoll of the selected worker
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET | EPOLLOUT;  // Edge-triggered
        ev.data.fd = client_fd;
        
        if (epoll_ctl(g_epoll_fds[worker_id], EPOLL_CTL_ADD, client_fd, &ev) < 0) {
          perror("epoll_ctl client_fd");
          g_connections.remove_connection(client_fd);
          close(client_fd);
          continue;
        }
        
        if (FLAGS_debug) {
          char client_ip[INET_ADDRSTRLEN];
          inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
          std::cout << "New connection from " << client_ip << ":" << ntohs(client_addr.sin_port) 
                    << " (fd=" << client_fd << ", worker=" << worker_id << ")" << std::endl;
        }
      }
    }
  }
  
  close(acceptor_epoll);
}

// Signal handler
void signal_handler(int signal) {
  std::cout << "Received signal " << signal << ", shutting down..." << std::endl;
  g_running = false;
}

// Main function
int main(int argc, char** argv) {
  gflags::SetUsageMessage("LeanStore RPC Server");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  
  // Register signal handlers
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  
  // Initialize LeanStore
  LeanStore db;
  g_db = &db;
  auto& crm = db.getCRManager();
  
  // Create table
  LeanStoreAdapter<KVTable>* table_ptr = new LeanStoreAdapter<KVTable>();
  crm.scheduleJobSync(0, [&]() { 
    *table_ptr = LeanStoreAdapter<KVTable>(db, "KVStore"); 
  });
  g_table = table_ptr;
  
  // Create server socket
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    perror("socket");
    return 1;
  }
  
  // Set socket options
  int opt = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
    perror("setsockopt");
    close(server_fd);
    return 1;
  }
  
  // Set non-blocking mode
  if (set_nonblocking(server_fd) < 0) {
    close(server_fd);
    return 1;
  }
  
  // Bind to port
  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(FLAGS_port);
  
  if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    perror("bind");
    close(server_fd);
    return 1;
  }
  
  // Listen for connections
  if (listen(server_fd, SOMAXCONN) < 0) {
    perror("listen");
    close(server_fd);
    return 1;
  }
  
  std::cout << "Server listening on port " << FLAGS_port << std::endl;
  
  // Create epoll instances for worker threads
  for (uint32_t i = 0; i < FLAGS_worker_count; i++) {
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
      perror("epoll_create1 worker");
      close(server_fd);
      return 1;
    }
    g_epoll_fds.push_back(epoll_fd);
  }
  
  // Start worker threads
  for (uint32_t i = 0; i < FLAGS_worker_count; i++) {
    g_worker_threads.emplace_back(worker_thread_func, i);
  }
  
  // Start acceptor thread
  std::thread acceptor_thread(acceptor_thread_func, server_fd);
  
  // Start profiling thread
  db.startProfilingThread();
  
  std::cout << "Server started with " << FLAGS_worker_count << " worker threads" << std::endl;
  std::cout << "Press Ctrl+C to exit" << std::endl;
  
  // Wait for the acceptor thread
  acceptor_thread.join();
  
  // Cleanup
  std::cout << "Shutting down worker threads..." << std::endl;
  for (auto& thread : g_worker_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  
  // Close all connections
  for (uint32_t fd = 0; fd < 1024; fd++) {
    if (g_connections.is_active(fd)) {
      close(fd);
      g_connections.remove_connection(fd);
    }
  }
  
  // Close all epoll instances
  for (int epoll_fd : g_epoll_fds) {
    close(epoll_fd);
  }
  
  // Close server socket
  close(server_fd);
  
  // Clean up allocated resources
  delete table_ptr;
  
  std::cout << "Server shutdown complete" << std::endl;
  return 0;
}