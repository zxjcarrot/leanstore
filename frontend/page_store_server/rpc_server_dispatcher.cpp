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
#include <sys/eventfd.h>
// -------------------------------------------------------------------------------------
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <cstring>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <functional>
#include <memory>

// -------------------------------------------------------------------------------------
// Command line flags
DEFINE_uint32(port, 12345, "Server port");
DEFINE_uint32(dispatcher_count, 2, "Number of dispatcher threads");
DEFINE_uint32(worker_count, 4, "Number of worker threads");
DEFINE_uint32(epoll_timeout, 1, "Epoll timeout in ms");
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

// Message header (12 bytes)
struct MessageHeader {
  uint8_t type;
  uint8_t reserved[3];
  uint32_t request_id;
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




// -------------------------------------------------------------------------------------
// Connection state
enum ConnectionState {
    READING_HEADER,
    READING_PAYLOAD,
    WRITING_RESPONSE
  };
  
  // Connection context
  struct ConnectionContext {
    int fd;
    ConnectionState state;
    uint32_t dispatcher_id;
    
    // Read buffer
    char header_buf[sizeof(MessageHeader)];
    uint32_t header_bytes_read;
    MessageHeader header;
    
    std::vector<char> payload_buf;
    uint32_t payload_bytes_read;
    
    // Write buffer
    std::vector<char> response_buf;
    uint32_t response_bytes_written;
    
    ConnectionContext(int fd, uint32_t dispatcher_id) 
      : fd(fd), state(READING_HEADER), dispatcher_id(dispatcher_id),
        header_bytes_read(0), payload_bytes_read(0), response_bytes_written(0) {}
  
    ConnectionContext() : fd(-1), state(READING_HEADER), dispatcher_id(0),
        header_bytes_read(0), payload_bytes_read(0), response_bytes_written(0) {}
  };
  
// -------------------------------------------------------------------------------------
// Global running flag
std::atomic<bool> g_running(true);
LeanStore* g_db = nullptr;
LeanStoreAdapter<KVTable>* g_table = nullptr;



// -------------------------------------------------------------------------------------
// Forward declarations
class Dispatcher;  // Forward declaration

// Define Task and Response structures before any class that uses them
struct Task {
  int fd;
  uint32_t dispatcher_id;
  MessageHeader header;
  std::vector<char> payload;
};

struct Response {
  int fd;
  uint32_t dispatcher_id;
  std::vector<char> data;
};

class WorkerThreadPool;
// Finally, the full Dispatcher implementation
class Dispatcher {
private:
    uint32_t id;
    int epoll_fd;
    WorkerThreadPool& worker_pool;
    std::thread thread;
    std::atomic<bool> running;
    
    std::queue<Response> response_queue;
    std::mutex response_mutex;
    std::condition_variable response_cv;
    
    // Maps file descriptors to connection contexts
    std::unordered_map<int, ConnectionContext> connections;
    std::mutex connections_mutex;
   
    void dispatcher_thread_func();

    bool handle_read(int fd, ConnectionContext& ctx);
    bool handle_write(int fd, ConnectionContext& ctx);

    // Add eventfd for notifications
    int notification_fd;
    
    // Update epoll registration based on current state
    void update_epoll_registration(int fd, const ConnectionContext& ctx) {
        struct epoll_event ev;
        ev.data.fd = fd;
        
        // Always monitor for read events and errors
        ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
        
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
    }
    
    // Process pending responses - simplified version that writes directly to connections
    void process_responses() {
        std::vector<Response> responses_to_process;
        
        // Get all queued responses
        {
            std::lock_guard<std::mutex> lock(response_mutex);
            while (!response_queue.empty()) {
                responses_to_process.push_back(std::move(response_queue.front()));
                response_queue.pop();
            }
        }
        
        // Process each response
        for (const auto& response : responses_to_process) {
            int fd = response.fd;
            
            // Check if the connection still exists
            std::lock_guard<std::mutex> lock(connections_mutex);
            auto it = connections.find(fd);
            if (it == connections.end()) {
                continue; // Connection no longer exists
            }
            
            ConnectionContext& ctx = it->second;
            
            // Try to write directly to the connection
            ssize_t bytes_written = write(fd, response.data.data(), response.data.size());
            
            if (bytes_written < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // Would block - store in connection context for later handling
                    ctx.response_buf = response.data;
                    ctx.response_bytes_written = 0;
                    ctx.state = WRITING_RESPONSE;
                    
                    // Update epoll to monitor for write events
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP;
                    ev.data.fd = fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
                } else {
                    // Error occurred
                    if (FLAGS_debug) perror("write response");
                    remove_connection(fd);
                }
            } else if (static_cast<size_t>(bytes_written) < response.data.size()) {
                // Partial write - store remaining data for later
                ctx.response_buf = response.data;
                ctx.response_bytes_written = bytes_written;
                ctx.state = WRITING_RESPONSE;
                
                // Update epoll to monitor for write events
                struct epoll_event ev;
                ev.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP;
                ev.data.fd = fd;
                epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
            }
            // Else: full write succeeded, nothing more to do
        }
    }

public:
    Dispatcher(uint32_t id, WorkerThreadPool& worker_pool)
    : id(id), worker_pool(worker_pool), running(true) {
        epoll_fd = epoll_create1(0);
        if (epoll_fd < 0) {
            perror("epoll_create1 dispatcher");
            throw std::runtime_error("Failed to create epoll instance");
        }
        
        // Create eventfd for notifications
        notification_fd = eventfd(0, EFD_NONBLOCK);
        if (notification_fd < 0) {
            perror("eventfd");
            close(epoll_fd);
            throw std::runtime_error("Failed to create eventfd");
        }
        
        // Add notification_fd to epoll
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = notification_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, notification_fd, &ev) < 0) {
            perror("epoll_ctl notification_fd");
            close(notification_fd);
            close(epoll_fd);
            throw std::runtime_error("Failed to add notification_fd to epoll");
        }
        
        thread = std::thread(&Dispatcher::dispatcher_thread_func, this);
    }
    
    ~Dispatcher() {
        running = false;
        // Notify the thread to exit
        uint64_t value = 1;
        write(notification_fd, &value, sizeof(value));
        
        if (thread.joinable()) {
            thread.join();
        }
        close(notification_fd);
        close(epoll_fd);
    }
    
    // Add a new connection to this dispatcher
    bool add_connection(int fd) {
        std::lock_guard<std::mutex> lock(connections_mutex);
        
        ConnectionContext ctx(fd, id);
        connections[fd] = std::move(ctx);
        
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
        ev.data.fd = fd;
        
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            perror("epoll_ctl add connection");
            connections.erase(fd);
            return false;
        }
        
        return true;
    }
    
    // Remove a connection from this dispatcher
    void remove_connection(int fd) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
        
        std::lock_guard<std::mutex> lock(connections_mutex);
        connections.erase(fd);
        
        // Close the file descriptor
        close(fd);
    }
    
    // Modified enqueue_response to use eventfd instead of condition variable
    void enqueue_response(Response response) {
        {
            std::lock_guard<std::mutex> lock(response_mutex);
            response_queue.push(std::move(response));
        }
        
        // Notify the dispatcher using eventfd
        uint64_t value = 1;
        ssize_t ret = write(notification_fd, &value, sizeof(value));
        if (ret != sizeof(value)) {
            if (FLAGS_debug) {
                perror("write to eventfd failed");
            }
        }
    }
    
    uint32_t get_id() const { return id; }
};

// Redesigned WorkerThreadPool with per-worker queues
class WorkerThreadPool {
private:
    // Task queue and synchronization per worker
    struct WorkerState {
        std::queue<Task> task_queue;
        std::mutex queue_mutex;
        std::condition_variable queue_cv;
        uint32_t worker_id;
    };
    
    std::vector<std::thread> threads;
    std::vector<std::unique_ptr<WorkerState>> worker_states;
    std::atomic<bool> running;
    std::vector<Dispatcher*>& dispatchers;
    std::atomic<uint32_t> next_worker{0}; // For round-robin task assignment
    
    // Worker thread function - now uses its own queue and CV
    void worker_thread_func(uint32_t worker_id) {
        // Get the worker's state
        WorkerState* state = worker_states[worker_id].get();
        
        // Run the worker thread as a LeanStore job
        g_db->getCRManager().scheduleJobSync(worker_id, [this, state]() {
            while (running) {
                Task task;
                bool have_task = false;
                
                // Use condition variable to wait for tasks
                {
                    std::unique_lock<std::mutex> lock(state->queue_mutex);
                    
                    // Wait until queue has tasks or server is shutting down
                    state->queue_cv.wait(lock, [&]() {
                        return !state->task_queue.empty() || !running;
                    });
                    
                    // Check if we're shutting down
                    if (!running && state->task_queue.empty()) {
                        break;
                    }
                    
                    // Get task from queue
                    if (!state->task_queue.empty()) {
                        task = std::move(state->task_queue.front());
                        state->task_queue.pop();
                        have_task = true;
                    }
                }
                
                if (have_task) {
                    if (FLAGS_debug) {
                        printf("Worker %u processing task for dispatcher %u\n", 
                               state->worker_id, task.dispatcher_id);
                    }
                    
                    // Process the task and generate a response
                    Response response = process_task(task);
                    
                    // Send the response back to the appropriate dispatcher
                    if (task.dispatcher_id < dispatchers.size()) {
                        dispatchers[task.dispatcher_id]->enqueue_response(std::move(response));
                    }
                }
            }
        });
    }
    
    // Process a task and generate an appropriate response
    Response process_task(const Task& task);

public:
    WorkerThreadPool(uint32_t num_workers, std::vector<Dispatcher*>& dispatchers)
    : running(true), dispatchers(dispatchers) {
        // Create worker states
        for (uint32_t i = 0; i < num_workers; i++) {
            auto state = std::make_unique<WorkerState>();
            state->worker_id = i;
            worker_states.push_back(std::move(state));
        }
        
        // Create worker threads
        for (uint32_t i = 0; i < num_workers; i++) {
            threads.emplace_back(&WorkerThreadPool::worker_thread_func, this, i);
        }
    }
    
    ~WorkerThreadPool() {
        running = false;
        
        // Notify all worker threads to check shutdown condition
        for (auto& state : worker_states) {
            std::lock_guard<std::mutex> lock(state->queue_mutex);
            state->queue_cv.notify_one();
        }
        
        // Join threads
        for (auto& thread : threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }
    
    // Enqueue task in round-robin fashion to worker queues
    void enqueue_task(Task task) {
        // Select worker in round-robin fashion
        uint32_t worker_id = next_worker.fetch_add(1, std::memory_order_relaxed) % worker_states.size();
        
        // Add task to the selected worker's queue
        {
            std::lock_guard<std::mutex> lock(worker_states[worker_id]->queue_mutex);
            worker_states[worker_id]->task_queue.push(std::move(task));
            worker_states[worker_id]->queue_cv.notify_one();
        }
    }
    
    // Get the number of worker threads
    uint32_t get_worker_count() const {
        return worker_states.size();
    }
};

int set_nonblocking(int fd);

// Process a task and generate an appropriate response
Response WorkerThreadPool::process_task(const Task& task) {
  Response response;
  response.fd = task.fd;
  response.dispatcher_id = task.dispatcher_id;
  
  try {
    switch (task.header.type) {
      case GET_REQUEST: {
        if (task.payload.size() < sizeof(GetRequest)) {
          throw std::runtime_error("Invalid GET request: payload too small");
        }
        
        // Parse the request
        const GetRequest* req = reinterpret_cast<const GetRequest*>(task.payload.data());
        BinaryKey key = req->key;
        
        if (FLAGS_debug) {
          std::cout << "Processing GET request for key=" << key << std::endl;
        }
        
        // Try to get value from database
        bool found = false;
        BinaryPayload found_payload;
        
        jumpmuTry() {
          
          // Use lookup1 with a lambda to check if the key exists and extract the payload
          typename KVTable::Key k_key;
          k_key.my_key = key;  // Using my_key from the schema
          
          g_table->lookup1(k_key, [&](const KVTable& record) {
            // Copy data from record to our value
            found_payload = record.my_payload;  // Using my_payload from schema
            found = true;
          });
          
        } jumpmuCatch() {
          if (FLAGS_debug) {
            std::cout << "Transaction aborted during GET" << std::endl;
          }
          found = false;
        }
        
        if (found) {
          // Prepare successful GET response
          MessageHeader header;
          header.type = GET_RESPONSE;
          header.request_id = task.header.request_id;
          header.payload_size = sizeof(found_payload.value);  // Using exact size of the value array
          
          // Allocate response buffer
          response.data.resize(sizeof(header) + header.payload_size);
          
          // Copy header and payload (value)
          memcpy(response.data.data(), &header, sizeof(header));
          memcpy(response.data.data() + sizeof(header), found_payload.value, sizeof(found_payload.value));
        } else {
          // Key not found, send error response
          MessageHeader header;
          header.type = ERROR_RESPONSE;
          header.request_id = task.header.request_id;
          
          std::string error_message = "Key not found";
          header.payload_size = sizeof(ErrorResponse) + error_message.size();
          
          // Allocate response buffer
          response.data.resize(sizeof(header) + header.payload_size);
          
          // Copy header
          memcpy(response.data.data(), &header, sizeof(header));
          
          // Set up error response
          ErrorResponse err_resp;
          err_resp.error_code = 404; // Not found
          
          // Copy error response and message
          memcpy(response.data.data() + sizeof(header), &err_resp, sizeof(err_resp));
          memcpy(response.data.data() + sizeof(header) + sizeof(err_resp), 
                 error_message.c_str(), error_message.size());
        }
        break;
      }
      
      case PUT_REQUEST: {
        if (task.payload.size() < sizeof(PutRequest)) {
          throw std::runtime_error("Invalid PUT request: payload too small");
        }
        
        // Parse the request
        const PutRequest* req = reinterpret_cast<const PutRequest*>(task.payload.data());
        BinaryKey key = req->key;
        
        // Value follows the key in the payload
        size_t value_size = task.payload.size() - sizeof(PutRequest);
        
        if (FLAGS_debug) {
          std::cout << "Processing PUT request for key=" << key 
                    << " value_size=" << value_size << std::endl;
        }
        
        bool success = false;
        
        // Check if the value size is valid
        if (value_size > sizeof(BinaryPayload::value)) {
          // Value too large error
          MessageHeader header;
          header.type = ERROR_RESPONSE;
          header.request_id = task.header.request_id;
          
          std::string error_message = "Value too large, max size: " + 
                                      std::to_string(sizeof(BinaryPayload::value));
          header.payload_size = sizeof(ErrorResponse) + error_message.size();
          
          response.data.resize(sizeof(header) + header.payload_size);
          memcpy(response.data.data(), &header, sizeof(header));
          
          ErrorResponse err_resp;
          err_resp.error_code = 413; // Payload Too Large
          
          memcpy(response.data.data() + sizeof(header), &err_resp, sizeof(err_resp));
          memcpy(response.data.data() + sizeof(header) + sizeof(err_resp), 
                 error_message.c_str(), error_message.size());
          break;
        }
        
        // Extract value data
        const void* value_data = task.payload.data() + sizeof(PutRequest);
        
        // START TRANSACTION
        jumpmuTry() {
          cr::Worker::my().startTX(TX_MODE::OLTP, TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, false);
          
          // Check if key exists
          bool exists = false;
          typename KVTable::Key k_key;
          k_key.my_key = key;  // Should be my_key based on the schema
          
          g_table->lookup1(k_key, [&](const KVTable& record) {
            exists = true;
          });
          
          if (exists) {
            UpdateDescriptorGenerator1(tabular_update_descriptor, KVTable, my_payload);
            // Key exists, use update
            g_table->update1(k_key, [&](KVTable& record) {
              // Copy the value data into the record
              // Using my_payload.value from the schema
              memcpy(record.my_payload.value, value_data, value_size);
            }, tabular_update_descriptor);
          } else {
            // Key doesn't exist, insert new record
            KVTable record;
            // Using my_payload.value from the schema
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
        
        // Prepare PUT response
        MessageHeader header;
        header.type = PUT_RESPONSE;
        header.request_id = task.header.request_id;
        header.payload_size = sizeof(PutResponse);
        
        // Allocate response buffer
        response.data.resize(sizeof(header) + sizeof(PutResponse));
        
        // Copy header
        memcpy(response.data.data(), &header, sizeof(header));
        
        // Set up PUT response
        PutResponse put_resp;
        put_resp.success = success ? 1 : 0;
        
        // Copy PUT response
        memcpy(response.data.data() + sizeof(header), &put_resp, sizeof(put_resp));
        break;
      }
      
      default: {
        // Unknown request type
        MessageHeader header;
        header.type = ERROR_RESPONSE;
        header.request_id = task.header.request_id;
        
        std::string error_message = "Unknown request type";
        header.payload_size = sizeof(ErrorResponse) + error_message.size();
        
        // Allocate response buffer
        response.data.resize(sizeof(header) + header.payload_size);
        
        // Copy header
        memcpy(response.data.data(), &header, sizeof(header));
        
        // Set up error response
        ErrorResponse err_resp;
        err_resp.error_code = 400; // Bad request
        
        // Copy error response and message
        memcpy(response.data.data() + sizeof(header), &err_resp, sizeof(err_resp));
        memcpy(response.data.data() + sizeof(header) + sizeof(err_resp), 
               error_message.c_str(), error_message.size());
      }
    }
  } catch (const std::exception& e) {
    // Handle any exception during processing
    MessageHeader header;
    header.type = ERROR_RESPONSE;
    header.request_id = task.header.request_id;
    
    std::string error_message = "Internal server error: ";
    error_message += e.what();
    header.payload_size = sizeof(ErrorResponse) + error_message.size();
    
    // Allocate response buffer
    response.data.resize(sizeof(header) + header.payload_size);
    
    // Copy header
    memcpy(response.data.data(), &header, sizeof(header));
    
    // Set up error response
    ErrorResponse err_resp;
    err_resp.error_code = 500; // Internal server error
    
    // Copy error response and message
    memcpy(response.data.data() + sizeof(header), &err_resp, sizeof(err_resp));
    memcpy(response.data.data() + sizeof(header) + sizeof(err_resp), 
           error_message.c_str(), error_message.size());
  }
  
  return response;
}


// -------------------------------------------------------------------------------------
// Acceptor class - accepts new connections and distributes them to dispatchers
class Acceptor {
private:
  int server_fd;
  std::vector<Dispatcher*>& dispatchers;
  std::thread thread;
  uint32_t next_dispatcher;
  
  // Acceptor thread function
  void acceptor_thread_func() {
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
          if (client_fd >= static_cast<int>(FLAGS_max_connections)) {
            close(client_fd);
            std::cerr << "Connection rejected: fd " << client_fd << " exceeds limit" << std::endl;
            continue;
          }
          
          // Set non-blocking mode
          if (set_nonblocking(client_fd) < 0) {
            close(client_fd);
            continue;
          }
          
          // Distribute connection to next dispatcher in round-robin fashion
          uint32_t dispatcher_id = next_dispatcher % dispatchers.size();
          next_dispatcher++;
          
          if (!dispatchers[dispatcher_id]->add_connection(client_fd)) {
            close(client_fd);
            continue;
          }
          
          if (FLAGS_debug) {
            char client_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
            std::cout << "New connection from " << client_ip << ":" << ntohs(client_addr.sin_port) 
                      << " (fd=" << client_fd << ", dispatcher=" << dispatcher_id << ")" << std::endl;
          }
        }
      }
    }
    
    close(acceptor_epoll);
  }
  
public:
  Acceptor(int server_fd, std::vector<Dispatcher*>& dispatchers)
    : server_fd(server_fd), dispatchers(dispatchers), next_dispatcher(0) {
    thread = std::thread(&Acceptor::acceptor_thread_func, this);
  }
  
  ~Acceptor() {
    if (thread.joinable()) {
      thread.join();
    }
  }
};

// -------------------------------------------------------------------------------------
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

// Dispatcher thread function
void Dispatcher::dispatcher_thread_func() {
    const int MAX_EVENTS = 64;
    struct epoll_event events[MAX_EVENTS];

    while (running) {
        // Wait for I/O events
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, FLAGS_epoll_timeout);
        for (int i = 0; i < num_events; i++) {
            int fd = events[i].data.fd;
            uint32_t event_mask = events[i].events;
            
            // Check if this is a notification event
            if (fd == notification_fd) {
                //printf("Dispatcher %u received notification\n", id);
                // Read from eventfd to clear the notification
                uint64_t value;
                read(notification_fd, &value, sizeof(value));
                
                // Process any pending responses
                process_responses();
                continue;
            }
            
            // Lock the connections map to access the connection context
            std::unique_lock<std::mutex> lock(connections_mutex);
            auto it = connections.find(fd);
            if (it == connections.end()) {
                continue;
            }
            
            ConnectionContext& ctx = it->second;
            lock.unlock();
            
            bool keep_connection = true;
            
            // Handle error conditions
            if (event_mask & EPOLLERR || event_mask & EPOLLHUP) {
                keep_connection = false;
            }
            
            // Handle read events
            if (keep_connection && (event_mask & EPOLLIN)) {
                keep_connection = handle_read(fd, ctx);
            }
            
            // // Handle write events
            // if (keep_connection && (event_mask & EPOLLOUT)) {
            //     keep_connection = handle_write(fd, ctx);
            // }
            
            // Update epoll registration or remove the connection
            if (keep_connection) {
                //update_epoll_registration(fd, ctx);
            } else {
                remove_connection(fd);
            }
        }
    }
}

// Handle read events for a connection
bool Dispatcher::handle_read(int fd, ConnectionContext& ctx) {
    //printf("Dispatcher %u handling read event on fd %d\n", id, fd);
    if (ctx.state == READING_HEADER) {
        // We're reading the message header
        ssize_t bytes_read = read(fd, 
                                ctx.header_buf + ctx.header_bytes_read, 
                                sizeof(MessageHeader) - ctx.header_bytes_read);
        
        if (bytes_read <= 0) {
        if (bytes_read < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return true; // No more data available right now
        }
        
        if (bytes_read < 0) {
            if (FLAGS_debug) perror("read header");
        } else if (bytes_read == 0) {
            if (FLAGS_debug) std::cout << "Connection closed by client, fd=" << fd << std::endl;
        }
        return false; // Error or connection closed
        }
        
        ctx.header_bytes_read += bytes_read;
        
        // Check if we have a complete header
        if (ctx.header_bytes_read == sizeof(MessageHeader)) {
        // Parse the header
        memcpy(&ctx.header, ctx.header_buf, sizeof(MessageHeader));
        
        if (FLAGS_debug) {
            std::cout << "Received header from fd=" << fd 
                    << " type=" << static_cast<int>(ctx.header.type) 
                    << " request_id=" << ctx.header.request_id 
                    << " payload_size=" << ctx.header.payload_size << std::endl;
        }
        
        // Prepare for payload reading if needed
        if (ctx.header.payload_size > 0) {
            ctx.payload_buf.resize(ctx.header.payload_size);
            ctx.payload_bytes_read = 0;
            ctx.state = READING_PAYLOAD;
        } else {
            // No payload, create a task directly
            Task task;
            task.fd = fd;
            task.dispatcher_id = id;
            task.header = ctx.header;
            
            // Submit task to worker pool
            worker_pool.enqueue_task(std::move(task));

            //printf("Dispatcher %u enqueued task for fd %d\n", id, fd);
            
            // Reset for next request
            ctx.header_bytes_read = 0;
            ctx.state = READING_HEADER;
        }
        }
        
        return true;
    } else if (ctx.state == READING_PAYLOAD) {
        // We're reading the message payload
        ssize_t bytes_read = read(fd, 
                                ctx.payload_buf.data() + ctx.payload_bytes_read, 
                                ctx.header.payload_size - ctx.payload_bytes_read);
        
        if (bytes_read <= 0) {
        if (bytes_read < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return true; // No more data available right now
        }
        
        if (bytes_read < 0) {
            if (FLAGS_debug) perror("read payload");
        } else if (bytes_read == 0) {
            if (FLAGS_debug) std::cout << "Connection closed by client, fd=" << fd << std::endl;
        }
        return false; // Error or connection closed
        }
        
        ctx.payload_bytes_read += bytes_read;
        
        // Check if we have the complete payload
        if (ctx.payload_bytes_read == ctx.header.payload_size) {
        // Create task for worker
        Task task;
        task.fd = fd;
        task.dispatcher_id = id;
        task.header = ctx.header;
        task.payload = std::move(ctx.payload_buf);
        
        // Submit task to worker pool
        worker_pool.enqueue_task(std::move(task));
        

        //printf("Dispatcher %u enqueued task for fd %d\n", id, fd);
        // Reset for next request
        ctx.header_bytes_read = 0;
        ctx.state = READING_HEADER;
        ctx.payload_buf.clear();
        ctx.payload_bytes_read = 0;
        }
        
        return true;
    }

    return true;
}

bool Dispatcher::handle_write(int fd, ConnectionContext& ctx) {
    if (ctx.state != WRITING_RESPONSE) {
        // Nothing to write
        return true;
    }

    // Write as much as possible
    ssize_t bytes_written = write(fd, 
                                ctx.response_buf.data() + ctx.response_bytes_written, 
                                ctx.response_buf.size() - ctx.response_bytes_written);

    if (bytes_written < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return true; // Would block, try again later
        }
        
        if (FLAGS_debug) perror("write response");
        return false; // Error occurred
    }

    ctx.response_bytes_written += bytes_written;

    // Check if we've written the entire response
    if (ctx.response_bytes_written == ctx.response_buf.size()) {
        // Reset for next request
        ctx.response_buf.clear();
        ctx.response_bytes_written = 0;
        ctx.state = READING_HEADER;
    }

    return true;
}

// -------------------------------------------------------------------------------------
// Signal handler
void signal_handler(int signal) {
  std::cout << "Received signal " << signal << ", shutting down..." << std::endl;
  g_running = false;
}

// -------------------------------------------------------------------------------------
// Main function
int main(int argc, char** argv) {
  // Parse command line arguments
  gflags::SetUsageMessage("LeanStore RPC Server");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  
  // Register signal handlers
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  signal(SIGPIPE, SIG_IGN);
  
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
  std::cout << "Dispatcher threads: " << FLAGS_dispatcher_count << std::endl;
  std::cout << "Worker threads: " << FLAGS_worker_count << std::endl;

  // Create the worker pool and dispatchers
  std::vector<Dispatcher*> dispatchers;
  
  // We need to initialize these in two phases because they refer to each other
  WorkerThreadPool worker_pool(FLAGS_worker_count, dispatchers);
  
  // Create dispatcher threads
  for (uint32_t i = 0; i < FLAGS_dispatcher_count; i++) {
    dispatchers.push_back(new Dispatcher(i, worker_pool));
  }
  
  // Create the acceptor
  Acceptor acceptor(server_fd, dispatchers);
  
  // Start profiling thread
  db.startProfilingThread();
  
  std::cout << "Server started" << std::endl;
  std::cout << "Press Ctrl+C to exit" << std::endl;
  
  // Wait until shutdown signal
  while (g_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  
  // Cleanup
  for (auto dispatcher : dispatchers) {
    delete dispatcher;
  }
  
  // Close server socket
  close(server_fd);
  
  std::cout << "Server shutdown complete" << std::endl;
  return 0;
}