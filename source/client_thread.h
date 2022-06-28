#pragma once

#include <optional>

#include <fmt/format.h>

#include "message.h"
#include "shared.h"
#include "kv_store.h"

class ClientThread {
public:
  void connect_to_the_server(int port, char const * /*hostname*/) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    hostent *he = hostip;

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
      fmt::print("socket\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }

    // connector.s address information
    sockaddr_in their_addr{};
    their_addr.sin_family = AF_INET;
    their_addr.sin_port = htons(port);
    their_addr.sin_addr = *(reinterpret_cast<in_addr *>(he->h_addr));
    memset(&(their_addr.sin_zero), 0, sizeof(their_addr.sin_zero));

    if (connect(sockfd, reinterpret_cast<sockaddr *>(&their_addr),
                sizeof(struct sockaddr)) == -1) {
      fmt::print("connect issue\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }

    // init the listening socket
    int ret = 1;
    port = client_base_addr + thread_id;
    sent_init_connection_request(port);

    int repfd = socket(AF_INET, SOCK_STREAM, 0);
    if (repfd == -1) {
      fmt::print("socket\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }

    if (setsockopt(repfd, SOL_SOCKET, SO_REUSEADDR, &ret, sizeof(int)) == -1) {
      fmt::print("setsockopt\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }

    sockaddr_in my_addr{};
    my_addr.sin_family = AF_INET;         // host byte order
    my_addr.sin_port = htons(port);       // short, network byte order
    my_addr.sin_addr.s_addr = INADDR_ANY; // automatically fill with my IP
    memset(&(my_addr.sin_zero), 0,
           sizeof(my_addr.sin_zero)); // zero the rest of the struct

    if (bind(repfd, reinterpret_cast<sockaddr *>(&my_addr), sizeof(sockaddr)) ==
        -1) {
      fmt::print("bind\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }
    constexpr int max_backlog = 1024;
    if (listen(repfd, max_backlog) == -1) {
      fmt::print("listen\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }

    socklen_t sin_size = sizeof(sockaddr_in);
    fmt::print("waiting for new connections ..\n");
    sockaddr_in tmp_addr{};
    auto new_fd = accept4(repfd, reinterpret_cast<sockaddr *>(&tmp_addr),
                          &sin_size, SOCK_CLOEXEC);
    if (new_fd == -1) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      fmt::print("accecpt() failed ..{}\n", std::strerror(errno));
    }

    fcntl(new_fd, F_SETFL, O_NONBLOCK);
    fmt::print("accept succeeded on socket {} {} ..\n", new_fd, repfd);
    rep_fd = new_fd;
  }

  void sent_init_connection_request(int port) const {
    sockets::client_msg msg;
    auto *operation_data = msg.add_ops();
    operation_data->set_type(sockets::client_msg::INIT);
    operation_data->set_port(port);
    std::string msg_str;
    msg.SerializeToString(&msg_str);

    auto msg_size = msg_str.size();
    auto buf = std::make_unique<char[]>(msg_size + length_size_field);
    construct_message(buf.get(), msg_str.c_str(), msg_size);

    secure_send(sockfd, buf.get(), msg_size + length_size_field);
  }

  void sent_request(char *request, size_t size) const {
    if (auto numbytes = secure_send(sockfd, request, size); !numbytes) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      fmt::print("{}\n", std::strerror(errno));
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }
    // fmt::print("{} sents {} bytes\n", __func__, size);
  }

  auto recv_ack() -> std::pair<int, std::unique_ptr<char[]>> {
    auto [bytecount, buffer] = secure_recv(rep_fd);
    if (buffer == nullptr) {
      fmt::print("ERROR\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(2);
    }
    if (bytecount == 0) {
      fmt::print("ERROR\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(2);
    }

    replies++;
    // fmt::print("[{}] {} {}\n", __func__, bytecount, buffer.get());
    return {bytecount, std::move(buffer)};
  }

  ClientThread() = delete;
  explicit ClientThread(int tid) : thread_id(tid) {
	  local_kv = KvStore::init();
  }
  ClientThread(int port, char const *hostname) {
    connect_to_the_server(port, hostname);
    local_kv = KvStore::init();
  }

  ClientThread(ClientThread const &) = delete;
  auto operator=(ClientThread const &) -> ClientThread & = delete;

  ClientThread(ClientThread &&other) noexcept : sockfd(other.sockfd) {
    other.sockfd = -1;
  }

  auto operator=(ClientThread &&other) noexcept -> ClientThread & {
    sockfd = other.sockfd;
    other.sockfd = -1;
    return *this;
  }

  ~ClientThread() {
    if (sockfd != -1) {
      ::close(sockfd);
    }
  }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  int32_t replies = 0;

  void local_kv_put(int key, std::string_view value) {
	  local_kv->put(key, value);
  }

                  inline auto local_kv_get(int key) const -> std::optional<std::string_view> {
			  return local_kv->get(key);
		  }
	
  
  void local_kv_init_it() { local_kv->init_it(); }

  auto local_kv_get_next_key() -> int {
	  return local_kv->get_next_key();
  }

  auto get_thread_id() -> int {
	  return thread_id;
  }
private:
  int sockfd = -4, rep_fd = -1;
  int thread_id = 0;
  std::shared_ptr<KvStore> local_kv;
};

