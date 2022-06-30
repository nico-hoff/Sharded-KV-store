#pragma once

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <message.h>

#if !defined(NDEBUG)
#include <fmt/format.h>
#if 0
template<class... Args>
void debug_print(fmt::format_string<Args...> fmt, Args &&... args) {
  fmt::print(fmt, std::forward<Args>(args)...);
}
#else // 0
// NOLINTNEXTLINE(readability-identifier-naming)
#define debug_print(...) fmt::print(__VA_ARGS__)
#endif // 0
#else  // !defined(NDEBUG)
template <class... Args> void debug_print(Args &&...) {}
#endif // !defined(NDEBUG)

#if !defined(LITTLE_ENDIAN)
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__)
#define LITTLE_ENDIAN __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#else
#define LITTLE_ENDIAN true
#endif
#endif

static constexpr auto length_size_field = sizeof(uint32_t);
static constexpr auto client_base_addr = 30500;
static constexpr auto number_of_connect_attempts = 20;
static constexpr auto gets_per_mille = 200;

template <class... Ts> struct overloaded : Ts... { // NOLINT
  using Ts::operator()...;
};
template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

/**
 ** It takes as an argument a ptr to an array of size 4 or bigger and
 ** converts the char array into an integer.
 **/
inline auto convert_byte_array_to_int(char *b) noexcept -> uint32_t {
  if constexpr (LITTLE_ENDIAN) {
#if defined(__GNUC__)
    uint32_t res = 0;
    memcpy(&res, b, sizeof(res));
    return __builtin_bswap32(res);
#else  // defined(__GNUC__)
    return (b[0] << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) |
           (b[3] & 0xFF);
#endif // defined(__GNUC__)
  }
  uint32_t result = 0;
  memcpy(&result, b, sizeof(result));
  return result;
}

/**
 ** It takes as arguments one char[] array of 4 or bigger size and an integer.
 ** It converts the integer into a byte array.
 **/
inline auto convert_int_to_byte_array(char *dst, uint32_t sz) noexcept -> void {
  if constexpr (LITTLE_ENDIAN) {
#if defined(__GNUC__)
    auto tmp = __builtin_bswap32(sz);
    memcpy(dst, &tmp, sizeof(tmp));
#else      // defined(__GNUC__)
    auto tmp = dst;
    tmp[0] = (sz >> 24) & 0xFF;
    tmp[1] = (sz >> 16) & 0xFF;
    tmp[2] = (sz >> 8) & 0xFF;
    tmp[3] = sz & 0xFF;
#endif     // defined(__GNUC__)
  } else { // BIG_ENDIAN
    memcpy(dst, &sz, sizeof(sz));
  }
}

class ErrNo {
  int err_no;

public:
  [[nodiscard]] inline ErrNo() noexcept : err_no(errno) {}
  inline explicit ErrNo(int err_no) noexcept : err_no(err_no) {}

  [[nodiscard]] inline auto msg() const noexcept -> std::string_view {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    return strerror(err_no);
  }

  [[nodiscard]] inline auto get_err_no() const noexcept -> int {
    return err_no;
  }

  [[nodiscard]] inline explicit operator int() const noexcept { return err_no; }
};

[[nodiscard]] auto secure_recv(int fd)
    -> std::pair<size_t, std::unique_ptr<char[]>>;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern hostent *hostip;

auto secure_send(int fd, char *data, size_t len) -> std::optional<size_t>;

/**
 * It constructs the message to be sent.
 * It takes as arguments a destination char ptr, the payload (data to be
 sent)
 * and the payload size.
 * It returns the expected message format at dst ptr;
 *
 *  |<---msg size (4 bytes)--->|<---payload (msg size bytes)--->|
 *
 *
 */
inline void construct_message(char *dst, const char *payload,
                              size_t payload_size) {
  convert_int_to_byte_array(dst, payload_size);
  ::memcpy(dst + length_size_field, payload, payload_size);
}

int connect_to(int port, std::string server_address, int flag, int timeout_flag);
void accept_connections(int port, std::vector<int> *connections, int flag);
bool recv_clt_message(int sockfd, sockets::client_msg *message);
bool recv_svr_message(int sockfd, server::server_response::reply *message);
void send_clt_message(int sockfd, sockets::client_msg message);
void send_svr_message(int sockfd, server::server_response::reply message);
void close_socket(int sock_fd, int flag);
void close_sockets(int recv_sockfd, int send_sockfd, int flag);