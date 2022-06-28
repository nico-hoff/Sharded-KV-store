#include <charconv>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string_view>

#include "generate_traces.h"

#include <fcntl.h>
#include <fmt/format.h>
#include <sys/mman.h>
#include <unistd.h>

namespace Workload {
namespace {

struct FD {
  int fd;
  operator int() const noexcept { return fd; }
  explicit FD(int fd) : fd(fd) {}
  FD(FD &&other) noexcept : fd(other.fd) { other.fd = -1; }
  ~FD() {
    if (fd >= 0) {
      ::close(fd);
    }
  }
};

struct Unmap {
  size_t size;
  inline Unmap() = default;
  inline Unmap(size_t size) : size(size) {}
  inline Unmap(Unmap &&other) noexcept : size(other.size) { other.size = 0; }
  inline auto operator=(Unmap &&other) noexcept -> Unmap & {
    size = other.size;
    other.size = 0;
    return *this;
  }
  inline auto operator()(char const *ptr) -> void {
    if (size && ptr) {
      ::munmap(const_cast<char *>(ptr), size);
    }
  }
};

auto split(std::string_view str, std::string_view delims, int is_tx,
           int ops_per_tx) -> std::vector<TraceCmd> {
  std::vector<TraceCmd> tokens;
  for (auto first = str.data(), second = str.data(), last = first + str.size();
       second != last; first = second + 1) {
    if (!is_tx) {
      second = std::find_first_of(first, last, std::cbegin(delims),
                                  std::cend(delims));
      if (first != second) {
        tokens.emplace_back(std::string_view(first, second - first),
                            default_read_permille);
      }
    } else {
      // it is tx so
      fmt::print("tx start .... \n");
      std::vector<TraceCmd::KvPair> txs;
      txs.emplace_back(TraceCmd::KvPair{
          .key_hash = (uint32_t)-1, .value = "invalid", .op = TraceCmd::txn_start});
      for (size_t tx_op = 0; tx_op < ops_per_tx; tx_op++) {
        second = std::find_first_of(first, last, std::cbegin(delims),
                                    std::cend(delims));
        if (second == last) {
          break;
        }
        if (first != second) {
          auto str = std::string_view(first, second - first);

          txs.emplace_back(TraceCmd::KvPair{
              .key_hash = [str]() -> uint32_t {
                uint32_t result;
                std::from_chars(str.data(), str.data() + str.size(), result);
                // TODO error handling
                return result;
              }(),
              .value = "1",
              .op = TraceCmd::txn_put});
          fmt::print("{} \n", str);
          first = second + 1;
          if (first == last) {
            break;
          }
        }
      }
      fmt::print("tx end .... \n");
      if (txs.size() > 1) {
        txs.emplace_back(TraceCmd::KvPair{
            .key_hash = (uint32_t)-1, .value = "invalid", .op = TraceCmd::txn_commit});
        tokens.emplace_back(std::move(txs));
      }
    }
  }

  // fmt::print("{}, ", tokens.size());
  for (auto i : tokens) {
    // fmt::print("{}, ", i.operation.size());
    /*
     if (i.operation.size() == 0)
       fmt::print("{} \n\n\n\n", __func__);
       */
  }
  return tokens;
}

auto parse_trace(uint16_t /* unused */, const std::string &path,
                 int read_permille) -> std::vector<TraceCmd> {
  FD fd(open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (fd < 0) {
    fmt::print(stderr, "Failed to open trace file: {}\n", path);
    return {};
  }
  Unmap size(lseek(fd, 0, SEEK_END));
  auto tmp_size = size.size;
  std::unique_ptr<char const, Unmap> ptr(
      static_cast<char *>(
          ::mmap(nullptr, tmp_size, PROT_READ, MAP_PRIVATE, fd, 0)),
      std::move(size));
  if (ptr.get() == MAP_FAILED) {
    fmt::print(stderr, "Failed to mmap trace file: {}\n", path);
    return {};
  }
  std::string_view content(ptr.get(), tmp_size);
  // @dimitra: make this true for txs
  return split(content, "\n", false, 2);
}

auto manufacture_trace(uint16_t unused /* unused */, size_t trace_size,
                       size_t nb_keys, int read_permille)
    -> std::vector<TraceCmd> {
  std::vector<TraceCmd> res;
  res.reserve(trace_size);
  for (auto i = 0ULL; i < trace_size; ++i) {
    res.emplace_back(static_cast<uint32_t>(rand() % nb_keys), read_permille);
  }
  return res;
}

} // namespace

void TraceCmd::init(uint32_t key_id, int read_permille) {
  auto op = (rand() % 1000) < read_permille ? get : put;
  // memcpy(key_hash, &key_id, sizeof(key_id));
  //  key_hash = key_id;
  operation.emplace_back(KvPair{.key_hash = key_id, .value = "1", .op = op});
}

TraceCmd::TraceCmd(std::vector<KvPair> &&operations)
    : operation(std::move(operations)) {}

TraceCmd::TraceCmd(uint32_t key_id, int read_permille) {
  init(key_id, read_permille);
}

TraceCmd::TraceCmd(std::string const &s, int read_permille) {
  init(static_cast<uint32_t>(strtoul(s.c_str(), nullptr, 10)), read_permille);
}

TraceCmd::TraceCmd(std::string_view s, int read_permille) {
  uint32_t result;
  std::from_chars(s.data(), s.data() + s.size(), result);
  init(result, read_permille);
}

auto trace_init(uint16_t t_id, std::string const &path)
    -> std::vector<TraceCmd> {
  return parse_trace(t_id, path, default_read_permille);
}

auto trace_init(std::string const &file_path, int read_permile)
    -> std::vector<TraceCmd> {
  return parse_trace(0, file_path, read_permile);
}

auto trace_init(uint16_t t_id, size_t trace_size, size_t nb_keys,
                int read_permille, int rand_start) -> std::vector<TraceCmd> {
  srand(rand_start);
  return manufacture_trace(t_id, trace_size, nb_keys, read_permille);
}

} // namespace Workload
