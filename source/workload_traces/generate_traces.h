#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace Workload {
static constexpr int default_read_permille = 500;
struct TraceCmd {
  enum {
    put,
    get,
    txn_start,
    txn_put,
    txn_get,
    txn_get_and_execute,
    txn_commit,
    txn_rollback
  } op;

  static constexpr size_t key_size = sizeof(uint32_t);
  // uint8_t key_hash[key_size];
  struct KvPair {
    uint32_t key_hash;
    std::string value;
    int op;
  };

  std::vector<KvPair> operation;

  explicit TraceCmd(uint32_t key_id, int read_permille = default_read_permille);
  explicit TraceCmd(std::vector<KvPair> &&operations);
  explicit TraceCmd(std::string const &s, int read_permille);
  explicit TraceCmd(std::string_view s, int read_permille);

private:
  void init(uint32_t key_id, int read_permille);
};

auto trace_init(uint16_t t_id, size_t trace_size, size_t nb_keys,
                int read_permille = default_read_permille, int rand_start = 0)
    -> std::vector<TraceCmd>;

auto trace_init(uint16_t /* t_id */, const std::string & /* path */)
    -> std::vector<TraceCmd>;
auto trace_init(const std::string & /* path */, int /* read_permille */)
    -> std::vector<TraceCmd>;

} // namespace Workload
