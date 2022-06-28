#include <charconv>
#include <fstream>
#include <string>
#include <unordered_map>
#include <utility>

#include "load_txn.h"

#include <fmt/format.h>

#include "../shared.h"
#include "message.h"

namespace Workload {

namespace {

using OpValue = int64_t;
using namespace std::literals;

auto convert_key_to_int(std::string_view key) -> uint32_t {
  uint32_t res{0};
  memcpy(&key, key.data(), std::min(key.size(), sizeof(res)));
  return res;
}

template <class T>
auto str_value_to_op_value(std::string_view value) -> std::optional<T> {
  T res{};
  auto err = std::from_chars(value.data(), value.data() + value.size(), res);
  if (err.ec != std::errc()) {
    return std::nullopt;
  }
  return res;
}

auto set(KVStoreState *state, TxnCmd const &cmd) -> bool {
  return state->insert_or_assign(cmd.key, cmd.value).second;
}

template <class T, class F>
auto op_wrapper(KVStoreState *state, TxnCmd const &cmd, F &&f) -> bool {
  auto const &lhs = state->find(cmd.key);
  auto rhs_key = convert_key_to_int(cmd.value);
  auto const &rhs = state->find(rhs_key);
  if (lhs == state->end() || rhs == state->end()) {
    return false;
  }
  auto lhs_v = str_value_to_op_value<T>(lhs->second);
  auto rhs_v = str_value_to_op_value<T>(rhs->second);
  if (!lhs_v || !rhs_v) {
    return false;
  }
  auto res = f(*lhs_v, *rhs_v);
  return state->insert_or_assign(cmd.key, fmt::to_string(res)).second;
}

auto add(KVStoreState *state, TxnCmd const &cmd) -> bool {
  return op_wrapper<OpValue>(
      state, cmd, [](auto const &lhs, auto const &rhs) { return lhs + rhs; });
}

auto sub(KVStoreState *state, TxnCmd const &cmd) -> bool {
  return op_wrapper<OpValue>(
      state, cmd, [](auto const &lhs, auto const &rhs) { return lhs - rhs; });
}

auto mult(KVStoreState *state, TxnCmd const &cmd) -> bool {
  return op_wrapper<OpValue>(
      state, cmd, [](auto const &lhs, auto const &rhs) { return lhs * rhs; });
}

auto div(KVStoreState *state, TxnCmd const &cmd) -> bool {
  return op_wrapper<OpValue>(
      state, cmd, [](auto const &lhs, auto const &rhs) { return lhs / rhs; });
}

auto mod(KVStoreState *state, TxnCmd const &cmd) -> bool {
  return op_wrapper<OpValue>(
      state, cmd, [](auto const &lhs, auto const &rhs) { return lhs % rhs; });
}

template <size_t N> struct BySize : BySize<N + 1> {};
template <size_t N> using BySizeT = typename BySize<N>::type;
template <class T> struct Tag {
  // NOLINTNEXTLINE(readability-identifier-naming)
  using type = T;
};

template <> struct BySize<sizeof(int8_t)> : Tag<uint_least8_t> {};
template <> struct BySize<sizeof(int16_t)> : Tag<uint_least16_t> {};
template <> struct BySize<sizeof(int32_t)> : Tag<uint_least32_t> {};
template <> struct BySize<sizeof(int64_t)> : Tag<uint_least64_t> {};

template <class T, class F>
auto bin_op_wrapper(KVStoreState *state, TxnCmd const &cmd, F &&f) -> bool {
  return op_wrapper<T>(state, cmd, [&f](auto const &lhs, auto const &rhs) {
    if constexpr (std::is_unsigned_v<T>) {
      return f(lhs, rhs);
    }
    using U = BySizeT<sizeof(T)>;
    U lhs_u{};
    U rhs_u{};
    memcpy(&lhs_u, &lhs, sizeof(lhs));
    memcpy(&rhs_u, &rhs, sizeof(rhs));
    return f(lhs_u, rhs_u);
  });
}

auto and_func(KVStoreState *state, TxnCmd const &cmd) -> bool {
  return bin_op_wrapper<OpValue>(
      state, cmd, [](auto const &lhs, auto const &rhs) { return lhs & rhs; });
}

auto or_func(KVStoreState *state, TxnCmd const &cmd) -> bool {
  return bin_op_wrapper<OpValue>(
      state, cmd, [](auto const &lhs, auto const &rhs) { return lhs | rhs; });
}

auto xor_func(KVStoreState *state, TxnCmd const &cmd) -> bool {
  return bin_op_wrapper<OpValue>(
      state, cmd, [](auto const &lhs, auto const &rhs) { return lhs ^ rhs; });
}

auto not_func(KVStoreState *state, TxnCmd const &cmd) -> bool {
  auto const &lhs = state->find(cmd.key);
  if (lhs == state->end()) {
    return false;
  }
  auto lhs_v = str_value_to_op_value<OpValue>(lhs->second);
  if (!lhs_v) {
    return false;
  }
  auto res = *lhs_v;
  if constexpr (std::is_unsigned_v<decltype(res)>) {
    // NOLINTNEXTLINE(hicpp-signed-bitwise)
    res = ~res;
  } else {
    using U = BySizeT<sizeof(decltype(res))>;
    U tmp{};
    memcpy(&tmp, &res, sizeof(res));
    tmp = ~tmp;
    memcpy(&res, &tmp, sizeof(res));
  }
  return state->insert_or_assign(cmd.key, fmt::format("{}", res)).second;
}

auto nand_func(KVStoreState *state, TxnCmd const &cmd) -> bool {
  return bin_op_wrapper<OpValue>(
      state, cmd,
      [](auto const &lhs, auto const &rhs) { return ~(lhs & rhs); });
}

auto nor_func(KVStoreState *state, TxnCmd const &cmd) -> bool {
  return bin_op_wrapper<OpValue>(
      state, cmd,
      [](auto const &lhs, auto const &rhs) { return ~(lhs | rhs); });
}

auto send_cmd(std::string_view cmd_str, TxnCmd const &cmd) -> bool {
  fmt::print("CMD: {} {}\n", cmd_str, cmd.key);
  std::flush(std::cout);
  int res;
  std::cin >> res;
  return res;
}

auto pause_func(KVStoreState * /*state*/, TxnCmd const &cmd) -> bool {
  return send_cmd("pause"sv, cmd);
}

auto kill_func(KVStoreState * /*state*/, TxnCmd const &cmd) -> bool {
  return send_cmd("kill"sv, cmd);
}

// NOLINTNEXTLINE(cert-err58-cpp)
std::unordered_map<TxnTest::OpType, TxnCmd::OpType> const op_type_map{
    {TxnTest::OP_PUT, TxnCmd::put},
    {TxnTest::OP_GET, TxnCmd::get},
    {TxnTest::OP_SEND_AND_EXECUTE, TxnCmd::send_and_execute},
    {TxnTest::OP_PREPARE, TxnCmd::prepare},
    {TxnTest::OP_COMMIT, TxnCmd::commit},
    {TxnTest::OP_ABORT, TxnCmd::abort},
    {TxnTest::OP_PAUSE, pause_func},
    {TxnTest::OP_KILL, kill_func},
    {TxnTest::OP_SET, set},
    {TxnTest::OP_ADD, add},
    {TxnTest::OP_SUB, sub},
    {TxnTest::OP_MULT, mult},
    {TxnTest::OP_DIV, div},
    {TxnTest::OP_MOD, mod},
    {TxnTest::OP_AND, and_func},
    {TxnTest::OP_OR, or_func},
    {TxnTest::OP_XOR, xor_func},
    {TxnTest::OP_NOT, not_func},
    {TxnTest::OP_NAND, nand_func},
    {TxnTest::OP_NOR, nor_func},
};

} // namespace

auto parse_txn_trace(std::string const &path) -> std::unique_ptr<TxnGraph> {
  std::unique_ptr<TxnGraph> result = std::make_unique<TxnGraph>();
  ::TxnTest::Test test;
  std::fstream input(path, std::ios::in | std::ios::binary);
  if (!test.ParseFromIstream(&input)) {
    fmt::print("Failed to parse input\n");
    return result;
  }
  std::unordered_map<uint64_t, std::pair<std::shared_ptr<TxnGraph::GraphNode>,
                                         std::vector<uint64_t>>>
      nodes;

  for (auto const &txn : test.txns()) {
    auto node = std::make_shared<TxnGraph::GraphNode>();
    node->graph = result.get();
    node->is_txn = txn.is_txn();
    for (auto const &cmd : txn.cmds()) {
      TxnCmd txn_cmd;
      txn_cmd.op = op_type_map.at(cmd.op());
      txn_cmd.key = convert_key_to_int(cmd.key());
      txn_cmd.value = cmd.value();
      node->cmds.push_back(txn_cmd);
    }
    nodes[txn.txn_id()] =
        std::make_pair(std::move(node), std::vector(txn.depends_on().begin(),
                                                    txn.depends_on().end()));
  }

  for (auto &[key, node] : nodes) {
    for (auto const &dep : node.second) {
      auto &[dep_node, depends_on] = nodes[dep];
      dep_node->next.push_back(node.first);
    }
  }
  result->num_nodes = nodes.size();
  result->remaining_nodes = result->num_nodes;
  return result;
}

namespace {

auto deep_copy_network(
    TxnGraph::Node *node, TxnGraph *graph,
    std::unordered_map<TxnGraph::GraphNode *,
                       std::shared_ptr<TxnGraph::GraphNode>> &known_nodes)
    -> void {
  for (auto &next : node->next) {
    if (auto it = known_nodes.find(next.get()); it != known_nodes.end()) {
      next = it->second;
      continue;
    }
    auto new_next = std::make_shared<TxnGraph::GraphNode>(*next);
    new_next->graph = graph;
    known_nodes[next.get()] = new_next;
    next = std::move(new_next);
    deep_copy_network(next.get(), graph, known_nodes);
  }
}

auto change_graph_nodes_graph_address(TxnGraph *graph,
                                      TxnGraph::GraphNode *node) -> void {
  if (node->graph == graph) {
    return;
  }
  node->graph = graph;
  for (auto &next : node->next) {
    change_graph_nodes_graph_address(graph, next.get());
  }
}

} // namespace

auto TxnGraph::change_graph_nodes_graph_address() -> void {
  for (auto const &node : ready_nodes) {
    for (auto const &next : node.next) {
      ::Workload::change_graph_nodes_graph_address(this, next.get());
    }
  }
}

auto TxnGraph::deep_copy() const -> std::unique_ptr<TxnGraph> {
  auto result = std::make_unique<TxnGraph>(*this);
  std::unordered_map<TxnGraph::GraphNode *,
                     std::shared_ptr<TxnGraph::GraphNode>>
      nodes;
  for (auto &node : result->ready_nodes) {
    deep_copy_network(&node, result.get(), nodes);
  }
  return result;
}

namespace {
auto get_possible_results(TxnGraph &graph, KVStoreState &state)
    -> std::vector<KVStoreState> {
  if (graph.ready_nodes.empty()) {
    return {state};
  }
  if (graph.ready_nodes.size() == 1) {
    {
      auto node = std::move(graph.ready_nodes.front());
      graph.ready_nodes.pop_front();
      if (node.is_txn || node.id == 0) {
        for (auto &cmd : node.cmds) {
          std::visit(
              overloaded{[&state, &cmd](TxnCmd::Op const &op) {
                           if (op == TxnCmd::put) {
                             state[cmd.key] = cmd.value;
                           }
                         },
                         [&state, &cmd](OpFunc const &op) { op(&state, cmd); }},
              cmd.op);
        }
      }
    }
    return get_possible_results(graph, state);
  }
  std::vector<KVStoreState> results;
  for (auto i = 0ULL; i < graph.ready_nodes.size(); ++i) {
    auto new_graph = graph.deep_copy();
    auto new_state = state;
    {
      auto it = new_graph->ready_nodes.begin();
      std::advance(it, i);
      auto node = std::move(*it);
      new_graph->ready_nodes.erase(it);
      if (node.is_txn || node.id == 0) {
        for (auto &cmd : node.cmds) {
          std::visit(overloaded{[&new_state, &cmd](TxnCmd::Op const &op) {
                                  if (op == TxnCmd::put) {
                                    new_state[cmd.key] = cmd.value;
                                  }
                                },
                                [&new_state, &cmd](OpFunc const &op) {
                                  op(&new_state, cmd);
                                }},
                     cmd.op);
        }
      }
    }
    auto tmp = get_possible_results(*new_graph, new_state);
    results.insert(results.end(), tmp.begin(), tmp.end());
  }
  return results;
}
} // namespace

auto get_possible_results(TxnGraph const &graph) -> std::vector<KVStoreState> {
  KVStoreState state;
  auto tmp = graph.deep_copy();
  return get_possible_results(*tmp, state);
}

} // namespace Workload