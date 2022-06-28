#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include "message.h"

namespace Workload {

struct TxnCmd;

using Key = uint32_t;
using Value = std::string;
using KVStoreState = std::unordered_map<Key, Value>;
using OpFunc = std::function<bool(KVStoreState *, TxnCmd const &)>;

struct TxnCmd {
  enum Op {
    get = ::TxnTest::OP_GET,
    put = ::TxnTest::OP_PUT,
    send_and_execute = ::TxnTest::OP_SEND_AND_EXECUTE,
    prepare = ::TxnTest::OP_PREPARE,
    commit = ::TxnTest::OP_COMMIT,
    abort = ::TxnTest::OP_ABORT,
    kill = ::TxnTest::OP_KILL,
    pause = ::TxnTest::OP_PAUSE,
  };
  using OpType = std::variant<Op, OpFunc>;
  OpType op{get};
  Key key{0};
  Value value;
};

class TxnGraph {
public:
  struct GraphNode;
  std::mutex ready_lock;
  std::condition_variable ready_cond;
  bool valid{true};
  size_t num_nodes{0};
  std::atomic<size_t> remaining_nodes{0};

  struct Node {
    uint64_t id{0};
    bool is_txn{true};
    std::vector<TxnCmd> cmds;
    std::vector<std::shared_ptr<TxnGraph::GraphNode>> next;
  };

  std::deque<Node> ready_nodes;

  TxnGraph() = default;

  auto change_graph_nodes_graph_address() -> void;

  TxnGraph(TxnGraph &&other) noexcept
      : num_nodes(other.num_nodes),
        remaining_nodes(other.remaining_nodes.load()),
        ready_nodes(std::move(other.ready_nodes)) {
    other.num_nodes = 0;
    other.remaining_nodes = 0;
    other.valid = false;
    change_graph_nodes_graph_address();
  }

  TxnGraph(TxnGraph const &other)
      : num_nodes(other.num_nodes),
        remaining_nodes(other.remaining_nodes.load()),
        ready_nodes(other.ready_nodes) {
    // TODO: deep copy
  }

  auto operator=(TxnGraph &&other) noexcept -> TxnGraph & {
    ready_nodes = std::move(other.ready_nodes);
    other.num_nodes = 0;
    other.remaining_nodes = 0;
    other.valid = false;
    change_graph_nodes_graph_address();
    return *this;
  }

  ~TxnGraph() { valid = false; }

  auto operator=(TxnGraph const &other) -> TxnGraph & {
    if (this == &other) {
      return *this;
    }
    ready_nodes = other.ready_nodes;
    num_nodes = other.num_nodes;
    remaining_nodes = other.remaining_nodes.load();
    // TODO: deep copy
    return *this;
  }

  inline auto add_to_ready(Node const &node) -> void {
    {
      std::lock_guard l(ready_lock);
      ready_nodes.push_back(node);
    }
    ready_cond.notify_one();
  }

  inline auto add_to_ready(Node &&node) -> void {
    {
      std::lock_guard l(ready_lock);
      ready_nodes.push_back(std::move(node));
    }
    ready_cond.notify_one();
  }

  template <class... Args> inline auto add_to_ready(Args &&...args) -> void {
    {
      std::lock_guard l(ready_lock);
      ready_nodes.emplace_back(std::forward<Args>(args)...);
    }
    ready_cond.notify_one();
  }

  inline auto get_ready() -> std::optional<Node> {
    std::unique_lock l(ready_lock);
    if (ready_nodes.empty()) {
      if (remaining_nodes.load() == 0) {
        return std::nullopt;
      }
      ready_cond.wait(l, [this] {
        return !ready_nodes.empty() || remaining_nodes.load() == 0;
      });
      if (ready_nodes.empty()) {
        return std::nullopt;
      }
    }
    auto result = std::move(ready_nodes.front());
    ready_nodes.pop_front();
    remaining_nodes.fetch_add(-1);
    return result;
  }

  [[nodiscard]] auto deep_copy() const -> std::unique_ptr<TxnGraph>;

  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
  struct GraphNode : public Node {
    TxnGraph *graph = nullptr;
    inline ~GraphNode() {
      if (graph && graph->valid) {
        graph->add_to_ready(std::move(*this));
      }
      graph = nullptr;
    }
  };
};

auto parse_txn_trace(std::string const &path) -> std::unique_ptr<TxnGraph>;

auto get_possible_results(TxnGraph const &graph) -> std::vector<KVStoreState>;

} // namespace Workload
