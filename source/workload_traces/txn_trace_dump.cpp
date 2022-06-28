#include <fstream>

#include <fmt/format.h>

#include "load_txn.h"
#include "message.h"

auto test_graph(::Workload::TxnGraph *graph) -> void {
  while (!graph->ready_nodes.empty()) {
    fmt::print("Size: {}\n", graph->ready_nodes.size());
    auto node = std::move(graph->ready_nodes.front());
    graph->ready_nodes.pop_front();
    for (auto const &cmd : node.cmds) {
      if (auto const *op = std::get_if<::Workload::TxnCmd::Op>(&(cmd.op))) {
        fmt::print("{} {:x} {:x}\n", *op, cmd.key, *cmd.value.data());
      }
    }
  }
}

auto test_graph(std::string const &path) -> void {
  auto graph = ::Workload::parse_txn_trace(path);
  auto copy = graph->deep_copy();
  test_graph(graph.get());
  test_graph(copy.get());
}

auto possible_combinations(std::string const &path) -> void {
  auto graph = ::Workload::parse_txn_trace(path);
  auto combinations = ::Workload::get_possible_results(*graph);
  for (auto i = 0ULL; i < combinations.size(); ++i) {
    fmt::print("{}\n", i);
    for (auto const &[key, value] : combinations[i]) {
      fmt::print("\t{:x} {:x}\n", key, *value.data());
    }
  }
}

auto main() -> int {
  ::TxnTest::Test test;
  std::string path{"./small_test.bin"};
  std::fstream input(path, std::ios::in | std::ios::binary);
  if (!test.ParseFromIstream(&input)) {
    fmt::print("Failed to parse input\n");
    return 1;
  }
  for (auto const &txn : test.txns()) {
    fmt::print("{}\n", txn.txn_id());
    fmt::print("Depends:\n");
    for (auto const &dep : txn.depends_on()) {
      fmt::print("\t{}\n", dep);
    }
    fmt::print("cmd\n");
    for (auto const &cmd : txn.cmds()) {
      fmt::print("\t{}\n", OpType_Name(cmd.op()));
      fmt::print("\tkey: ");
      for (auto const &key : cmd.key()) {
        fmt::print("{:x} ", key);
      }
      fmt::print("\n\tvalue:");
      for (auto const &val : cmd.value()) {
        fmt::print("{:x} ", val);
      }
      fmt::print("\n");
    }
  }
  test_graph(path);
  fmt::print("Combinations\n");
  possible_combinations(path);
  return 0;
}