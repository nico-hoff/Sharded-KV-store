#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <unordered_map>

#include <fmt/printf.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using namespace rocksdb;

class KvStore {
public:
  static inline auto init() -> std::shared_ptr<KvStore> {
    return std::make_shared<KvStore>();
  }

  inline auto put(int key, std::string_view value) -> bool {
    std::lock_guard<std::mutex> l(db_mtx);
    no_keys++;
    kv_store.insert_or_assign(key, value);
    return true;
  }

  inline auto get(int key) const -> std::optional<std::string_view> {
    std::lock_guard<std::mutex> l(db_mtx);
    auto it = kv_store.find(key);
    if (it == kv_store.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  inline auto tx_start(int tx_id) -> bool {
    std::lock_guard<std::mutex> l(txs_mtx);
    auto it = live_txs.find(tx_id);
    if (it != live_txs.end()) {
      return false;
    }
    live_txs.insert({tx_id, local_tx_buf{}});
    return true;
  }

  inline auto tx_put(int tx_id, int key, std::string_view value) -> bool {
    std::lock_guard<std::mutex> l(txs_mtx);
    auto it = live_txs.find(tx_id);
    if (it == live_txs.end()) {
      return false;
    }
    auto &tx_buf = live_txs[tx_id];
    tx_buf[key] = value;
    return true;
  }

  inline auto safe_get(int key) -> std::string_view {
    std::lock_guard<std::mutex> l(db_mtx);
    auto it = kv_store.find(key);
    return it->second;
  }

  inline auto tx_get(int tx_id, int key) -> std::tuple<bool, std::string> {
    std::lock_guard<std::mutex> l(gets_mtx);

    for (auto &tx_locked_keys : locked_keys) {
      if (tx_locked_keys.first != tx_id) {
        auto it = std::find(tx_locked_keys.second.begin(),
                            tx_locked_keys.second.end(), key);
        if (it != tx_locked_keys.second.end())
          return std::tuple{false, ""};
      }
    }

    auto ret_val = std::string{safe_get(key)};
    auto &tx_keys = locked_keys[tx_id];
    tx_keys.push_back(key);
    return std::tuple{true, ret_val};
  }

  inline auto unsafe_put(int key, std::string_view value) -> bool {
    kv_store.insert_or_assign(key, value);
    return true;
  }

  inline auto tx_commit(int tx_id) -> bool {
    std::lock_guard<std::mutex> lock_txs(txs_mtx);
    std::lock_guard<std::mutex> lock_db(db_mtx);
    auto cur_tx = live_txs[tx_id];
    for (auto &update : cur_tx) {
      unsafe_put(update.first, update.second);
    }

    live_txs.erase(tx_id);

    std::lock_guard<std::mutex> lock_keys(gets_mtx);
    locked_keys.erase(tx_id);
    return true;
  }

  inline auto tx_abort(int tx_id) -> bool {
    std::lock_guard<std::mutex> lock_keys(gets_mtx);
    locked_keys.erase(tx_id);
    std::lock_guard<std::mutex> lock_txs(txs_mtx);
    live_txs.erase(tx_id);
    return true;
  }

  ~KvStore() { fmt::print("[{}] no_keys={}\n", __func__, no_keys); }

  void init_it() { it = kv_store.begin(); }

  auto get_next_key() -> int {
    if (it != kv_store.end()) {
      auto ret = it->first;
      it++;
      return ret;
    }
    return -1;
  }

private:
  mutable std::mutex db_mtx; // lock for the kv_store
  std::unordered_map<int, std::string> kv_store;
  std::unordered_map<int, std::string>::iterator it;

  mutable std::mutex txs_mtx; // lock for the txs
  using local_tx_buf = std::unordered_map<int, std::string>;
  std::unordered_map<int, local_tx_buf> live_txs;

  uint64_t no_keys = 0;

  mutable std::mutex gets_mtx; // lock for the txs
  using tx_keys = std::vector<int>;
  std::unordered_map<int, tx_keys> locked_keys; // keys that are read by a tx

  DB *db;
  Options options;
};

