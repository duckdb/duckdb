
#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/execution/index/buffered_index_replays.hpp"

namespace duckdb {
IndexStorageInfo::IndexStorageInfo(IndexStorageInfo &&other) noexcept = default;
IndexStorageInfo &IndexStorageInfo::operator=(IndexStorageInfo &&other) noexcept = default;
IndexStorageInfo::~IndexStorageInfo() = default;
} // namespace duckdb
