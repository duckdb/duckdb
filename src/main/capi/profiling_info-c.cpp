#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_profiler.hpp"

using duckdb::Connection;
using duckdb::QueryProfileResult;
using duckdb::QueryProfileResultKind;

// Returns the total number of items across all LIST children of a node.
static idx_t CountListChildren(const QueryProfileResult &node) {
	idx_t count = 0;
	for (auto &child : node.children) {
		if (child->kind == QueryProfileResultKind::LIST) {
			count += child->children.size();
		}
	}
	return count;
}

// Returns the i-th item across all LIST children of a node (flattening the lists).
static QueryProfileResult *GetListChild(const QueryProfileResult &node, idx_t index) {
	idx_t offset = 0;
	for (auto &child : node.children) {
		if (child->kind != QueryProfileResultKind::LIST) {
			continue;
		}
		if (index < offset + child->children.size()) {
			return child->children[index - offset].get();
		}
		offset += child->children.size();
	}
	return nullptr;
}

duckdb_profiling_info duckdb_get_profiling_info(duckdb_connection connection) {
	if (!connection) {
		return nullptr;
	}
	auto *conn = reinterpret_cast<Connection *>(connection);
	try {
		auto &profiler = duckdb::QueryProfiler::Get(*conn->context);
		if (!profiler.IsEnabled() || !profiler.HasRoot()) {
			return nullptr;
		}
		return reinterpret_cast<duckdb_profiling_info>(&profiler.GetResult());
	} catch (...) {
		return nullptr;
	}
}

duckdb_value duckdb_profiling_info_get_value(duckdb_profiling_info info, const char *key) {
	if (!info || !key) {
		return nullptr;
	}
	auto &node = *reinterpret_cast<QueryProfileResult *>(info);
	for (auto &child : node.children) {
		if (child->kind != QueryProfileResultKind::VALUE) {
			continue;
		}
		if (duckdb::StringUtil::CIEquals(child->key, key)) {
			return reinterpret_cast<duckdb_value>(new duckdb::Value(child->value));
		}
	}
	return nullptr;
}

duckdb_value duckdb_profiling_info_get_metrics(duckdb_profiling_info info) {
	if (!info) {
		return nullptr;
	}
	auto &node = *reinterpret_cast<QueryProfileResult *>(info);
	duckdb::InsertionOrderPreservingMap<duckdb::string> metrics_map;
	for (auto &child : node.children) {
		if (child->kind != QueryProfileResultKind::VALUE) {
			continue;
		}
		auto key = duckdb::StringUtil::Upper(child->key);
		metrics_map.insert(key, child->value.ToString());
	}
	auto map = duckdb::Value::MAP(metrics_map);
	return reinterpret_cast<duckdb_value>(new duckdb::Value(map));
}

idx_t duckdb_profiling_info_get_child_count(duckdb_profiling_info info) {
	if (!info) {
		return 0;
	}
	auto &node = *reinterpret_cast<QueryProfileResult *>(info);
	return CountListChildren(node);
}

duckdb_profiling_info duckdb_profiling_info_get_child(duckdb_profiling_info info, idx_t index) {
	if (!info) {
		return nullptr;
	}
	auto &node = *reinterpret_cast<QueryProfileResult *>(info);
	return reinterpret_cast<duckdb_profiling_info>(GetListChild(node, index));
}
