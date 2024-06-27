#include "duckdb/main/capi/capi_internal.hpp"

using duckdb::Connection;
using duckdb::DuckDB;
using duckdb::EnumUtil;
using duckdb::MetricsType;
using duckdb::optional_ptr;
using duckdb::ProfilingNode;

duckdb_profiling_info duckdb_get_profiling_info(duckdb_connection connection) {
	if (!connection) {
		return nullptr;
	}
	Connection *conn = reinterpret_cast<Connection *>(connection);
	optional_ptr<ProfilingNode> profiling_info;
	try {
		profiling_info = conn->GetProfilingTree();
	} catch (std::exception &ex) {
		return nullptr;
	}

	ProfilingNode *profiling_info_ptr = profiling_info.get();
	return reinterpret_cast<duckdb_profiling_info>(profiling_info_ptr);
}

const char *duckdb_profiling_info_get_value(duckdb_profiling_info info, const char *key) {
	if (!info) {
		return nullptr;
	}
	auto &node = *reinterpret_cast<duckdb::ProfilingNode *>(info);
	auto &profiling_info = node.GetProfilingInfo();
	auto key_enum = EnumUtil::FromString<MetricsType>(duckdb::StringUtil::Upper(key));
	if (!profiling_info.Enabled(key_enum)) {
		return nullptr;
	}
	return strdup(profiling_info.GetMetricAsString(key_enum).c_str());
}

idx_t duckdb_profiling_info_get_child_count(duckdb_profiling_info info) {
	if (!info) {
		return 0;
	}
	auto &node = *reinterpret_cast<duckdb::ProfilingNode *>(info);
	return node.GetChildCount();
}

duckdb_profiling_info duckdb_profiling_info_get_child(duckdb_profiling_info info, idx_t index) {
	if (!info) {
		return nullptr;
	}
	auto &node = *reinterpret_cast<duckdb::ProfilingNode *>(info);
	if (index >= node.GetChildCount()) {
		return nullptr;
	}

	ProfilingNode *profiling_info_ptr = node.GetChild(index).get();
	return reinterpret_cast<duckdb_profiling_info>(profiling_info_ptr);
}

const char *duckdb_profiling_info_get_name(duckdb_profiling_info info) {
	if (!info) {
		return nullptr;
	}
	auto &node = *reinterpret_cast<duckdb::ProfilingNode *>(info);
	if (node.node_type != duckdb::ProfilingNodeType::OPERATOR) {
		return nullptr;
	}
	auto &op_node = node.Cast<duckdb::OperatorProfilingNode>();
	return strdup(op_node.name.c_str());
}

const char *duckdb_profiling_info_get_query(duckdb_profiling_info info) {
	if (!info) {
		return nullptr;
	}
	auto &node = *reinterpret_cast<duckdb::ProfilingNode *>(info);
	if (node.node_type != duckdb::ProfilingNodeType::QUERY_ROOT) {
		return nullptr;
	}
	auto &query_node = node.Cast<duckdb::QueryProfilingNode>();
	return strdup(query_node.query.c_str());
}
