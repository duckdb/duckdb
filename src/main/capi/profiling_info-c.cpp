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

duckdb_value duckdb_profiling_info_get_value(duckdb_profiling_info info, const char *key) {
	if (!info) {
		return nullptr;
	}
	auto &node = *reinterpret_cast<duckdb::ProfilingNode *>(info);
	auto &profiling_info = node.GetProfilingInfo();
	auto key_enum = EnumUtil::FromString<MetricsType>(duckdb::StringUtil::Upper(key));
	if (!profiling_info.Enabled(profiling_info.settings, key_enum)) {
		return nullptr;
	}

	auto str = profiling_info.GetMetricAsString(key_enum);
	return duckdb_create_varchar_length(str.c_str(), strlen(str.c_str()));
}

duckdb_value duckdb_profiling_info_get_metrics(duckdb_profiling_info info) {
	if (!info) {
		return nullptr;
	}

	auto &node = *reinterpret_cast<duckdb::ProfilingNode *>(info);
	auto &profiling_info = node.GetProfilingInfo();

	duckdb::unordered_map<duckdb::string, duckdb::string> metrics_map;
	for (const auto &metric : profiling_info.metrics) {
		auto key = EnumUtil::ToString(metric.first);
		if (!profiling_info.Enabled(profiling_info.settings, metric.first)) {
			continue;
		}

		if (key == EnumUtil::ToString(MetricsType::OPERATOR_TYPE)) {
			auto type = duckdb::PhysicalOperatorType(metric.second.GetValue<uint8_t>());
			metrics_map[key] = EnumUtil::ToString(type);
		} else {
			metrics_map[key] = metric.second.ToString();
		}
	}

	auto map = duckdb::Value::MAP(metrics_map);
	return reinterpret_cast<duckdb_value>(new duckdb::Value(map));
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
