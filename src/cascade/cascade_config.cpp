#include "duckdb/cascade/cascade_config.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"

#include <cstdlib>

namespace duckdb {

static bool EnvFlagSet(const char *name) {
	auto value = std::getenv(name);
	if (!value) {
		return false;
	}
	return value[0] != '\0' && value[0] != '0';
}

bool CascadeConfig::UseCascadeOptimizer() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE");
	return enabled;
}

bool CascadeConfig::KeepApply() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_KEEP_APPLY");
	return enabled;
}

bool CascadeConfig::RunDuckOptimizers() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_OPTIMIZE");
	return enabled;
}

bool CascadeConfig::ReorderGroupBy() {
	static const bool enabled = []() {
		auto value = std::getenv("DUCKDB_CASCADE_REORDER");
		if (!value) {
			return true;
		}
		return value[0] != '\0' && value[0] != '0';
	}();
	return enabled;
}

bool CascadeConfig::ReorderSemijoins() {
	static const bool enabled = []() {
		auto value = std::getenv("DUCKDB_CASCADE_REORDER_SEMIJOIN");
		if (!value) {
			return true;
		}
		return value[0] != '\0' && value[0] != '0';
	}();
	return enabled;
}

bool CascadeConfig::PushLocalAggregates() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_LOCAL_AGG");
	return enabled;
}

bool CascadeConfig::PullUpAggregates() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_AGG_PULLUP");
	return enabled;
}

bool CascadeConfig::PushDownAggregates() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_AGG_PUSHDOWN");
	return enabled;
}

bool CascadeConfig::IsDeclaredKey(const string &table_name, const string &column_name) {
	static const unordered_set<string> keys = []() {
		unordered_set<string> parsed;
		auto value = std::getenv("DUCKDB_CASCADE_KEYS");
		if (!value) {
			return parsed;
		}
		string current;
		for (const char *letter = value;; letter++) {
			if (*letter == ',' || *letter == '\0') {
				if (!current.empty()) {
					parsed.insert(StringUtil::Lower(current));
				}
				current.clear();
				if (*letter == '\0') {
					break;
				}
				continue;
			}
			current += *letter;
		}
		return parsed;
	}();
	if (keys.empty()) {
		return false;
	}
	return keys.find(StringUtil::Lower(table_name) + "." + StringUtil::Lower(column_name)) != keys.end();
}

bool CascadeConfig::FlattenFallback() {
	static const bool enabled = []() {
		auto value = std::getenv("DUCKDB_CASCADE_FLATTEN_FALLBACK");
		if (!value) {
			return true;
		}
		return value[0] != '\0' && value[0] != '0';
	}();
	return enabled;
}

bool CascadeConfig::PrintPlans() {
	static const bool enabled = []() {
		auto value = std::getenv("DUCKDB_CASCADE_PRINT");
		if (!value) {
			// there is no other way to see the rewrite, so print by default
			return true;
		}
		return value[0] != '\0' && value[0] != '0';
	}();
	return enabled;
}

} // namespace duckdb
