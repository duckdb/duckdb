#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct GenericCopyOption {
	string name;
	vector<Value> children; // Default value

	GenericCopyOption() {

	};

	GenericCopyOption(const string &name_p, const Value &value) : name(name_p) {
		children.push_back(value);
	}
};

} // namespace duckdb
