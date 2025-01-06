#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

struct ExtensionTypeInfo {
	vector<Value> modifiers;
	unordered_map<string, Value> properties;

public:
	void Serialize(Serializer &serializer) const;
	static unique_ptr<ExtensionTypeInfo> Deserialize(Deserializer &source);
	bool Equals(optional_ptr<ExtensionTypeInfo> other_p) const;
};

} // namespace duckdb
