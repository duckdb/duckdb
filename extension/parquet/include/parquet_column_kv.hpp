#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct ColumnKV;
class Deserializer;
class Serializer;

struct ChildColumnKV {
	ChildColumnKV();
	ChildColumnKV Copy() const;
	//! Look up a child carrier by (case-insensitive) column name; null if absent
	optional_ptr<const ColumnKV> GetChild(const string &name) const;
	unique_ptr<case_insensitive_map_t<ColumnKV>> children;

	void Serialize(Serializer &serializer) const;
	static ChildColumnKV Deserialize(Deserializer &source);
};

struct ColumnKV {
	ColumnKV();
	ColumnKV Copy() const;
	//! Leaf key/value metadata (empty for nesting nodes)
	vector<pair<string, string>> metadata;
	ChildColumnKV children;

	void Serialize(Serializer &serializer) const;
	static ColumnKV Deserialize(Deserializer &source);

public:
	static void GetColumnKV(const Value &kv_value, ChildColumnKV &kv,
	                        const case_insensitive_map_t<LogicalType> &name_to_type_map);
};

} // namespace duckdb
