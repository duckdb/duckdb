//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_structure.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "json_common.hpp"

namespace duckdb {

struct JSONStructureDescription;

struct JSONStructureNode {
public:
	JSONStructureNode();
	JSONStructureNode(yyjson_val *key_p, yyjson_val *val_p);

	JSONStructureDescription &GetOrCreateDescription(LogicalTypeId type);

public:
	string key;
	vector<JSONStructureDescription> descriptions;
};

struct JSONStructureDescription {
public:
	explicit JSONStructureDescription(LogicalTypeId type_p);

	JSONStructureNode &GetOrCreateChild();
	JSONStructureNode &GetOrCreateChild(yyjson_val *key, yyjson_val *val);

public:
	LogicalTypeId type = LogicalTypeId::INVALID;
	json_key_map_t<idx_t> key_map;
	vector<JSONStructureNode> children;
};

struct JSONStructure {
public:
	static void ExtractStructure(yyjson_val *val, JSONStructureNode &node);
	static LogicalType StructureToType(ClientContext &context, const JSONStructureNode &node,
	                                   const idx_t max_unnest_level, idx_t current_level = 0);
};

} // namespace duckdb
