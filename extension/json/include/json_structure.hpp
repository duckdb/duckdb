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
	vector<JSONStructureNode> children;
};

struct JSONStructure {
public:
	static JSONStructureNode ExtractStructure(yyjson_val *val);
};

} // namespace duckdb
