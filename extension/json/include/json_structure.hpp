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

struct LogicalTypeIdHash {
	inline std::size_t operator()(const LogicalTypeId &id) const {
		return (size_t)id;
	}
};

struct JSONStructureNode {
public:
	JSONStructureNode();
	JSONStructureNode(yyjson_val *key_p, yyjson_val *val_p);

	JSONStructureDescription &GetOrCreateDescription(LogicalTypeId type);

	bool ContainsVarchar() const;
	void InitializeCandidateTypes(const idx_t max_depth, idx_t depth = 0);
	void RefineCandidateTypes(yyjson_val *vals[], idx_t count, Vector &string_vector, ArenaAllocator &allocator);

private:
	void RefineCandidateTypesArray(yyjson_val *vals[], idx_t count, Vector &string_vector, ArenaAllocator &allocator);
	void RefineCandidateTypesObject(yyjson_val *vals[], idx_t count, Vector &string_vector, ArenaAllocator &allocator);
	void RefineCandidateTypesString(yyjson_val *vals[], idx_t count, Vector &string_vector);
	void EliminateCandidateTypes(idx_t count, Vector &string_vector);
	void EliminateCandidateFormats(idx_t count, Vector &string_vector, Vector &result_vector);

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
	//! Type of this description
	LogicalTypeId type = LogicalTypeId::INVALID;

	//! Map to children and children
	json_key_map_t<idx_t> key_map;
	vector<JSONStructureNode> children;

	//! Candidate types (if auto-detecting and type == LogicalTypeId::VARCHAR)
	vector<LogicalTypeId> candidate_types;
	//! Format candidates (if auto-detecting)
	unordered_map<LogicalTypeId, vector<const char *>, LogicalTypeIdHash> format_templates;
};

struct JSONStructure {
public:
	static void ExtractStructure(yyjson_val *val, JSONStructureNode &node);
	static LogicalType StructureToType(ClientContext &context, const JSONStructureNode &node, const idx_t max_depth,
	                                   idx_t depth = 0);
};

} // namespace duckdb
