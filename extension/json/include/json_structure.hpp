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
struct DateFormatMap;
struct StrpTimeFormat;

struct JSONStructureNode {
public:
	JSONStructureNode();
	JSONStructureNode(yyjson_val *key_p, yyjson_val *val_p);

	//! Disable copy constructors
	JSONStructureNode(const JSONStructureNode &other) = delete;
	JSONStructureNode &operator=(const JSONStructureNode &) = delete;
	//! Enable move constructors
	JSONStructureNode(JSONStructureNode &&other) noexcept;
	JSONStructureNode &operator=(JSONStructureNode &&) noexcept;

	JSONStructureDescription &GetOrCreateDescription(LogicalTypeId type);

	bool ContainsVarchar() const;
	void InitializeCandidateTypes(const idx_t max_depth, idx_t depth = 0);
	void RefineCandidateTypes(yyjson_val *vals[], idx_t count, Vector &string_vector, ArenaAllocator &allocator,
	                          DateFormatMap &date_format_map);

private:
	void RefineCandidateTypesArray(yyjson_val *vals[], idx_t count, Vector &string_vector, ArenaAllocator &allocator,
	                               DateFormatMap &date_format_map);
	void RefineCandidateTypesObject(yyjson_val *vals[], idx_t count, Vector &string_vector, ArenaAllocator &allocator,
	                                DateFormatMap &date_format_map);
	void RefineCandidateTypesString(yyjson_val *vals[], idx_t count, Vector &string_vector,
	                                DateFormatMap &date_format_map);
	void EliminateCandidateTypes(idx_t count, Vector &string_vector, DateFormatMap &date_format_map);
	bool EliminateCandidateFormats(idx_t count, Vector &string_vector, Vector &result_vector,
	                               vector<StrpTimeFormat> &formats);

public:
	duckdb::unique_ptr<string> key;
	bool initialized = false;
	vector<JSONStructureDescription> descriptions;
};

struct JSONStructureDescription {
public:
	explicit JSONStructureDescription(LogicalTypeId type_p);
	//! Disable copy constructors
	JSONStructureDescription(const JSONStructureDescription &other) = delete;
	JSONStructureDescription &operator=(const JSONStructureDescription &) = delete;
	//! Enable move constructors
	JSONStructureDescription(JSONStructureDescription &&other) noexcept;
	JSONStructureDescription &operator=(JSONStructureDescription &&) noexcept;

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
};

struct JSONStructure {
public:
	static void ExtractStructure(yyjson_val *val, JSONStructureNode &node);
	static LogicalType StructureToType(ClientContext &context, const JSONStructureNode &node, const idx_t max_depth,
	                                   idx_t depth = 0);
};

} // namespace duckdb
