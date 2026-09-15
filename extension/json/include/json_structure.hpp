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
class MutableDateFormatMap;

//! Sticky ISO-8601 offset observation across refinement vectors and parallel merges (issue #14919)
enum class TimestampOffsetState : uint8_t { UNKNOWN, WITH_OFFSET, WITHOUT_OFFSET, MIXED };

struct JSONStructureNode {
public:
	JSONStructureNode();
	JSONStructureNode(const char *key_ptr, const size_t key_len);
	JSONStructureNode(yyjson_val *key_p, yyjson_val *val_p, bool ignore_errors, bool detect_geojson);

	//! Disable copy constructors
	JSONStructureNode(const JSONStructureNode &other) = delete;
	JSONStructureNode &operator=(const JSONStructureNode &) = delete;
	//! Enable move constructors
	JSONStructureNode(JSONStructureNode &&other) noexcept;
	JSONStructureNode &operator=(JSONStructureNode &&) noexcept;

	JSONStructureDescription &GetOrCreateDescription(LogicalTypeId type);

	bool ContainsVarchar() const;
	void InitializeCandidateTypes(idx_t max_depth, bool convert_strings_to_integers,
	                              bool user_specified_timestamp_format = false, idx_t depth = 0);
	void RefineCandidateTypes(yyjson_val *vals[], idx_t val_count, Vector &string_vector, ArenaAllocator &allocator,
	                          MutableDateFormatMap &date_format_map, bool user_specified_timestamp_format = false);

private:
	void RefineCandidateTypesArray(yyjson_val *vals[], idx_t val_count, Vector &string_vector,
	                               ArenaAllocator &allocator, MutableDateFormatMap &date_format_map,
	                               bool user_specified_timestamp_format);
	void RefineCandidateTypesObject(yyjson_val *vals[], idx_t val_count, Vector &string_vector,
	                                ArenaAllocator &allocator, MutableDateFormatMap &date_format_map,
	                                bool user_specified_timestamp_format);
	void RefineCandidateTypesString(yyjson_val *vals[], idx_t val_count, Vector &string_vector,
	                                MutableDateFormatMap &date_format_map, bool user_specified_timestamp_format);
	void EliminateCandidateTypes(idx_t vec_count, Vector &string_vector, MutableDateFormatMap &date_format_map,
	                             bool user_specified_timestamp_format);
	bool EliminateCandidateFormats(idx_t vec_count, Vector &string_vector, const Vector &result_vector,
	                               MutableDateFormatMap &date_format_map);

public:
	unique_ptr<string> key;
	bool initialized = false;
	vector<JSONStructureDescription> descriptions;
	idx_t count;
	idx_t null_count;
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
	JSONStructureNode &GetOrCreateChild(const char *key_ptr, size_t key_size);
	JSONStructureNode &GetOrCreateChild(yyjson_val *key, yyjson_val *val, bool ignore_errors, bool detect_geojson);

public:
	//! Type of this description
	LogicalTypeId type = LogicalTypeId::INVALID;

	//! Map to children and children
	json_key_map_t<idx_t> key_map;
	vector<JSONStructureNode> children;

	//! Candidate types (if auto-detecting and type == LogicalTypeId::VARCHAR)
	vector<LogicalTypeId> candidate_types;

	//! Whether any UBIGINT value exceeds the BIGINT range (i.e., >= 2^63)
	bool has_large_ubigint = false;

	//! MIXED clears the timestamp candidates (VARCHAR): neither TIMESTAMPTZ nor TIMESTAMP is safe
	TimestampOffsetState timestamp_offset_state = TimestampOffsetState::UNKNOWN;
};

struct JSONStructure {
public:
	static void ExtractStructure(yyjson_val *val, JSONStructureNode &node, bool ignore_errors, bool detect_geojson);
	static void MergeNodes(JSONStructureNode &merged, const JSONStructureNode &node);
	static LogicalType StructureToType(ClientContext &context, const JSONStructureNode &node, idx_t max_depth,
	                                   double field_appearance_threshold, idx_t map_inference_threshold,
	                                   idx_t depth = 0, const LogicalType &null_type = LogicalType::JSON());
};

} // namespace duckdb
