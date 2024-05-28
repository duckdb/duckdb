//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/arrow_duck_schema.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/table/arrow/arrow_type_info.hpp"

namespace duckdb {

class ArrowType {
public:
	//! From a DuckDB type
	explicit ArrowType(LogicalType type_p, unique_ptr<ArrowTypeInfo> type_info = nullptr)
	    : type(std::move(type_p)), type_info(std::move(type_info)) {
	}

public:
	LogicalType GetDuckType(bool use_dictionary = false) const;

	void SetDictionary(unique_ptr<ArrowType> dictionary);
	bool HasDictionary() const;
	const ArrowType &GetDictionary() const;

	bool RunEndEncoded() const;
	void SetRunEndEncoded();

	template <class T>
	const T &GetTypeInfo() const {
		return type_info->Cast<T>();
	}

private:
	LogicalType type;
	//! Hold the optional type if the array is a dictionary
	unique_ptr<ArrowType> dictionary_type;
	//! Is run-end-encoded
	bool run_end_encoded = false;
	unique_ptr<ArrowTypeInfo> type_info;
};

using arrow_column_map_t = unordered_map<idx_t, unique_ptr<ArrowType>>;

struct ArrowTableType {
public:
	void AddColumn(idx_t index, unique_ptr<ArrowType> type);
	const arrow_column_map_t &GetColumns() const;

private:
	arrow_column_map_t arrow_convert_data;
};

} // namespace duckdb
