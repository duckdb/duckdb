//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/arrow/arrow_duck_schema.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/validity_mask.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/table/arrow/arrow_type_info.hpp"
#include "duckdb/common/arrow/arrow.hpp"

namespace duckdb {
struct DBConfig;

struct ArrowArrayScanState;

typedef void (*cast_t)(ClientContext &context, Vector &source, Vector &result, idx_t count);
enum ArrowTypeEnum { BASE = 0, EXTENSION = 1 };

class ArrowType {
public:
	//! From a DuckDB type
	explicit ArrowType(LogicalType type_p, unique_ptr<ArrowTypeInfo> type_info = nullptr)
	    : type(std::move(type_p)), type_info(std::move(type_info)) {
	}
	explicit ArrowType(string error_message_p, bool not_implemented_p = false)
	    : type(LogicalTypeId::INVALID), type_info(nullptr), error_message(std::move(error_message_p)),
	      not_implemented(not_implemented_p) {
	}

public:
	LogicalType GetDuckType(bool use_dictionary = false) const;

	void SetDictionary(shared_ptr<ArrowType> dictionary);
	bool HasDictionary() const;
	const ArrowType &GetDictionary() const;

	bool RunEndEncoded() const;
	void SetRunEndEncoded();

	template <class T>
	const T &GetTypeInfo() const {
		return type_info->Cast<T>();
	}
	void ThrowIfInvalid() const;

	static shared_ptr<ArrowType> GetTypeFromFormat(DBConfig &config, ArrowSchema &schema, string &format);

	static shared_ptr<ArrowType> GetTypeFromSchema(DBConfig &config, ArrowSchema &schema);

	static shared_ptr<ArrowType> CreateListType(DBConfig &config, ArrowSchema &child, ArrowVariableSizeType size_type,
	                                            bool view);

	static shared_ptr<ArrowType> GetArrowLogicalType(DBConfig &config, ArrowSchema &schema);

	bool IsExtension() const;

protected:
	ArrowTypeEnum type_enum = BASE;

	LogicalType type;
	//! Hold the optional type if the array is a dictionary
	shared_ptr<ArrowType> dictionary_type;
	//! Is run-end-encoded
	bool run_end_encoded = false;
	unique_ptr<ArrowTypeInfo> type_info;
	//! Error message in case of an invalid type (i.e., from an unsupported extension)
	string error_message;
	//! In case of an error do we throw not implemented?
	bool not_implemented = false;
};

class ArrowExtensionType : public ArrowType {
public:
	ArrowExtensionType(LogicalType type_p, LogicalType internal_type_p, cast_t arrow_to_duckdb, cast_t duckdb_to_arrow,
	                   unique_ptr<ArrowTypeInfo> type_info = nullptr)
	    : ArrowType(std::move(type_p), std::move(type_info)), arrow_to_duckdb(arrow_to_duckdb),
	      duckdb_to_arrow(duckdb_to_arrow), internal_type(std::move(internal_type_p)) {
		type_enum = EXTENSION;
	}

	explicit ArrowExtensionType(const LogicalType &type_p, unique_ptr<ArrowTypeInfo> type_info = nullptr)
	    : ArrowType(type_p, std::move(type_info)), internal_type(type_p) {
		type_enum = EXTENSION;
	}
	//! (Optional) Callback to function that converts an Arrow Array to a DuckDB Vector
	cast_t arrow_to_duckdb = nullptr;
	//! (Optional) Callback to function that converts an Arrow Array to a DuckDB Vector
	cast_t duckdb_to_arrow = nullptr;

	LogicalType GetInternalType() const;

	ArrowType GetInternalArrowType() const;

	//! This function returns possible extension types to given DuckDB types
	static unordered_map<idx_t, const shared_ptr<ArrowExtensionType>>
	GetExtensionTypes(ClientContext &context, const vector<LogicalType> &duckdb_types);

private:
	//! Internal type is a type that refers to the actual arrow format
	LogicalType internal_type;
};

using arrow_column_map_t = unordered_map<idx_t, shared_ptr<ArrowType>>;

struct ArrowTableType {
public:
	void AddColumn(idx_t index, shared_ptr<ArrowType> type);
	const arrow_column_map_t &GetColumns() const;

private:
	arrow_column_map_t arrow_convert_data;
};

} // namespace duckdb
