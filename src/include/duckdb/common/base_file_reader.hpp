//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/base_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

struct MultiFileReaderColumnDefinition {
public:
	MultiFileReaderColumnDefinition(const string &name, const LogicalType &type) : name(name), type(type) {
	}

	MultiFileReaderColumnDefinition(const MultiFileReaderColumnDefinition &other)
	    : name(other.name), type(other.type), children(other.children),
	      default_expression(other.default_expression ? other.default_expression->Copy() : nullptr),
	      identifier(other.identifier) {
	}

	MultiFileReaderColumnDefinition &operator=(const MultiFileReaderColumnDefinition &other) {
		if (this != &other) {
			name = other.name;
			type = other.type;
			children = other.children;
			default_expression = other.default_expression ? other.default_expression->Copy() : nullptr;
			identifier = other.identifier;
		}
		return *this;
	}

public:
	static vector<MultiFileReaderColumnDefinition> ColumnsFromNamesAndTypes(const vector<string> &names,
	                                                                        const vector<LogicalType> &types) {
		vector<MultiFileReaderColumnDefinition> columns;
		D_ASSERT(names.size() == types.size());
		for (idx_t i = 0; i < names.size(); i++) {
			auto &name = names[i];
			auto &type = types[i];
			columns.emplace_back(name, type);
		}
		return columns;
	}

	static void ExtractNamesAndTypes(const vector<MultiFileReaderColumnDefinition> &columns, vector<string> &names,
	                                 vector<LogicalType> &types) {
		D_ASSERT(names.empty());
		D_ASSERT(types.empty());
		for (auto &column : columns) {
			names.push_back(column.name);
			types.push_back(column.type);
		}
	}

	int32_t GetIdentifierFieldId() const {
		D_ASSERT(!identifier.IsNull());
		D_ASSERT(identifier.type().id() == LogicalTypeId::INTEGER);
		return identifier.GetValue<int32_t>();
	}

	string GetIdentifierName() const {
		if (identifier.IsNull()) {
			// No identifier was provided, assume the name as the identifier
			return name;
		}
		D_ASSERT(identifier.type().id() == LogicalTypeId::VARCHAR);
		return identifier.GetValue<string>();
	}

	Value GetDefaultValue() const {
		D_ASSERT(default_expression);
		if (default_expression->type != ExpressionType::VALUE_CONSTANT) {
			throw NotImplementedException("Default expression that isn't constant is not supported yet");
		}
		auto &constant_expr = default_expression->Cast<ConstantExpression>();
		return constant_expr.value;
	}

public:
	string name;
	LogicalType type;
	vector<MultiFileReaderColumnDefinition> children;
	unique_ptr<ParsedExpression> default_expression;

	//! Either the field_id or the name to map on
	Value identifier;
};

struct MultiFileFilterEntry {
	idx_t index = DConstants::INVALID_INDEX;
	bool is_constant = false;
};

struct MultiFileConstantEntry {
	MultiFileConstantEntry(idx_t column_id, Value value_p) : column_id(column_id), value(std::move(value_p)) {
	}
	//! The (global) column id to apply the constant value to
	idx_t column_id;
	//! The constant value
	Value value;
};

struct MultiFileReaderData {
	//! The column ids to read from the file
	vector<idx_t> column_ids;
	//! The column indexes to read from the file
	vector<ColumnIndex> column_indexes;
	//! The mapping of column id -> result column id
	//! The result chunk will be filled as follows: chunk.data[column_mapping[i]] = ReadColumn(column_ids[i]);
	vector<idx_t> column_mapping;
	//! Whether or not there are no columns to read. This can happen when a file only consists of constants
	bool empty_columns = false;
	//! Filters can point to either (1) local columns in the file, or (2) constant values in the `constant_map`
	//! This map specifies where the to-be-filtered value can be found
	vector<MultiFileFilterEntry> filter_map;
	//! The set of table filters
	optional_ptr<TableFilterSet> filters;
	//! The constants that should be applied at the various positions
	vector<MultiFileConstantEntry> constant_map;
	//! Map of column_id -> cast, used when reading multiple files when files have diverging types
	//! for the same column
	unordered_map<column_t, LogicalType> cast_map;
	//! (Optionally) The MultiFileReader-generated metadata corresponding to the currently read file
	optional_idx file_list_idx;
};

class BaseFileReader {
public:
	explicit BaseFileReader(string file_name_p) : file_name(std::move(file_name_p)) {}
	DUCKDB_API virtual ~BaseFileReader() = default;

	string file_name;
	vector<MultiFileReaderColumnDefinition> columns;
	MultiFileReaderData reader_data;
	//! Table column names - set when using COPY tbl FROM file.parquet
	vector<string> table_columns;

public:
	const vector<MultiFileReaderColumnDefinition> &GetColumns() {
		return columns;
	}
	const string &GetFileName() {
		return file_name;
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};


} // namespace duckdb
