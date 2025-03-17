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

//! fwd declare
struct MultiFileCastMap;
struct MultiFileFilterEntry;

struct MultiFileLocalColumnId {
	friend struct MultiFileCastMap;

public:
	explicit MultiFileLocalColumnId(column_t column_id) : column_id(column_id) {
	}

public:
	operator idx_t() { // NOLINT: allow implicit conversion
		return column_id;
	}
	idx_t GetId() const {
		return column_id;
	}

private:
	column_t column_id;
};

//! fwd declare
template <class T>
struct MultiFileLocalColumnIds;
struct MultiFileGlobalColumnIds;
struct MultiFileColumnMapping;
struct MultiFileFilterMap;
struct MultiFileConstantMap;

struct MultiFileLocalIndex {
	//! these are allowed to access the index
	template <class T>
	friend struct MultiFileLocalColumnIds;
	friend struct MultiFileColumnMapping;
	friend struct MultiFileFilterEntry;

public:
	explicit MultiFileLocalIndex(idx_t index) : index(index) {
	}

public:
	operator idx_t() { // NOLINT: allow implicit conversion
		return index;
	}
	idx_t GetIndex() const {
		return index;
	}

private:
	idx_t index;
};

struct MultiFileGlobalIndex {
	friend struct MultiFileGlobalColumnIds;
	friend struct MultiFileFilterMap;

public:
	explicit MultiFileGlobalIndex(idx_t index) : index(index) {
	}

public:
	operator idx_t() { // NOLINT: allow implicit conversion
		return index;
	}
	idx_t GetIndex() const {
		return index;
	}

private:
	idx_t index;
};

struct MultiFileConstantEntry {
	MultiFileConstantEntry(MultiFileGlobalIndex column_idx, Value value_p)
	    : column_idx(column_idx), value(std::move(value_p)) {
	}
	//! The (global) column idx to apply the constant value to
	MultiFileGlobalIndex column_idx;
	//! The constant value
	Value value;
};

//! index used to access the constant map
struct MultiFileConstantMapIndex {
	friend struct MultiFileConstantMap;
	friend struct MultiFileFilterEntry;

public:
	explicit MultiFileConstantMapIndex(idx_t index) : index(index) {
	}

public:
private:
	idx_t index;
};

struct MultiFileFilterEntry {
public:
	MultiFileFilterEntry() : is_set(false), is_constant(false), index(DConstants::INVALID_INDEX) {
	}

public:
	void Set(MultiFileConstantMapIndex constant_index) {
		is_set = true;
		is_constant = true;
		index = constant_index.index;
	}
	void Set(MultiFileLocalIndex local_index) {
		is_set = true;
		is_constant = false;
		index = local_index.index;
	}

	bool IsSet() const {
		return is_set;
	}
	bool IsConstant() const {
		D_ASSERT(is_set);
		return is_constant;
	}

	MultiFileConstantMapIndex GetConstantIndex() const {
		D_ASSERT(is_set);
		D_ASSERT(is_constant);
		return MultiFileConstantMapIndex(index);
	}
	MultiFileLocalIndex GetLocalIndex() const {
		D_ASSERT(is_set);
		D_ASSERT(!is_constant);
		return MultiFileLocalIndex(index);
	}

private:
	bool is_set;
	bool is_constant;
	//! ConstantMapIndex if 'is_constant' else this is a LocalIndex
	idx_t index;
};

template <class T>
struct MultiFileLocalColumnIds {
public:
	void push_back(T column_id) { // NOLINT: matching name of std
		column_ids.push_back(column_id);
	}
	template <typename... Args>
	void emplace_back(Args &&...args) {
		column_ids.emplace_back(std::forward<Args>(args)...);
	}
	const T &operator[](MultiFileLocalIndex index) {
		return column_ids[index.index];
	}
	bool empty() const { // NOLINT: matching name of std
		return column_ids.empty();
	}
	idx_t size() const { // NOLINT: matching name of std
		return column_ids.size();
	}

private:
	vector<T> column_ids;
};

struct MultiFileColumnMapping {
public:
	void push_back(MultiFileGlobalIndex global_index) { // NOLINT: matching name of std
		column_mapping.push_back(global_index);
	}
	const MultiFileGlobalIndex &operator[](MultiFileLocalIndex local_index) {
		return column_mapping[local_index.index];
	}
	idx_t size() const { // NOLINT: matching name of std
		return column_mapping.size();
	}

private:
	vector<MultiFileGlobalIndex> column_mapping;
};

struct MultiFileFilterMap {
public:
	void push_back(const MultiFileFilterEntry &filter_entry) { // NOLINT: matching name of std
		filter_map.push_back(filter_entry);
	}
	MultiFileFilterEntry &operator[](MultiFileGlobalIndex global_index) {
		return filter_map[global_index.index];
	}
	void resize(idx_t size) { // NOLINT: matching name of std
		filter_map.resize(size);
	}

private:
	vector<MultiFileFilterEntry> filter_map;
};

struct MultiFileConstantMap {
public:
	using iterator = vector<MultiFileConstantEntry>::iterator;
	using const_iterator = vector<MultiFileConstantEntry>::const_iterator;

public:
	template <typename... Args>
	void Add(Args &&...args) {
		constant_map.emplace_back(std::forward<Args>(args)...);
	}
	const MultiFileConstantEntry &operator[](MultiFileConstantMapIndex constant_index) {
		return constant_map[constant_index.index];
	}
	idx_t size() const { // NOLINT: matching name of std
		return constant_map.size();
	}
	// Iterator support
	iterator begin() {
		return constant_map.begin();
	}
	iterator end() {
		return constant_map.end();
	}
	const_iterator begin() const {
		return constant_map.begin();
	}
	const_iterator end() const {
		return constant_map.end();
	}

private:
	vector<MultiFileConstantEntry> constant_map;
};

struct MultiFileCastMap {
public:
	using iterator = unordered_map<column_t, LogicalType>::iterator;
	using const_iterator = unordered_map<column_t, LogicalType>::const_iterator;

public:
	LogicalType &operator[](MultiFileLocalColumnId column_id) {
		return cast_map[column_id.column_id];
	}
	// Iterator support
	iterator begin() {
		return cast_map.begin();
	}
	iterator end() {
		return cast_map.end();
	}
	const_iterator begin() const {
		return cast_map.begin();
	}
	const_iterator end() const {
		return cast_map.end();
	}
	iterator find(MultiFileLocalColumnId column_id) {
		return cast_map.find(column_id.column_id);
	}
	bool empty() const {
		return cast_map.empty();
	}

private:
	unordered_map<column_t, LogicalType> cast_map;
};

struct MultiFileReaderData {
	//! The column ids to read from the file
	MultiFileLocalColumnIds<MultiFileLocalColumnId> column_ids;
	//! The column indexes to read from the file
	vector<ColumnIndex> column_indexes;
	//! The mapping of column id -> result column id
	//! The result chunk will be filled as follows: chunk.data[column_mapping[i]] = ReadColumn(column_ids[i]);
	MultiFileColumnMapping column_mapping;
	//! Whether or not there are no columns to read. This can happen when a file only consists of constants
	bool empty_columns = false;
	//! Filters can point to either (1) local columns in the file, or (2) constant values in the `constant_map`
	//! This map specifies where the to-be-filtered value can be found
	MultiFileFilterMap filter_map;
	//! The set of table filters
	optional_ptr<TableFilterSet> filters;
	//! The constants that should be applied at the various positions
	MultiFileConstantMap constant_map;
	//! Map of (local) column_id -> cast, used when reading multiple files when files have diverging types
	//! for the same column
	MultiFileCastMap cast_map;
	//! (Optionally) The MultiFileReader-generated metadata corresponding to the currently read file
	optional_idx file_list_idx;
};

//! Parent class of single-file readers - this must be inherited from for readers implementing the MultiFileReader
//! interface
class BaseFileReader {
public:
	explicit BaseFileReader(string file_name_p) : file_name(std::move(file_name_p)) {
	}
	DUCKDB_API virtual ~BaseFileReader() = default;

	string file_name;
	vector<MultiFileReaderColumnDefinition> columns;
	MultiFileReaderData reader_data;
	//! Table column names - set when using COPY tbl FROM file.parquet
	vector<string> table_columns;

public:
	const vector<MultiFileReaderColumnDefinition> &GetColumns() const {
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

//! Parent class of file reader options
class BaseFileReaderOptions {
public:
	DUCKDB_API virtual ~BaseFileReaderOptions() = default;

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

//! Parent class of union data - used for the UNION BY NAME. This is effectively a cache that is kept around per file
//! that can be used to speed up opening the same file again afterwards - to avoid doing double work.
class BaseUnionData {
public:
	explicit BaseUnionData(string file_name_p) : file_name(std::move(file_name_p)) {
	}
	DUCKDB_API virtual ~BaseUnionData() = default;

	string file_name;
	shared_ptr<BaseFileReader> reader;
	vector<string> names;
	vector<LogicalType> types;

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
