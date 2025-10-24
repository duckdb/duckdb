//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/open_file_info.hpp"
#include <numeric>

namespace duckdb {

enum class MultiFileFileState : uint8_t { UNOPENED, OPENING, OPEN, SKIPPED, CLOSED };

class DeleteFilter {
public:
	virtual ~DeleteFilter() = default;

public:
	virtual idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) = 0;
};

struct HivePartitioningIndex {
	HivePartitioningIndex(string value, idx_t index);

	string value;
	idx_t index;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static HivePartitioningIndex Deserialize(Deserializer &deserializer);
};

struct MultiFileColumnDefinition {
public:
	MultiFileColumnDefinition(const string &name, const LogicalType &type) : name(name), type(type) {
	}

	MultiFileColumnDefinition(const MultiFileColumnDefinition &other)
	    : name(other.name), type(other.type), children(other.children),
	      default_expression(other.default_expression ? other.default_expression->Copy() : nullptr),
	      identifier(other.identifier) {
	}

	MultiFileColumnDefinition &operator=(const MultiFileColumnDefinition &other) {
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
	static MultiFileColumnDefinition CreateFromNameAndType(const string &name, const LogicalType &type) {
		MultiFileColumnDefinition result(name, type);
		if (type.id() == LogicalTypeId::STRUCT) {
			// recursively create for children
			for (auto &child_entry : StructType::GetChildTypes(type)) {
				result.children.push_back(CreateFromNameAndType(child_entry.first, child_entry.second));
			}
		}
		return result;
	}

	static vector<MultiFileColumnDefinition> ColumnsFromNamesAndTypes(const vector<string> &names,
	                                                                  const vector<LogicalType> &types) {
		vector<MultiFileColumnDefinition> columns;
		D_ASSERT(names.size() == types.size());
		for (idx_t i = 0; i < names.size(); i++) {
			auto &name = names[i];
			auto &type = types[i];
			columns.push_back(CreateFromNameAndType(name, type));
		}
		return columns;
	}

	static void ExtractNamesAndTypes(const vector<MultiFileColumnDefinition> &columns, vector<string> &names,
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
	vector<MultiFileColumnDefinition> children;
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
	operator idx_t() const { // NOLINT: allow implicit conversion
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
	operator idx_t() const { // NOLINT: allow implicit conversion
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

} // namespace duckdb
