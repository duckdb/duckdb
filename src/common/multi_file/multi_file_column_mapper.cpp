#include "duckdb/common/multi_file/multi_file_column_mapper.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/filter/list.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

MultiFileColumnMapper::MultiFileColumnMapper(ClientContext &context, MultiFileReader &multi_file_reader,
                                             MultiFileReaderData &reader_data,
                                             const vector<MultiFileColumnDefinition> &global_columns,
                                             const vector<ColumnIndex> &global_column_ids,
                                             optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
                                             const virtual_column_map_t &virtual_columns)
    : context(context), multi_file_reader(multi_file_reader), multi_file_list(multi_file_list),
      reader_data(reader_data), global_columns(global_columns), global_column_ids(global_column_ids),
      global_filters(filters), virtual_columns(virtual_columns) {
}

struct MultiFileIndexMapping {
public:
	explicit MultiFileIndexMapping(idx_t index) : index(index) {
	}

public:
	idx_t index;
	unordered_map<idx_t, unique_ptr<MultiFileIndexMapping>> child_mapping;
};

//! The local to global conversion required for table filters on this column
enum class FilterConversionType { COPY_DIRECTLY, CAST_FILTER, CANNOT_CONVERT };

struct MultiFileColumnMap {
	MultiFileColumnMap(idx_t index, const LogicalType &local_type, const LogicalType &global_type)
	    : mapping(index), local_type(local_type), global_type(global_type),
	      filter_conversion(local_type == global_type ? FilterConversionType::COPY_DIRECTLY
	                                                  : FilterConversionType::CAST_FILTER) {
	}
	MultiFileColumnMap(MultiFileIndexMapping mapping_p, const LogicalType &local_type, const LogicalType &global_type,
	                   FilterConversionType filter_conversion)
	    : mapping(std::move(mapping_p)), local_type(local_type), global_type(global_type),
	      filter_conversion(filter_conversion) {
	}

	MultiFileIndexMapping mapping;
	const LogicalType &local_type;
	const LogicalType &global_type;
	FilterConversionType filter_conversion;
};

struct ResultColumnMapping {
	unordered_map<idx_t, MultiFileColumnMap> global_to_local;
	string error;

public:
	bool HasError() const {
		return !error.empty();
	}
};

struct ColumnMapResult {
	//! Contains the name of the local column that corresponds to this field
	Value column_map;
	unique_ptr<Expression> default_value;
	optional_ptr<const MultiFileColumnDefinition> local_column;
	unique_ptr<ColumnIndex> column_index;
	unique_ptr<MultiFileIndexMapping> mapping;
};

struct ColumnMapper {
	virtual ~ColumnMapper() = default;
	virtual unique_ptr<ColumnMapper> Create(const vector<MultiFileColumnDefinition> &columns) const = 0;
	virtual optional_idx Find(const MultiFileColumnDefinition &column) const = 0;
	virtual unique_ptr<Expression> GetDefaultExpression(const MultiFileColumnDefinition &column,
	                                                    bool is_root) const = 0;
	virtual idx_t MapCount() const = 0;
};

struct FieldIdMapper : public ColumnMapper {
	explicit FieldIdMapper(const vector<MultiFileColumnDefinition> &columns) {
		for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
			auto &column = columns[col_idx];
			if (column.identifier.IsNull()) {
				// Extra columns at the end will not have a field_id
				break;
			}
			auto field_id = column.GetIdentifierFieldId();
			field_id_map.emplace(field_id, MultiFileLocalColumnId(col_idx));
		}
	}

	unique_ptr<ColumnMapper> Create(const vector<MultiFileColumnDefinition> &columns) const override {
		return make_uniq<FieldIdMapper>(columns);
	}
	optional_idx Find(const MultiFileColumnDefinition &column) const override {
		D_ASSERT(!column.identifier.IsNull());
		auto entry = field_id_map.find(column.GetIdentifierFieldId());
		if (entry == field_id_map.end()) {
			return optional_idx();
		}
		return entry->second.GetId();
	}
	static unique_ptr<Expression> GetDefault(const MultiFileColumnDefinition &column) {
		auto &default_val = column.default_expression;
		if (!default_val) {
			throw InternalException("No default expression in FieldId Map");
		}
		if (default_val->type != ExpressionType::VALUE_CONSTANT) {
			throw NotImplementedException("Default expression that isn't constant is not supported yet");
		}
		auto &constant_expr = default_val->Cast<ConstantExpression>();
		// return only the expression
		return make_uniq<BoundConstantExpression>(constant_expr.value);
	}

	unique_ptr<Expression> GetDefaultExpression(const MultiFileColumnDefinition &column, bool is_root) const override {
		return GetDefault(column);
	}
	idx_t MapCount() const override {
		return field_id_map.size();
	}

private:
	unordered_map<int32_t, MultiFileLocalColumnId> field_id_map;
};

struct NameMapper : public ColumnMapper {
	NameMapper(MultiFileColumnMapper &mapper, const vector<MultiFileColumnDefinition> &columns) : mapper(mapper) {
		for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
			auto &column = columns[col_idx];
			name_map.emplace(column.name, MultiFileLocalColumnId(col_idx));
		}
	}

	unique_ptr<ColumnMapper> Create(const vector<MultiFileColumnDefinition> &columns) const override {
		return make_uniq<NameMapper>(mapper, columns);
	}
	optional_idx Find(const MultiFileColumnDefinition &column) const override {
		auto entry = name_map.find(column.GetIdentifierName());
		if (entry == name_map.end()) {
			return optional_idx();
		}
		return entry->second.GetId();
	}
	unique_ptr<Expression> GetDefaultExpression(const MultiFileColumnDefinition &column, bool is_root) const override {
		if (column.default_expression) {
			// we have an explicit default - return it
			return FieldIdMapper::GetDefault(column);
		}
		// no explicit default and no match
		if (is_root) {
			// no match found in a root column - throw an error
			mapper.ThrowColumnNotFoundError(column.GetIdentifierName());
		}
		// no match found in a struct field - replace with NULL
		return make_uniq<BoundConstantExpression>(Value(column.type));
	}

	idx_t MapCount() const override {
		return name_map.size();
	}

private:
	MultiFileColumnMapper &mapper;
	case_insensitive_map_t<MultiFileLocalColumnId> name_map;
};

void MultiFileColumnMapper::ThrowColumnNotFoundError(const string &global_column_name) const {
	auto &reader = *reader_data.reader;
	auto &local_columns = reader.GetColumns();

	string candidate_names;
	for (auto &column : local_columns) {
		if (!candidate_names.empty()) {
			candidate_names += ", ";
		}
		candidate_names += column.name;
	}
	auto &file_name = reader.GetFileName();
	throw InvalidInputException("Failed to read file \"%s\": schema mismatch in glob: column \"%s\" was read from "
	                            "the original file \"%s\", but could not be found in file \"%s\".\nCandidate names: "
	                            "%s\nIf you are trying to "
	                            "read files with different schemas, try setting union_by_name=True",
	                            file_name, global_column_name, multi_file_list.GetFirstFile().path, file_name,
	                            candidate_names);
}

//! Check if a column is trivially mappable (i.e. the column is effectively identical to the global column)
bool IsTriviallyMappable(const MultiFileColumnDefinition &global_column,
                         const vector<MultiFileColumnDefinition> &local_columns, const ColumnMapper &mapper,
                         optional_idx expected_idx = optional_idx()) {
	auto entry = mapper.Find(global_column);
	if (!entry.IsValid()) {
		return false;
	}
	auto local_id = entry.GetIndex();
	if (expected_idx.IsValid() && local_id != expected_idx.GetIndex()) {
		return false;
	}
	auto &local_column = local_columns[local_id];
	if (local_column.type != global_column.type) {
		return false;
	}
	if (local_column.children.size() != global_column.children.size()) {
		// child count difference - cannot map trivially
		return false;
	}
	auto nested_mapper = mapper.Create(local_column.children);
	for (idx_t i = 0; i < global_column.children.size(); i++) {
		auto &global_child = global_column.children[i];
		bool trivially_mappable = IsTriviallyMappable(global_child, local_column.children, *nested_mapper, i);
		if (!trivially_mappable) {
			return false;
		}
	}
	return true;
}

static ColumnMapResult MapColumn(ClientContext &context, const MultiFileColumnDefinition &global_column,
                                 const ColumnIndex &global_index,
                                 const vector<MultiFileColumnDefinition> &local_columns, const ColumnMapper &mapper,
                                 optional_idx top_level_index = optional_idx());

ColumnMapResult MapColumnList(ClientContext &context, const MultiFileColumnDefinition &global_column,
                              const ColumnIndex &global_index, const MultiFileColumnDefinition &local_column,
                              const MultiFileLocalColumnId &local_id, const ColumnMapper &mapper,
                              unique_ptr<MultiFileIndexMapping> mapping, const bool is_root) {
	const idx_t expected_list_children = 1;
	if (global_column.children.size() != expected_list_children) {
		throw InvalidInputException(
		    "Mismatch between field id children in global_column.children (%d) and list child in type",
		    global_column.children.size());
	}

	auto nested_mapper = mapper.Create(local_column.children);
	child_list_t<Value> column_mapping;
	unique_ptr<Expression> default_expression;
	unordered_map<idx_t, const_reference<ColumnIndex>> selected_children;
	if (global_index.HasChildren()) {
		//! FIXME: is this expected for lists??
		for (auto &index : global_index.GetChildIndexes()) {
			selected_children.emplace(index.GetPrimaryIndex(), index);
		}
	}

	vector<ColumnIndex> child_indexes;
	auto &global_child = global_column.children[0];

	bool is_selected = true;
	const_reference<ColumnIndex> global_child_index = global_index;
	if (!selected_children.empty()) {
		auto entry = selected_children.find(0);
		if (entry != selected_children.end()) {
			// the column is relevent - set the child index
			global_child_index = entry->second;
		} else {
			// not relevant - ignore the column
			is_selected = false;
		}
	}

	ColumnMapResult child_map;
	if (is_selected) {
		child_map = MapColumn(context, global_child, global_child_index.get(), local_column.children, *nested_mapper);
	} else {
		// column is not relevant for the query - push a NULL value
		child_map.default_value = make_uniq<BoundConstantExpression>(Value(global_child.type));
	}

	if (child_map.column_index) {
		child_indexes.push_back(std::move(*child_map.column_index));
		mapping->child_mapping.insert(make_pair(0, std::move(child_map.mapping)));
	}
	if (!child_map.column_map.IsNull()) {
		// found a column mapping for this child - emplace it
		column_mapping.emplace_back("list", std::move(child_map.column_map));
	}

	ColumnMapResult result;
	result.local_column = local_column;
	if (!column_mapping.empty()) {
		// we have column mappings at this level - construct the struct
		result.column_map = Value::STRUCT(std::move(column_mapping));
		if (!is_root) {
			// if this is nested we need to refer to the current column at this level
			child_list_t<Value> child_list;
			child_list.emplace_back(string(), Value(local_column.name));
			child_list.emplace_back(string(), std::move(result.column_map));
			result.column_map = Value::STRUCT(std::move(child_list));
		}
	}
	if (is_selected && child_map.default_value) {
		// we have default values at a previous level wrap it in a "list"
		child_list_t<LogicalType> default_type_list;
		default_type_list.emplace_back("list", child_map.default_value->return_type);
		vector<unique_ptr<Expression>> default_expressions;
		child_map.default_value->alias = "list";
		default_expressions.push_back(std::move(child_map.default_value));
		auto default_type = LogicalType::STRUCT(std::move(default_type_list));
		auto struct_pack_fun = StructPackFun::GetFunction();
		auto bind_data = make_uniq<VariableReturnBindData>(default_type);
		result.default_value = make_uniq<BoundFunctionExpression>(std::move(default_type), std::move(struct_pack_fun),
		                                                          std::move(default_expressions), std::move(bind_data));
	}
	result.column_index = make_uniq<ColumnIndex>(local_id.GetId(), std::move(child_indexes));
	result.mapping = std::move(mapping);
	return result;
}

static ColumnMapResult
MapColumnMapComponent(ClientContext &context,
                      const unordered_map<idx_t, const_reference<ColumnIndex>> &selected_children,
                      const ColumnIndex &global_index, const ColumnMapper &nested_mapper, idx_t component_idx,
                      const MultiFileColumnDefinition &component, const MultiFileColumnDefinition &local_map_column) {
	bool is_selected = true;
	const_reference<ColumnIndex> child_index = global_index;
	if (!selected_children.empty()) {
		auto entry = selected_children.find(component_idx);
		if (entry != selected_children.end()) {
			// the column is relevent - set the child index
			child_index = entry->second;
		} else {
			// not relevant - ignore the column
			is_selected = false;
		}
	}

	ColumnMapResult child_map;
	if (is_selected) {
		child_map = MapColumn(context, component, child_index.get(), local_map_column.children, nested_mapper);
	} else {
		// column is not relevant for the query - push a NULL value
		child_map.default_value = make_uniq<BoundConstantExpression>(Value(component.type));
	}
	return child_map;
}

ColumnMapResult MapColumnMap(ClientContext &context, const MultiFileColumnDefinition &global_column,
                             const ColumnIndex &global_index, const MultiFileColumnDefinition &local_column,
                             const MultiFileLocalColumnId &local_id, const ColumnMapper &mapper,
                             unique_ptr<MultiFileIndexMapping> mapping, const bool is_root) {
	const idx_t expected_map_children = 2;
	if (global_column.children.size() != expected_map_children) {
		throw InvalidInputException(
		    "Mismatch between field id children in global_column.children (%d) and map children in type",
		    global_column.children.size());
	}

	D_ASSERT(local_column.children.size() == 1);
	D_ASSERT(local_column.children[0].name == "key_value");
	auto &local_key_value = local_column.children[0];

	auto nested_mapper = mapper.Create(local_key_value.children);
	child_list_t<Value> column_mapping;
	vector<unique_ptr<Expression>> default_expressions;
	unordered_map<idx_t, const_reference<ColumnIndex>> selected_children;
	if (global_index.HasChildren()) {
		//! FIXME: is this expected for maps??
		for (auto &index : global_index.GetChildIndexes()) {
			selected_children.emplace(index.GetPrimaryIndex(), index);
		}
	}

	vector<ColumnIndex> child_indexes;
	auto &global_key = global_column.children[0];
	auto &global_value = global_column.children[1];

	child_list_t<reference<const MultiFileColumnDefinition>> map_components;
	map_components.emplace_back("key", global_key);
	map_components.emplace_back("value", global_value);

	for (idx_t i = 0; i < map_components.size(); i++) {
		auto &name = map_components[i].first;
		auto &global_component = map_components[i].second;

		auto map_result = MapColumnMapComponent(context, selected_children, global_index, *nested_mapper, i,
		                                        global_component, local_key_value);
		if (map_result.column_index) {
			child_indexes.push_back(std::move(*map_result.column_index));
			mapping->child_mapping.insert(make_pair(i, std::move(map_result.mapping)));
		}
		if (!map_result.column_map.IsNull()) {
			// found a column mapping for the component - emplace it
			column_mapping.emplace_back(name, std::move(map_result.column_map));
		}
		if (map_result.default_value) {
			map_result.default_value->alias = name;
			default_expressions.push_back(std::move(map_result.default_value));
		}
	}

	ColumnMapResult result;
	result.local_column = local_column;
	if (!column_mapping.empty()) {
		// we have column mappings at this level - construct the struct
		result.column_map = Value::STRUCT(std::move(column_mapping));
		if (!is_root) {
			// if this is nested we need to refer to the current column at this level
			child_list_t<Value> child_list;
			child_list.emplace_back(string(), Value(local_column.name));
			child_list.emplace_back(string(), std::move(result.column_map));
			result.column_map = Value::STRUCT(std::move(child_list));
		}
	}
	if (!default_expressions.empty()) {
		// we have default values at a previous level wrap it in a "list"
		child_list_t<LogicalType> default_type_list;
		for (auto &expr : default_expressions) {
			default_type_list.emplace_back(expr->GetAlias(), expr->return_type);
		}
		auto default_type = LogicalType::STRUCT(std::move(default_type_list));
		auto struct_pack_fun = StructPackFun::GetFunction();
		auto bind_data = make_uniq<VariableReturnBindData>(default_type);
		result.default_value = make_uniq<BoundFunctionExpression>(std::move(default_type), std::move(struct_pack_fun),
		                                                          std::move(default_expressions), std::move(bind_data));
	}
	vector<ColumnIndex> map_indexes;
	map_indexes.emplace_back(0, std::move(child_indexes));

	result.column_index = make_uniq<ColumnIndex>(local_id.GetId(), std::move(map_indexes));
	result.mapping = std::move(mapping);
	return result;
}

ColumnMapResult MapColumnStruct(ClientContext &context, const MultiFileColumnDefinition &global_column,
                                const ColumnIndex &global_index, const MultiFileColumnDefinition &local_column,
                                const MultiFileLocalColumnId &local_id, const ColumnMapper &mapper,
                                unique_ptr<MultiFileIndexMapping> mapping, const bool is_root) {
	auto &struct_children = StructType::GetChildTypes(global_column.type);
	if (struct_children.size() != global_column.children.size()) {
		throw InvalidInputException(
		    "Mismatch between field id children in global_column.children and struct children in type");
	}

	auto nested_mapper = mapper.Create(local_column.children);
	child_list_t<Value> column_mapping;
	vector<unique_ptr<Expression>> default_expressions;
	unordered_map<idx_t, const_reference<ColumnIndex>> selected_children;
	if (global_index.HasChildren()) {
		for (auto &index : global_index.GetChildIndexes()) {
			selected_children.emplace(index.GetPrimaryIndex(), index);
		}
	}

	vector<ColumnIndex> child_indexes;
	for (idx_t i = 0; i < global_column.children.size(); i++) {
		bool is_selected = true;
		const_reference<ColumnIndex> global_child_index = global_index;
		if (!selected_children.empty()) {
			auto entry = selected_children.find(i);
			if (entry != selected_children.end()) {
				// the column is relevent - set the child index
				global_child_index = entry->second;
			} else {
				// not relevant - ignore the column
				is_selected = false;
			}
		}
		auto &global_child = global_column.children[i];
		ColumnMapResult child_map;
		if (is_selected) {
			child_map =
			    MapColumn(context, global_child, global_child_index.get(), local_column.children, *nested_mapper);
		} else {
			// column is not relevant for the query - push a NULL value
			child_map.default_value = make_uniq<BoundConstantExpression>(Value(global_child.type));
		}

		if (child_map.column_index) {
			child_indexes.push_back(std::move(*child_map.column_index));
			mapping->child_mapping.insert(make_pair(i, std::move(child_map.mapping)));
		}
		if (!child_map.column_map.IsNull()) {
			// found a column mapping for this child - emplace it
			column_mapping.emplace_back(global_child.name, std::move(child_map.column_map));
		}
		//! FIXME: the 'default_value' should only be used if the STRUCT's default value is not NULL
		if (child_map.default_value) {
			// found a default value for this child - emplace it
			child_map.default_value->alias = global_child.name;
			default_expressions.push_back(std::move(child_map.default_value));
		}
	}

	ColumnMapResult result;
	result.local_column = local_column;
	if (!column_mapping.empty()) {
		// we have column mappings at this level - construct the struct
		result.column_map = Value::STRUCT(std::move(column_mapping));
		if (!is_root) {
			// if this is nested we need to refer to the current column at this level
			child_list_t<Value> child_list;
			child_list.emplace_back(string(), Value(local_column.name));
			child_list.emplace_back(string(), std::move(result.column_map));
			result.column_map = Value::STRUCT(std::move(child_list));
		}
	}

	if (!default_expressions.empty()) {
		// we have default values at this level - construct the struct pack
		child_list_t<LogicalType> default_type_list;
		for (auto &expr : default_expressions) {
			default_type_list.emplace_back(expr->GetAlias(), expr->return_type);
		}
		auto default_type = LogicalType::STRUCT(std::move(default_type_list));
		auto struct_pack_fun = StructPackFun::GetFunction();
		auto bind_data = make_uniq<VariableReturnBindData>(default_type);
		result.default_value = make_uniq<BoundFunctionExpression>(std::move(default_type), std::move(struct_pack_fun),
		                                                          std::move(default_expressions), std::move(bind_data));
	}
	result.column_index = make_uniq<ColumnIndex>(local_id.GetId(), std::move(child_indexes));
	result.mapping = std::move(mapping);
	return result;
}

static ColumnMapResult MapColumn(ClientContext &context, const MultiFileColumnDefinition &global_column,
                                 const ColumnIndex &global_index,
                                 const vector<MultiFileColumnDefinition> &local_columns, const ColumnMapper &mapper,
                                 optional_idx top_level_index) {
	bool is_root = top_level_index.IsValid();
	ColumnMapResult result;
	auto entry = mapper.Find(global_column);
	if (!entry.IsValid()) {
		// entry not present in map, use default value
		result.default_value = mapper.GetDefaultExpression(global_column, is_root);
		return result;
	}
	// the field exists! get the local column
	MultiFileLocalColumnId local_id(entry.GetIndex());
	auto &local_column = local_columns[local_id.GetId()];
	unique_ptr<MultiFileIndexMapping> mapping;
	idx_t mapping_idx;
	if (is_root) {
		// root expression - refer to it directly
		mapping_idx = top_level_index.GetIndex();
	} else {
		// extract the field from the parent
		mapping_idx = local_id.GetId();
	}

	mapping = make_uniq<MultiFileIndexMapping>(mapping_idx);
	if (global_column.children.empty()) {
		// not a struct - map the column directly
		result.column_map = Value(local_column.name);
		result.column_index = make_uniq<ColumnIndex>(local_id.GetId());
		result.mapping = std::move(mapping);
		result.local_column = local_column;
		return result;
	}

	// nested type - check if the field identifiers match and if we need to remap
	D_ASSERT(global_column.type.IsNested());
	switch (global_column.type.id()) {
	case LogicalTypeId::STRUCT:
		return MapColumnStruct(context, global_column, global_index, local_column, local_id, mapper, std::move(mapping),
		                       is_root);
	case LogicalTypeId::LIST:
		return MapColumnList(context, global_column, global_index, local_column, local_id, mapper, std::move(mapping),
		                     is_root);
	case LogicalTypeId::MAP:
		return MapColumnMap(context, global_column, global_index, local_column, local_id, mapper, std::move(mapping),
		                    is_root);
	case LogicalTypeId::ARRAY: {
		throw NotImplementedException("Can't map an ARRAY with nested children!");
	}
	default:
		throw NotImplementedException("MapColumn for children of type %s not implemented",
		                              global_column.type.ToString());
	}
}

unique_ptr<Expression> ConstructMapExpression(ClientContext &context, idx_t local_idx, ColumnMapResult &mapping,
                                              const MultiFileColumnDefinition &global_column,
                                              bool is_trivially_mappable) {
	auto &local_column = *mapping.local_column;
	unique_ptr<Expression> expr;
	expr = make_uniq<BoundReferenceExpression>(local_column.type, local_idx);
	if (!global_column.type.IsNested() ||
	    (!mapping.column_map.IsNull() && mapping.column_map.type().id() != LogicalTypeId::STRUCT) ||
	    is_trivially_mappable) {
		// not a struct - potentially add a cast
		if (local_column.type != global_column.type) {
			expr = BoundCastExpression::AddCastToType(context, std::move(expr), global_column.type);
		}
		return expr;
	}
	// generate the remap_struct function call
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(expr));
	children.push_back(make_uniq<BoundConstantExpression>(Value(global_column.type)));
	children.push_back(make_uniq<BoundConstantExpression>(std::move(mapping.column_map)));
	if (!mapping.default_value) {
		children.push_back(make_uniq<BoundConstantExpression>(Value()));
	} else {
		children.push_back(std::move(mapping.default_value));
	}
	auto remap_fun = RemapStructFun::GetFunction();
	auto bind_data = remap_fun.GetBindCallback()(context, remap_fun, children);
	children[0] = BoundCastExpression::AddCastToType(context, std::move(children[0]), remap_fun.arguments[0]);
	return make_uniq<BoundFunctionExpression>(global_column.type, std::move(remap_fun), std::move(children),
	                                          std::move(bind_data));
}

ResultColumnMapping MultiFileColumnMapper::CreateColumnMappingByMapper(const ColumnMapper &mapper) {
	auto &reader = *reader_data.reader;
	auto &local_columns = reader.GetColumns();

	ResultColumnMapping result;

	// loop through the schema definition
	auto &expressions = reader_data.expressions;
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto global_idx = MultiFileGlobalIndex(i);

		optional_idx constant_idx;
		for (idx_t j = 0; j < reader_data.constant_map.size(); j++) {
			auto constant_index = MultiFileConstantMapIndex(j);
			auto &entry = reader_data.constant_map[constant_index];
			if (entry.column_idx.GetIndex() == i) {
				constant_idx = j;
				break;
			}
		}
		if (constant_idx.IsValid()) {
			// this column is constant for this file
			auto constant_index = MultiFileConstantMapIndex(constant_idx.GetIndex());
			auto &constant_entry = reader_data.constant_map[constant_index];
			expressions.push_back(make_uniq<BoundConstantExpression>(constant_entry.value));
			continue;
		}

		// Handle any generate columns that are not in the schema (currently only file_row_number)
		auto &global_id = global_column_ids[i];
		auto global_column_id = global_id.GetPrimaryIndex();
		optional_ptr<MultiFileColumnDefinition> global_column_reference;

		auto local_idx = MultiFileLocalIndex(reader.column_ids.size());
		if (IsVirtualColumn(global_column_id)) {
			// virtual column - look it up in the virtual column entry map
			auto virtual_entry = virtual_columns.find(global_column_id);
			if (virtual_entry == virtual_columns.end()) {
				throw InternalException("Virtual column id %d not found in virtual columns map", global_column_id);
			}
			auto &virtual_column_type = virtual_entry->second.type;
			// check if this column is constant for the entire file
			auto constant_expr =
			    multi_file_reader.GetConstantVirtualColumn(reader_data, global_column_id, virtual_column_type);
			if (constant_expr) {
				// the column is constant for the entire file - handle it
				expressions.push_back(std::move(constant_expr));
				continue;
			}
			// the column is not constant for the file
			// get the expression to evaluate the column OR the global column to read into
			auto expr =
			    multi_file_reader.GetVirtualColumnExpression(context, reader_data, local_columns, global_column_id,
			                                                 virtual_column_type, local_idx, global_column_reference);
			if (expr && expr->type == ExpressionType::VALUE_CONSTANT) {
				// the column is constant after all - handle it
				expressions.push_back(std::move(expr));
				continue;
			}
			if (!global_column_reference) {
				auto is_reference = expr->type == ExpressionType::BOUND_REF;
				expressions.push_back(std::move(expr));

				MultiFileLocalColumnId local_id(reader.columns.size());
				ColumnIndex local_index(local_id.GetId());

				// add the virtual column to the reader
				reader.columns.emplace_back(virtual_entry->second.name, virtual_column_type);
				reader.AddVirtualColumn(global_column_id);

				// set it as being projected in this spot
				MultiFileColumnMap index_mapping(local_idx, virtual_column_type, virtual_column_type);
				if (!is_reference) {
					index_mapping.filter_conversion = FilterConversionType::CANNOT_CONVERT;
				}
				result.global_to_local.insert(make_pair(global_idx.GetIndex(), std::move(index_mapping)));
				reader.column_ids.push_back(local_id);
				reader.column_indexes.push_back(std::move(local_index));
				continue;
			}
		}

		const auto &global_column =
		    global_column_reference ? *global_column_reference : global_columns[global_column_id];
		if (reader.UseCastMap()) {
			// reader is responsible for converting types - perform a top-level match only
			auto entry = mapper.Find(global_column);
			if (!entry.IsValid()) {
				ThrowColumnNotFoundError(global_column.name);
			}
			MultiFileLocalColumnId local_id(entry.GetIndex());
			ColumnIndex local_index(local_id.GetId());
			auto &local_type = local_columns[local_id.GetId()].type;
			auto &global_type = global_column.type;
			auto expr = make_uniq<BoundReferenceExpression>(global_type, local_idx.GetIndex());
			if (global_type != local_type) {
				reader.cast_map[local_id.GetId()] = global_type;
			} else {
				// if types are equivalent we can push the parent ColumnIndex mapping
				local_index = ColumnIndex(local_id.GetId(), global_id.GetChildIndexes());
			}
			reader_data.expressions.push_back(std::move(expr));

			MultiFileColumnMap index_mapping(local_idx, local_type, global_type);
			result.global_to_local.insert(make_pair(global_idx.GetIndex(), std::move(index_mapping)));
			reader.column_ids.push_back(local_id);
			reader.column_indexes.push_back(std::move(local_index));
			continue;
		}

		auto column_map = MapColumn(context, global_column, global_id, local_columns, mapper, local_idx.GetIndex());
		if (!column_map.column_index) {
			// no columns were emitted
			reader_data.expressions.push_back(std::move(column_map.default_value));
			continue;
		}
		auto trivial_map = IsTriviallyMappable(global_column, local_columns, mapper);
		auto local_index = std::move(column_map.column_index);
		auto local_id = local_index->GetPrimaryIndex();
		auto &local_type = local_columns[local_id].type;
		auto expr = ConstructMapExpression(context, local_idx.GetIndex(), column_map, global_column, trivial_map);
		reader_data.expressions.push_back(std::move(expr));
		auto filter_conversion = trivial_map ? FilterConversionType::COPY_DIRECTLY : FilterConversionType::CAST_FILTER;

		MultiFileColumnMap index_mapping(std::move(*column_map.mapping), local_type, global_column.type,
		                                 filter_conversion);
		result.global_to_local.insert(make_pair(global_idx.GetIndex(), std::move(index_mapping)));
		reader.column_ids.emplace_back(local_id);
		reader.column_indexes.push_back(std::move(*local_index));
	}
	D_ASSERT(global_column_ids.size() == reader_data.expressions.size());
	return result;
}

ResultColumnMapping MultiFileColumnMapper::CreateColumnMapping(MultiFileColumnMappingMode mapping_mode) {
	auto &reader = *reader_data.reader;
	auto &local_columns = reader.GetColumns();
	switch (mapping_mode) {
	case MultiFileColumnMappingMode::BY_NAME: {
		// we have expected types: create a map of name -> (local) column id
		NameMapper name_map(*this, local_columns);
		return CreateColumnMappingByMapper(name_map);
	}
	case MultiFileColumnMappingMode::BY_FIELD_ID: {
#ifdef DEBUG
		//! Make sure the global columns have field_ids to match on
		for (auto &column : global_columns) {
			D_ASSERT(!column.identifier.IsNull());
			D_ASSERT(column.identifier.type().id() == LogicalTypeId::INTEGER);
		}
#endif

		// we have expected types: create a map of field_id -> column index
		FieldIdMapper field_id_map(local_columns);
		return CreateColumnMappingByMapper(field_id_map);
	}
	default: {
		throw InternalException("Unsupported MultiFileColumnMappingMode type");
	}
	}
}

bool MultiFileColumnMapper::EvaluateFilterAgainstConstant(TableFilter &filter, const Value &constant) {
	const auto type = filter.filter_type;

	switch (type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		if (constant.IsNull()) {
			return false;
		}
		return constant_filter.Compare(constant);
	}
	case TableFilterType::IS_NULL: {
		return constant.IsNull();
	}
	case TableFilterType::IS_NOT_NULL: {
		return !constant.IsNull();
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		for (auto &val : in_filter.values) {
			if (!constant.IsNull() && val == constant) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		for (auto &it : or_filter.child_filters) {
			if (EvaluateFilterAgainstConstant(*it, constant)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		auto res = make_uniq<ConjunctionAndFilter>();
		for (auto &it : and_filter.child_filters) {
			if (!EvaluateFilterAgainstConstant(*it, constant)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		auto &child_filter = struct_filter.child_filter;

		if (constant.type().id() != LogicalTypeId::STRUCT) {
			throw InternalException(
			    "Constant for this column is not of type struct, but used in a STRUCT_EXTRACT TableFilter");
		}
		auto &struct_fields = StructValue::GetChildren(constant);
		auto field_index = struct_filter.child_idx;
		if (field_index >= struct_fields.size()) {
			throw InternalException("STRUCT_EXTRACT looks for child_idx %d, but constant only has %d children",
			                        field_index, struct_fields.size());
		}
		auto &field_name = StructType::GetChildName(constant.type(), field_index);
		if (!StringUtil::CIEquals(field_name, struct_filter.child_name)) {
			throw InternalException("STRUCT_EXTRACT looks for a child with name '%s' at index %d, but constant has a "
			                        "field with '%s' as the name for that index",
			                        struct_filter.child_name, field_index, field_name);
		}
		auto &child_constant = struct_fields[field_index];
		return EvaluateFilterAgainstConstant(*child_filter, child_constant);
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (optional_filter.child_filter) {
			return EvaluateFilterAgainstConstant(*optional_filter.child_filter, constant);
		}
		return true;
	}
	case TableFilterType::DYNAMIC_FILTER: {
		auto &dynamic_filter = filter.Cast<DynamicFilter>();
		if (!dynamic_filter.filter_data) {
			//! No filter_data assigned (does this mean the DynamicFilter is broken??)
			return true;
		}
		lock_guard<mutex> lock(dynamic_filter.filter_data->lock);
		if (!dynamic_filter.filter_data->initialized) {
			//! Not initialized
			return true;
		}
		if (!dynamic_filter.filter_data->filter) {
			//! No filter present
			return true;
		}
		return EvaluateFilterAgainstConstant(*dynamic_filter.filter_data->filter, constant);
	}
	case TableFilterType::EXPRESSION_FILTER: {
		auto &expr_filter = filter.Cast<ExpressionFilter>();
		return expr_filter.EvaluateWithConstant(context, constant);
	}
	case TableFilterType::BLOOM_FILTER: {
		auto &bloom_filter = filter.Cast<BFTableFilter>();
		return bloom_filter.FilterValue(constant);
	}
	default:
		throw NotImplementedException("Can't evaluate TableFilterType (%s) against a constant",
		                              EnumUtil::ToString(type));
	}
}

Value MultiFileColumnMapper::GetConstantValue(idx_t global_index) {
	auto global_column_id = global_column_ids[global_index].GetPrimaryIndex();
	auto &expr = reader_data.expressions[global_index];
	if (expr->type == ExpressionType::VALUE_CONSTANT) {
		return expr->Cast<BoundConstantExpression>().value;
	}
	for (idx_t i = 0; i < reader_data.constant_map.size(); i++) {
		auto &constant_map_entry = reader_data.constant_map[MultiFileConstantMapIndex(i)];
		if (constant_map_entry.column_idx.GetIndex() == global_index) {
			return constant_map_entry.value;
		}
	}
	auto &global_column = global_columns[global_column_id];
	throw InternalException("Column '%s' is not present in the file, but no constant_map entry exists for it!",
	                        global_column.name);
}

ReaderInitializeType
MultiFileColumnMapper::EvaluateConstantFilters(ResultColumnMapping &mapping,
                                               map<idx_t, reference<TableFilter>> &remaining_filters) {
	if (!global_filters) {
		return ReaderInitializeType::INITIALIZED;
	}
	auto &global_to_local = mapping.global_to_local;
	for (auto &it : global_filters->filters) {
		auto &global_index = it.first;
		auto &global_filter = it.second;

		auto local_it = global_to_local.find(it.first);
		if (local_it != global_to_local.end()) {
			//! File has this column, filter needs to be evaluated later
			remaining_filters.emplace(global_index, *global_filter);
			continue;
		}

		//! FIXME: this does not check for filters against struct fields that are not present in the file
		auto constant_value = GetConstantValue(global_index);
		if (!EvaluateFilterAgainstConstant(*global_filter, constant_value)) {
			return ReaderInitializeType::SKIP_READING_FILE;
		}
	}
	return ReaderInitializeType::INITIALIZED;
}

static unique_ptr<TableFilter> TryCastTableFilter(const TableFilter &global_filter, MultiFileIndexMapping &mapping,
                                                  const LogicalType &target_type) {
	auto type = global_filter.filter_type;

	switch (type) {
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = global_filter.Cast<ConjunctionOrFilter>();
		auto res = make_uniq<ConjunctionOrFilter>();
		for (auto &it : or_filter.child_filters) {
			auto child_filter = TryCastTableFilter(*it, mapping, target_type);
			if (!child_filter) {
				return nullptr;
			}
			res->child_filters.push_back(std::move(child_filter));
		}
		return std::move(res);
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = global_filter.Cast<ConjunctionAndFilter>();
		auto res = make_uniq<ConjunctionAndFilter>();
		for (auto &it : and_filter.child_filters) {
			auto child_filter = TryCastTableFilter(*it, mapping, target_type);
			if (!child_filter) {
				return nullptr;
			}
			res->child_filters.push_back(std::move(child_filter));
		}
		return std::move(res);
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = global_filter.Cast<StructFilter>();
		auto &child_filter = struct_filter.child_filter;

		// find the corresponding local entry
		auto entry = mapping.child_mapping.find(struct_filter.child_idx);
		if (entry == mapping.child_mapping.end()) {
			// this is constant for this file - abort
			// FIXME: should be handled before
			return nullptr;
		}
		auto &struct_mapping = *entry->second;
		auto &struct_type = StructType::GetChildTypes(target_type);
		auto &child_type = struct_type[struct_mapping.index].second;
		auto new_child_filter = TryCastTableFilter(*child_filter, struct_mapping, child_type);
		if (!new_child_filter) {
			return nullptr;
		}
		auto child_name = struct_type[struct_mapping.index].first;
		return make_uniq<StructFilter>(struct_mapping.index, std::move(child_name), std::move(new_child_filter));
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = global_filter.Cast<OptionalFilter>();
		auto child_result = TryCastTableFilter(*optional_filter.child_filter, mapping, target_type);
		if (!child_result) {
			return nullptr;
		}
		return make_uniq<OptionalFilter>(std::move(child_result));
	}
	case TableFilterType::DYNAMIC_FILTER: {
		// we can't transfer dynamic filters over casts directly
		// BUT we can copy the current state of the filter and push that
		// FIXME: we could solve this in a different manner as well by pushing the dynamic filter directly
		auto &dynamic_filter = global_filter.Cast<DynamicFilter>();
		if (!dynamic_filter.filter_data) {
			return nullptr;
		}
		if (!dynamic_filter.filter_data->initialized) {
			return nullptr;
		}
		if (!dynamic_filter.filter_data->filter) {
			return nullptr;
		}
		lock_guard<mutex> lock(dynamic_filter.filter_data->lock);
		return TryCastTableFilter(*dynamic_filter.filter_data->filter, mapping, target_type);
	}
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
		// these filters can just be copied as they don't depend on type
		return global_filter.Copy();
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = global_filter.Cast<ConstantFilter>();
		auto new_constant = constant_filter.constant;
		if (!StatisticsPropagator::CanPropagateCast(constant_filter.constant.type(), target_type)) {
			// type cannot be converted - abort
			return nullptr;
		}
		if (!new_constant.DefaultTryCastAs(target_type)) {
			return nullptr;
		}
		return make_uniq<ConstantFilter>(constant_filter.comparison_type, std::move(new_constant));
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = global_filter.Cast<InFilter>();
		auto in_list = in_filter.values;
		if (!in_list.empty() && !StatisticsPropagator::CanPropagateCast(in_list[0].type(), target_type)) {
			// type cannot be converted - abort
			return nullptr;
		}
		for (auto &val : in_list) {
			if (!val.DefaultTryCastAs(target_type)) {
				return nullptr;
			}
		}
		return make_uniq<InFilter>(std::move(in_list));
	}
	case TableFilterType::EXPRESSION_FILTER:
		// unsupported
		return nullptr;
	default:
		throw NotImplementedException("Can't convert TableFilterType (%s) from global to local indexes",
		                              EnumUtil::ToString(type));
	}
}

void SetIndexToZero(unique_ptr<Expression> &root_expr) {
	ExpressionIterator::VisitExpressionMutable<BoundReferenceExpression>(
	    root_expr, [&](BoundReferenceExpression &ref, unique_ptr<Expression> &expr) { ref.index = 0; });
}

bool CanPropagateCast(const MultiFileIndexMapping &mapping, const LogicalType &local_type,
                      const LogicalType &global_type) {
	if (local_type.id() == LogicalTypeId::STRUCT && global_type.id() == LogicalTypeId::STRUCT) {
		// struct fields - check along the mapping
		// mapping is global to local
		throw InternalException("Propagate cast - check mapping");
	}
	return StatisticsPropagator::CanPropagateCast(local_type, global_type);
}

unique_ptr<TableFilterSet> MultiFileColumnMapper::CreateFilters(map<idx_t, reference<TableFilter>> &filters,
                                                                ResultColumnMapping &mapping) {
	if (filters.empty()) {
		return nullptr;
	}
	auto &reader = *reader_data.reader;
	auto &global_to_local = mapping.global_to_local;
	auto result = make_uniq<TableFilterSet>();
	for (auto &it : filters) {
		auto &global_index = it.first;
		auto &global_filter = it.second.get();

		auto local_it = global_to_local.find(global_index);
		if (local_it == global_to_local.end()) {
			throw InternalException(
			    "Error in 'EvaluateConstantFilters', this filter should not end up in CreateFilters!");
		}
		auto &map_entry = local_it->second;
		auto local_id = map_entry.mapping.index;
		auto filter_idx = reader.column_indexes[local_id].GetPrimaryIndex();
		auto &local_type = map_entry.local_type;
		auto &global_type = map_entry.global_type;

		unique_ptr<TableFilter> local_filter;
		switch (map_entry.filter_conversion) {
		case FilterConversionType::COPY_DIRECTLY:
			// no conversion required - just copy the filter
			local_filter = global_filter.Copy();
			break;
		case FilterConversionType::CAST_FILTER:
			// types are different - try to convert
			local_filter = TryCastTableFilter(global_filter, map_entry.mapping, local_type);
			break;
		default:
			// we need to execute the filter globally
			break;
		}
		if (local_filter) {
			// succeeded in casting - push the local filter
			result->filters.emplace(local_id, std::move(local_filter));
		} else {
			// failed to cast - copy the global filter and evaluate the conversion expression in the reader
			result->filters.emplace(local_id, global_filter.Copy());

			// add the expression to the expression map - we are now evaluating this inside the reader directly
			// we need to set the index of the references inside the expression to 0
			auto &expr = reader_data.expressions[global_index];
			SetIndexToZero(expr);
			reader.expression_map[filter_idx] = std::move(expr);

			// reset the expression - since we are evaluating it in the reader we can just reference it
			expr = make_uniq<BoundReferenceExpression>(global_type, local_id);
		}
	}
	return result;
}

ReaderInitializeType MultiFileColumnMapper::CreateMapping(MultiFileColumnMappingMode mapping_mode) {
	// copy global columns and inject any different defaults
	auto result = CreateColumnMapping(mapping_mode);
	//! Evaluate the filters against the column(s) that are constant for this file (not present in the local schema)
	//! If any of these fail, the file can be skipped entirely
	map<idx_t, reference<TableFilter>> remaining_filters;
	auto evaluate_result = EvaluateConstantFilters(result, remaining_filters);
	if (evaluate_result == ReaderInitializeType::SKIP_READING_FILE) {
		return ReaderInitializeType::SKIP_READING_FILE;
	}

	reader_data.reader->filters = CreateFilters(remaining_filters, result);

	// for any remaining casts - push them as expressions
	return ReaderInitializeType::INITIALIZED;
}

} // namespace duckdb
