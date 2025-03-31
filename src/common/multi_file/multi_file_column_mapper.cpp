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

namespace duckdb {

MultiFileColumnMapper::MultiFileColumnMapper(ClientContext &context, MultiFileReaderData &reader_data,
                                             const vector<MultiFileColumnDefinition> &global_columns,
                                             const vector<ColumnIndex> &global_column_ids,
                                             optional_ptr<TableFilterSet> filters, const string &initial_file,
                                             const MultiFileReaderBindData &bind_data,
                                             const virtual_column_map_t &virtual_columns)
    : context(context), reader_data(reader_data), global_columns(global_columns), global_column_ids(global_column_ids),
      global_filters(filters), initial_file(initial_file), bind_data(bind_data), virtual_columns(virtual_columns) {
}

struct MultiFileIndexMapping {
public:
	explicit MultiFileIndexMapping(idx_t index) : index(index) {
	}

public:
	idx_t index;
	unordered_map<idx_t, unique_ptr<MultiFileIndexMapping>> child_mapping;
};

struct MultiFileColumnMap {
	MultiFileColumnMap(idx_t index, const LogicalType &local_type, const LogicalType &global_type)
	    : mapping(index), local_type(local_type), global_type(global_type), require_filter_cast(local_type != global_type) {
	}
	MultiFileColumnMap(MultiFileIndexMapping mapping_p, const LogicalType &local_type, const LogicalType &global_type, bool require_filter_cast)
	    : mapping(std::move(mapping_p)), local_type(local_type), global_type(global_type), require_filter_cast(require_filter_cast) {
	}

	MultiFileIndexMapping mapping;
	const LogicalType &local_type;
	const LogicalType &global_type;
	bool require_filter_cast;
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
	unique_ptr<Expression> expression;
	unique_ptr<ColumnIndex> column_index;
	unique_ptr<MultiFileIndexMapping> mapping;
};

unique_ptr<Expression> ReferenceChildColumn(const MultiFileColumnDefinition &local_column, idx_t current_index, Expression &parent) {
	vector<unique_ptr<Expression>> children;
	children.push_back(parent.Copy());
	auto struct_extract_fun = StructExtractAtFun::GetFunction();
	auto bind_data = StructExtractAtFun::GetBindData(current_index);
	auto &parent_types = StructType::GetChildTypes(parent.return_type);
	auto &result_type = parent_types[current_index].second;
	return make_uniq<BoundFunctionExpression>(result_type, std::move(struct_extract_fun), std::move(children), std::move(bind_data));
}

struct ColumnMapper {
	virtual ~ColumnMapper() = default;
	virtual unique_ptr<ColumnMapper> Create(const vector<MultiFileColumnDefinition> &columns) const = 0;
	virtual optional_idx Find(const MultiFileColumnDefinition &column) const = 0;
	virtual unique_ptr<Expression> GetDefaultExpression(const MultiFileColumnDefinition &column, optional_ptr<Expression> parent) const = 0;
};

ColumnMapResult MapColumn(ClientContext &context, const MultiFileColumnDefinition &global_column, const vector<MultiFileColumnDefinition> &local_columns, const ColumnMapper &mapper, idx_t top_level_index, optional_ptr<Expression> parent = nullptr) {
	ColumnMapResult result;
	auto entry = mapper.Find(global_column);
	if (!entry.IsValid()) {
		// entry not present in map, use default value
		result.expression = mapper.GetDefaultExpression(global_column, parent);
		return result;
	}
	// the field exists! get the local column
	MultiFileLocalColumnId local_id(entry.GetIndex());
	auto &local_column = local_columns[local_id.GetId()];
	unique_ptr<Expression> expr;
	unique_ptr<MultiFileIndexMapping> mapping;
	idx_t column_idx;
	if (!parent) {
		// root expression - refer to it directly
		column_idx = top_level_index;
		expr = make_uniq<BoundReferenceExpression>(local_column.type, top_level_index);
	} else {
		// extract the field from the parent
		column_idx = local_id.GetId();
		expr = ReferenceChildColumn(local_column, column_idx, *parent);
	}
	mapping = make_uniq<MultiFileIndexMapping>(column_idx);
	if (global_column.children.empty()) {
		// not a nested type - return the column directly
		if (global_column.type != local_column.type) {
			expr = BoundCastExpression::AddCastToType(context, std::move(expr), global_column.type);
		}
		result.expression = std::move(expr);
		result.column_index = make_uniq<ColumnIndex>(column_idx);
		result.mapping = std::move(mapping);
		return result;
	}
	// nested type - check if the field identifiers match and if we need to remap
	// FIXME: we don't need to remap if all identifiers and types match (common case)
	auto nested_mapper = mapper.Create(local_column.children);
	auto &struct_children = StructType::GetChildTypes(global_column.type);
	if (struct_children.size() != global_column.children.size()) {
		throw InternalException("Mismatch between field id children in global_column.children and struct children in type");
	}
	vector<unique_ptr<Expression>> mapped_expressions;
	vector<ColumnIndex> child_indexes;
	for(idx_t i = 0; i < global_column.children.size(); i++) {
		auto &global_child = global_column.children[i];
		auto child_map = MapColumn(context, global_child, local_column.children, *nested_mapper, top_level_index, expr.get());
		if (child_map.column_index) {
			child_indexes.push_back(std::move(*child_map.column_index));
			mapping->child_mapping.insert(make_pair(i, std::move(child_map.mapping)));
		}
		child_map.expression->alias = struct_children[i].first;
		mapped_expressions.push_back(std::move(child_map.expression));
	}
	auto struct_pack_fun = StructPackFun::GetFunction();
	auto bind_data = make_uniq<VariableReturnBindData>(global_column.type);
	result.expression = make_uniq<BoundFunctionExpression>(global_column.type, std::move(struct_pack_fun), std::move(mapped_expressions), std::move(bind_data));
	result.column_index = make_uniq<ColumnIndex>(column_idx, std::move(child_indexes));
	result.mapping = std::move(mapping);
	return result;
}

struct FieldIdMapper : public ColumnMapper {
	FieldIdMapper(const vector<MultiFileColumnDefinition> &columns) {
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
		auto entry = field_id_map.find(column.GetIdentifierFieldId());
		if (entry == field_id_map.end()) {
			return optional_idx();
		}
		return entry->second.GetId();
	}
	unique_ptr<Expression> GetDefaultExpression(const MultiFileColumnDefinition &column, optional_ptr<Expression> parent) const override {
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

	idx_t MapCount() {
		return field_id_map.size();
	}

private:
	unordered_map<int32_t, MultiFileLocalColumnId> field_id_map;
};

struct NameMapper : public ColumnMapper {
	NameMapper(const vector<MultiFileColumnDefinition> &columns) {
		for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
			auto &column = columns[col_idx];
			name_map.emplace(column.GetIdentifierName(), MultiFileLocalColumnId(col_idx));
		}
	}

	unique_ptr<ColumnMapper> Create(const vector<MultiFileColumnDefinition> &columns) const override {
		return make_uniq<NameMapper>(columns);
	}
	optional_idx Find(const MultiFileColumnDefinition &column) const override {
		auto entry = name_map.find(column.GetIdentifierName());
		if (entry == name_map.end()) {
			return optional_idx();
		}
		return entry->second.GetId();
	}
	unique_ptr<Expression> GetDefaultExpression(const MultiFileColumnDefinition &column, optional_ptr<Expression> parent) const override {
		if (parent) {
			return make_uniq<BoundConstantExpression>(Value(column.type));
		}
		throw InternalException("FIXME: missing default expression in root");
	}

private:
	case_insensitive_map_t<MultiFileLocalColumnId> name_map;
};

void MultiFileColumnMapper::PushColumnMapping(const LogicalType &global_type, const LogicalType &local_type,
                                              MultiFileLocalColumnId local_id, ResultColumnMapping &result,
                                              MultiFileGlobalIndex global_idx, const ColumnIndex &global_id) {
	auto &reader = *reader_data.reader;
	auto local_idx = reader.column_ids.size();

	unique_ptr<Expression> expr;

	ColumnIndex local_index(local_id.GetId());
	expr = make_uniq<BoundReferenceExpression>(local_type, local_idx);
	if (global_type != local_type) {
		expr = BoundCastExpression::AddCastToType(context, std::move(expr), global_type);
	} else {
		//! FIXME: local fields are not guaranteed to match with the global fields for this struct
		local_index = ColumnIndex(local_id.GetId(), global_id.GetChildIndexes());
	}
	reader_data.expressions.push_back(std::move(expr));

	MultiFileColumnMap index_mapping(local_idx, local_type, global_type);
	result.global_to_local.insert(make_pair(global_idx.GetIndex(), std::move(index_mapping)));
	reader.column_ids.push_back(local_id);
	reader.column_indexes.push_back(std::move(local_index));
}

ResultColumnMapping MultiFileColumnMapper::CreateColumnMappingByName() {
	auto &reader = *reader_data.reader;
	auto &local_columns = reader.GetColumns();
	auto &file_name = reader.GetFileName();
	// we have expected types: create a map of name -> (local) column id
	NameMapper name_map(local_columns);

	ResultColumnMapping result;
	auto &expressions = reader_data.expressions;
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto global_idx = MultiFileGlobalIndex(i);
		// check if this is a constant column
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
		// not constant - look up the column in the name map
		auto &global_id = global_column_ids[i];
		auto global_column_id = global_id.GetPrimaryIndex();
		if (IsVirtualColumn(global_column_id)) {
			// virtual column - these are emitted for every file
			auto virtual_entry = virtual_columns.find(global_column_id);
			if (virtual_entry == virtual_columns.end()) {
				throw InternalException("Virtual column id %d not found in virtual columns map", global_column_id);
			}
			expressions.push_back(make_uniq<BoundConstantExpression>(Value(virtual_entry->second.type)));
			continue;
		}
		if (global_column_id >= global_columns.size()) {
			throw InternalException(
			    "MultiFileReader::CreateColumnMappingByName - global_id is out of range in global_types for this file");
		}
		auto &global_column = global_columns[global_column_id];
		auto entry = name_map.Find(global_column);
		if (!entry.IsValid()) {
			// identifier not present in file, use default value
			if (global_column.default_expression) {
				reader_data.constant_map.Add(global_idx, global_column.GetDefaultValue());
				expressions.push_back(make_uniq<BoundConstantExpression>(global_column.GetDefaultValue()));
				continue;
			} else {
				string candidate_names;
				for (auto &column : local_columns) {
					if (!candidate_names.empty()) {
						candidate_names += ", ";
					}
					candidate_names += column.name;
				}
				throw InvalidInputException(
				    "Failed to read file \"%s\": schema mismatch in glob: column \"%s\" was read from "
				    "the original file \"%s\", but could not be found in file \"%s\".\nCandidate names: "
				    "%s\nIf you are trying to "
				    "read files with different schemas, try setting union_by_name=True",
				    file_name, global_column.GetIdentifierName(), initial_file, file_name, candidate_names);
			}
		}
		// we found the top-level column in the local file - check if the types are the same
		MultiFileLocalColumnId local_id(entry.GetIndex());
		D_ASSERT(global_column_id < global_columns.size());
		D_ASSERT(local_id.GetId() < local_columns.size());
		auto &global_type = global_columns[global_column_id].type;
		auto &local_type = local_columns[local_id.GetId()].type;
		if (reader.UseCastMap()) {
			// reader is responsible for converting types - just push a reference at this layer
			ColumnIndex local_index(local_id.GetId());
			auto local_idx = reader.column_ids.size();
			auto expr = make_uniq<BoundReferenceExpression>(global_type, local_idx);
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
		auto local_idx = MultiFileLocalIndex(reader.column_ids.size());
		// construct the expression to construct this column
		auto column_map = MapColumn(context, global_column, local_columns, name_map, local_idx);
		if (!column_map.column_index) {
			// no columns were presented
			reader_data.expressions.push_back(std::move(column_map.expression));
			continue;
		}
		auto local_index = std::move(column_map.column_index);
		D_ASSERT(local_index->GetPrimaryIndex() == local_id.GetId());
		reader_data.expressions.push_back(std::move(column_map.expression));

		MultiFileColumnMap index_mapping(std::move(*column_map.mapping), local_type, global_column.type, true);
		result.global_to_local.insert(make_pair(global_idx.GetIndex(), std::move(index_mapping)));
		reader.column_ids.emplace_back(local_id);
		reader.column_indexes.push_back(std::move(*local_index));
	}
	D_ASSERT(global_column_ids.size() == reader_data.expressions.size());
	return result;
}

ResultColumnMapping MultiFileColumnMapper::CreateColumnMappingByFieldId() {
#ifdef DEBUG
	//! Make sure the global columns have field_ids to match on
	for (auto &column : global_columns) {
		D_ASSERT(!column.identifier.IsNull());
		D_ASSERT(column.identifier.type().id() == LogicalTypeId::INTEGER);
	}
#endif

	auto &reader = *reader_data.reader;
	auto &local_columns = reader.GetColumns();

	ResultColumnMapping result;
	// we have expected types: create a map of field_id -> column index
	FieldIdMapper field_id_map(local_columns);

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
		auto global_column_id = global_column_ids[i].GetPrimaryIndex();

		if (IsVirtualColumn(global_column_id)) {
			// virtual column - these are emitted for every file
			auto virtual_entry = virtual_columns.find(global_column_id);
			if (virtual_entry == virtual_columns.end()) {
				throw InternalException("Virtual column id %d not found in virtual columns map", global_column_id);
			}
			expressions.push_back(make_uniq<BoundConstantExpression>(Value(virtual_entry->second.type)));
			continue;
		}

		auto local_idx = MultiFileLocalIndex(reader.column_ids.size());
		if (global_column_id >= global_columns.size()) {
			if (bind_data.file_row_number_idx == global_column_id) {
				// FIXME: this needs a more extensible solution
				auto new_column_id = MultiFileLocalColumnId(field_id_map.MapCount());
				reader.column_ids.push_back(new_column_id);
				reader.column_indexes.emplace_back(field_id_map.MapCount());
				//! FIXME: what to do here???
				expressions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, local_idx));
			} else {
				throw InternalException("Unexpected generated column");
			}
			continue;
		}

		const auto &global_column = global_columns[global_column_id];
		D_ASSERT(!global_column.identifier.IsNull());
		if (reader.UseCastMap()) {
			throw InternalException("Cast map is not supported for field-id based mapping");
		}
		
		// construct the expression to construct this column
		auto column_map = MapColumn(context, global_column, local_columns, field_id_map, local_idx);
		if (!column_map.column_index) {
			// no columns were presented
		    reader_data.expressions.push_back(std::move(column_map.expression));
		    continue;
		}
		auto local_index = std::move(column_map.column_index);
		auto local_id = local_index->GetPrimaryIndex();
		auto &local_type = local_columns[local_id].type;
	    reader_data.expressions.push_back(std::move(column_map.expression));

	    MultiFileColumnMap index_mapping(std::move(*column_map.mapping), local_type, global_column.type, true);
	    result.global_to_local.insert(make_pair(global_idx.GetIndex(), std::move(index_mapping)));
	    reader.column_ids.emplace_back(local_id);
	    reader.column_indexes.push_back(std::move(*local_index));
	}
	D_ASSERT(global_column_ids.size() == reader_data.expressions.size());
	return result;
}

ResultColumnMapping MultiFileColumnMapper::CreateColumnMapping() {
	switch (bind_data.mapping) {
	case MultiFileColumnMappingMode::BY_NAME: {
		return CreateColumnMappingByName();
	}
	case MultiFileColumnMappingMode::BY_FIELD_ID: {
		return CreateColumnMappingByFieldId();
	}
	default: {
		throw InternalException("Unsupported MultiFileColumnMappingMode type");
	}
	}
}

static bool EvaluateFilterAgainstConstant(TableFilter &filter, const Value &constant) {
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
		if (!dynamic_filter.filter_data->initialized) {
			//! Not initialized
			return true;
		}
		if (!dynamic_filter.filter_data->filter) {
			//! No filter present
			return true;
		}
		lock_guard<mutex> lock(dynamic_filter.filter_data->lock);
		return EvaluateFilterAgainstConstant(*dynamic_filter.filter_data->filter, constant);
	}
	default:
		throw NotImplementedException("Can't evaluate TableFilterType (%s) against a constant",
		                              EnumUtil::ToString(type));
	}
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
		auto global_column_id = global_column_ids[global_index].GetPrimaryIndex();
		Value constant_value;
		auto virtual_it = virtual_columns.find(global_column_ids[global_index].GetPrimaryIndex());
		if (virtual_it != virtual_columns.end()) {
			auto &virtual_column = virtual_it->second;
			if (virtual_column.name == "filename") {
				constant_value = Value(reader_data.reader->GetFileName());
			} else {
				throw InternalException("Unrecognized virtual column found: %s", virtual_column.name);
			}
		} else {
			bool has_constant = false;
			for (idx_t i = 0; i < reader_data.constant_map.size(); i++) {
				auto &constant_map_entry = reader_data.constant_map[MultiFileConstantMapIndex(i)];
				if (constant_map_entry.column_idx.GetIndex() == global_index) {
					has_constant = true;
					constant_value = constant_map_entry.value;
					break;
				}
			}
			if (!has_constant) {
				auto &global_column = global_columns[global_column_id];
				throw InternalException(
				    "Column '%s' is not present in the file, but no constant_map entry exists for it!",
				    global_column.name);
			}
		}

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
	default:
		throw NotImplementedException("Can't convert TableFilterType (%s) from global to local indexes",
		                              EnumUtil::ToString(type));
	}
}

void SetIndexToZero(Expression &expr) {
	if (expr.type == ExpressionType::BOUND_REF) {
		auto &ref = expr.Cast<BoundReferenceExpression>();
		ref.index = 0;
		return;
	}

	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { SetIndexToZero(child); });
}

bool CanPropagateCast(const MultiFileIndexMapping &mapping, const LogicalType &local_type, const LogicalType &global_type) {
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
		if (!map_entry.require_filter_cast) {
			// no conversion required - just copy the filter
			local_filter = global_filter.Copy();
		} else {
			// types are different - try to convert
			local_filter = TryCastTableFilter(global_filter, map_entry.mapping, local_type);
		}
		if (local_filter) {
			// succeeded in casting - push the local filter
			result->filters.emplace(local_id, std::move(local_filter));
		} else {
			// failed to cast - copy the global filter and evaluate the conversion expression in the reader
			result->filters.emplace(local_id, global_filter.Copy());

			// add the expression to the expression map - we are now evaluating this inside the reader directly
			// we need to set the index of the references inside the expression to 0
			SetIndexToZero(*reader_data.expressions[local_id]);
			reader.expression_map[filter_idx] = std::move(reader_data.expressions[local_id]);

			// reset the expression - since we are evaluating it in the reader we can just reference it
			reader_data.expressions[local_id] = make_uniq<BoundReferenceExpression>(global_type, local_id);
		}
	}
	return result;
}

ReaderInitializeType MultiFileColumnMapper::CreateMapping() {
	// copy global columns and inject any different defaults
	auto result = CreateColumnMapping();
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
