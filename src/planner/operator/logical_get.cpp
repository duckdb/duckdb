#include "duckdb/planner/operator/logical_get.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_serialization.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

LogicalGet::LogicalGet() : LogicalOperator(LogicalOperatorType::LOGICAL_GET) {
}

LogicalGet::LogicalGet(idx_t table_index, TableFunction function, unique_ptr<FunctionData> bind_data,
                       vector<LogicalType> returned_types, vector<string> returned_names, LogicalType rowid_type)
    : LogicalOperator(LogicalOperatorType::LOGICAL_GET), table_index(table_index), function(std::move(function)),
      bind_data(std::move(bind_data)), returned_types(std::move(returned_types)), names(std::move(returned_names)),
      extra_info(), rowid_type(std::move(rowid_type)) {
}

optional_ptr<TableCatalogEntry> LogicalGet::GetTable() const {
	if (!function.get_bind_info) {
		return nullptr;
	}
	return function.get_bind_info(bind_data.get()).table;
}

InsertionOrderPreservingMap<string> LogicalGet::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;

	string filters_info;
	bool first_item = true;
	for (auto &kv : table_filters.filters) {
		auto &column_index = kv.first;
		auto &filter = kv.second;
		if (column_index < names.size()) {
			if (!first_item) {
				filters_info += "\n";
			}
			first_item = false;
			filters_info += filter->ToString(names[column_index]);
		}
	}
	result["Filters"] = filters_info;

	if (extra_info.sample_options) {
		result["Sample Method"] = "System: " + extra_info.sample_options->sample_size.ToString() + "%";
	}

	if (!extra_info.file_filters.empty()) {
		result["File Filters"] = extra_info.file_filters;
		if (extra_info.filtered_files.IsValid() && extra_info.total_files.IsValid()) {
			result["Scanning Files"] = StringUtil::Format("%llu/%llu", extra_info.filtered_files.GetIndex(),
			                                              extra_info.total_files.GetIndex());
		}
	}

	if (function.to_string) {
		TableFunctionToStringInput input(function, bind_data.get());
		auto to_string_result = function.to_string(input);
		for (const auto &it : to_string_result) {
			result[it.first] = it.second;
		}
	}
	SetParamsEstimatedCardinality(result);
	return result;
}

void LogicalGet::SetColumnIds(vector<ColumnIndex> &&column_ids) {
	this->column_ids = std::move(column_ids);
}

void LogicalGet::AddColumnId(column_t column_id) {
	column_ids.emplace_back(column_id);
}

void LogicalGet::ClearColumnIds() {
	column_ids.clear();
}

const vector<ColumnIndex> &LogicalGet::GetColumnIds() const {
	return column_ids;
}

vector<ColumnIndex> &LogicalGet::GetMutableColumnIds() {
	return column_ids;
}

vector<ColumnBinding> LogicalGet::GetColumnBindings() {
	if (column_ids.empty()) {
		return {ColumnBinding(table_index, 0)};
	}
	vector<ColumnBinding> result;
	if (projection_ids.empty()) {
		for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
			result.emplace_back(table_index, col_idx);
		}
	} else {
		for (auto proj_id : projection_ids) {
			result.emplace_back(table_index, proj_id);
		}
	}
	if (!projected_input.empty()) {
		if (children.size() != 1) {
			throw InternalException("LogicalGet::project_input can only be set for table-in-out functions");
		}
		auto child_bindings = children[0]->GetColumnBindings();
		for (auto entry : projected_input) {
			D_ASSERT(entry < child_bindings.size());
			result.emplace_back(child_bindings[entry]);
		}
	}
	return result;
}

void LogicalGet::ResolveTypes() {
	if (column_ids.empty()) {
		column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	types.clear();
	if (projection_ids.empty()) {
		for (auto &index : column_ids) {
			if (index.IsRowIdColumn()) {
				types.emplace_back(LogicalType(rowid_type));
			} else {
				types.push_back(returned_types[index.GetPrimaryIndex()]);
			}
		}
	} else {
		for (auto &proj_index : projection_ids) {
			auto &index = column_ids[proj_index];
			if (index.IsRowIdColumn()) {
				types.emplace_back(LogicalType(rowid_type));
			} else {
				types.push_back(returned_types[index.GetPrimaryIndex()]);
			}
		}
	}
	if (!projected_input.empty()) {
		if (children.size() != 1) {
			throw InternalException("LogicalGet::project_input can only be set for table-in-out functions");
		}
		for (auto entry : projected_input) {
			D_ASSERT(entry < children[0]->types.size());
			types.push_back(children[0]->types[entry]);
		}
	}
}

idx_t LogicalGet::EstimateCardinality(ClientContext &context) {
	// join order optimizer does better cardinality estimation.
	if (has_estimated_cardinality) {
		return estimated_cardinality;
	}
	if (function.cardinality) {
		auto node_stats = function.cardinality(context, bind_data.get());
		if (node_stats && node_stats->has_estimated_cardinality) {
			return node_stats->estimated_cardinality;
		}
	}
	if (!children.empty()) {
		return children[0]->EstimateCardinality(context);
	}
	return 1;
}

void LogicalGet::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WriteProperty(200, "table_index", table_index);
	serializer.WriteProperty(201, "returned_types", returned_types);
	serializer.WriteProperty(202, "names", names);
	/* [Deleted] (vector<column_t>) "column_ids" */
	serializer.WriteProperty(204, "projection_ids", projection_ids);
	serializer.WriteProperty(205, "table_filters", table_filters);
	FunctionSerializer::Serialize(serializer, function, bind_data.get());
	if (!function.serialize) {
		D_ASSERT(!function.serialize);
		// no serialize method: serialize input values and named_parameters for rebinding purposes
		serializer.WriteProperty(206, "parameters", parameters);
		serializer.WriteProperty(207, "named_parameters", named_parameters);
		serializer.WriteProperty(208, "input_table_types", input_table_types);
		serializer.WriteProperty(209, "input_table_names", input_table_names);
	}
	serializer.WriteProperty(210, "projected_input", projected_input);
	serializer.WritePropertyWithDefault(211, "column_indexes", column_ids);
}

unique_ptr<LogicalOperator> LogicalGet::Deserialize(Deserializer &deserializer) {
	vector<column_t> legacy_column_ids;

	auto result = unique_ptr<LogicalGet>(new LogicalGet());
	deserializer.ReadProperty(200, "table_index", result->table_index);
	deserializer.ReadProperty(201, "returned_types", result->returned_types);
	deserializer.ReadProperty(202, "names", result->names);
	deserializer.ReadPropertyWithDefault(203, "column_ids", legacy_column_ids);
	deserializer.ReadProperty(204, "projection_ids", result->projection_ids);
	deserializer.ReadProperty(205, "table_filters", result->table_filters);
	auto entry = FunctionSerializer::DeserializeBase<TableFunction, TableFunctionCatalogEntry>(
	    deserializer, CatalogType::TABLE_FUNCTION_ENTRY);
	result->function = entry.first;
	auto &function = result->function;
	auto has_serialize = entry.second;
	unique_ptr<FunctionData> bind_data;
	if (!has_serialize) {
		deserializer.ReadProperty(206, "parameters", result->parameters);
		deserializer.ReadProperty(207, "named_parameters", result->named_parameters);
		deserializer.ReadProperty(208, "input_table_types", result->input_table_types);
		deserializer.ReadProperty(209, "input_table_names", result->input_table_names);
	} else {
		bind_data = FunctionSerializer::FunctionDeserialize(deserializer, function);
	}
	deserializer.ReadProperty(210, "projected_input", result->projected_input);
	deserializer.ReadPropertyWithDefault(211, "column_indexes", result->column_ids);
	if (!legacy_column_ids.empty()) {
		if (!result->column_ids.empty()) {
			throw SerializationException(
			    "LogicalGet::Deserialize - either column_ids or column_indexes should be set - not both");
		}
		for (auto &col_id : legacy_column_ids) {
			result->column_ids.emplace_back(col_id);
		}
	}
	if (!has_serialize) {
		TableFunctionRef empty_ref;
		TableFunctionBindInput input(result->parameters, result->named_parameters, result->input_table_types,
		                             result->input_table_names, function.function_info.get(), nullptr, result->function,
		                             empty_ref);

		vector<LogicalType> bind_return_types;
		vector<string> bind_names;
		if (!function.bind) {
			throw InternalException("Table function \"%s\" has neither bind nor (de)serialize", function.name);
		}
		bind_data = function.bind(deserializer.Get<ClientContext &>(), input, bind_return_types, bind_names);

		for (auto &col_id : result->column_ids) {
			if (col_id.IsRowIdColumn()) {
				// rowid
				continue;
			}
			auto idx = col_id.GetPrimaryIndex();
			auto &ret_type = result->returned_types[idx];
			auto &col_name = result->names[idx];
			if (bind_return_types[idx] != ret_type) {
				throw SerializationException("Table function deserialization failure in function \"%s\" - column with "
				                             "name %s was serialized with type %s, but now has type %s",
				                             function.name, col_name, ret_type, bind_return_types[idx]);
			}
		}
		result->returned_types = std::move(bind_return_types);
	}
	result->bind_data = std::move(bind_data);
	return std::move(result);
}

vector<idx_t> LogicalGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return StringUtil::Upper(function.name) + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return StringUtil::Upper(function.name);
}

} // namespace duckdb
