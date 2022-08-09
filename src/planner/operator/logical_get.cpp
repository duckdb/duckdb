#include "duckdb/planner/operator/logical_get.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/function/table/table_scan.hpp"

#include "duckdb.hpp" // FIXME
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

namespace duckdb {

LogicalGet::LogicalGet(idx_t table_index, TableFunction function, unique_ptr<FunctionData> bind_data,
                       vector<LogicalType> returned_types, vector<string> returned_names)
    : LogicalOperator(LogicalOperatorType::LOGICAL_GET), table_index(table_index), function(move(function)),
      bind_data(move(bind_data)), returned_types(move(returned_types)), names(move(returned_names)) {
}

string LogicalGet::GetName() const {
	return StringUtil::Upper(function.name);
}

TableCatalogEntry *LogicalGet::GetTable() const {
	return TableScanFunction::GetTableEntry(function, bind_data.get());
}

string LogicalGet::ParamsToString() const {
	string result;
	for (auto &kv : table_filters.filters) {
		auto &column_index = kv.first;
		auto &filter = kv.second;
		if (column_index < names.size()) {
			result += filter->ToString(names[column_index]);
		}
		result += "\n";
	}
	if (!function.to_string) {
		return string();
	}
	return function.to_string(bind_data.get());
}

vector<ColumnBinding> LogicalGet::GetColumnBindings() {
	if (column_ids.empty()) {
		return {ColumnBinding(table_index, 0)};
	}
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		result.emplace_back(table_index, i);
	}
	return result;
}

void LogicalGet::ResolveTypes() {
	if (column_ids.empty()) {
		column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	for (auto &index : column_ids) {
		if (index == COLUMN_IDENTIFIER_ROW_ID) {
			types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			types.push_back(returned_types[index]);
		}
	}
}

idx_t LogicalGet::EstimateCardinality(ClientContext &context) {
	if (function.cardinality) {
		auto node_stats = function.cardinality(context, bind_data.get());
		if (node_stats && node_stats->has_estimated_cardinality) {
			return node_stats->estimated_cardinality;
		}
	}
	return 1;
}

void LogicalGet::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteRegularSerializableList(returned_types);
	writer.WriteList<string>(names);
	writer.WriteList<column_t>(column_ids);
	writer.WriteSerializable(table_filters);
	D_ASSERT(!function.name.empty());
	writer.WriteString(function.name);
	writer.WriteRegularSerializableList(function.arguments);
	writer.WriteField(bind_data != nullptr);
	if (bind_data && !function.serialize) {
		throw InvalidInputException("Can't serialize table function %s", function.name);
	}
	function.serialize(writer, *bind_data, function);
}

unique_ptr<LogicalOperator> LogicalGet::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                    FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto returned_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto returned_names = reader.ReadRequiredList<string>();
	auto column_ids = reader.ReadRequiredList<column_t>();
	auto table_filters = reader.ReadRequiredSerializable<TableFilterSet>();
	auto name = reader.ReadRequired<string>();
	auto arguments = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();

	auto has_bind_data = reader.ReadRequired<bool>();

	auto &catalog = Catalog::GetCatalog(context);

	auto func_catalog = catalog.GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, DEFAULT_SCHEMA, name);

	if (!func_catalog || func_catalog->type != CatalogType::TABLE_FUNCTION_ENTRY) {
		throw InternalException("Cant find catalog entry for function %s", name);
	}

	auto functions = (TableFunctionCatalogEntry *)func_catalog;
	auto function = functions->functions.GetFunctionByArguments(arguments);
	unique_ptr<FunctionData> bind_data;

	if (has_bind_data) {
		if (!function.deserialize) {
			throw SerializationException("Have bind info but no deserialization function for %s", function.name);
		}
		bind_data = function.deserialize(context, reader, function);
	}

	auto result = make_unique<LogicalGet>(table_index, function, move(bind_data), returned_types, returned_names);
	result->column_ids = column_ids;
	result->table_filters = move(*table_filters);
	return result;
}

} // namespace duckdb
