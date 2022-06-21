#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/storage/data_table.hpp"

#include <algorithm>

namespace duckdb {

struct PragmaStorageFunctionData : public TableFunctionData {
	explicit PragmaStorageFunctionData(TableCatalogEntry *table_entry) : table_entry(table_entry) {
	}

	TableCatalogEntry *table_entry;
	vector<vector<Value>> storage_info;
};

struct PragmaStorageOperatorData : public GlobalTableFunctionState {
	PragmaStorageOperatorData() : offset(0) {
	}

	idx_t offset;
};

static unique_ptr<FunctionData> PragmaStorageInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("row_group_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("column_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("column_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("column_path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("segment_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("segment_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("start");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("compression");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("stats");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("has_updates");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("persistent");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("block_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("block_offset");
	return_types.emplace_back(LogicalType::BIGINT);

	auto qname = QualifiedName::Parse(input.inputs[0].GetValue<string>());

	// look up the table name in the catalog
	auto &catalog = Catalog::GetCatalog(context);
	auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, qname.schema, qname.name);
	if (entry->type != CatalogType::TABLE_ENTRY) {
		throw Exception("storage_info requires a table as parameter");
	}
	auto table_entry = (TableCatalogEntry *)entry;

	auto result = make_unique<PragmaStorageFunctionData>(table_entry);
	result->storage_info = table_entry->storage->GetStorageInfo();
	return move(result);
}

unique_ptr<GlobalTableFunctionState> PragmaStorageInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_unique<PragmaStorageOperatorData>();
}

static void PragmaStorageInfoFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (PragmaStorageFunctionData &)*data_p.bind_data;
	auto &data = (PragmaStorageOperatorData &)*data_p.global_state;
	idx_t count = 0;
	while (data.offset < bind_data.storage_info.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.storage_info[data.offset++];
		D_ASSERT(entry.size() + 1 == output.ColumnCount());
		idx_t result_idx = 0;
		for (idx_t col_idx = 0; col_idx < entry.size(); col_idx++, result_idx++) {
			if (col_idx == 1) {
				// write the column name
				auto column_index = entry[col_idx].GetValue<int64_t>();
				output.SetValue(result_idx, count, Value(bind_data.table_entry->columns[column_index].Name()));
				result_idx++;
			}
			output.SetValue(result_idx, count, entry[col_idx]);
		}

		count++;
	}
	output.SetCardinality(count);
}

void PragmaStorageInfo::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_storage_info", {LogicalType::VARCHAR}, PragmaStorageInfoFunction,
	                              PragmaStorageInfoBind, PragmaStorageInfoInit));
}

} // namespace duckdb
