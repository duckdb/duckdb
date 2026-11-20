#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/function/table/system_catalog_scanner.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

struct DuckDBTablesData : public GlobalTableFunctionState {
	explicit DuckDBTablesData(idx_t sample_row_groups) : sample_row_groups(sample_row_groups) {
	}

	vector<reference<CatalogEntry>> entries;
	vector<column_t> column_ids;
	idx_t offset = 0;
	idx_t sample_row_groups;
	SystemCatalogScannerStats stats;
};

static unique_ptr<FunctionData> DuckDBTablesBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("database_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("comment");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("tags");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("temporary");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("has_primary_key");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("estimated_size");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("column_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("index_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("check_constraint_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("estimated_data_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	SystemCatalogIdentityColumns columns;
	columns.entry_name = "table_name";
	columns.entry_oid = "table_oid";
	return SystemCatalogScanner::CreateBindData(names, return_types, columns);
}

unique_ptr<GlobalTableFunctionState> DuckDBTablesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBTablesData>(Settings::Get<TableSizeSampleRowGroupsSetting>(context));

	// collect the tables that survive the pushed-down filters
	result->entries = SystemCatalogScanner::GetEntries(context, input, CatalogType::TABLE_ENTRY, result->stats);
	result->column_ids = std::move(input.column_ids);
	return std::move(result);
}

static idx_t CheckConstraintCount(TableCatalogEntry &table) {
	idx_t check_count = 0;
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type == ConstraintType::CHECK) {
			check_count++;
		}
	}
	return check_count;
}

static Value GetTableColumnValue(ClientContext &context, TableCatalogEntry &table, column_t column_id,
                                 idx_t sample_row_groups, optional<TableStorageInfo> &storage_info) {
	auto get_storage_info = [&]() -> const TableStorageInfo & {
		if (!storage_info) {
			storage_info = table.GetStorageInfo(context);
		}
		return *storage_info;
	};
	switch (column_id) {
	case 0: // database_name
		return Value(table.catalog.GetName());
	case 1: // database_oid
		return Value::BIGINT(NumericCast<int64_t>(table.catalog.GetOid()));
	case 2: // schema_name
		return Value(table.schema.name);
	case 3: // schema_oid
		return Value::BIGINT(NumericCast<int64_t>(table.schema.oid));
	case 4: // table_name
		return Value(table.name);
	case 5: // table_oid
		return Value::BIGINT(NumericCast<int64_t>(table.oid));
	case 6: // comment
		return Value(table.comment);
	case 7: // tags
		return Value::MAP(table.tags);
	case 8: // internal
		return Value::BOOLEAN(table.internal);
	case 9: // temporary
		return Value::BOOLEAN(table.temporary);
	case 10: // has_primary_key
		return Value::BOOLEAN(table.HasPrimaryKey());
	case 11: { // estimated_size
		auto cardinality = get_storage_info().cardinality;
		return cardinality.IsValid() ? Value::BIGINT(NumericCast<int64_t>(cardinality.GetIndex())) : Value();
	}
	case 12: // column_count
		return Value::BIGINT(NumericCast<int64_t>(table.GetColumns().LogicalColumnCount()));
	case 13: // index_count
		return Value::BIGINT(NumericCast<int64_t>(get_storage_info().index_info.size()));
	case 14: // check_constraint_count
		return Value::BIGINT(NumericCast<int64_t>(CheckConstraintCount(table)));
	case 15: { // sql
		auto info = table.GetInfo();
		info->StripCatalogQualification();
		return Value(info->ToString());
	}
	case 16: { // estimated_data_bytes
		if (!table.IsDuckTable()) {
			return Value();
		}
		auto bytes = table.GetStorage().GetEstimatedDataSize(context, sample_row_groups);
		return bytes.IsValid() ? Value::BIGINT(NumericCast<int64_t>(bytes.GetIndex())) : Value();
	}
	default:
		throw InternalException("Unsupported column index for duckdb_tables");
	}
}

void DuckDBTablesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBTablesData>();
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++].get();

		if (entry.type != CatalogType::TABLE_ENTRY) {
			continue;
		}
		auto &table = entry.Cast<TableCatalogEntry>();
		optional<TableStorageInfo> storage_info;
		for (idx_t col = 0; col < data.column_ids.size(); col++) {
			output.data[col].Append(
			    GetTableColumnValue(context, table, data.column_ids[col], data.sample_row_groups, storage_info));
		}
		count++;
	}
}

static void DuckDBTablesGetMetrics(TableFunctionGetMetricsInput &input) {
	SystemCatalogScanner::ReportMetrics(input, input.global_state->Cast<DuckDBTablesData>().stats);
}

void DuckDBTablesFun::RegisterFunction(BuiltinFunctions &set) {
	auto function = TableFunction("duckdb_tables", {}, DuckDBTablesFunction, DuckDBTablesBind, DuckDBTablesInit);
	function.projection_pushdown = true;
	SystemCatalogScanner::RegisterPushdown(function);
	function.get_metrics = DuckDBTablesGetMetrics;
	function.stability = FunctionStability::VOLATILE;
	set.AddFunction(std::move(function));
}

} // namespace duckdb
