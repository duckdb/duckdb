#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/function/table/system_catalog_scanner.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/storage/data_table.hpp"

#include <algorithm>

namespace duckdb {

struct DuckDBTableStorageBindData : public SystemCatalogScannerData {
	DuckDBTableStorageBindData(const SystemCatalogScannerData &scanner, idx_t sample_row_groups_p)
	    : SystemCatalogScannerData(scanner), sample_row_groups(sample_row_groups_p) {
	}

	idx_t sample_row_groups;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DuckDBTableStorageBindData>(*this);
	}

	bool Equals(const FunctionData &other) const override {
		return SystemCatalogScannerData::Equals(other) &&
		       sample_row_groups == other.Cast<DuckDBTableStorageBindData>().sample_row_groups;
	}
};

struct DuckDBTableStorageState : public GlobalTableFunctionState {
	vector<reference<CatalogEntry>> entries;
	vector<column_t> column_ids;
	idx_t offset = 0;
	SystemCatalogScannerStats stats;

	TableCatalogEntry &TableAt(idx_t row) const {
		return entries[offset + row].get().Cast<TableCatalogEntry>();
	}
};

static unique_ptr<FunctionData> DuckDBTableStorageBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &budget = input.named_parameters.at("sample_row_groups");
	if (budget.IsNull() || BigIntValue::Get(budget) <= 0) {
		throw InvalidInputException("sample_row_groups must be a positive integer");
	}
	names = {"database_name", "schema_name", "table_name", "estimated_data_bytes"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT};
	SystemCatalogIdentityColumns columns;
	columns.entry_name = "table_name";
	auto scanner = SystemCatalogScanner::CreateBindData(names, return_types, columns);
	return make_uniq<DuckDBTableStorageBindData>(scanner->Cast<SystemCatalogScannerData>(),
	                                             NumericCast<idx_t>(BigIntValue::Get(budget)));
}

static unique_ptr<GlobalTableFunctionState> DuckDBTableStorageInit(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto state = make_uniq<DuckDBTableStorageState>();
	auto &entries = state->entries;
	entries = SystemCatalogScanner::GetEntries(context, input, CatalogType::TABLE_ENTRY, state->stats);
	// A schema's table catalog set can also contain views.
	entries.erase(std::remove_if(entries.begin(), entries.end(),
	                             [](const CatalogEntry &entry) { return entry.type != CatalogType::TABLE_ENTRY; }),
	              entries.end());
	state->column_ids = std::move(input.column_ids);
	return std::move(state);
}

static const Identifier &TableStorageName(const TableCatalogEntry &table, column_t column_id) {
	switch (column_id) {
	case 0:
		return table.catalog.GetName();
	case 1:
		return table.schema.name;
	case 2:
		return table.name;
	default:
		throw InternalException("Unsupported name column for duckdb_table_storage");
	}
}

static void DuckDBTableStorageFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.global_state->Cast<DuckDBTableStorageState>();
	auto &bind_data = input.bind_data->Cast<DuckDBTableStorageBindData>();
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, data.entries.size() - data.offset);
	for (idx_t col = 0; col < data.column_ids.size(); col++) {
		auto column_id = data.column_ids[col];
		if (column_id == 3) {
			auto writer = FlatVector::Writer<int64_t>(output.data[col], count);
			for (idx_t i = 0; i < count; i++) {
				auto &table = data.TableAt(i);
				optional_idx bytes;
				if (table.IsDuckTable()) {
					bytes = table.GetStorage().GetEstimatedDataSize(context, bind_data.sample_row_groups);
				}
				if (bytes.IsValid()) {
					writer.WriteValue(NumericCast<int64_t>(bytes.GetIndex()));
				} else {
					writer.WriteNull();
				}
			}
			continue;
		}
		auto writer = FlatVector::Writer<string_t>(output.data[col], count);
		for (idx_t i = 0; i < count; i++) {
			writer.WriteValue(TableStorageName(data.TableAt(i), column_id).GetIdentifierName());
		}
	}
	data.offset += count;
}

void DuckDBTableStorageFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction function("duckdb_table_storage", {}, DuckDBTableStorageFunction, DuckDBTableStorageBind,
	                       DuckDBTableStorageInit);
	function.projection_pushdown = true;
	function.GetSignature().AddKeywordOnly("sample_row_groups", LogicalType::BIGINT, Value::BIGINT(4));
	SystemCatalogScanner::RegisterPushdown(function);
	function.get_metrics = [](TableFunctionGetMetricsInput &input) {
		SystemCatalogScanner::ReportMetrics(input, input.global_state->Cast<DuckDBTableStorageState>().stats);
	};
	function.stability = FunctionStability::VOLATILE;
	set.AddFunction(std::move(function));
}

} // namespace duckdb
