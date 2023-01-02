#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/collate_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct PragmaCollateData : public GlobalTableFunctionState {
	PragmaCollateData() : offset(0) {
	}

	vector<string> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> PragmaCollateBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("collname");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> PragmaCollateInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<PragmaCollateData>();

	auto schemas = Catalog::GetAllSchemas(context);
	for (auto schema : schemas) {
		schema->Scan(context, CatalogType::COLLATION_ENTRY,
		             [&](CatalogEntry *entry) { result->entries.push_back(entry->name); });
	}
	return std::move(result);
}

static void PragmaCollateFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (PragmaCollateData &)*data_p.global_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	idx_t next = MinValue<idx_t>(data.offset + STANDARD_VECTOR_SIZE, data.entries.size());
	output.SetCardinality(next - data.offset);
	for (idx_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		output.SetValue(0, index, Value(data.entries[i]));
	}

	data.offset = next;
}

void PragmaCollations::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("pragma_collations", {}, PragmaCollateFunction, PragmaCollateBind, PragmaCollateInit));
}

} // namespace duckdb
