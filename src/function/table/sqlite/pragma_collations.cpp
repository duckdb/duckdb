#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/collate_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"

using namespace std;

namespace duckdb {

struct PragmaCollateData : public FunctionOperatorData {
	PragmaCollateData() : offset(0) {
	}

	vector<string> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> pragma_collate_bind(ClientContext &context, vector<Value> &inputs,
                                                    unordered_map<string, Value> &named_parameters,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("collname");
	return_types.push_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<FunctionOperatorData> pragma_collate_init(ClientContext &context, const FunctionData *bind_data,
                                                     vector<column_t> &column_ids,
                                                     unordered_map<idx_t, vector<TableFilter>> &table_filters) {
	auto result = make_unique<PragmaCollateData>();

	Catalog::GetCatalog(context).schemas->Scan(context, [&](CatalogEntry *entry) {
		auto schema = (SchemaCatalogEntry *)entry;
		schema->collations.Scan(context, [&](CatalogEntry *entry) { result->entries.push_back(entry->name); });
	});

	return move(result);
}

static void pragma_collate(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                           DataChunk &output) {
	auto &data = (PragmaCollateData &)*operator_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	idx_t next = min(data.offset + STANDARD_VECTOR_SIZE, (idx_t)data.entries.size());
	output.SetCardinality(next - data.offset);
	for (idx_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		output.SetValue(0, index, Value(data.entries[i]));
	}

	data.offset = next;
}

void PragmaCollations::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_collations", {}, pragma_collate, pragma_collate_bind, pragma_collate_init));
}

} // namespace duckdb
