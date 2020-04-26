#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/collate_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/transaction/transaction.hpp"

using namespace std;

namespace duckdb {

struct PragmaCollateData : public TableFunctionData {
	PragmaCollateData() : initialized(false), offset(0) {
	}

	bool initialized;
	vector<CatalogEntry *> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> pragma_collate_bind(ClientContext &context, vector<Value> inputs,
                                                    vector<SQLType> &return_types, vector<string> &names) {
	names.push_back("collname");
	return_types.push_back(SQLType::VARCHAR);

	return make_unique<PragmaCollateData>();
}

static void pragma_collate_info(ClientContext &context, vector<Value> &input, DataChunk &output,
                                FunctionData *dataptr) {
	auto &data = *((PragmaCollateData *)dataptr);
	assert(input.size() == 0);
	if (!data.initialized) {
		// scan all the schemas
		auto &transaction = Transaction::GetTransaction(context);
		Catalog::GetCatalog(context).schemas.Scan(transaction, [&](CatalogEntry *entry) {
			auto schema = (SchemaCatalogEntry *)entry;
			schema->collations.Scan(transaction, [&](CatalogEntry *entry) { data.entries.push_back(entry); });
		});
		data.initialized = true;
	}

	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	idx_t next = min(data.offset + STANDARD_VECTOR_SIZE, (idx_t)data.entries.size());
	output.SetCardinality(next - data.offset);
	for (idx_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto entry = (CollateCatalogEntry *)data.entries[i];

		output.SetValue(0, index, Value(entry->name));
	}

	data.offset = next;
}

void PragmaCollations::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_collations", {}, pragma_collate_bind, pragma_collate_info, nullptr));
}

} // namespace duckdb
