#include "duckdb/function/table/information_schema_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/transaction/transaction.hpp"

using namespace std;

namespace duckdb {

struct InformationSchemaSchemataData : public FunctionOperatorData {
	InformationSchemaSchemataData() : offset(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> information_schema_schemata_bind(ClientContext &context, vector<Value> &inputs,
                                                                 unordered_map<string, Value> &named_parameters,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<string> &names) {
	names.push_back("catalog_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("schema_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("schema_owner");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("default_character_set_catalog");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("default_character_set_schema");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("default_character_set_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("sql_path");
	return_types.push_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<FunctionOperatorData>
information_schema_schemata_init(ClientContext &context, const FunctionData *bind_data, ParallelState *state,
                                 vector<column_t> &column_ids,
                                 unordered_map<idx_t, vector<TableFilter>> &table_filters) {
	auto result = make_unique<InformationSchemaSchemataData>();

	// scan all the schemas for tables and views and collect them
	auto &transaction = Transaction::GetTransaction(context);
	Catalog::GetCatalog(context).schemas->Scan(transaction,
	                                           [&](CatalogEntry *entry) { result->entries.push_back(entry); });

	return move(result);
}

void information_schema_schemata(ClientContext &context, const FunctionData *bind_data,
                                 FunctionOperatorData *operator_state, DataChunk &output) {
	auto &data = (InformationSchemaSchemataData &)*operator_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	idx_t next = min(data.offset + STANDARD_VECTOR_SIZE, (idx_t)data.entries.size());
	output.SetCardinality(next - data.offset);

	// start returning values
	// either fill up the chunk or return all the remaining columns
	for (idx_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto &entry = data.entries[i];

		// return values:
		// "catalog_name", PhysicalType::VARCHAR
		// TODO(jwills): how to determine this?
		output.SetValue(0, index, Value("main"));
		// "schema_name", PhysicalType::VARCHAR
		output.SetValue(1, index, Value(entry->name));
		// "schema_owner", PhysicalType::VARCHAR
		output.SetValue(2, index, Value());
		// "default_character_set_catalog", PhysicalType::VARCHAR
		output.SetValue(3, index, Value());
		// "default_character_set_schema", PhysicalType::VARCHAR
		output.SetValue(4, index, Value());
		// "default_character_set_name", PhysicalType::VARCHAR
		output.SetValue(5, index, Value());
		// "sql_path", PhysicalType::VARCHAR
		output.SetValue(6, index, Value());
	}
	data.offset = next;
}

void InformationSchemaSchemata::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("information_schema_schemata", {}, information_schema_schemata,
	                              information_schema_schemata_bind, information_schema_schemata_init));
}

} // namespace duckdb
