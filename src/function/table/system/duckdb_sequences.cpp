#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct DuckDBSequencesData : public GlobalTableFunctionState {
	DuckDBSequencesData() : offset(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBSequencesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("sequence_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("sequence_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("temporary");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("start_value");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("min_value");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("max_value");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("increment_by");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("cycle");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("last_value");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBSequencesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<DuckDBSequencesData>();

	// scan all the schemas for tables and collect themand collect them
	auto schemas = Catalog::GetCatalog(context).schemas->GetEntries<SchemaCatalogEntry>(context);
	for (auto &schema : schemas) {
		schema->Scan(context, CatalogType::SEQUENCE_ENTRY,
		             [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	};

	// check the temp schema as well
	ClientData::Get(context).temporary_objects->Scan(context, CatalogType::SEQUENCE_ENTRY,
	                                                 [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	return move(result);
}

void DuckDBSequencesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DuckDBSequencesData &)*data_p.global_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];

		auto &seq = (SequenceCatalogEntry &)*entry;
		// return values:
		// schema_name, VARCHAR
		output.SetValue(0, count, Value(seq.schema->name));
		// schema_oid, BIGINT
		output.SetValue(1, count, Value::BIGINT(seq.schema->oid));
		// sequence_name, VARCHAR
		output.SetValue(2, count, Value(seq.name));
		// sequence_oid, BIGINT
		output.SetValue(3, count, Value::BIGINT(seq.oid));
		// temporary, BOOLEAN
		output.SetValue(4, count, Value::BOOLEAN(seq.temporary));
		// start_value, BIGINT
		output.SetValue(5, count, Value::BIGINT(seq.start_value));
		// min_value, BIGINT
		output.SetValue(6, count, Value::BIGINT(seq.min_value));
		// max_value, BIGINT
		output.SetValue(7, count, Value::BIGINT(seq.max_value));
		// increment_by, BIGINT
		output.SetValue(8, count, Value::BIGINT(seq.increment));
		// cycle, BOOLEAN
		output.SetValue(9, count, Value::BOOLEAN(seq.cycle));
		// last_value, BIGINT
		output.SetValue(10, count, seq.usage_count == 0 ? Value() : Value::BOOLEAN(seq.last_value));
		// sql, LogicalType::VARCHAR
		output.SetValue(11, count, Value(seq.ToSQL()));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBSequencesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_sequences", {}, DuckDBSequencesFunction, DuckDBSequencesBind, DuckDBSequencesInit));
}

} // namespace duckdb
