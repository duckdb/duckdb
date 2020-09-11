#include "duckdb/function/table/information_schema_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction.hpp"

using namespace std;

namespace duckdb {

struct InformationSchemaTablesData : public FunctionOperatorData {
	InformationSchemaTablesData() : offset(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> information_schema_tables_bind(ClientContext &context, vector<Value> &inputs,
                                                               unordered_map<string, Value> &named_parameters,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	names.push_back("table_catalog");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("table_schema");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("table_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("table_type");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("self_referencing_column_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("reference_generation");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("user_defined_type_catalog");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("user_defined_type_schema");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("user_defined_type_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("is_insertable_into");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("is_typed");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("commit_action");
	return_types.push_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<FunctionOperatorData>
information_schema_tables_init(ClientContext &context, const FunctionData *bind_data, ParallelState *state,
                               vector<column_t> &column_ids, unordered_map<idx_t, vector<TableFilter>> &table_filters) {
	auto result = make_unique<InformationSchemaTablesData>();

	// scan all the schemas for tables and views and collect them
	auto &transaction = Transaction::GetTransaction(context);
	Catalog::GetCatalog(context).schemas->Scan(transaction, [&](CatalogEntry *entry) {
		auto schema = (SchemaCatalogEntry *)entry;
		schema->tables.Scan(transaction, [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	});

	// check the temp schema as well
	context.temporary_objects->tables.Scan(transaction, [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	return move(result);
}

void information_schema_tables(ClientContext &context, const FunctionData *bind_data,
                               FunctionOperatorData *operator_state, DataChunk &output) {
	auto &data = (InformationSchemaTablesData &)*operator_state;
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
		auto entry = (StandardEntry *)data.entries[i];

		const char *table_type;
		const char *is_insertable_into = "NO";
		switch (entry->type) {
		case CatalogType::TABLE_ENTRY:
			if (entry->temporary) {
				table_type = "LOCAL TEMPORARY";
			} else {
				table_type = "BASE TABLE";
			}
			is_insertable_into = "YES";
			break;
		case CatalogType::VIEW_ENTRY:
			table_type = "VIEW";
			break;
		default:
			table_type = "UNKNOWN";
			break;
		}

		// return values:
		// "table_catalog", PhysicalType::VARCHAR
		// TODO(jwills): how to determine this?
		output.SetValue(0, index, Value("main"));
		// "table_schema", PhysicalType::VARCHAR
		output.SetValue(1, index, Value(entry->schema->name));
		// "table_name", PhysicalType::VARCHAR
		output.SetValue(2, index, Value(entry->name));
		// "table_type", PhysicalType::VARCHAR
		output.SetValue(3, index, Value(table_type));
		// "self_referencing_column_name", PhysicalType::VARCHAR
		output.SetValue(4, index, Value());
		// "reference_generation", PhysicalType::VARCHAR
		output.SetValue(5, index, Value());
		// "user_defined_type_catalog", PhysicalType::VARCHAR
		output.SetValue(6, index, Value());
		// "user_defined_type_schema", PhysicalType::VARCHAR
		output.SetValue(7, index, Value());
		// "user_defined_type_name", PhysicalType::VARCHAR
		output.SetValue(8, index, Value());
		// "is_insertable_into", PhysicalType::VARCHAR (YES/NO)
		output.SetValue(9, index, Value(is_insertable_into));
		// "is_typed", PhysicalType::VARCHAR (YES/NO)
		output.SetValue(10, index, Value("NO"));
		// "commit_action", PhysicalType::VARCHAR
		output.SetValue(11, index, Value());
	}
	data.offset = next;
}

void InformationSchemaTables::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("information_schema_tables", {}, information_schema_tables,
	                              information_schema_tables_bind, information_schema_tables_init));
}

} // namespace duckdb
