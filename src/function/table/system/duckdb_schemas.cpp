#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct DuckDBSchemasData : public GlobalTableFunctionState {
	DuckDBSchemasData() : offset(0) {
	}

	vector<reference<SchemaCatalogEntry>> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBSchemasBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("database_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("comment");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("tags");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBSchemasInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBSchemasData>();

	// scan all the schemas and collect them
	result->entries = Catalog::GetAllSchemas(context);

	return std::move(result);
}

void DuckDBSchemasFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBSchemasData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;

	// oid, BIGINT
	auto &oid = output.data[0];
	// database_name, VARCHAR
	auto &database_name = output.data[1];
	// database_oid, BIGINT
	auto &database_oid = output.data[2];
	// schema_name, VARCHAR
	auto &schema_name = output.data[3];
	// comment, VARCHAR
	auto &comment = output.data[4];
	// tags, MAP(VARCHAR, VARCHAR)
	auto &tags = output.data[5];
	// internal, BOOLEAN
	auto &internal = output.data[6];
	// sql, VARCHAR
	auto &sql = output.data[7];

	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset].get();

		oid.Append(Value::BIGINT(NumericCast<int64_t>(entry.oid)));
		database_name.Append(Value(entry.catalog.GetName()));
		database_oid.Append(Value::BIGINT(NumericCast<int64_t>(entry.catalog.GetOid())));
		schema_name.Append(Value(entry.name));
		comment.Append(Value(entry.comment));
		tags.Append(Value::MAP(entry.tags));
		internal.Append(Value::BOOLEAN(entry.internal));
		sql.Append(Value());

		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBSchemasFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_schemas", {}, DuckDBSchemasFunction, DuckDBSchemasBind, DuckDBSchemasInit));
}

} // namespace duckdb
