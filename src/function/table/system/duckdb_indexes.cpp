#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct DuckDBIndexesData : public GlobalTableFunctionState {
	DuckDBIndexesData() : offset(0) {
	}

	vector<reference<CatalogEntry>> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBIndexesBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("database_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("index_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("index_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("comment");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("tags");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	names.emplace_back("is_unique");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("is_primary");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("expressions");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBIndexesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBIndexesData>();

	// scan all the schemas for tables and collect them
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::INDEX_ENTRY,
		                  [&](CatalogEntry &entry) { result->entries.push_back(entry); });
	};
	return std::move(result);
}

Value GetIndexExpressions(IndexCatalogEntry &index) {
	auto create_info = index.GetInfo();
	auto &create_index_info = create_info->Cast<CreateIndexInfo>();

	auto vec = create_index_info.ExpressionsToList();

	vector<Value> content;
	content.reserve(vec.size());
	for (auto &item : vec) {
		content.push_back(Value(item));
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(content));
}

void DuckDBIndexesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBIndexesData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++].get();

		auto &index = entry.Cast<IndexCatalogEntry>();
		// return values:

		idx_t col = 0;
		// database_name, VARCHAR
		output.SetValue(col++, count, index.catalog.GetName());
		// database_oid, BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(index.catalog.GetOid())));
		// schema_name, VARCHAR
		output.SetValue(col++, count, Value(index.schema.name));
		// schema_oid, BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(index.schema.oid)));
		// index_name, VARCHAR
		output.SetValue(col++, count, Value(index.name));
		// index_oid, BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(index.oid)));
		// find the table in the catalog
		auto &table_entry =
		    index.schema.catalog.GetEntry<TableCatalogEntry>(context, index.GetSchemaName(), index.GetTableName());
		// table_name, VARCHAR
		output.SetValue(col++, count, Value(table_entry.name));
		// table_oid, BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(table_entry.oid)));
		// comment, VARCHAR
		output.SetValue(col++, count, Value(index.comment));
		// tags, MAP
		output.SetValue(col++, count, Value::MAP(index.tags));
		// is_unique, BOOLEAN
		output.SetValue(col++, count, Value::BOOLEAN(index.IsUnique()));
		// is_primary, BOOLEAN
		output.SetValue(col++, count, Value::BOOLEAN(index.IsPrimary()));
		// expressions, VARCHAR
		output.SetValue(col++, count, GetIndexExpressions(index).ToString());
		// sql, VARCHAR
		auto sql = index.ToSQL();
		output.SetValue(col++, count, sql.empty() ? Value() : Value(std::move(sql)));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBIndexesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_indexes", {}, DuckDBIndexesFunction, DuckDBIndexesBind, DuckDBIndexesInit));
}

} // namespace duckdb
