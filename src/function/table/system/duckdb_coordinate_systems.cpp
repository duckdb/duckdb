#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/coordinate_system_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct DuckDBCoordinateSystemsData : public GlobalTableFunctionState {
	DuckDBCoordinateSystemsData() : offset(0) {
	}

	vector<reference<CoordinateSystemCatalogEntry>> entries;
	idx_t offset;
	unordered_set<int64_t> oids;
};

static unique_ptr<FunctionData> DuckDBCoordinateSystemsBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("database_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("crs_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("crs_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("auth_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("auth_code");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("projjson");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("wkt2_2019");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBCoordinateSystemsInit(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBCoordinateSystemsData>();
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::COORDINATE_SYSTEM_ENTRY, [&](CatalogEntry &entry) {
			result->entries.push_back(entry.Cast<CoordinateSystemCatalogEntry>());
		});
	};
	return std::move(result);
}

static void DuckDBCoordinateSystemsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBCoordinateSystemsData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &crs_entry = data.entries[data.offset++].get();

		// return values:
		idx_t col = 0;
		// database_name, VARCHAR
		output.SetValue(col++, count, crs_entry.catalog.GetName());
		// database_oid, BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(crs_entry.catalog.GetOid())));
		// schema_name, LogicalType::VARCHAR
		output.SetValue(col++, count, Value(crs_entry.schema.name));
		// schema_oid, LogicalType::BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(crs_entry.schema.oid)));
		// crs_oid, BIGINT
		int64_t oid = NumericCast<int64_t>(crs_entry.oid);
		Value oid_val;
		if (data.oids.find(oid) == data.oids.end()) {
			data.oids.insert(oid);
			oid_val = Value::BIGINT(oid);
		} else {
			oid_val = Value();
		}

		output.SetValue(col++, count, oid_val);
		// crs_name, VARCHAR
		output.SetValue(col++, count, Value(crs_entry.name));
		// auth_name, VARCHAR
		output.SetValue(col++, count, Value(crs_entry.authority));
		// auth_code, VARCHAR
		output.SetValue(col++, count, Value(crs_entry.code));
		// projjson, VARCHAR
		output.SetValue(col++, count, Value(crs_entry.projjson_definition));
		// wkt2_2019, VARCHAR
		output.SetValue(col++, count, Value(crs_entry.wkt2_2019_definition));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBCoordinateSystemsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_coordinate_systems", {}, DuckDBCoordinateSystemsFunction,
	                              DuckDBCoordinateSystemsBind, DuckDBCoordinateSystemsInit));
}

} // namespace duckdb
