#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct DuckDBTypesData : public GlobalTableFunctionState {
	DuckDBTypesData() : offset(0) {
	}

	vector<TypeCatalogEntry *> entries;
	idx_t offset;
	unordered_set<int64_t> oids;
};

static unique_ptr<FunctionData> DuckDBTypesBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("type_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("type_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type_size");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("logical_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	// NUMERIC, STRING, DATETIME, BOOLEAN, COMPOSITE, USER
	names.emplace_back("type_category");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBTypesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<DuckDBTypesData>();
	auto schemas = Catalog::GetCatalog(context).schemas->GetEntries<SchemaCatalogEntry>(context);
	for (auto &schema : schemas) {
		schema->Scan(context, CatalogType::TYPE_ENTRY,
		             [&](CatalogEntry *entry) { result->entries.push_back((TypeCatalogEntry *)entry); });
	};

	// check the temp schema as well
	ClientData::Get(context).temporary_objects->Scan(context, CatalogType::TYPE_ENTRY, [&](CatalogEntry *entry) {
		result->entries.push_back((TypeCatalogEntry *)entry);
	});
	return move(result);
}

void DuckDBTypesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DuckDBTypesData &)*data_p.global_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &type_entry = data.entries[data.offset++];
		auto &type = type_entry->user_type;

		// return values:
		// schema_name, LogicalType::VARCHAR
		output.SetValue(0, count, Value(type_entry->schema->name));
		// schema_oid, LogicalType::BIGINT
		output.SetValue(1, count, Value::BIGINT(type_entry->schema->oid));
		// type_oid, BIGINT
		int64_t oid;
		if (type_entry->internal) {
			oid = int64_t(type.id());
		} else {
			oid = type_entry->oid;
		}
		Value oid_val;
		if (data.oids.find(oid) == data.oids.end()) {
			data.oids.insert(oid);
			oid_val = Value::BIGINT(oid);
		} else {
			oid_val = Value();
		}
		output.SetValue(2, count, oid_val);
		// type_name, VARCHAR
		output.SetValue(3, count, Value(type_entry->name));
		// type_size, BIGINT
		auto internal_type = type.InternalType();
		output.SetValue(4, count,
		                internal_type == PhysicalType::INVALID ? Value() : Value::BIGINT(GetTypeIdSize(internal_type)));
		// logical_type, VARCHAR
		output.SetValue(5, count, Value(LogicalTypeIdToString(type.id())));
		// type_category, VARCHAR
		string category;
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::DECIMAL:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::HUGEINT:
			category = "NUMERIC";
			break;
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::INTERVAL:
		case LogicalTypeId::TIME_TZ:
		case LogicalTypeId::TIMESTAMP_TZ:
			category = "DATETIME";
			break;
		case LogicalTypeId::CHAR:
		case LogicalTypeId::VARCHAR:
			category = "STRING";
			break;
		case LogicalTypeId::BOOLEAN:
			category = "BOOLEAN";
			break;
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::LIST:
		case LogicalTypeId::MAP:
			category = "COMPOSITE";
			break;
		default:
			break;
		}
		output.SetValue(6, count, category.empty() ? Value() : Value(category));
		// internal, BOOLEAN
		output.SetValue(7, count, Value::BOOLEAN(type_entry->internal));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBTypesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_types", {}, DuckDBTypesFunction, DuckDBTypesBind, DuckDBTypesInit));
}

} // namespace duckdb
