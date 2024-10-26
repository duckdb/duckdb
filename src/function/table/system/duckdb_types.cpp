#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct DuckDBTypesData : public GlobalTableFunctionState {
	DuckDBTypesData() : offset(0) {
	}

	vector<reference<TypeCatalogEntry>> entries;
	idx_t offset;
	unordered_set<int64_t> oids;
};

static unique_ptr<FunctionData> DuckDBTypesBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("database_oid");
	return_types.emplace_back(LogicalType::BIGINT);

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

	names.emplace_back("comment");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("tags");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("labels");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBTypesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBTypesData>();
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::TYPE_ENTRY,
		                  [&](CatalogEntry &entry) { result->entries.push_back(entry.Cast<TypeCatalogEntry>()); });
	};
	return std::move(result);
}

void DuckDBTypesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBTypesData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &type_entry = data.entries[data.offset++].get();
		auto &type = type_entry.user_type;

		// return values:
		idx_t col = 0;
		// database_name, VARCHAR
		output.SetValue(col++, count, type_entry.catalog.GetName());
		// database_oid, BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(type_entry.catalog.GetOid())));
		// schema_name, LogicalType::VARCHAR
		output.SetValue(col++, count, Value(type_entry.schema.name));
		// schema_oid, LogicalType::BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(type_entry.schema.oid)));
		// type_oid, BIGINT
		int64_t oid;
		if (type_entry.internal) {
			oid = NumericCast<int64_t>(type.id());
		} else {
			oid = NumericCast<int64_t>(type_entry.oid);
		}
		Value oid_val;
		if (data.oids.find(oid) == data.oids.end()) {
			data.oids.insert(oid);
			oid_val = Value::BIGINT(oid);
		} else {
			oid_val = Value();
		}
		output.SetValue(col++, count, oid_val);
		// type_name, VARCHAR
		output.SetValue(col++, count, Value(type_entry.name));
		// type_size, BIGINT
		auto internal_type = type.InternalType();
		output.SetValue(col++, count,
		                internal_type == PhysicalType::INVALID
		                    ? Value()
		                    : Value::BIGINT(NumericCast<int64_t>(GetTypeIdSize(internal_type))));
		// logical_type, VARCHAR
		output.SetValue(col++, count, Value(EnumUtil::ToString(type.id())));
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
		case LogicalTypeId::UHUGEINT:
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
		case LogicalTypeId::UNION:
			category = "COMPOSITE";
			break;
		default:
			break;
		}
		output.SetValue(col++, count, category.empty() ? Value() : Value(category));
		// comment, VARCHAR
		output.SetValue(col++, count, Value(type_entry.comment));
		// tags, MAP
		output.SetValue(col++, count, Value::MAP(type_entry.tags));
		// internal, BOOLEAN
		output.SetValue(col++, count, Value::BOOLEAN(type_entry.internal));
		// labels, VARCHAR[]
		if (type.id() == LogicalTypeId::ENUM && type.AuxInfo()) {
			auto data = FlatVector::GetData<string_t>(EnumType::GetValuesInsertOrder(type));
			idx_t size = EnumType::GetSize(type);

			vector<Value> labels;
			for (idx_t i = 0; i < size; i++) {
				labels.emplace_back(data[i]);
			}

			output.SetValue(col++, count, Value::LIST(labels));
		} else {
			output.SetValue(col++, count, Value());
		}

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBTypesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_types", {}, DuckDBTypesFunction, DuckDBTypesBind, DuckDBTypesInit));
}

} // namespace duckdb
