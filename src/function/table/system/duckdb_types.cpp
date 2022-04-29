#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBTypesData : public FunctionOperatorData {
	DuckDBTypesData() : offset(0) {
	}

	vector<LogicalType> types;
	idx_t offset;
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

	// NUMERIC, STRING, DATETIME, BOOLEAN, COMPOSITE, USER
	names.emplace_back("type_category");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return nullptr;
}

unique_ptr<FunctionOperatorData> DuckDBTypesInit(ClientContext &context, const FunctionData *bind_data,
                                                 const vector<column_t> &column_ids, TableFilterCollection *filters) {
	auto result = make_unique<DuckDBTypesData>();
	result->types = LogicalType::AllTypes();
	// FIXME: add user-defined types here (when we have them)
	return move(result);
}

void DuckDBTypesFunction(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                         DataChunk &output) {
	auto &data = (DuckDBTypesData &)*operator_state;
	if (data.offset >= data.types.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.types.size() && count < STANDARD_VECTOR_SIZE) {
		auto &type = data.types[data.offset++];

		// return values:
		// schema_name, VARCHAR
		output.SetValue(0, count, Value());
		// schema_oid, BIGINT
		output.SetValue(1, count, Value());
		// type_oid, BIGINT
		output.SetValue(2, count, Value::BIGINT(int(type.id())));
		// type_name, VARCHAR
		output.SetValue(3, count, Value(type.ToString()));
		// type_size, BIGINT
		auto internal_type = type.InternalType();
		output.SetValue(4, count,
		                internal_type == PhysicalType::INVALID ? Value() : Value::BIGINT(GetTypeIdSize(internal_type)));
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
		output.SetValue(5, count, category.empty() ? Value() : Value(category));
		// internal, BOOLEAN
		output.SetValue(6, count, Value::BOOLEAN(true));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBTypesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_types", {}, DuckDBTypesFunction, DuckDBTypesBind, DuckDBTypesInit));
}

} // namespace duckdb
