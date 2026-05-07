#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {

struct DuckDBSecretTypesData : public GlobalTableFunctionState {
	DuckDBSecretTypesData() : offset(0) {
	}

	vector<SecretType> types;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBSecretTypesBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("default_provider");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("extension");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBSecretTypesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBSecretTypesData>();

	auto &secret_manager = SecretManager::Get(context);
	result->types = secret_manager.AllSecretTypes();

	return std::move(result);
}

void DuckDBSecretTypesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBSecretTypesData>();
	if (data.offset >= data.types.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;

	// type, LogicalType::VARCHAR
	auto &type = output.data[0];
	// default_provider, LogicalType::VARCHAR
	auto &default_provider = output.data[1];
	// extension, LogicalType::VARCHAR
	auto &extension = output.data[2];

	while (data.offset < data.types.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.types[data.offset++];

		type.Append(Value(entry.name));
		default_provider.Append(Value(entry.default_provider));
		extension.Append(Value(entry.extension));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBSecretTypesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_secret_types", {}, DuckDBSecretTypesFunction, DuckDBSecretTypesBind,
	                              DuckDBSecretTypesInit));
}

} // namespace duckdb
