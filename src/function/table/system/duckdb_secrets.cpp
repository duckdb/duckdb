#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret_manager.hpp"

namespace duckdb {

struct DuckDBSecretsData : public GlobalTableFunctionState {
	DuckDBSecretsData() : offset(0) {
	}
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBSecretsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("provider");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("scope");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("secret_string");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBSecretsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBSecretsData>();
	return std::move(result);
}

void DuckDBSecretsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBSecretsData>();

	auto &secret_manager = context.db->config.secret_manager;
	auto &secrets = secret_manager->AllSecrets();

	if (data.offset >= secrets.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < secrets.size() && count < STANDARD_VECTOR_SIZE) {
		auto &secret_entry = secrets[data.offset];

		vector<Value> scope_value;
		for (const auto& scope_entry : secret_entry->GetScope()) {
			scope_value.push_back(scope_entry);
		}

		output.SetValue(0, count, secret_entry->GetName());
		output.SetValue(1, count, Value(secret_entry->GetType()));
		output.SetValue(2, count, Value(secret_entry->GetType()));
		output.SetValue(3, count, Value::LIST(LogicalType::VARCHAR, scope_value));
		output.SetValue(4, count, secret_entry->ToString(true));

		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBSecretsFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet functions("duckdb_secrets");
	functions.AddFunction(TableFunction({}, DuckDBSecretsFunction, DuckDBSecretsBind, DuckDBSecretsInit));
	set.AddFunction(functions);
}

} // namespace duckdb
