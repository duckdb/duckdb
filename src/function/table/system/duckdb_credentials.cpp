#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

struct DuckDBCredentialsData : public GlobalTableFunctionState {
	DuckDBCredentialsData() : offset(0) {
	}
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBCredentialsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("alias");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("file_system");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("scope");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("credential_string");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBCredentialsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBCredentialsData>();
	return std::move(result);
}

void DuckDBCredentialsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBCredentialsData>();

	auto &credential_manager = context.db->config.credential_manager;
	auto &credentials = credential_manager->AllCredentials();

	if (data.offset >= credentials.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < credentials.size() && count < STANDARD_VECTOR_SIZE) {
		auto &credential_entry = credentials[data.offset];

		vector<Value> scope_value;
		for (const auto& scope_entry : credential_entry->GetScope()) {
			scope_value.push_back(scope_entry);
		}

		output.SetValue(0, count, Value((int64_t)credential_entry->GetId()));
		output.SetValue(1, count, credential_entry->GetAlias());
		output.SetValue(2, count, Value(credential_entry->GetFileSystemName()));
		output.SetValue(3, count, Value::LIST(scope_value));
		output.SetValue(4, count, credential_entry->GetCredentialsAsValue(true));

		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBCredentialsFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet functions("duckdb_credentials");
	functions.AddFunction(TableFunction({}, DuckDBCredentialsFunction, DuckDBCredentialsBind, DuckDBCredentialsInit));
	set.AddFunction(functions);
}

} // namespace duckdb
