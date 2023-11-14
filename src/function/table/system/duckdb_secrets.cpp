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

struct DuckDBSecretsBindData : public FunctionData {
public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DuckDBSecretsBindData>();
	};

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DuckDBSecretsBindData>();
		return redact == other.redact;
	}
	bool redact = true;
};

static unique_ptr<FunctionData> DuckDBSecretsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DuckDBSecretsBindData>();

	auto entry = input.named_parameters.find("redact");
	if (entry != input.named_parameters.end()) {
		result->redact = BooleanValue::Get(entry->second);
	}

	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("provider");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("storage");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("scope");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("secret_string");
	return_types.emplace_back(LogicalType::VARCHAR);

	return result;
}

unique_ptr<GlobalTableFunctionState> DuckDBSecretsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBSecretsData>();
	return std::move(result);
}

void DuckDBSecretsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBSecretsData>();
	auto &bind_data = data_p.bind_data->Cast<DuckDBSecretsBindData>();

	auto &secret_manager = context.db->config.secret_manager;
	auto secrets = secret_manager->AllSecrets();

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
		for (const auto& scope_entry : secret_entry.secret->GetScope()) {
			scope_value.push_back(scope_entry);
		}

		const auto secret_ptr = secret_entry.secret;

		output.SetValue(0, count, secret_ptr->GetName());
		output.SetValue(1, count, Value(secret_ptr->GetType()));
		output.SetValue(2, count, Value(secret_ptr->GetProvider()));
		output.SetValue(3, count, Value(secret_entry.storage_mode));
		output.SetValue(4, count, Value::LIST(LogicalType::VARCHAR, scope_value));
		output.SetValue(5, count, secret_ptr->ToString(bind_data.redact));

		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBSecretsFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet functions("duckdb_secrets");
	auto fun = TableFunction({}, DuckDBSecretsFunction, DuckDBSecretsBind, DuckDBSecretsInit);
	fun.named_parameters["redact"] = LogicalType::BOOLEAN;
	functions.AddFunction(fun);
	set.AddFunction(functions);
}

} // namespace duckdb
