#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/grammar_extension.hpp"

namespace duckdb {

struct GrammarExtensionData {
	string name;
	string description;
};

struct DuckDBGrammarExtensionsData : public GlobalTableFunctionState {
	vector<GrammarExtensionData> extensions;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckDBGrammarExtensionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<Identifier> &names) {
	names.emplace_back("name");
	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBGrammarExtensionsInit(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBGrammarExtensionsData>();
	auto &callback_manager = ExtensionCallbackManager::Get(context);
	for (auto &[name, extension] : callback_manager.GrammarExtensions()) {
		GrammarExtensionData data;
		data.name = name;
		data.description = extension->Description();
		result->extensions.push_back(data);
	}
	return std::move(result);
}

static void DuckDBGrammarExtensionsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBGrammarExtensionsData>();
	auto &name = output.data[0];
	auto &description = output.data[1];
	idx_t count = 0;
	while (data.offset < data.extensions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &extension_data = data.extensions[data.offset++];
		name.Append(Value(extension_data.name));
		description.Append(Value(extension_data.description));
		count++;
	}
}

void DuckDBGrammarExtensionsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_grammar_extensions", {}, DuckDBGrammarExtensionsFunction,
	                              DuckDBGrammarExtensionsBind, DuckDBGrammarExtensionsInit));
}

} // namespace duckdb
