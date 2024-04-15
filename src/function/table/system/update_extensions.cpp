#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_helper.hpp"

#include <cstdint>

namespace duckdb {

struct PragmaUpdateExtensionsState : public GlobalTableFunctionState {
	PragmaUpdateExtensionsState() : offset(0) {
	}

	vector<ExtensionUpdateResult> update_result_entries;
	idx_t offset;
};

struct PragmaUpdateExtensionsData : public TableFunctionData {
	PragmaUpdateExtensionsData() {
	}

	string extension_to_update;
};

static unique_ptr<FunctionData> PragmaUpdateExtensionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto res = make_uniq<PragmaUpdateExtensionsData>();

	if (input.inputs.size() > 0) {
		res->extension_to_update = input.inputs[0].ToString();
		if (res->extension_to_update.empty()) {
			throw InvalidInputException("Empty string is not a valid extension name");
		}
	}

	names.emplace_back("extension_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("repository");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("update_result");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("previous_version");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("current_version");
	return_types.emplace_back(LogicalType::VARCHAR);

	return res;
}

// Single extensions
static unique_ptr<GlobalTableFunctionState> PragmaUpdateExtensionsInit(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	auto result = make_uniq<PragmaUpdateExtensionsState>();
	auto &bind_data = input.bind_data->Cast<PragmaUpdateExtensionsData>();

	if (bind_data.extension_to_update.empty()) {
		// Update all
		result->update_result_entries = ExtensionHelper::UpdateExtensions(context);
	} else {
		// Update single
		result->update_result_entries = {ExtensionHelper::UpdateExtension(context, bind_data.extension_to_update)};
	}

	return std::move(result);
}

static void PragmaUpdateExtensionsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PragmaUpdateExtensionsState>();
	if (data.offset >= data.update_result_entries.size()) {
		// finished returning values
		return;
	}
	idx_t count = 0;
	while (data.offset < data.update_result_entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.update_result_entries[data.offset];

		// return values:
		idx_t col = 0;
		// extension_name LogicalType::VARCHAR
		output.SetValue(col++, count, Value(entry.extension_name));
		// repository LogicalType::VARCHAR
		output.SetValue(col++, count, Value(entry.repository));
		// update_result
		output.SetValue(col++, count, Value(EnumUtil::ToString(entry.tag)));
		// previous_version LogicalType::VARCHAR
		output.SetValue(col++, count, Value(entry.prev_version));
		// current_version LogicalType::VARCHAR
		output.SetValue(col++, count, Value(entry.installed_version));

		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void PragmaUpdateExtensions::RegisterFunction(BuiltinFunctions &set) {
	TableFunction update_all_function("duckdb_extensions_update", {}, PragmaUpdateExtensionsFunction);
	update_all_function.bind = PragmaUpdateExtensionsBind;
	update_all_function.init_global = PragmaUpdateExtensionsInit;
	set.AddFunction(update_all_function);

	TableFunction update_single_function("duckdb_extension_update", {LogicalType::VARCHAR},
	                                     PragmaUpdateExtensionsFunction);
	update_single_function.bind = PragmaUpdateExtensionsBind;
	update_single_function.init_global = PragmaUpdateExtensionsInit;
	set.AddFunction(update_single_function);
}

} // namespace duckdb
