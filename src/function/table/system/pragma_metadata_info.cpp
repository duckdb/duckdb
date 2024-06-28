#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/function/function_set.hpp"
namespace duckdb {

struct PragmaMetadataFunctionData : public TableFunctionData {
	explicit PragmaMetadataFunctionData() {
	}

	vector<MetadataBlockInfo> metadata_info;
};

struct PragmaMetadataOperatorData : public GlobalTableFunctionState {
	PragmaMetadataOperatorData() : offset(0) {
	}

	idx_t offset;
};

static unique_ptr<FunctionData> PragmaMetadataInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("block_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("total_blocks");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("free_blocks");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("free_list");
	return_types.emplace_back(LogicalType::LIST(LogicalType::BIGINT));

	string db_name;
	if (input.inputs.empty()) {
		db_name = DatabaseManager::GetDefaultDatabase(context);
	} else {
		if (input.inputs[0].IsNull()) {
			throw BinderException("Database argument for pragma_metadata_info cannot be NULL");
		}
		db_name = StringValue::Get(input.inputs[0]);
	}
	auto &catalog = Catalog::GetCatalog(context, db_name);
	auto result = make_uniq<PragmaMetadataFunctionData>();
	result->metadata_info = catalog.GetMetadataInfo(context);
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> PragmaMetadataInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaMetadataOperatorData>();
}

static void PragmaMetadataInfoFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<PragmaMetadataFunctionData>();
	auto &data = data_p.global_state->Cast<PragmaMetadataOperatorData>();
	idx_t count = 0;
	while (data.offset < bind_data.metadata_info.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.metadata_info[data.offset++];

		idx_t col_idx = 0;
		// block_id
		output.SetValue(col_idx++, count, Value::BIGINT(entry.block_id));
		// total_blocks
		output.SetValue(col_idx++, count, Value::BIGINT(NumericCast<int64_t>(entry.total_blocks)));
		// free_blocks
		output.SetValue(col_idx++, count, Value::BIGINT(NumericCast<int64_t>(entry.free_list.size())));
		// free_list
		vector<Value> list_values;
		for (auto &free_id : entry.free_list) {
			list_values.push_back(Value::BIGINT(NumericCast<int64_t>(free_id)));
		}
		output.SetValue(col_idx++, count, Value::LIST(LogicalType::BIGINT, std::move(list_values)));
		count++;
	}
	output.SetCardinality(count);
}

void PragmaMetadataInfo::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet metadata_info("pragma_metadata_info");
	metadata_info.AddFunction(
	    TableFunction({}, PragmaMetadataInfoFunction, PragmaMetadataInfoBind, PragmaMetadataInfoInit));
	metadata_info.AddFunction(TableFunction({LogicalType::VARCHAR}, PragmaMetadataInfoFunction, PragmaMetadataInfoBind,
	                                        PragmaMetadataInfoInit));
	set.AddFunction(metadata_info);
}

} // namespace duckdb
