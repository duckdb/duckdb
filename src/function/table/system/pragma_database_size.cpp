#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

struct PragmaDatabaseSizeData : public GlobalTableFunctionState {
	PragmaDatabaseSizeData() : index(0) {
	}

	idx_t index;
	vector<reference<AttachedDatabase>> databases;
	Value memory_usage;
	Value memory_limit;
};

static unique_ptr<FunctionData> PragmaDatabaseSizeBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("database_size");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("block_size");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("total_blocks");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("used_blocks");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("free_blocks");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("wal_size");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("memory_usage");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("memory_limit");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> PragmaDatabaseSizeInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<PragmaDatabaseSizeData>();
	result->databases = DatabaseManager::Get(context).GetDatabases(context);
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	result->memory_usage = Value(StringUtil::BytesToHumanReadableString(buffer_manager.GetUsedMemory()));
	auto max_memory = buffer_manager.GetMaxMemory();
	result->memory_limit =
	    max_memory == (idx_t)-1 ? Value("Unlimited") : Value(StringUtil::BytesToHumanReadableString(max_memory));

	return std::move(result);
}

void PragmaDatabaseSizeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PragmaDatabaseSizeData>();
	idx_t row = 0;
	for (; data.index < data.databases.size() && row < STANDARD_VECTOR_SIZE; data.index++) {
		auto &db = data.databases[data.index].get();
		if (db.IsSystem() || db.IsTemporary()) {
			continue;
		}
		auto ds = db.GetCatalog().GetDatabaseSize(context);
		idx_t col = 0;
		output.data[col++].SetValue(row, Value(db.GetName()));
		output.data[col++].SetValue(row, Value(StringUtil::BytesToHumanReadableString(ds.bytes)));
		output.data[col++].SetValue(row, Value::BIGINT(NumericCast<int64_t>(ds.block_size)));
		output.data[col++].SetValue(row, Value::BIGINT(NumericCast<int64_t>(ds.total_blocks)));
		output.data[col++].SetValue(row, Value::BIGINT(NumericCast<int64_t>(ds.used_blocks)));
		output.data[col++].SetValue(row, Value::BIGINT(NumericCast<int64_t>(ds.free_blocks)));
		output.data[col++].SetValue(
		    row, ds.wal_size == idx_t(-1) ? Value() : Value(StringUtil::BytesToHumanReadableString(ds.wal_size)));
		output.data[col++].SetValue(row, data.memory_usage);
		output.data[col++].SetValue(row, data.memory_limit);
		row++;
	}
	output.SetCardinality(row);
}

void PragmaDatabaseSize::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_database_size", {}, PragmaDatabaseSizeFunction, PragmaDatabaseSizeBind,
	                              PragmaDatabaseSizeInit));
}

} // namespace duckdb
