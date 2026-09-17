#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/storage/external_file_cache/external_file_cache.hpp"

namespace duckdb {

struct DuckDBExternalFileCacheStatsData : public GlobalTableFunctionState {
	DuckDBExternalFileCacheStatsData() : done(false) {
	}

	ExternalFileCacheStatsInformation stats;
	bool done;
};

static unique_ptr<FunctionData> DuckDBExternalFileCacheStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<Identifier> &names) {
	names.emplace_back("requested_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("hit_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("hit_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("miss_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("miss_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("eviction_refetch_count");
	return_types.emplace_back(LogicalType::BIGINT);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBExternalFileCacheStatsInit(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBExternalFileCacheStatsData>();
	result->stats = ExternalFileCache::Get(context).GetStats().GetSnapshot();
	return std::move(result);
}

void DuckDBExternalFileCacheStatsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBExternalFileCacheStatsData>();
	if (data.done) {
		return;
	}
	data.done = true;
	auto &stats = data.stats;
	output.data[0].Append(Value::BIGINT(NumericCast<int64_t>(stats.requested_bytes)));
	output.data[1].Append(Value::BIGINT(NumericCast<int64_t>(stats.hit_count)));
	output.data[2].Append(Value::BIGINT(NumericCast<int64_t>(stats.hit_bytes)));
	output.data[3].Append(Value::BIGINT(NumericCast<int64_t>(stats.miss_count)));
	output.data[4].Append(Value::BIGINT(NumericCast<int64_t>(stats.miss_bytes)));
	output.data[5].Append(Value::BIGINT(NumericCast<int64_t>(stats.eviction_refetch_count)));
}

void DuckDBExternalFileCacheStatsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_external_file_cache_stats", {}, DuckDBExternalFileCacheStatsFunction,
	                              DuckDBExternalFileCacheStatsBind, DuckDBExternalFileCacheStatsInit));
}

} // namespace duckdb
