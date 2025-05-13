#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> BaseFileReader::GetStatistics(ClientContext &context, const string &name) {
	return nullptr;
}

shared_ptr<BaseUnionData> BaseFileReader::GetUnionData(idx_t file_idx) {
	throw NotImplementedException("Union by name not supported for reader of type %s", GetReaderType());
}

void BaseFileReader::PrepareReader(ClientContext &context, GlobalTableFunctionState &) {
}

void BaseFileReader::FinishFile(ClientContext &context, GlobalTableFunctionState &gstate) {
}

double BaseFileReader::GetProgressInFile(ClientContext &context) {
	return 0;
}

} // namespace duckdb
