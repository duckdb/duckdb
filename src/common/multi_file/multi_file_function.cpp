#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

MultiFileReaderInterface::~MultiFileReaderInterface() {
}

void MultiFileReaderInterface::InitializeInterface(ClientContext &context, MultiFileReader &reader,
                                                   MultiFileList &file_list) {
}

void MultiFileReaderInterface::FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options,
                                                const vector<string> &expected_names,
                                                const vector<LogicalType> &expected_types) {
}

optional_idx MultiFileReaderInterface::MaxThreads(const MultiFileBindData &bind_data_p,
                                                  const MultiFileGlobalState &global_state,
                                                  FileExpandResult expand_result) {
	return optional_idx();
}

void MultiFileReaderInterface::FinalizeBindData(MultiFileBindData &multi_file_data) {
}

shared_ptr<BaseFileReader> MultiFileReaderInterface::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                                  BaseFileReaderOptions &options,
                                                                  const MultiFileOptions &file_options) {
	throw InternalException("MultiFileReaderInterface::CreateReader is not implemented for this file interface");
}

void MultiFileReaderInterface::GetBindInfo(const TableFunctionData &bind_data, BindInfo &info) {
}

void MultiFileReaderInterface::GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data,
                                                 virtual_column_map_t &result) {
}

void MultiFileReaderInterface::FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
                                             LocalTableFunctionState &local_state) {
}

unique_ptr<MultiFileReaderInterface> MultiFileReaderInterface::Copy() {
	throw InternalException("MultiFileReaderInterface::Copy is not implemented for this file interface");
}

FileGlobInput MultiFileReaderInterface::GetGlobInput() {
	return FileGlobOptions::DISALLOW_EMPTY;
}

} // namespace duckdb
