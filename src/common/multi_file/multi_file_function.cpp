#include "duckdb/common/multi_file/multi_file_function.hpp"

namespace duckdb {

MultiFileReaderInterface::~MultiFileReaderInterface() {
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

} // namespace duckdb
