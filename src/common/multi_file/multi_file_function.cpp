#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/multi_file/union_by_name.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

MultiFileGlobalState::MultiFileGlobalState(MultiFileList &file_list_p) : file_list(file_list_p) {
}

MultiFileGlobalState::MultiFileGlobalState(unique_ptr<MultiFileList> owned_file_list_p)
    : file_list(*owned_file_list_p), owned_file_list(std::move(owned_file_list_p)) {
}

MultiFileGlobalState::~MultiFileGlobalState() {
	if (read_ahead) {
		// the file opens scheduled on the async pool reference this state - wait for them before it goes away
		read_ahead->CancelAndDrain();
	}
}

void MultiFileGlobalState::ReportBytesScannedOnScanStart(ClientContext &context, MultiFileReaderData &reader_data) {
	if (reader_data.bytes_scanned_reported) {
		return;
	}
	reader_data.bytes_scanned_reported = true;
	if (!reader_data.reader) {
		return;
	}
	auto &reader = *reader_data.reader;
	switch (reader.GetBytesScannedReporting()) {
	case BytesScannedReporting::COUNTED_BY_READER:
		// the reader counts the bytes it reads as it reads them, through the metrics of its scan
		return;
	case BytesScannedReporting::STORED_FILE_SIZE: {
		// the scan reads the file whole - its stored size is the bytes scanned, known now
		auto file_size = reader.GetStoredFileSize();
		if (!file_size.IsValid()) {
			// the size of the file is not known - there is nothing to report
			return;
		}
		// handed over to the operator metrics once (MultiFileFunction::MultiFileGetMetrics), and added to the running
		// total of the query right away, like the Parquet reader does for a row group
		bytes_scanned_unreported += file_size.GetIndex();
		QueryProfiler::Get(context).TrackBytesScanned(file_size.GetIndex());
		return;
	}
	default:
		throw InternalException("Unknown BytesScannedReporting");
	}
}

MultiFileReaderInterface::~MultiFileReaderInterface() {
}

void MultiFileReaderInterface::InitializeInterface(ClientContext &context, MultiFileReader &reader,
                                                   MultiFileList &file_list) {
}

void MultiFileReaderInterface::InitializeFileOptions(MultiFileOptions &file_options) {
}

void MultiFileReaderInterface::FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options,
                                                const vector<Identifier> &expected_names,
                                                const vector<LogicalType> &expected_types) {
}

optional_idx MultiFileReaderInterface::MaxThreads(const MultiFileBindData &bind_data_p,
                                                  const MultiFileGlobalState &global_state,
                                                  FileExpandResult expand_result) {
	return optional_idx();
}

optional_idx MultiFileReaderInterface::MaxThreads(ClientContext &context, const MultiFileBindData &bind_data_p,
                                                  const MultiFileGlobalState &global_state,
                                                  FileExpandResult expand_result) {
	return MaxThreads(bind_data_p, global_state, expand_result);
}

void MultiFileReaderInterface::CombineSchemas(ClientContext &context,
                                              const vector<shared_ptr<BaseUnionData>> &union_data, bool union_by_name,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	identifier_map_t<idx_t> union_names_map;
	for (auto &data : union_data) {
		UnionByName::CombineUnionTypes(data->names, data->types, return_types, names, union_names_map);
	}
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
