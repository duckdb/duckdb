#include "duckdb/execution/operator/csv_scanner/csv_multi_file_info.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/global_csv_state.hpp"

namespace duckdb {

unique_ptr<BaseFileReaderOptions> CSVMultiFileInfo::InitializeOptions(ClientContext &context) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

bool CSVMultiFileInfo::ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
                     BaseFileReaderOptions &options) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

bool CSVMultiFileInfo::ParseOption(ClientContext &context, const string &key, const Value &val,
                 MultiFileReaderOptions &file_options, BaseFileReaderOptions &options) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

void CSVMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
                MultiFileBindData &bind_data) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

unique_ptr<TableFunctionData> CSVMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                 unique_ptr<BaseFileReaderOptions> options) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

void CSVMultiFileInfo::FinalizeBindData(MultiFileBindData &multi_file_data) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

void CSVMultiFileInfo::GetBindInfo(const TableFunctionData &bind_data, BindInfo &info) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

idx_t CSVMultiFileInfo::MaxThreads(const TableFunctionData &bind_data_p) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

unique_ptr<GlobalTableFunctionState> CSVMultiFileInfo::InitializeGlobalState() {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

unique_ptr<LocalTableFunctionState> CSVMultiFileInfo::InitializeLocalState() {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

shared_ptr<BaseFileReader> CSVMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate_p,
													   BaseUnionData &union_data_p, TableFunctionData &bind_data_p) {
	auto &union_data = union_data_p.Cast<CSVUnionData>();
	auto &gstate = gstate_p.Cast<CSVGlobalState>();
	// union readers - use cached options
	auto &csv_names = union_data.names;
	auto &csv_types = union_data.types;
	auto options = union_data.options;
	options.auto_detect = false;
	return make_uniq<CSVFileScan>(context, union_data.GetFileName(), std::move(options), csv_names, csv_types,
								  gstate.file_schema, gstate.single_threaded, nullptr, false);
}

shared_ptr<BaseFileReader> CSVMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
											const string &filename, TableFunctionData &bind_data) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

void CSVMultiFileInfo::FinalizeReader(ClientContext &context, BaseFileReader &reader) {
	auto &csv_file_scan = reader.Cast<CSVFileScan>();
	csv_file_scan.InitializeFileNamesTypes();
	csv_file_scan.SetStart();
}


void CSVMultiFileInfo::Scan(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &global_state,
          LocalTableFunctionState &local_state, DataChunk &chunk) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

bool CSVMultiFileInfo::TryInitializeScan(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &gstate,
                       LocalTableFunctionState &lstate) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

void CSVMultiFileInfo::FinishFile(ClientContext &context, GlobalTableFunctionState &global_state) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

unique_ptr<NodeStatistics> CSVMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) {
	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
	// determined through the scientific method as the average amount of rows in a CSV file
	idx_t per_file_cardinality = 42;
	if (csv_data.buffer_manager && csv_data.buffer_manager->file_handle) {
	 	auto estimated_row_width = (bind_data.types.size() * 5);
	 	per_file_cardinality = csv_data.buffer_manager->file_handle->FileSize() / estimated_row_width;
	}
	return make_uniq<NodeStatistics>(file_count * per_file_cardinality);
}

unique_ptr<BaseStatistics> CSVMultiFileInfo::GetStatistics(ClientContext &context, BaseFileReader &reader, const string &name) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

double CSVMultiFileInfo::GetProgressInFile(ClientContext &context, GlobalTableFunctionState &gstate) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}


}
