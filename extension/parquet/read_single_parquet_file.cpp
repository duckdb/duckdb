#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "parquet_multi_file_info.hpp"
#include "duckdb/common/enum_util.hpp"
#include "parquet_reader.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

//! Bind data of read_single_parquet_file - the single file that is read, and how it is read
struct ReadSingleParquetFileData : public TableFunctionData {
	OpenFileInfo file;
	ParquetOptions options;
	//! The names/types this file is read with
	vector<Identifier> parquet_names;
	vector<LogicalType> parquet_types;
	//! The columns of the file, with their nested structure and field ids
	vector<MultiFileColumnDefinition> columns;
	//! The metadata of the file, read during binding and reused when the file is scanned
	shared_ptr<ParquetFileMetadataCache> metadata;
	//! The number of rows of the file, if it was read during binding
	optional_idx cardinality;

	//! The reader the bind opened for this file, handed to the first scan of this bind data so that the file does
	//! not have to be opened again. Only set when the bind data is used to read the file
	shared_ptr<ParquetReader> TakeBindReader() const {
		lock_guard<mutex> guard(bind_reader_lock);
		return std::move(bind_reader);
	}
	void SetBindReader(shared_ptr<ParquetReader> reader) {
		lock_guard<mutex> guard(bind_reader_lock);
		bind_reader = std::move(reader);
	}
	//! A reader over the metadata of the file, created on first use. The row groups it describes refer to the schema
	//! it holds, so it is kept for as long as this bind data is
	ParquetReader &GetMetadataReader(ClientContext &context) const {
		lock_guard<mutex> guard(bind_reader_lock);
		if (!metadata_reader) {
			metadata_reader = ParquetReader::CreateMetadataReader(context, options, metadata);
		}
		return *metadata_reader;
	}

private:
	//! Guards the readers below
	mutable mutex bind_reader_lock;
	mutable shared_ptr<ParquetReader> bind_reader;
	mutable shared_ptr<ParquetReader> metadata_reader;
};

struct ReadSingleParquetFileGlobalState : public GlobalTableFunctionState {
	explicit ReadSingleParquetFileGlobalState(optional_ptr<const PhysicalOperator> op) : state(op) {
	}

public:
	idx_t MaxThreads() const override {
		return max_threads;
	}

public:
	//! The file that is read - declared before the state below, which refers to it
	shared_ptr<ParquetReader> reader;
	ParquetReadGlobalState state;
	//! Handing out the next row group is done single-threadedly
	mutex lock;
	idx_t max_threads = 1;
};

struct ReadSingleParquetFileLocalState : public LocalTableFunctionState {
	ParquetReadLocalState state;
	//! Whether our caller claims the row groups we read - see table_function_claim_batch_t
	bool claimed_externally = false;
	//! Whether this thread holds a row group that it has not finished scanning
	bool has_row_group = false;
};

static unique_ptr<FunctionData> ReadSingleParquetFileBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types,
                                                          vector<Identifier> &names) {
	auto result = make_uniq<ReadSingleParquetFileData>();
	if (input.inputs[0].IsNull()) {
		throw BinderException("read_single_parquet_file requires a non-NULL file name");
	}
	result->file = OpenFileInfo(StringValue::Get(input.inputs[0]));

	// the named parameters of this function are those of the multi-file parquet reader
	ParquetMultiFileInfo interface;
	MultiFileOptions file_options;
	if (input.multi_file_options) {
		file_options = *input.multi_file_options;
	}
	ParquetFileReaderOptions options(context);
	for (auto &kv : input.named_parameters) {
		interface.ParseOption(context, kv.first, kv.second, file_options, options);
	}
	result->options = std::move(options.options);

	// read the metadata of the file to determine its schema - when this file was bound before, to determine the
	// schema of the scan it is part of, its metadata is taken from that bind instead of being read again
	shared_ptr<ParquetFileMetadataCache> known_metadata;
	if (input.file_bind_data) {
		auto &previous = input.file_bind_data->Cast<ReadSingleParquetFileData>();
		if (previous.file.path == result->file.path) {
			known_metadata = previous.metadata;
		}
	}
	auto reader = make_shared_ptr<ParquetReader>(context, result->file, result->options, std::move(known_metadata));
	for (auto &column : reader->GetColumns()) {
		names.push_back(column.name);
		return_types.push_back(column.type);
	}
	result->columns = reader->GetColumns();
	result->metadata = reader->metadata;
	result->cardinality = reader->NumRows();
	result->parquet_names = names;
	result->parquet_types = return_types;
	if (!input.schema_only) {
		// this file is read with this bind data - keep the reader so the scan does not open the file a second time
		result->SetBindReader(std::move(reader));
	}
	return std::move(result);
}

//! The virtual columns the parquet reader can produce for a file
static virtual_column_map_t ReadSingleParquetFileVirtualColumns(ClientContext &context,
                                                                optional_ptr<FunctionData> bind_data);

//! The columns of this file, as the parquet reader describes them
static vector<MultiFileColumnDefinition> ReadSingleParquetFileColumns(ClientContext &context,
                                                                      const FunctionData &bind_data) {
	return bind_data.Cast<ReadSingleParquetFileData>().columns;
}

static unique_ptr<GlobalTableFunctionState> ReadSingleParquetFileInitGlobal(ClientContext &context,
                                                                            TableFunctionInitInput &input) {
	auto &parquet_data = input.bind_data->Cast<ReadSingleParquetFileData>();
	auto result = make_uniq<ReadSingleParquetFileGlobalState>(input.op);

	result->reader = parquet_data.TakeBindReader();
	if (!result->reader) {
		result->reader =
		    make_shared_ptr<ParquetReader>(context, parquet_data.file, parquet_data.options, parquet_data.metadata);
	}
	auto &reader = *result->reader;
	auto virtual_column_types = ReadSingleParquetFileVirtualColumns(context, nullptr);
	// perform projection pushdown - the reader emits the columns in the order they are requested
	vector<ColumnIndex> column_indexes;
	for (auto &column_index : input.column_indexes) {
		const auto col_id = column_index.GetPrimaryIndex();
		// a virtual column is either projected under its own id, or - when our caller reads several files - under
		// an index of its own past the columns of this file
		auto virtual_id = col_id;
		if (input.virtual_columns) {
			auto entry = input.virtual_columns->find(col_id);
			if (entry != input.virtual_columns->end()) {
				virtual_id = entry->second;
			}
		}
		if (!IsVirtualColumn(virtual_id)) {
			reader.column_ids.push_back(MultiFileLocalColumnId(col_id));
			column_indexes.push_back(column_index);
			continue;
		}
		// the reader produces a virtual column as a column of its own, appended after the ones the file has
		auto virtual_entry = virtual_column_types.find(virtual_id);
		if (virtual_entry == virtual_column_types.end()) {
			throw InternalException("Unsupported virtual column id %d for read_single_parquet_file", virtual_id);
		}
		const auto local_id = reader.columns.size();
		reader.column_ids.push_back(MultiFileLocalColumnId(local_id));
		column_indexes.push_back(column_index.RemapRootIndex(local_id));
		reader.columns.emplace_back(virtual_entry->second.name.GetIdentifierName(), virtual_entry->second.type);
		reader.AddVirtualColumn(virtual_id);
	}
	reader.column_indexes = std::move(column_indexes);
	if (input.filters) {
		// the filters are evaluated by the reader itself, so that row groups can be pruned on them
		reader.filters = input.filters->Copy();
	}
	if (input.filter_global_indices) {
		reader.filter_global_indices = *input.filter_global_indices;
	}
	if (input.expression_map) {
		// our caller could not express some of its filters in the types this file stores its columns as - the
		// reader evaluates these expressions on them first, and applies the filters to the result
		for (auto &entry : *input.expression_map) {
			vector<ColumnIndex> expression_column_indexes = entry.second.column_indexes;
			reader.expression_map.emplace(entry.first, BaseFileReaderExpression(entry.second.expression->Copy(),
			                                                                    std::move(expression_column_indexes)));
		}
	}
	// NOTE: the parquet reader does not read columns as a type other than the one they have in the file, so
	// "supports_cast_map" is not set and our caller casts the columns it needs converted itself
	if (input.deletion_filter) {
		// our caller has deleted rows from this file - the reader skips them while scanning. The filter stays owned
		// by the caller, which outlives this scan
		reader.deletion_filter = make_uniq<BorrowedDeleteFilter>(*input.deletion_filter);
	}
	result->max_threads = MaxValue<idx_t>(reader.NumRowGroups(), 1);
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> ReadSingleParquetFileInitLocal(ExecutionContext &context,
                                                                          TableFunctionInitInput &input,
                                                                          GlobalTableFunctionState *global_state) {
	return make_uniq<ReadSingleParquetFileLocalState>();
}

//! Assign the next row group of the file to this thread
static bool ClaimNextRowGroup(ClientContext &context, ReadSingleParquetFileGlobalState &gstate,
                              ReadSingleParquetFileLocalState &lstate) {
	{
		lock_guard<mutex> guard(gstate.lock);
		if (!gstate.reader->TryInitializeScan(context, gstate.state, lstate.state)) {
			return false;
		}
	}
	// preparing the scan of the claimed row group is done without the lock
	gstate.reader->PrepareScan(context, gstate.state, lstate.state);
	lstate.has_row_group = true;
	return true;
}

//! Register and run the reads of the row group this thread claimed. Our caller schedules this I/O itself when it
//! drives the scan through "schedule_io"
static void LoadClaimedRowGroup(ClientContext &context, ReadSingleParquetFileGlobalState &gstate,
                                ReadSingleParquetFileLocalState &lstate) {
	auto io_result = gstate.reader->ScheduleIO(context, gstate.state, lstate.state);
	if (io_result.GetResultType() == AsyncResultType::BLOCKED) {
		io_result.ExecuteTasksSynchronously();
	}
}

static bool ReadSingleParquetFileClaimBatch(ClientContext &context, TableFunctionInput &input) {
	auto &gstate = input.global_state->Cast<ReadSingleParquetFileGlobalState>();
	auto &lstate = input.local_state->Cast<ReadSingleParquetFileLocalState>();
	// our caller hands out the row groups, so we must not claim the next one ourselves
	lstate.claimed_externally = true;
	return ClaimNextRowGroup(context, gstate, lstate);
}

//! Pre-open the handle the scan reads the file through, so that claiming a row group does not have to open the file
static void ReadSingleParquetFilePrepareReadAhead(ClientContext &context, TableFunctionInput &input) {
	auto &gstate = input.global_state->Cast<ReadSingleParquetFileGlobalState>();
	gstate.reader->PrepareReadAhead(context, gstate.state);
}

//! The parquet reader can always be read ahead - the row group a thread claimed is loaded before it is scanned
static bool ReadSingleParquetFileSupportsReadAhead(const FunctionData &bind_data) {
	return true;
}

//! Load the data of the claimed row group
static AsyncResult ReadSingleParquetFileScheduleIO(ClientContext &context, TableFunctionInput &input) {
	auto &gstate = input.global_state->Cast<ReadSingleParquetFileGlobalState>();
	auto &lstate = input.local_state->Cast<ReadSingleParquetFileLocalState>();
	if (!lstate.has_row_group) {
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	return gstate.reader->ScheduleIO(context, gstate.state, lstate.state);
}

//! Release the row group this thread was reading
static void ReadSingleParquetFileFinishBatch(ClientContext &context, TableFunctionInput &input) {
	auto &lstate = input.local_state->Cast<ReadSingleParquetFileLocalState>();
	lstate.has_row_group = false;
}

static void ReadSingleParquetFileFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &gstate = input.global_state->Cast<ReadSingleParquetFileGlobalState>();
	auto &lstate = input.local_state->Cast<ReadSingleParquetFileLocalState>();

	bool resuming_blocked_scan = false;
	while (true) {
		if (!lstate.has_row_group) {
			if (lstate.claimed_externally) {
				// the next row group is claimed by our caller
				return;
			}
			if (!ClaimNextRowGroup(context, gstate, lstate)) {
				// there is nothing left for us to read in this file
				return;
			}
			LoadClaimedRowGroup(context, gstate, lstate);
		}
		if (!resuming_blocked_scan) {
			// a blocked scan left partial data in the chunk that resuming it completes - everything else starts
			// from an empty chunk
			output.Reset();
		}
		auto scan_result = gstate.reader->Scan(context, gstate.state, lstate.state, output);
		resuming_blocked_scan = scan_result.GetResultType() == AsyncResultType::BLOCKED;
		switch (scan_result.GetResultType()) {
		case AsyncResultType::BLOCKED:
			// the reader needs data that is not in memory yet - run the I/O it scheduled and resume. It left
			// partial data in the chunk that the resumed scan completes, so the chunk is not reset
			scan_result.ExecuteTasksSynchronously();
			continue;
		case AsyncResultType::HAVE_MORE_OUTPUT:
			if (output.size() == 0) {
				// every row of this chunk was filtered out - there is more to read in this row group
				continue;
			}
			return;
		case AsyncResultType::FINISHED:
			// this row group is done - the next one is claimed above
			lstate.has_row_group = false;
			if (output.size() != 0) {
				return;
			}
			continue;
		default:
			throw InternalException("Unexpected result type %s scanning a parquet file",
			                        EnumUtil::ToChars(scan_result.GetResultType()));
		}
	}
}

static double ReadSingleParquetFileProgress(ClientContext &context, const FunctionData *bind_data,
                                            const GlobalTableFunctionState *global_state) {
	if (!global_state) {
		return 0;
	}
	auto &gstate = global_state->Cast<ReadSingleParquetFileGlobalState>();
	if (!gstate.reader) {
		return 0;
	}
	return gstate.reader->GetProgressInFile(context);
}

static unique_ptr<NodeStatistics> ReadSingleParquetFileCardinality(ClientContext &context,
                                                                   const FunctionData *bind_data) {
	auto &parquet_data = bind_data->Cast<ReadSingleParquetFileData>();
	if (!parquet_data.cardinality.IsValid()) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(parquet_data.cardinality.GetIndex());
}

//! The virtual columns the parquet reader can produce for a file
static virtual_column_map_t ReadSingleParquetFileVirtualColumns(ClientContext &context,
                                                                optional_ptr<FunctionData> bind_data) {
	virtual_column_map_t result;
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
	result.insert(make_pair(ParquetReader::COLUMN_IDENTIFIER_FILE_ROW_GROUP_NUMBER,
	                        TableColumn("file_row_group_number", LogicalType::UBIGINT)));
	return result;
}

//! The statistics of a column of this file, read from its metadata
static unique_ptr<BaseStatistics>
ReadSingleParquetFileStatistics(ClientContext &context, const FunctionData *bind_data_p, column_t column_index) {
	auto &parquet_data = bind_data_p->Cast<ReadSingleParquetFileData>();
	if (IsVirtualColumn(column_index)) {
		return ParquetReader::ReadVirtualColumnStatistics(context, parquet_data.options, parquet_data.metadata,
		                                                  column_index);
	}
	if (column_index >= parquet_data.parquet_names.size()) {
		return nullptr;
	}
	return ParquetReader::ReadStatistics(context, parquet_data.options, parquet_data.metadata,
	                                     parquet_data.parquet_names[column_index]);
}

//! The columns that identify a row of a parquet scan
static vector<column_t> ReadSingleParquetFileRowIdColumns(ClientContext &context,
                                                          optional_ptr<FunctionData> bind_data) {
	vector<column_t> result;
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX);
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	return result;
}

//! The row groups read from this file. The counts accumulate, so that a scan over several files reports their sum
static void ReadSingleParquetFileGetMetrics(TableFunctionGetMetricsInput &input) {
	if (!input.global_state) {
		return;
	}
	auto &gstate = input.global_state->Cast<ReadSingleParquetFileGlobalState>();
	// the row groups scanned are drained, so each is reported once; the number to scan is the size of this file,
	// which our caller reports as-is
	input.operator_metrics.row_groups_scanned += gstate.state.row_groups_scanned_unreported.exchange(0);
	input.operator_metrics.total_row_groups_to_scan += gstate.state.total_row_groups_to_scan.load();
}

//! The row groups of this file, read from its metadata
static vector<PartitionStatistics> ReadSingleParquetFilePartitionStats(ClientContext &context,
                                                                       GetPartitionStatsInput &input) {
	vector<PartitionStatistics> result;
	auto &parquet_data = input.bind_data->Cast<ReadSingleParquetFileData>();
	if (!parquet_data.metadata) {
		return result;
	}
	// the row groups carry the statistics of their columns, which lets e.g. min/max be answered from the metadata
	parquet_data.GetMetadataReader(context).GetPartitionStats(result);
	return result;
}

TableFunction ParquetScanFunction::GetSingleFileFunction() {
	TableFunction read_parquet("read_single_parquet_file", {LogicalType::VARCHAR}, ReadSingleParquetFileFunction,
	                           ReadSingleParquetFileBind, ReadSingleParquetFileInitGlobal,
	                           ReadSingleParquetFileInitLocal);
	read_parquet.claim_batch = ReadSingleParquetFileClaimBatch;
	read_parquet.finish_batch = ReadSingleParquetFileFinishBatch;
	read_parquet.supports_read_ahead = ReadSingleParquetFileSupportsReadAhead;
	read_parquet.schedule_io = ReadSingleParquetFileScheduleIO;
	read_parquet.prepare_read_ahead = ReadSingleParquetFilePrepareReadAhead;
	read_parquet.table_scan_progress = ReadSingleParquetFileProgress;
	read_parquet.cardinality = ReadSingleParquetFileCardinality;
	read_parquet.statistics = ReadSingleParquetFileStatistics;
	read_parquet.get_virtual_columns = ReadSingleParquetFileVirtualColumns;
	read_parquet.get_file_columns = ReadSingleParquetFileColumns;
	read_parquet.get_partition_stats = ReadSingleParquetFilePartitionStats;
	read_parquet.get_metrics = ReadSingleParquetFileGetMetrics;
	read_parquet.projection_pushdown = true;
	read_parquet.late_materialization = true;
	// the order the filters are best applied in is learned while reading, and carries over to the next file
	read_parquet.reuses_local_state = true;
	read_parquet.filter_pushdown = true;
	read_parquet.filter_prune = true;
	ParquetScanFunction::AddNamedParameters(read_parquet);
	return read_parquet;
}

} // namespace duckdb
