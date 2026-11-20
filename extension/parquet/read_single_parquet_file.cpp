#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "parquet_multi_file_info.hpp"
#include "duckdb/common/enum_util.hpp"
#include "parquet_reader.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/once_flag.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"

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
	//! The expressions the reader evaluates on columns instead of reading them (e.g. their length for strlen)
	unordered_map<idx_t, ParquetReaderProjectionExpression> projection_expressions;

	//! Read the given column as the result of an expression on it - the column then has the type of that result
	void AddProjectionExpression(idx_t column_idx, const ParquetReaderProjectionExpression &expression) {
		projection_expressions[column_idx] = expression;
		if (column_idx < parquet_types.size()) {
			parquet_types[column_idx] = expression.return_type;
		}
		if (column_idx < columns.size()) {
			columns[column_idx].type = expression.return_type;
		}
	}

	unique_ptr<FunctionData> Copy() const override {
		// the copy does not take over the reader the bind opened - its scans open the file themselves
		auto result = make_uniq<ReadSingleParquetFileData>();
		result->file = file;
		result->options = options;
		result->parquet_names = parquet_names;
		result->parquet_types = parquet_types;
		result->columns = columns;
		result->metadata = metadata;
		result->cardinality = cardinality;
		result->projection_expressions = projection_expressions;
		return std::move(result);
	}

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
		CallOnce(metadata_reader_initialized,
		         [&] { metadata_reader = ParquetReader::CreateMetadataReader(context, options, metadata); });
		return *metadata_reader;
	}

private:
	//! Guards the transfer of the bind reader
	mutable mutex bind_reader_lock;
	mutable shared_ptr<ParquetReader> bind_reader;
	mutable OnceFlag metadata_reader_initialized;
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
	auto &file_input = TableFunctionFileBindInput::Get(input);
	auto result = make_uniq<ReadSingleParquetFileData>();
	if (input.inputs[0].IsNull()) {
		throw BinderException("read_single_parquet_file requires a non-NULL file name");
	}
	result->file = TableFunctionFileBindInput::GetFile(input);

	// the named parameters of this function are those of the multi-file parquet reader
	ParquetMultiFileInfo file_info;
	MultiFileOptions file_options;
	if (file_input.multi_file_options) {
		file_options = *file_input.multi_file_options;
	}
	ParquetFileReaderOptions options(context);
	for (auto &kv : input.named_parameters) {
		file_info.ParseOption(context, kv.first, kv.second, file_options, options);
	}
	result->options = std::move(options.options);

	// read the metadata of the file to determine its schema - when this file was bound before, to determine the
	// schema of the scan it is part of, its metadata is taken from that bind instead of being read again
	shared_ptr<ParquetFileMetadataCache> known_metadata;
	if (file_input.file_bind_data) {
		auto &previous = file_input.file_bind_data->Cast<ReadSingleParquetFileData>();
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
	if (file_input.expected_bind_data) {
		// expressions pushed into the scan after the schema was bound are read from this file in the same way
		auto &expected = file_input.expected_bind_data->Cast<ReadSingleParquetFileData>();
		if (expected.file.path == result->file.path) {
			for (auto &entry : expected.projection_expressions) {
				result->AddProjectionExpression(entry.first, entry.second);
				if (entry.first < return_types.size()) {
					return_types[entry.first] = entry.second.return_type;
				}
			}
		}
	}
	if (!file_input.schema_only) {
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
	auto &file_input = TableFunctionFileInitInput::Get(input);
	auto &parquet_data = input.bind_data->Cast<ReadSingleParquetFileData>();
	auto result = make_uniq<ReadSingleParquetFileGlobalState>(input.op);

	result->reader = parquet_data.TakeBindReader();
	if (!result->reader) {
		result->reader =
		    make_shared_ptr<ParquetReader>(context, parquet_data.file, parquet_data.options, parquet_data.metadata);
	}
	auto &reader = *result->reader;
	for (auto &entry : parquet_data.projection_expressions) {
		// the reader may have been opened before these expressions were pushed into the scan
		auto column_idx = entry.first;
		reader.projection_expressions[column_idx] = entry.second;
		if (column_idx < reader.columns.size()) {
			reader.columns[column_idx].type = entry.second.return_type;
		}
		if (column_idx < reader.root_schema->children.size()) {
			reader.root_schema->children[column_idx].type = entry.second.return_type;
		}
	}
	auto virtual_column_types = ReadSingleParquetFileVirtualColumns(context, nullptr);
	// perform projection pushdown - the reader emits the columns in the order they are requested
	vector<ColumnIndex> column_indexes;
	for (auto &column_index : input.column_indexes) {
		const auto col_id = column_index.GetPrimaryIndex();
		// a virtual column is either projected under its own id, or - when our caller reads several files - under
		// an index of its own past the columns of this file
		auto virtual_id = col_id;
		if (file_input.virtual_columns) {
			auto entry = file_input.virtual_columns->find(col_id);
			if (entry != file_input.virtual_columns->end()) {
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
	if (file_input.filter_global_indices) {
		reader.filter_global_indices = *file_input.filter_global_indices;
	}
	if (file_input.expression_map) {
		// our caller could not express some of its filters in the types this file stores its columns as - the
		// reader evaluates these expressions on them first, and applies the filters to the result
		for (auto &entry : *file_input.expression_map) {
			vector<ColumnIndex> expression_column_indexes = entry.second.column_indexes;
			reader.expression_map.emplace(entry.first, BaseFileReaderExpression(entry.second.expression->Copy(),
			                                                                    std::move(expression_column_indexes)));
		}
	}
	// NOTE: the parquet reader does not read columns as a type other than the one they have in the file, so
	// "supports_cast_map" is not set and our caller casts the columns it needs converted itself
	if (file_input.deletion_filter) {
		// our caller has deleted rows from this file - the reader skips them while scanning. The filter stays owned
		// by the caller, which outlives this scan
		reader.deletion_filter = make_uniq<BorrowedDeleteFilter>(*file_input.deletion_filter);
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
		io_result.ExecuteTasksSynchronously(context);
	}
}

static bool ReadSingleParquetFileClaimBatch(ClientContext &context, TableFunctionInput &input) {
	auto &gstate = input.global_state->Cast<ReadSingleParquetFileGlobalState>();
	auto &lstate = input.local_state->Cast<ReadSingleParquetFileLocalState>();
	// our caller hands out the row groups, so we must not claim the next one ourselves
	lstate.claimed_externally = true;
	// our caller holds the lock of its scan here - the claimed row group is prepared in the prepare_batch callback
	lock_guard<mutex> guard(gstate.lock);
	return gstate.reader->TryInitializeScan(context, gstate.state, lstate.state);
}

static void ReadSingleParquetFilePrepareBatch(ClientContext &context, TableFunctionInput &input) {
	auto &gstate = input.global_state->Cast<ReadSingleParquetFileGlobalState>();
	auto &lstate = input.local_state->Cast<ReadSingleParquetFileLocalState>();
	gstate.reader->PrepareScan(context, gstate.state, lstate.state);
	lstate.has_row_group = true;
}

//! Pre-open the handle the scan reads the file through, so that claiming a row group does not have to open the file
static void ReadSingleParquetFilePrepareReadAhead(ClientContext &context, TableFunctionInput &input) {
	auto &gstate = input.global_state->Cast<ReadSingleParquetFileGlobalState>();
	gstate.reader->PrepareReadAhead(context, gstate.state);
}

//! The parquet reader can always be read ahead - the row group a thread claimed is loaded before it is scanned
static bool ReadSingleParquetFileSupportsReadAhead(optional_ptr<const FunctionData> bind_data) {
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
			scan_result.ExecuteTasksSynchronously(context);
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

static optional<TableFunctionFilterConditionedStatisticsCost>
EstimateSingleParquetFileFilterConditionedStatisticsCost(ClientContext &context, const FunctionData &bind_data,
                                                         const vector<ColumnIndex> &column_indexes) {
	auto &parquet_data = bind_data.Cast<ReadSingleParquetFileData>();
	if (!parquet_data.metadata) {
		return nullopt;
	}
	auto &metadata_reader = parquet_data.GetMetadataReader(context);
	auto &row_groups = metadata_reader.GetFileMetadata()->row_groups;
	unordered_set<idx_t> physical_columns;
	TableFunctionFilterConditionedStatisticsCost result {0, 0, 0};
	for (auto &row_group : row_groups) {
		if (row_group.num_rows < 0 ||
		    !TryAddOperator::Operation(result.rows, static_cast<idx_t>(row_group.num_rows), result.rows)) {
			return nullopt;
		}
	}
	for (auto &column_index : column_indexes) {
		if (!column_index.HasPrimaryIndex() || column_index.HasChildren() || column_index.IsVirtualColumn()) {
			return nullopt;
		}
		auto primary_index = column_index.GetPrimaryIndex();
		if (primary_index >= parquet_data.parquet_types.size() ||
		    primary_index >= metadata_reader.root_schema->children.size()) {
			return nullopt;
		}
		if (!physical_columns.insert(primary_index).second) {
			continue;
		}
		auto file_column_index = metadata_reader.root_schema->children[primary_index].column_index;
		for (auto &row_group : row_groups) {
			if (file_column_index >= row_group.columns.size()) {
				return nullopt;
			}
			auto &column = row_group.columns[file_column_index];
			if (!column.__isset.meta_data || column.meta_data.total_compressed_size < 0 ||
			    column.meta_data.total_uncompressed_size < 0 ||
			    !TryAddOperator::Operation(result.compressed_bytes,
			                               static_cast<idx_t>(column.meta_data.total_compressed_size),
			                               result.compressed_bytes) ||
			    !TryAddOperator::Operation(result.uncompressed_bytes,
			                               static_cast<idx_t>(column.meta_data.total_uncompressed_size),
			                               result.uncompressed_bytes)) {
				return nullopt;
			}
		}
	}
	return result;
}

static bool FilterConditionedStatisticsFiltersSupported(const ReadSingleParquetFileData &parquet_data,
                                                        const TableFunctionFilterConditionedStatisticsInput &input) {
	// Multi-column filters are not associated with one projected column, so they cannot be forwarded to this reader.
	if (input.filters.HasMultiColumnFilters()) {
		return false;
	}
	for (auto &entry : input.filters) {
		// Optional filters are pruning hints and do not define the filtered population represented by the result.
		if (ExpressionFilter::IsOptionalFilter(entry.Filter())) {
			return false;
		}
		auto filter_index = entry.GetIndex().GetIndex();
		if (filter_index >= input.scan_column_indexes.size()) {
			return false;
		}
		auto &column_index = input.scan_column_indexes[filter_index];
		if (!column_index.HasPrimaryIndex() || column_index.HasChildren() || column_index.IsVirtualColumn()) {
			return false;
		}
		auto primary_index = column_index.GetPrimaryIndex();
		if (primary_index >= parquet_data.parquet_types.size() ||
		    !parquet_data.parquet_types[primary_index].IsIntegral()) {
			return false;
		}
		auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "Parquet filter-conditioned statistics");
		if (!BoundComparisonExpression::IsComparison(*filter.expr) ||
		    (filter.expr->GetExpressionType() != ExpressionType::COMPARE_EQUAL &&
		     filter.expr->GetExpressionType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM)) {
			return false;
		}
	}
	return true;
}

static bool TryCollectFilterConditionedStatisticsTypes(const ReadSingleParquetFileData &parquet_data,
                                                       const vector<ColumnIndex> &column_indexes,
                                                       vector<LogicalType> &types) {
	types.reserve(column_indexes.size());
	for (auto &column_index : column_indexes) {
		if (!column_index.HasPrimaryIndex() || column_index.HasChildren() || column_index.IsVirtualColumn()) {
			return false;
		}
		auto primary_index = column_index.GetPrimaryIndex();
		if (primary_index >= parquet_data.parquet_types.size()) {
			return false;
		}
		types.push_back(parquet_data.parquet_types[primary_index]);
	}
	return true;
}

static idx_t FilterConditionedStatisticsMaxCompressedBytes(ClientContext &context) {
	Value setting;
	if (!context.TryGetCurrentSetting(Identifier("parquet_filter_conditioned_statistics_max_compressed_bytes"),
	                                  setting) ||
	    setting.IsNull()) {
		return 128ULL * 1024ULL * 1024ULL;
	}
	return UBigIntValue::Get(setting);
}

static bool FilterConditionedStatisticsCostSupported(ClientContext &context,
                                                     const ReadSingleParquetFileData &parquet_data,
                                                     const vector<ColumnIndex> &column_indexes) {
	auto cost = EstimateSingleParquetFileFilterConditionedStatisticsCost(context, parquet_data, column_indexes);
	return cost && cost->compressed_bytes <= FilterConditionedStatisticsMaxCompressedBytes(context);
}

static bool FilterConditionedStatisticsSupportsMinMax(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		return true;
	default:
		return false;
	}
}

template <class T>
static void UpdateFilterConditionedMinMax(BaseStatistics &statistics, const Vector &input) {
	auto values = input.Values<T>();
	for (auto entry : values) {
		if (!entry.IsValid()) {
			statistics.SetHasNullFast();
			continue;
		}
		statistics.SetHasNoNullFast();
		statistics.UpdateNumericStats(entry.GetValue());
	}
}

static void UpdateFilterConditionedMinMax(BaseStatistics &statistics, const Vector &input) {
	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
		UpdateFilterConditionedMinMax<bool>(statistics, input);
		break;
	case PhysicalType::INT8:
		UpdateFilterConditionedMinMax<int8_t>(statistics, input);
		break;
	case PhysicalType::INT16:
		UpdateFilterConditionedMinMax<int16_t>(statistics, input);
		break;
	case PhysicalType::INT32:
		UpdateFilterConditionedMinMax<int32_t>(statistics, input);
		break;
	case PhysicalType::INT64:
		UpdateFilterConditionedMinMax<int64_t>(statistics, input);
		break;
	case PhysicalType::UINT8:
		UpdateFilterConditionedMinMax<uint8_t>(statistics, input);
		break;
	case PhysicalType::UINT16:
		UpdateFilterConditionedMinMax<uint16_t>(statistics, input);
		break;
	case PhysicalType::UINT32:
		UpdateFilterConditionedMinMax<uint32_t>(statistics, input);
		break;
	case PhysicalType::UINT64:
		UpdateFilterConditionedMinMax<uint64_t>(statistics, input);
		break;
	default:
		break;
	}
}

static unique_ptr<TableFunctionFilterConditionedStatistics>
CollectSingleParquetFileFilterConditionedStatisticsInternal(ClientContext &context,
                                                            TableFunctionFilterConditionedStatisticsInput &input) {
	if (!input.bind_data) {
		return nullptr;
	}
	auto &parquet_data = input.bind_data->Cast<ReadSingleParquetFileData>();
	if (!parquet_data.metadata || !FilterConditionedStatisticsFiltersSupported(parquet_data, input) ||
	    !FilterConditionedStatisticsCostSupported(context, parquet_data, input.scan_column_indexes)) {
		return nullptr;
	}
	vector<LogicalType> scan_types;
	if (!TryCollectFilterConditionedStatisticsTypes(parquet_data, input.scan_column_indexes, scan_types)) {
		return nullptr;
	}
	vector<LogicalType> statistics_types;
	if (!TryCollectFilterConditionedStatisticsTypes(parquet_data, input.statistics_column_indexes, statistics_types)) {
		return nullptr;
	}
	vector<idx_t> statistics_scan_indexes;
	statistics_scan_indexes.reserve(input.statistics_column_indexes.size());
	for (auto &column_index : input.statistics_column_indexes) {
		auto entry = std::find(input.scan_column_indexes.begin(), input.scan_column_indexes.end(), column_index);
		if (entry == input.scan_column_indexes.end()) {
			return nullptr;
		}
		statistics_scan_indexes.push_back(NumericCast<idx_t>(entry - input.scan_column_indexes.begin()));
	}

	auto reader =
	    make_shared_ptr<ParquetReader>(context, parquet_data.file, parquet_data.options, parquet_data.metadata);
	for (auto &column_index : input.scan_column_indexes) {
		reader->column_ids.push_back(MultiFileLocalColumnId(column_index.GetPrimaryIndex()));
		reader->column_indexes.push_back(column_index);
	}
	reader->filters = input.filters.Copy();
	ParquetReadGlobalState global_state(nullptr);
	ParquetReadLocalState local_state;
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), scan_types);
	vector<unique_ptr<DistinctStatistics>> distinct_statistics;
	distinct_statistics.reserve(statistics_types.size());
	vector<BaseStatistics> filtered_statistics;
	filtered_statistics.reserve(statistics_types.size());
	for (auto &type : statistics_types) {
		distinct_statistics.push_back(DistinctStatistics::TypeIsSupported(type) ? make_uniq<DistinctStatistics>()
		                                                                        : nullptr);
		filtered_statistics.push_back(FilterConditionedStatisticsSupportsMinMax(type)
		                                  ? BaseStatistics::CreateEmpty(type)
		                                  : BaseStatistics::CreateUnknown(type));
	}
	Vector hashes(LogicalType::HASH);
	idx_t filtered_cardinality = 0;
	while (reader->TryInitializeScan(context, global_state, local_state)) {
		reader->PrepareScan(context, global_state, local_state);
		auto io_result = reader->ScheduleIO(context, global_state, local_state);
		if (io_result.GetResultType() == AsyncResultType::BLOCKED) {
			io_result.ExecuteTasksSynchronously(context);
		}
		bool resuming_blocked_scan = false;
		while (true) {
			if (!resuming_blocked_scan) {
				chunk.Reset();
			}
			auto scan_result = reader->Scan(context, global_state, local_state, chunk);
			resuming_blocked_scan = scan_result.GetResultType() == AsyncResultType::BLOCKED;
			if (resuming_blocked_scan) {
				scan_result.ExecuteTasksSynchronously(context);
				continue;
			}
			if (chunk.size() > 0) {
				filtered_cardinality += chunk.size();
				for (idx_t column_idx = 0; column_idx < distinct_statistics.size(); column_idx++) {
					auto &input_vector = chunk.data[statistics_scan_indexes[column_idx]];
					if (distinct_statistics[column_idx]) {
						distinct_statistics[column_idx]->Update(input_vector, chunk.size(), hashes);
					}
					if (FilterConditionedStatisticsSupportsMinMax(statistics_types[column_idx])) {
						UpdateFilterConditionedMinMax(filtered_statistics[column_idx], input_vector);
					}
				}
			}
			if (scan_result.GetResultType() == AsyncResultType::FINISHED) {
				break;
			}
			if (scan_result.GetResultType() != AsyncResultType::HAVE_MORE_OUTPUT) {
				throw InternalException("Unexpected result type %s collecting Parquet filter-conditioned statistics",
				                        EnumUtil::ToChars(scan_result.GetResultType()));
			}
		}
	}

	vector<unique_ptr<BaseStatistics>> result;
	result.reserve(statistics_types.size());
	for (idx_t column_idx = 0; column_idx < statistics_types.size(); column_idx++) {
		if (!distinct_statistics[column_idx]) {
			result.push_back(nullptr);
			continue;
		}
		auto statistics = std::move(filtered_statistics[column_idx]);
		statistics.SetDistinctCount(distinct_statistics[column_idx]->GetCount());
		result.push_back(statistics.ToUnique());
	}
	return make_uniq<TableFunctionFilterConditionedStatistics>(filtered_cardinality, std::move(result),
	                                                           std::move(distinct_statistics));
}

static unique_ptr<TableFunctionFilterConditionedStatistics>
CollectSingleParquetFileFilterConditionedStatistics(ClientContext &context,
                                                    TableFunctionFilterConditionedStatisticsInput &input) {
	try {
		return CollectSingleParquetFileFilterConditionedStatisticsInternal(context, input);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		auto type = error.Type();
		if (type == ExceptionType::INTERNAL || type == ExceptionType::INTERRUPT ||
		    type == ExceptionType::OUT_OF_MEMORY || Exception::InvalidatesDatabase(type)) {
			throw;
		}
		return nullptr;
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
	auto result = make_uniq<NodeStatistics>(parquet_data.cardinality.GetIndex());
	if (!parquet_data.metadata) {
		return result;
	}
	// Execution reuses these row groups; the footer's total is not validated against their population.
	idx_t maximum = 0;
	for (auto &group : parquet_data.metadata->metadata->file_metadata.row_groups) {
		if (group.num_rows < 0 || !TryAddOperator::Operation(maximum, static_cast<idx_t>(group.num_rows), maximum)) {
			return result;
		}
	}
	result->has_max_cardinality = true;
	result->max_cardinality = maximum;
	return result;
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

//! The row number within the file identifies a row of a single file
static vector<column_t> ReadSingleParquetFileRowIdColumns(ClientContext &context,
                                                          optional_ptr<const FunctionData> bind_data) {
	return {MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER};
}

//! The statistics of a column of this file, read from its metadata
static unique_ptr<BaseStatistics> ReadSingleParquetFileStatistics(ClientContext &context,
                                                                  TableFunctionGetStatisticsInput &input) {
	auto &parquet_data = input.bind_data->Cast<ReadSingleParquetFileData>();
	if (!input.column_index.HasPrimaryIndex() || input.column_index.HasChildren()) {
		return nullptr;
	}
	const auto column_index = input.column_index.GetPrimaryIndex();
	if (IsVirtualColumn(column_index)) {
		return ParquetReader::ReadVirtualColumnStatistics(context, parquet_data.options, parquet_data.metadata,
		                                                  column_index);
	}
	if (column_index >= parquet_data.parquet_names.size()) {
		return nullptr;
	}
	return parquet_data.GetMetadataReader(context).ReadStatistics(parquet_data.parquet_names[column_index]);
}

//! The row groups read from this file. The counts accumulate, so that a scan over several files reports their sum
static void ReadSingleParquetFileGetMetrics(TableFunctionGetMetricsInput &input) {
	if (!input.global_state) {
		return;
	}
	auto &gstate = input.global_state->Cast<ReadSingleParquetFileGlobalState>();
	// the row groups and bytes scanned are drained, so each is reported once; the number to scan is the size of this
	// file, which our caller reports as-is
	input.operator_metrics.row_groups_scanned += gstate.state.row_groups_scanned_unreported.exchange(0);
	input.operator_metrics.bytes_scanned += gstate.state.bytes_scanned_unreported.exchange(0);
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

bool ParquetScanFunction::ProjectionExpressionPushdown(ClientContext &context,
                                                       const TableFunctionProjectionExpressionInput &input) {
	if (input.expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	const auto &fn = input.expr.Cast<BoundFunctionExpression>();
	if (const Identifier &name = fn.Function().GetName(); name != "strlen" && name != "octet_length") {
		return false;
	}
	auto &bind_data = input.get.bind_data.write().Cast<MultiFileBindData>();
	// Don't do pushdown on custom schema users like Ducklake, or when the schemas of several files are unified
	if (!bind_data.reader_bind.schema.empty() || bind_data.file_options.union_by_name) {
		return false;
	}
	// Don't do pushdown with multiple files - the metadata of all but the first file is not available upfront, and
	// the files may have their columns in a different order
	if (bind_data.file_list->GetExpandResult() != FileExpandResult::SINGLE_FILE ||
	    bind_data.union_readers.size() != 1) {
		return false;
	}
	const auto &column_id = input.get.GetColumnIds()[input.column_index];
	// Pushdown extract columns i.e. SELECT x.y.z have a complex nested type update
	if (column_id.IsPushdownExtract()) {
		return false;
	}
	const idx_t idx = column_id.GetPrimaryIndex();
	// Generated values do not come from the physical column, even when its name and type match.
	if (bind_data.IsGeneratedColumn(idx)) {
		return false;
	}
	// We run scalar function pushdown after filter pushdown - a filter pushed into the scan needs the column itself
	if (input.get.table_filters.HasFilter(input.column_index)) {
		return false;
	}
	auto &file = bind_data.union_readers[0]->Cast<TableFunctionUnionData>();
	if (!file.bind_data) {
		return false;
	}
	auto &parquet_data = file.bind_data->Cast<ReadSingleParquetFileData>();
	auto &metadata_reader = parquet_data.GetMetadataReader(context);
	auto &children = metadata_reader.root_schema->children;
	if (idx >= children.size() || idx >= bind_data.types.size() || idx >= file.types.size()) {
		return false;
	}
	if (idx >= file.names.size() ||
	    !bind_data.MatchesPhysicalColumn(idx, Identifier(file.names[idx]), file.types[idx])) {
		// The pushed expression addresses a physical column, before name mapping and conversion.
		return false;
	}
	const idx_t column_flat_idx = children[idx].column_index;
	for (const auto &group : metadata_reader.GetFileMetadata()->row_groups) {
		D_ASSERT(column_flat_idx < group.columns.size());
		for (const Encoding::type type : group.columns[column_flat_idx].meta_data.encodings) {
			if (type == duckdb_parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY) {
				return false;
			}
		}
	}

	const LogicalType type = LogicalType::BIGINT;
	const ParquetReaderProjectionExpression expression {ParquetReaderProjectionExpressionType::BYTE_LENGTH, type};
	// the file is read with this bind data, and every other bind of it takes the expression from here
	parquet_data.AddProjectionExpression(idx, expression);
	bind_data.types[idx] = type;
	bind_data.columns[idx].type = type;
	file.types[idx] = type;
	if (bind_data.initial_reader) {
		// The cached reader was bound before the expression was pushed down.
		auto &reader = bind_data.initial_reader->Cast<TableFunctionFileReader>();
		reader.types[idx] = type;
		if (idx < reader.columns.size()) {
			reader.columns[idx].type = type;
		}
	}
	auto &data = bind_data.bind_data->Cast<TableFunctionMultiFileData>();
	if (data.options.schema_bind_data && data.options.schema_bind_data != file.bind_data) {
		data.options.schema_bind_data->Cast<ReadSingleParquetFileData>().AddProjectionExpression(idx, expression);
	}
	if (idx < data.options.expected_types.size()) {
		data.options.expected_types[idx] = type;
	}
	return true;
}

const ParquetOptions &ParquetScanFunction::GetFileOptions(const FunctionData &file_bind_data) {
	return file_bind_data.Cast<ReadSingleParquetFileData>().options;
}

TableFunction ParquetScanFunction::GetSingleFileFunction() {
	TableFunction read_parquet("read_single_parquet_file",
	                           FunctionSignature().AddPositionalOnly("file", LogicalType::ANY),
	                           ReadSingleParquetFileFunction, ReadSingleParquetFileBind,
	                           ReadSingleParquetFileInitGlobal, ReadSingleParquetFileInitLocal);
	read_parquet.table_scan_progress = ReadSingleParquetFileProgress;
	read_parquet.cardinality = ReadSingleParquetFileCardinality;
	read_parquet.statistics_extended = ReadSingleParquetFileStatistics;
	read_parquet.collect_filter_conditioned_statistics = CollectSingleParquetFileFilterConditionedStatistics;
	read_parquet.estimate_filter_conditioned_statistics_cost = EstimateSingleParquetFileFilterConditionedStatisticsCost;
	read_parquet.get_virtual_columns = ReadSingleParquetFileVirtualColumns;
	read_parquet.get_partition_stats = ReadSingleParquetFilePartitionStats;
	read_parquet.get_metrics = ReadSingleParquetFileGetMetrics;
	read_parquet.projection_pushdown = true;
	read_parquet.late_materialization = true;
	read_parquet.get_row_id_columns = ReadSingleParquetFileRowIdColumns;
	read_parquet.filter_pushdown = true;
	read_parquet.filter_prune = true;
	ParquetScanFunction::AddNamedParameters(read_parquet);
	return read_parquet;
}

TableFunctionMultiFileSettings ParquetScanFunction::GetMultiFileSettings() {
	TableFunctionMultiFileSettings settings;
	settings.glob_input = FileGlobInput(FileGlobOptions::FALLBACK_GLOB, "parquet");
	settings.reader_type = "Parquet";
	settings.claim_batch = ReadSingleParquetFileClaimBatch;
	settings.prepare_batch = ReadSingleParquetFilePrepareBatch;
	settings.finish_batch = ReadSingleParquetFileFinishBatch;
	settings.supports_read_ahead = ReadSingleParquetFileSupportsReadAhead;
	settings.schedule_io = ReadSingleParquetFileScheduleIO;
	settings.prepare_read_ahead = ReadSingleParquetFilePrepareReadAhead;
	settings.get_file_columns = ReadSingleParquetFileColumns;
	// the order the filters are best applied in is learned while reading, and carries over to the next file
	settings.reuses_local_state = true;
	settings.reports_bytes_scanned = true;
	return settings;
}

} // namespace duckdb
