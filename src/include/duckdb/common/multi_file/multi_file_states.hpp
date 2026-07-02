//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_states.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"

namespace duckdb {
struct MultiFileReaderInterface;

//! The bind data for the multi-file reader, obtained through MultiFileReader::BindReader
struct MultiFileReaderBindData {
	//! The (global) column id of the filename column (if any)
	optional_idx filename_idx;
	//! The set of hive partitioning indexes (if any)
	vector<HivePartitioningIndex> hive_partitioning_indexes;
	//! (optional) The schema set by the multi file reader
	vector<MultiFileColumnDefinition> schema;
	//! The method used to map local -> global columns
	MultiFileColumnMappingMode mapping = MultiFileColumnMappingMode::BY_NAME;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static MultiFileReaderBindData Deserialize(Deserializer &deserializer);
};

//! Global state for MultiFileReads
struct MultiFileReaderGlobalState {
	MultiFileReaderGlobalState(vector<LogicalType> extra_columns_p, optional_ptr<const MultiFileList> file_list_p)
	    : extra_columns(std::move(extra_columns_p)), file_list(file_list_p) {};
	virtual ~MultiFileReaderGlobalState();

	//! extra columns that will be produced during scanning
	const vector<LogicalType> extra_columns;
	// the file list driving the current scan
	const optional_ptr<const MultiFileList> file_list;

	//! Indicates that the MultiFileReader has added columns to be scanned that are not in the projection
	bool RequiresExtraColumns() {
		return !extra_columns.empty();
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct MultiFileBindData : public TableFunctionData {
	~MultiFileBindData() override;

	unique_ptr<TableFunctionData> bind_data;
	shared_ptr<MultiFileList> file_list;
	unique_ptr<MultiFileReader> multi_file_reader;
	unique_ptr<MultiFileReaderInterface> interface;
	vector<MultiFileColumnDefinition> columns;
	MultiFileReaderBindData reader_bind;
	MultiFileOptions file_options;
	vector<LogicalType> types;
	vector<string> names;
	virtual_column_map_t virtual_columns;
	//! Table column names - set when using COPY tbl FROM file.parquet
	vector<string> table_columns;
	shared_ptr<BaseFileReader> initial_reader;
	// The union readers are created (when the union_by_name option is on) during binding
	vector<shared_ptr<BaseUnionData>> union_readers;

	void Initialize(shared_ptr<BaseFileReader> reader) {
		initial_reader = std::move(reader);
	}
	void Initialize(ClientContext &, BaseUnionData &union_data) {
		Initialize(std::move(union_data.reader));
	}
	bool SupportStatementCache() const override {
		return false;
	}

	unique_ptr<FunctionData> Copy() const override;
};

//! Per-file data for the multi file reader
struct MultiFileReaderData {
	// Create data for an unopened file
	explicit MultiFileReaderData(const OpenFileInfo &file_to_be_opened)
	    : reader(nullptr), file_state(MultiFileFileState::UNOPENED), file_mutex(make_uniq<mutex>()),
	      file_to_be_opened(file_to_be_opened) {
	}
	// Create data for an existing reader
	explicit MultiFileReaderData(shared_ptr<BaseFileReader> reader_p)
	    : reader(std::move(reader_p)), file_state(MultiFileFileState::OPEN), file_mutex(make_uniq<mutex>()) {
	}
	// Create data for an existing reader
	explicit MultiFileReaderData(shared_ptr<BaseUnionData> union_data_p) : file_mutex(make_uniq<mutex>()) {
		if (union_data_p->reader) {
			reader = std::move(union_data_p->reader);
			file_state = MultiFileFileState::OPEN;
		} else {
			union_data = std::move(union_data_p);
			file_state = MultiFileFileState::UNOPENED;
		}
	}

	//! Currently opened reader for the file
	shared_ptr<BaseFileReader> reader;
	//! The file reader after we have started all scans to the file
	weak_ptr<BaseFileReader> closed_reader;
	//! Flag to indicate the file is being opened
	MultiFileFileState file_state;
	//! Mutexes to wait for the file when it is being opened
	unique_ptr<mutex> file_mutex;
	//! Options for opening the file
	shared_ptr<BaseUnionData> union_data;
	//! The constants that should be applied at the various positions
	MultiFileConstantMap constant_map;
	//! The set of expressions that should be evaluated to obtain the final result
	vector<unique_ptr<Expression>> expressions;

	//! (only set when file_state is UNOPENED) the file to be opened
	OpenFileInfo file_to_be_opened;
};

struct MultiFileGlobalState : public GlobalTableFunctionState {
	explicit MultiFileGlobalState(MultiFileList &file_list_p) : file_list(file_list_p) {
	}
	explicit MultiFileGlobalState(unique_ptr<MultiFileList> owned_file_list_p)
	    : file_list(*owned_file_list_p), owned_file_list(std::move(owned_file_list_p)) {
	}

	//! The file list to scan
	MultiFileList &file_list;
	//! The scan over the file_list
	MultiFileListScanData file_list_scan;
	//! Owned multi file list - if filters have been dynamically pushed into the reader
	unique_ptr<MultiFileList> owned_file_list;
	//! Reader state
	unique_ptr<MultiFileReaderGlobalState> multi_file_reader_state;
	//! Lock
	mutable mutex lock;
	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	//! Index of file currently up for scanning
	atomic<idx_t> file_index;
	//! Index of the lowest file we know we have completely read
	mutable idx_t completed_file_index = 0;
	//! The current set of readers
	vector<unique_ptr<MultiFileReaderData>> readers;

	idx_t batch_index = 0;

	idx_t max_threads = 1;
	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;
	vector<ColumnIndex> column_indexes;
	optional_ptr<TableFilterSet> filters;
	atomic<bool> finished {false};

	unique_ptr<GlobalTableFunctionState> global_state;

	optional_ptr<const PhysicalOperator> op;

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveColumns() const {
		return !projection_ids.empty();
	}
};

struct MultiFileLocalState : public LocalTableFunctionState {
public:
	explicit MultiFileLocalState(ClientContext &context) : executor(context) {
	}

public:
	shared_ptr<BaseFileReader> reader;
	optional_ptr<MultiFileReaderData> reader_data;
	bool is_parallel;
	idx_t batch_index;
	idx_t file_index = DConstants::INVALID_INDEX;
	unique_ptr<LocalTableFunctionState> local_state;
	//! The chunk written to by the reader, handed to FinalizeChunk to transform to the global schema
	DataChunk scan_chunk;
	//! The executor to transform scan_chunk into the final result with FinalizeChunk
	ExpressionExecutor executor;
};

} // namespace duckdb
