#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/union_by_name.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/operator/persistent/csv_line_info.hpp"
#include <limits>

namespace duckdb {

unique_ptr<CSVFileHandle> ReadCSV::OpenCSV(const string &file_path, FileCompressionType compression,
                                           ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto &allocator = BufferAllocator::Get(context);
	return CSVFileHandle::OpenFile(fs, allocator, file_path, compression, false);
}

void ReadCSVData::FinalizeRead(ClientContext &context) {
	BaseCSVData::Finalize();
	// Here we identify if we can run this CSV file on parallel or not.
	bool null_or_empty = options.delimiter.empty() || options.escape.empty() || options.quote.empty() ||
	                     options.delimiter[0] == '\0' || options.escape[0] == '\0' || options.quote[0] == '\0';
	bool complex_options = options.delimiter.size() > 1 || options.escape.size() > 1 || options.quote.size() > 1;
	bool not_supported_options = options.null_padding;

	auto number_of_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	if (options.parallel_mode != ParallelMode::PARALLEL && int64_t(files.size() * 2) >= number_of_threads) {
		single_threaded = true;
	}
	if (options.parallel_mode == ParallelMode::SINGLE_THREADED || null_or_empty || not_supported_options ||
	    complex_options || options.new_line == NewLineIdentifier::MIX) {
		// not supported for parallel CSV reading
		single_threaded = true;
	}
}

uint8_t GetCandidateSpecificity(const LogicalType &candidate_type) {
	//! Const ht with accepted auto_types and their weights in specificity
	const duckdb::unordered_map<uint8_t, uint8_t> auto_type_candidates_specificity {
	    {(uint8_t)LogicalTypeId::VARCHAR, 0},  {(uint8_t)LogicalTypeId::TIMESTAMP, 1},
	    {(uint8_t)LogicalTypeId::DATE, 2},     {(uint8_t)LogicalTypeId::TIME, 3},
	    {(uint8_t)LogicalTypeId::DOUBLE, 4},   {(uint8_t)LogicalTypeId::FLOAT, 5},
	    {(uint8_t)LogicalTypeId::BIGINT, 6},   {(uint8_t)LogicalTypeId::INTEGER, 7},
	    {(uint8_t)LogicalTypeId::SMALLINT, 8}, {(uint8_t)LogicalTypeId::TINYINT, 9},
	    {(uint8_t)LogicalTypeId::BOOLEAN, 10}, {(uint8_t)LogicalTypeId::SQLNULL, 11}};

	auto id = (uint8_t)candidate_type.id();
	auto it = auto_type_candidates_specificity.find(id);
	if (it == auto_type_candidates_specificity.end()) {
		throw BinderException("Auto Type Candidate of type %s is not accepted as a valid input",
		                      EnumUtil::ToString(candidate_type.id()));
	}
	return it->second;
}

static unique_ptr<FunctionData> ReadCSVBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ReadCSVData>();
	auto &options = result->options;
	result->files = MultiFileReader::GetFileList(context, input.inputs[0], "CSV");

	bool explicitly_set_columns = false;
	for (auto &kv : input.named_parameters) {
		if (MultiFileReader::ParseOption(kv.first, kv.second, options.file_options)) {
			continue;
		}
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "columns") {
			explicitly_set_columns = true;
			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException("read_csv columns requires a struct as input");
			}
			auto &struct_children = StructValue::GetChildren(kv.second);
			D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
			for (idx_t i = 0; i < struct_children.size(); i++) {
				auto &name = StructType::GetChildName(child_type, i);
				auto &val = struct_children[i];
				names.push_back(name);
				if (val.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_csv requires a type specification as string");
				}
				return_types.emplace_back(TransformStringToLogicalType(StringValue::Get(val), context));
			}
			if (names.empty()) {
				throw BinderException("read_csv requires at least a single column as input!");
			}
		} else if (loption == "auto_type_candidates") {
			options.auto_type_candidates.clear();
			map<uint8_t, LogicalType> candidate_types;
			// We always have the extremes of Null and Varchar, so we can default to varchar if the
			// sniffer is not able to confidently detect that column type
			candidate_types[GetCandidateSpecificity(LogicalType::VARCHAR)] = LogicalType::VARCHAR;
			candidate_types[GetCandidateSpecificity(LogicalType::SQLNULL)] = LogicalType::SQLNULL;

			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::LIST) {
				throw BinderException("read_csv auto_types requires a list as input");
			}
			auto &list_children = ListValue::GetChildren(kv.second);
			if (list_children.empty()) {
				throw BinderException("auto_type_candidates requires at least one type");
			}
			for (auto &child : list_children) {
				if (child.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("auto_type_candidates requires a type specification as string");
				}
				auto candidate_type = TransformStringToLogicalType(StringValue::Get(child), context);
				candidate_types[GetCandidateSpecificity(candidate_type)] = candidate_type;
			}
			for (auto &candidate_type : candidate_types) {
				options.auto_type_candidates.emplace_back(candidate_type.second);
			}
		} else if (loption == "column_names" || loption == "names") {
			if (!options.name_list.empty()) {
				throw BinderException("read_csv_auto column_names/names can only be supplied once");
			}
			if (kv.second.IsNull()) {
				throw BinderException("read_csv_auto %s cannot be NULL", kv.first);
			}
			auto &children = ListValue::GetChildren(kv.second);
			for (auto &child : children) {
				options.name_list.push_back(StringValue::Get(child));
			}
		} else if (loption == "column_types" || loption == "types" || loption == "dtypes") {
			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::STRUCT && child_type.id() != LogicalTypeId::LIST) {
				throw BinderException("read_csv_auto %s requires a struct or list as input", kv.first);
			}
			if (!options.sql_type_list.empty()) {
				throw BinderException("read_csv_auto column_types/types/dtypes can only be supplied once");
			}
			vector<string> sql_type_names;
			if (child_type.id() == LogicalTypeId::STRUCT) {
				auto &struct_children = StructValue::GetChildren(kv.second);
				D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
				for (idx_t i = 0; i < struct_children.size(); i++) {
					auto &name = StructType::GetChildName(child_type, i);
					auto &val = struct_children[i];
					if (val.type().id() != LogicalTypeId::VARCHAR) {
						throw BinderException("read_csv_auto %s requires a type specification as string", kv.first);
					}
					sql_type_names.push_back(StringValue::Get(val));
					options.sql_types_per_column[name] = i;
				}
			} else {
				auto &list_child = ListType::GetChildType(child_type);
				if (list_child.id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_csv_auto %s requires a list of types (varchar) as input", kv.first);
				}
				auto &children = ListValue::GetChildren(kv.second);
				for (auto &child : children) {
					sql_type_names.push_back(StringValue::Get(child));
				}
			}
			options.sql_type_list.reserve(sql_type_names.size());
			for (auto &sql_type : sql_type_names) {
				auto def_type = TransformStringToLogicalType(sql_type);
				if (def_type.id() == LogicalTypeId::USER) {
					throw BinderException("Unrecognized type \"%s\" for read_csv_auto %s definition", sql_type,
					                      kv.first);
				}
				options.sql_type_list.push_back(std::move(def_type));
			}
		} else if (loption == "all_varchar") {
			options.all_varchar = BooleanValue::Get(kv.second);
		} else if (loption == "normalize_names") {
			options.normalize_names = BooleanValue::Get(kv.second);
		} else {
			options.SetReadOption(loption, kv.second, names);
		}
	}
	if (options.file_options.auto_detect_hive_partitioning) {
		options.file_options.hive_partitioning = MultiFileReaderOptions::AutoDetectHivePartitioning(result->files);
	}

	if (!options.auto_detect && return_types.empty()) {
		throw BinderException("read_csv requires columns to be specified through the 'columns' option. Use "
		                      "read_csv_auto or set read_csv(..., "
		                      "AUTO_DETECT=TRUE) to automatically guess columns.");
	}
	if (options.auto_detect) {
		options.file_path = result->files[0];
		auto initial_reader = make_uniq<BufferedCSVReader>(context, options);
		return_types.assign(initial_reader->return_types.begin(), initial_reader->return_types.end());
		if (names.empty()) {
			names.assign(initial_reader->names.begin(), initial_reader->names.end());
		} else {
			if (explicitly_set_columns) {
				// The user has influenced the names, can't assume they are valid anymore
				if (return_types.size() != names.size()) {
					throw BinderException("The amount of names specified (%d) and the observed amount of types (%d) in "
					                      "the file don't match",
					                      names.size(), return_types.size());
				}
			} else {
				D_ASSERT(return_types.size() == names.size());
			}
			initial_reader->names = names;
		}
		options = initial_reader->options;
		result->initial_reader = std::move(initial_reader);
	} else {
		D_ASSERT(return_types.size() == names.size());
	}
	result->csv_types = return_types;
	result->csv_names = names;

	if (options.file_options.union_by_name) {
		result->reader_bind =
		    MultiFileReader::BindUnionReader<BufferedCSVReader>(context, return_types, names, *result, options);
		if (result->union_readers.size() > 1) {
			result->column_info.emplace_back(result->csv_names, result->csv_types);
			for (idx_t i = 1; i < result->union_readers.size(); i++) {
				result->column_info.emplace_back(result->union_readers[i]->names,
				                                 result->union_readers[i]->return_types);
			}
		}
		if (!options.sql_types_per_column.empty()) {
			auto exception = BufferedCSVReader::ColumnTypesError(options.sql_types_per_column, names);
			if (!exception.empty()) {
				throw BinderException(exception);
			}
		}
	} else {
		result->reader_bind = MultiFileReader::BindOptions(options.file_options, result->files, return_types, names);
	}
	result->return_types = return_types;
	result->return_names = names;
	result->FinalizeRead(context);
	return std::move(result);
}

static unique_ptr<FunctionData> ReadCSVAutoBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	input.named_parameters["auto_detect"] = Value::BOOLEAN(true);
	return ReadCSVBind(context, input, return_types, names);
}

//===--------------------------------------------------------------------===//
// Parallel CSV Reader CSV Global State
//===--------------------------------------------------------------------===//

struct ParallelCSVGlobalState : public GlobalTableFunctionState {
public:
	ParallelCSVGlobalState(ClientContext &context, unique_ptr<CSVFileHandle> file_handle_p,
	                       const vector<string> &files_path_p, idx_t system_threads_p, idx_t buffer_size_p,
	                       idx_t rows_to_skip, bool force_parallelism_p, vector<column_t> column_ids_p, bool has_header)
	    : file_handle(std::move(file_handle_p)), system_threads(system_threads_p), buffer_size(buffer_size_p),
	      force_parallelism(force_parallelism_p), column_ids(std::move(column_ids_p)),
	      line_info(main_mutex, batch_to_tuple_end, tuple_start, tuple_end) {
		file_handle->DisableReset();
		current_file_path = files_path_p[0];
		line_info.lines_read[0] = rows_to_skip;
		if (has_header) {
			line_info.lines_read[0]++;
		}
		file_size = file_handle->FileSize();
		first_file_size = file_size;
		on_disk_file = file_handle->OnDiskFile();
		bytes_read = 0;
		if (buffer_size < file_size || file_size == 0) {
			bytes_per_local_state = buffer_size / ParallelCSVGlobalState::MaxThreads();
		} else {
			bytes_per_local_state = file_size / MaxThreads();
		}
		if (bytes_per_local_state == 0) {
			// In practice, I think this won't happen, it only happens because we are mocking up test scenarios
			// this boy needs to be at least one.
			bytes_per_local_state = 1;
		}
		for (idx_t i = 0; i < rows_to_skip; i++) {
			file_handle->ReadLine();
		}
		first_position = current_csv_position;
		current_buffer = make_shared<CSVBuffer>(context, buffer_size, *file_handle, current_csv_position, file_number);
		next_buffer = shared_ptr<CSVBuffer>(
		    current_buffer->Next(*file_handle, buffer_size, current_csv_position, file_number).release());
		running_threads = MaxThreads();

		// Initialize all the book-keeping variables
		auto file_count = files_path_p.size();
		line_info.current_batches.resize(file_count);
		tuple_start.resize(file_count);
		tuple_end.resize(file_count);
		tuple_end_to_batch.resize(file_count);
		batch_to_tuple_end.resize(file_count);
	}
	ParallelCSVGlobalState() : line_info(main_mutex, batch_to_tuple_end, tuple_start, tuple_end) {
		running_threads = MaxThreads();
	}

	~ParallelCSVGlobalState() override {
	}

	//! How many bytes were read up to this point
	atomic<idx_t> bytes_read;
	//! Size of current file
	idx_t file_size;

public:
	idx_t MaxThreads() const override;
	//! Updates the CSV reader with the next buffer to read. Returns false if no more buffers are available.
	bool Next(ClientContext &context, const ReadCSVData &bind_data, unique_ptr<ParallelCSVReader> &reader);
	//! Verify if the CSV File was read correctly
	void Verify();

	void UpdateVerification(VerificationPositions positions, idx_t file_number, idx_t batch_idx);

	void UpdateLinesRead(CSVBufferRead &buffer_read, idx_t file_idx);

	void IncrementThread();

	void DecrementThread();

	bool Finished();

	double GetProgress(const ReadCSVData &bind_data) const {
		idx_t total_files = bind_data.files.size();

		// get the progress WITHIN the current file
		double progress;
		if (file_size == 0) {
			progress = 1.0;
		} else {
			progress = double(bytes_read) / double(file_size);
		}
		// now get the total percentage of files read
		double percentage = double(file_index - 1) / total_files;
		percentage += (double(1) / double(total_files)) * progress;
		return percentage * 100;
	}

private:
	//! File Handle for current file
	unique_ptr<CSVFileHandle> file_handle;
	shared_ptr<CSVBuffer> current_buffer;
	shared_ptr<CSVBuffer> next_buffer;

	//! The index of the next file to read (i.e. current file + 1)
	idx_t file_index = 1;
	string current_file_path;

	//! Mutex to lock when getting next batch of bytes (Parallel Only)
	mutex main_mutex;
	//! Byte set from for last thread
	idx_t next_byte = 0;
	//! How many bytes we should execute per local state
	idx_t bytes_per_local_state;
	//! Size of first file
	idx_t first_file_size;
	//! Whether or not this is an on-disk file
	bool on_disk_file = true;
	//! Basically max number of threads in DuckDB
	idx_t system_threads;
	//! Size of the buffers
	idx_t buffer_size;
	//! Current batch index
	idx_t batch_index = 0;
	idx_t local_batch_index = 0;

	//! Forces parallelism for small CSV Files, should only be used for testing.
	bool force_parallelism = false;
	//! Current (Global) position of CSV
	idx_t current_csv_position = 0;
	//! First Position of First Buffer
	idx_t first_position = 0;
	//! Current File Number
	idx_t file_number = 0;
	idx_t max_tuple_end = 0;
	//! The vector stores positions where threads ended the last line they read in the CSV File, and the set stores
	//! Positions where they started reading the first line.
	vector<vector<idx_t>> tuple_end;
	vector<set<idx_t>> tuple_start;
	//! Tuple end to batch
	vector<unordered_map<idx_t, idx_t>> tuple_end_to_batch;
	//! Batch to Tuple End
	vector<unordered_map<idx_t, idx_t>> batch_to_tuple_end;
	idx_t running_threads = 0;
	//! The column ids to read
	vector<column_t> column_ids;
	//! Line Info used in error messages
	LineInfo line_info;
};

idx_t ParallelCSVGlobalState::MaxThreads() const {
	if (force_parallelism || !on_disk_file) {
		return system_threads;
	}
	idx_t one_mb = 1000000; // We initialize max one thread per Mb
	idx_t threads_per_mb = first_file_size / one_mb + 1;
	if (threads_per_mb < system_threads || threads_per_mb == 1) {
		return threads_per_mb;
	}

	return system_threads;
}

void ParallelCSVGlobalState::IncrementThread() {
	lock_guard<mutex> parallel_lock(main_mutex);
	running_threads++;
}

void ParallelCSVGlobalState::DecrementThread() {
	lock_guard<mutex> parallel_lock(main_mutex);
	D_ASSERT(running_threads > 0);
	running_threads--;
}

bool ParallelCSVGlobalState::Finished() {
	lock_guard<mutex> parallel_lock(main_mutex);
	return running_threads == 0;
}

void ParallelCSVGlobalState::Verify() {
	// All threads are done, we run some magic sweet verification code
	lock_guard<mutex> parallel_lock(main_mutex);
	if (running_threads == 0) {
		D_ASSERT(tuple_end.size() == tuple_start.size());
		for (idx_t i = 0; i < tuple_start.size(); i++) {
			auto &current_tuple_end = tuple_end[i];
			auto &current_tuple_start = tuple_start[i];
			// figure out max value of last_pos
			if (current_tuple_end.empty()) {
				return;
			}
			auto max_value = *max_element(std::begin(current_tuple_end), std::end(current_tuple_end));
			for (idx_t tpl_idx = 0; tpl_idx < current_tuple_end.size(); tpl_idx++) {
				auto last_pos = current_tuple_end[tpl_idx];
				auto first_pos = current_tuple_start.find(last_pos);
				if (first_pos == current_tuple_start.end()) {
					// this might be necessary due to carriage returns outside buffer scopes.
					first_pos = current_tuple_start.find(last_pos + 1);
				}
				if (first_pos == current_tuple_start.end() && last_pos != max_value) {
					auto batch_idx = tuple_end_to_batch[i][last_pos];
					auto problematic_line = line_info.GetLine(batch_idx);
					throw InvalidInputException(
					    "CSV File not supported for multithreading. This can be a problematic line in your CSV File or "
					    "that this CSV can't be read in Parallel. Please, inspect if the line %llu is correct. If so, "
					    "please run single-threaded CSV Reading by setting parallel=false in the read_csv call.",
					    problematic_line);
				}
			}
		}
	}
}

void LineInfo::Verify(idx_t file_idx, idx_t batch_idx, idx_t cur_first_pos) {
	auto &tuple_start_set = tuple_start[file_idx];
	auto &processed_batches = batch_to_tuple_end[file_idx];
	auto &tuple_end_vec = tuple_end[file_idx];
	bool has_error = false;
	idx_t problematic_line;
	if (batch_idx == 0 || tuple_start_set.empty()) {
		return;
	}
	for (idx_t cur_batch = 0; cur_batch < batch_idx - 1; cur_batch++) {
		auto cur_end = tuple_end_vec[processed_batches[cur_batch]];
		auto first_pos = tuple_start_set.find(cur_end);
		if (first_pos == tuple_start_set.end()) {
			has_error = true;
			problematic_line = GetLine(cur_batch);
			break;
		}
	}
	if (!has_error) {
		auto cur_end = tuple_end_vec[processed_batches[batch_idx - 1]];
		if (cur_end != cur_first_pos) {
			has_error = true;
			problematic_line = GetLine(batch_idx);
		}
	}
	if (has_error) {
		throw InvalidInputException(
		    "CSV File not supported for multithreading. This can be a problematic line in your CSV File or "
		    "that this CSV can't be read in Parallel. Please, inspect if the line %llu is correct. If so, "
		    "please run single-threaded CSV Reading by setting parallel=false in the read_csv call.",
		    problematic_line);
	}
}

bool ParallelCSVGlobalState::Next(ClientContext &context, const ReadCSVData &bind_data,
                                  unique_ptr<ParallelCSVReader> &reader) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (!current_buffer) {
		// This means we are done with the current file, we need to go to the next one (if exists).
		if (file_index < bind_data.files.size()) {
			current_file_path = bind_data.files[file_index++];
			file_handle = ReadCSV::OpenCSV(current_file_path, bind_data.options.compression, context);
			current_csv_position = 0;
			file_number++;
			local_batch_index = 0;
			current_buffer =
			    make_shared<CSVBuffer>(context, buffer_size, *file_handle, current_csv_position, file_number);
			next_buffer = shared_ptr<CSVBuffer>(
			    current_buffer->Next(*file_handle, buffer_size, current_csv_position, file_number).release());
		} else {
			// We are done scanning.
			reader.reset();
			return false;
		}
	}
	// set up the current buffer
	line_info.current_batches.back().insert(local_batch_index);
	auto result = make_uniq<CSVBufferRead>(current_buffer, next_buffer, next_byte, next_byte + bytes_per_local_state,
	                                       batch_index++, local_batch_index++, &line_info);
	// move the byte index of the CSV reader to the next buffer
	next_byte += bytes_per_local_state;
	if (next_byte >= current_buffer->GetBufferSize()) {
		// We replace the current buffer with the next buffer
		next_byte = 0;
		bytes_read += current_buffer->GetBufferSize();
		current_buffer = next_buffer;
		if (next_buffer) {
			// Next buffer gets the next-next buffer
			next_buffer = shared_ptr<CSVBuffer>(
			    next_buffer->Next(*file_handle, buffer_size, current_csv_position, file_number).release());
		}
	}
	if (!reader || reader->options.file_path != current_file_path) {
		// we either don't have a reader, or the reader was created for a different file
		// we need to create a new reader and instantiate it
		if (file_index > 0 && file_index <= bind_data.union_readers.size() && bind_data.union_readers[file_index - 1]) {
			// we are doing UNION BY NAME - fetch the options from the union reader for this file
			auto &union_reader = *bind_data.union_readers[file_index - 1];
			reader = make_uniq<ParallelCSVReader>(context, union_reader.options, std::move(result), first_position,
			                                      union_reader.GetTypes(), file_index - 1);
			reader->names = union_reader.GetNames();
		} else if (file_index <= bind_data.column_info.size()) {
			// Serialized Union By name
			reader = make_uniq<ParallelCSVReader>(context, bind_data.options, std::move(result), first_position,
			                                      bind_data.column_info[file_index - 1].types, file_index - 1);
			reader->names = bind_data.column_info[file_index - 1].names;
		} else {
			// regular file - use the standard options
			reader = make_uniq<ParallelCSVReader>(context, bind_data.options, std::move(result), first_position,
			                                      bind_data.csv_types, file_index - 1);
			reader->names = bind_data.csv_names;
		}
		reader->options.file_path = current_file_path;
		MultiFileReader::InitializeReader(*reader, bind_data.options.file_options, bind_data.reader_bind,
		                                  bind_data.return_types, bind_data.return_names, column_ids, nullptr,
		                                  bind_data.files.front());
	} else {
		// update the current reader
		reader->SetBufferRead(std::move(result));
	}
	return true;
}
void ParallelCSVGlobalState::UpdateVerification(VerificationPositions positions, idx_t file_number_p, idx_t batch_idx) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (positions.end_of_last_line > max_tuple_end) {
		max_tuple_end = positions.end_of_last_line;
	}
	tuple_end_to_batch[file_number_p][positions.end_of_last_line] = batch_idx;
	batch_to_tuple_end[file_number_p][batch_idx] = tuple_end[file_number_p].size();
	tuple_start[file_number_p].insert(positions.beginning_of_first_line);
	tuple_end[file_number_p].push_back(positions.end_of_last_line);
}

void ParallelCSVGlobalState::UpdateLinesRead(CSVBufferRead &buffer_read, idx_t file_idx) {
	auto batch_idx = buffer_read.local_batch_index;
	auto lines_read = buffer_read.lines_read;
	lock_guard<mutex> parallel_lock(main_mutex);
	line_info.current_batches[file_idx].erase(batch_idx);
	line_info.lines_read[batch_idx] += lines_read;
}

bool LineInfo::CanItGetLine(idx_t file_idx, idx_t batch_idx) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (current_batches.empty() || done) {
		return true;
	}
	if (file_idx >= current_batches.size() || current_batches[file_idx].empty()) {
		return true;
	}
	auto min_value = *current_batches[file_idx].begin();
	if (min_value >= batch_idx) {
		return true;
	}
	return false;
}

// Returns the 1-indexed line number
idx_t LineInfo::GetLine(idx_t batch_idx, idx_t line_error, idx_t file_idx, idx_t cur_start, bool verify) {
	unique_ptr<lock_guard<mutex>> parallel_lock;
	if (!verify) {
		parallel_lock = duckdb::make_uniq<lock_guard<mutex>>(main_mutex);
	}
	idx_t line_count = 0;
	if (done) {
		// line count is 0-indexed, but we want to return 1-indexed
		return first_line + 1;
	}
	for (idx_t i = 0; i <= batch_idx; i++) {
		if (lines_read.find(i) == lines_read.end() && i != batch_idx) {
			throw InternalException("Missing batch index on Parallel CSV Reader GetLine");
		}
		line_count += lines_read[i];
	}

	// before we are done, if this is not a call in Verify() we must check Verify up to this batch
	if (!verify) {
		Verify(file_idx, batch_idx, cur_start);
	}
	done = true;
	first_line = line_count + line_error;
	// line count is 0-indexed, but we want to return 1-indexed
	return first_line + 1;
}

static unique_ptr<GlobalTableFunctionState> ParallelCSVInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<ReadCSVData>();
	if (bind_data.files.empty()) {
		// This can happen when a filename based filter pushdown has eliminated all possible files for this scan.
		return make_uniq<ParallelCSVGlobalState>();
	}
	unique_ptr<CSVFileHandle> file_handle;

	bind_data.options.file_path = bind_data.files[0];

	if (bind_data.initial_reader) {
		file_handle = std::move(bind_data.initial_reader->file_handle);
		file_handle->Reset();
		file_handle->DisableReset();
		bind_data.initial_reader.reset();
	} else {
		file_handle = ReadCSV::OpenCSV(bind_data.options.file_path, bind_data.options.compression, context);
	}
	return make_uniq<ParallelCSVGlobalState>(
	    context, std::move(file_handle), bind_data.files, context.db->NumberOfThreads(), bind_data.options.buffer_size,
	    bind_data.options.skip_rows, ClientConfig::GetConfig(context).verify_parallelism, input.column_ids,
	    bind_data.options.header && bind_data.options.has_header);
}

//===--------------------------------------------------------------------===//
// Read CSV Local State
//===--------------------------------------------------------------------===//
struct ParallelCSVLocalState : public LocalTableFunctionState {
public:
	explicit ParallelCSVLocalState(unique_ptr<ParallelCSVReader> csv_reader_p) : csv_reader(std::move(csv_reader_p)) {
	}

	//! The CSV reader
	unique_ptr<ParallelCSVReader> csv_reader;
	CSVBufferRead previous_buffer;
	bool done = false;
};

unique_ptr<LocalTableFunctionState> ParallelReadCSVInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state_p) {
	auto &csv_data = input.bind_data->Cast<ReadCSVData>();
	auto &global_state = global_state_p->Cast<ParallelCSVGlobalState>();
	unique_ptr<ParallelCSVReader> csv_reader;
	auto has_next = global_state.Next(context.client, csv_data, csv_reader);
	if (!has_next) {
		global_state.DecrementThread();
		csv_reader.reset();
	}
	return make_uniq<ParallelCSVLocalState>(std::move(csv_reader));
}

static void ParallelReadCSVFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ReadCSVData>();
	auto &csv_global_state = data_p.global_state->Cast<ParallelCSVGlobalState>();
	auto &csv_local_state = data_p.local_state->Cast<ParallelCSVLocalState>();

	if (!csv_local_state.csv_reader) {
		// no csv_reader was set, this can happen when a filename-based filter has filtered out all possible files
		return;
	}

	do {
		if (output.size() != 0) {
			MultiFileReader::FinalizeChunk(bind_data.reader_bind, csv_local_state.csv_reader->reader_data, output);
			break;
		}
		if (csv_local_state.csv_reader->finished) {
			auto verification_updates = csv_local_state.csv_reader->GetVerificationPositions();
			csv_global_state.UpdateVerification(verification_updates,
			                                    csv_local_state.csv_reader->buffer->buffer->GetFileNumber(),
			                                    csv_local_state.csv_reader->buffer->local_batch_index);
			csv_global_state.UpdateLinesRead(*csv_local_state.csv_reader->buffer, csv_local_state.csv_reader->file_idx);
			auto has_next = csv_global_state.Next(context, bind_data, csv_local_state.csv_reader);
			if (csv_local_state.csv_reader) {
				csv_local_state.csv_reader->linenr = 0;
			}
			if (!has_next) {
				csv_global_state.DecrementThread();
				break;
			}
		}
		csv_local_state.csv_reader->ParseCSV(output);

	} while (true);
	if (csv_global_state.Finished()) {
		csv_global_state.Verify();
	}
}

//===--------------------------------------------------------------------===//
// Single-Threaded CSV Reader
//===--------------------------------------------------------------------===//
struct SingleThreadedCSVState : public GlobalTableFunctionState {
	explicit SingleThreadedCSVState(idx_t total_files) : total_files(total_files), next_file(0), progress_in_files(0) {
	}

	mutex csv_lock;
	unique_ptr<BufferedCSVReader> initial_reader;
	//! The total number of files to read from
	idx_t total_files;
	//! The index of the next file to read (i.e. current file + 1)
	atomic<idx_t> next_file;
	//! How far along we are in reading the current set of open files
	//! This goes from [0...next_file] * 100
	atomic<idx_t> progress_in_files;
	//! The set of SQL types
	vector<LogicalType> csv_types;
	//! The set of SQL names to be read from the file
	vector<string> csv_names;
	//! The column ids to read
	vector<column_t> column_ids;

	idx_t MaxThreads() const override {
		return total_files;
	}

	double GetProgress(const ReadCSVData &bind_data) const {
		D_ASSERT(total_files == bind_data.files.size());
		D_ASSERT(progress_in_files <= total_files * 100);
		return (double(progress_in_files) / double(total_files));
	}

	unique_ptr<BufferedCSVReader> GetCSVReader(ClientContext &context, ReadCSVData &bind_data, idx_t &file_index,
	                                           idx_t &total_size) {
		auto reader = GetCSVReaderInternal(context, bind_data, file_index, total_size);
		if (reader) {
			reader->file_handle->DisableReset();
		}
		return reader;
	}

private:
	unique_ptr<BufferedCSVReader> GetCSVReaderInternal(ClientContext &context, ReadCSVData &bind_data,
	                                                   idx_t &file_index, idx_t &total_size) {
		BufferedCSVReaderOptions options;
		{
			lock_guard<mutex> l(csv_lock);
			if (initial_reader) {
				total_size = initial_reader->file_handle ? initial_reader->file_handle->FileSize() : 0;
				return std::move(initial_reader);
			}
			if (next_file >= total_files) {
				return nullptr;
			}
			options = bind_data.options;
			file_index = next_file;
			next_file++;
		}
		// reuse csv_readers was created during binding
		unique_ptr<BufferedCSVReader> result;
		if (file_index < bind_data.union_readers.size() && bind_data.union_readers[file_index]) {
			result = std::move(bind_data.union_readers[file_index]);
		} else {
			auto union_by_name = options.file_options.union_by_name;
			options.file_path = bind_data.files[file_index];
			result = make_uniq<BufferedCSVReader>(context, std::move(options), csv_types);
			if (!union_by_name) {
				result->names = csv_names;
			}
			MultiFileReader::InitializeReader(*result, bind_data.options.file_options, bind_data.reader_bind,
			                                  bind_data.return_types, bind_data.return_names, column_ids, nullptr,
			                                  bind_data.files.front());
		}
		total_size = result->file_handle->FileSize();
		return result;
	}
};

struct SingleThreadedCSVLocalState : public LocalTableFunctionState {
public:
	explicit SingleThreadedCSVLocalState() : bytes_read(0), total_size(0), current_progress(0), file_index(0) {
	}

	//! The CSV reader
	unique_ptr<BufferedCSVReader> csv_reader;
	//! The current amount of bytes read by this reader
	idx_t bytes_read;
	//! The total amount of bytes in the file
	idx_t total_size;
	//! The current progress from 0..100
	idx_t current_progress;
	//! The file index of this reader
	idx_t file_index;
};

static unique_ptr<GlobalTableFunctionState> SingleThreadedCSVInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<ReadCSVData>();
	auto result = make_uniq<SingleThreadedCSVState>(bind_data.files.size());
	if (bind_data.files.empty()) {
		// This can happen when a filename based filter pushdown has eliminated all possible files for this scan.
		return std::move(result);
	} else {
		bind_data.options.file_path = bind_data.files[0];
		if (bind_data.initial_reader) {
			// If this is a pipe and an initial reader already exists due to read_csv_auto
			// We must re-use it, since we can't restart the reader due for it being a pipe.
			result->initial_reader = std::move(bind_data.initial_reader);
		} else {
			result->initial_reader = make_uniq<BufferedCSVReader>(context, bind_data.options, bind_data.csv_types);
		}
		if (!bind_data.options.file_options.union_by_name) {
			result->initial_reader->names = bind_data.csv_names;
		}
		if (bind_data.options.auto_detect) {
			bind_data.options = result->initial_reader->options;
		}
	}
	MultiFileReader::InitializeReader(*result->initial_reader, bind_data.options.file_options, bind_data.reader_bind,
	                                  bind_data.return_types, bind_data.return_names, input.column_ids, input.filters,
	                                  bind_data.files.front());
	for (auto &reader : bind_data.union_readers) {
		if (!reader) {
			continue;
		}
		MultiFileReader::InitializeReader(*reader, bind_data.options.file_options, bind_data.reader_bind,
		                                  bind_data.return_types, bind_data.return_names, input.column_ids,
		                                  input.filters, bind_data.files.front());
	}
	result->column_ids = input.column_ids;

	if (!bind_data.options.file_options.union_by_name) {
		// if we are reading multiple files - run auto-detect only on the first file
		// UNLESS union by name is turned on - in that case we assume that different files have different schemas
		// as such, we need to re-run the auto detection on each file
		bind_data.options.auto_detect = false;
	}
	result->csv_types = bind_data.csv_types;
	result->csv_names = bind_data.csv_names;
	result->next_file = 1;
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> SingleThreadedReadCSVInitLocal(ExecutionContext &context,
                                                                   TableFunctionInitInput &input,
                                                                   GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->CastNoConst<ReadCSVData>();
	auto &data = global_state_p->Cast<SingleThreadedCSVState>();
	auto result = make_uniq<SingleThreadedCSVLocalState>();
	result->csv_reader = data.GetCSVReader(context.client, bind_data, result->file_index, result->total_size);
	return std::move(result);
}

static void SingleThreadedCSVFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<ReadCSVData>();
	auto &data = data_p.global_state->Cast<SingleThreadedCSVState>();
	auto &lstate = data_p.local_state->Cast<SingleThreadedCSVLocalState>();
	if (!lstate.csv_reader) {
		// no csv_reader was set, this can happen when a filename-based filter has filtered out all possible files
		return;
	}

	do {
		lstate.csv_reader->ParseCSV(output);
		// update the number of bytes read
		D_ASSERT(lstate.bytes_read <= lstate.csv_reader->bytes_in_chunk);
		auto bytes_read = MinValue<idx_t>(lstate.total_size, lstate.csv_reader->bytes_in_chunk);
		auto current_progress = lstate.total_size == 0 ? 100 : 100 * bytes_read / lstate.total_size;
		if (current_progress > lstate.current_progress) {
			if (current_progress > 100) {
				throw InternalException("Progress should never exceed 100");
			}
			data.progress_in_files += current_progress - lstate.current_progress;
			lstate.current_progress = current_progress;
		}
		if (output.size() == 0) {
			// exhausted this file, but we might have more files we can read
			auto csv_reader = data.GetCSVReader(context, bind_data, lstate.file_index, lstate.total_size);
			// add any left-over progress for this file to the progress bar
			if (lstate.current_progress < 100) {
				data.progress_in_files += 100 - lstate.current_progress;
			}
			// reset the current progress
			lstate.current_progress = 0;
			lstate.bytes_read = 0;
			lstate.csv_reader = std::move(csv_reader);
			if (!lstate.csv_reader) {
				// no more files - we are done
				return;
			}
			lstate.bytes_read = 0;
		} else {
			MultiFileReader::FinalizeChunk(bind_data.reader_bind, lstate.csv_reader->reader_data, output);
			break;
		}
	} while (true);
}

//===--------------------------------------------------------------------===//
// Read CSV Functions
//===--------------------------------------------------------------------===//
static unique_ptr<GlobalTableFunctionState> ReadCSVInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadCSVData>();
	if (bind_data.single_threaded) {
		return SingleThreadedCSVInit(context, input);
	} else {
		return ParallelCSVInitGlobal(context, input);
	}
}

unique_ptr<LocalTableFunctionState> ReadCSVInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state_p) {
	auto &csv_data = input.bind_data->Cast<ReadCSVData>();
	if (csv_data.single_threaded) {
		return SingleThreadedReadCSVInitLocal(context, input, global_state_p);
	} else {
		return ParallelReadCSVInitLocal(context, input, global_state_p);
	}
}

static void ReadCSVFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ReadCSVData>();
	if (bind_data.single_threaded) {
		SingleThreadedCSVFunction(context, data_p, output);
	} else {
		ParallelReadCSVFunction(context, data_p, output);
	}
}

static idx_t CSVReaderGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                    LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &bind_data = bind_data_p->Cast<ReadCSVData>();
	if (bind_data.single_threaded) {
		auto &data = local_state->Cast<SingleThreadedCSVLocalState>();
		return data.file_index;
	}
	auto &data = local_state->Cast<ParallelCSVLocalState>();
	return data.csv_reader->buffer->batch_index;
}

static void ReadCSVAddNamedParameters(TableFunction &table_function) {
	table_function.named_parameters["sep"] = LogicalType::VARCHAR;
	table_function.named_parameters["delim"] = LogicalType::VARCHAR;
	table_function.named_parameters["quote"] = LogicalType::VARCHAR;
	table_function.named_parameters["new_line"] = LogicalType::VARCHAR;
	table_function.named_parameters["escape"] = LogicalType::VARCHAR;
	table_function.named_parameters["nullstr"] = LogicalType::VARCHAR;
	table_function.named_parameters["columns"] = LogicalType::ANY;
	table_function.named_parameters["auto_type_candidates"] = LogicalType::ANY;
	table_function.named_parameters["header"] = LogicalType::BOOLEAN;
	table_function.named_parameters["auto_detect"] = LogicalType::BOOLEAN;
	table_function.named_parameters["sample_size"] = LogicalType::BIGINT;
	table_function.named_parameters["sample_chunk_size"] = LogicalType::BIGINT;
	table_function.named_parameters["sample_chunks"] = LogicalType::BIGINT;
	table_function.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
	table_function.named_parameters["dateformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["timestampformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["normalize_names"] = LogicalType::BOOLEAN;
	table_function.named_parameters["compression"] = LogicalType::VARCHAR;
	table_function.named_parameters["skip"] = LogicalType::BIGINT;
	table_function.named_parameters["max_line_size"] = LogicalType::VARCHAR;
	table_function.named_parameters["maximum_line_size"] = LogicalType::VARCHAR;
	table_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	table_function.named_parameters["buffer_size"] = LogicalType::UBIGINT;
	table_function.named_parameters["decimal_separator"] = LogicalType::VARCHAR;
	table_function.named_parameters["parallel"] = LogicalType::BOOLEAN;
	table_function.named_parameters["null_padding"] = LogicalType::BOOLEAN;
	table_function.named_parameters["allow_quoted_nulls"] = LogicalType::BOOLEAN;
	table_function.named_parameters["column_types"] = LogicalType::ANY;
	table_function.named_parameters["dtypes"] = LogicalType::ANY;
	table_function.named_parameters["types"] = LogicalType::ANY;
	table_function.named_parameters["names"] = LogicalType::LIST(LogicalType::VARCHAR);
	table_function.named_parameters["column_names"] = LogicalType::LIST(LogicalType::VARCHAR);
	MultiFileReader::AddParameters(table_function);
}

double CSVReaderProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *global_state) {
	auto &bind_data = bind_data_p->Cast<ReadCSVData>();
	if (bind_data.single_threaded) {
		auto &data = global_state->Cast<SingleThreadedCSVState>();
		return data.GetProgress(bind_data);
	} else {
		auto &data = global_state->Cast<ParallelCSVGlobalState>();
		return data.GetProgress(bind_data);
	}
}

void CSVComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                              vector<unique_ptr<Expression>> &filters) {
	auto &data = bind_data_p->Cast<ReadCSVData>();
	auto reset_reader =
	    MultiFileReader::ComplexFilterPushdown(context, data.files, data.options.file_options, get, filters);
	if (reset_reader) {
		MultiFileReader::PruneReaders(data);
	}
}

unique_ptr<NodeStatistics> CSVReaderCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ReadCSVData>();
	idx_t per_file_cardinality = 0;
	if (bind_data.initial_reader && bind_data.initial_reader->file_handle) {
		auto estimated_row_width = (bind_data.csv_types.size() * 5);
		per_file_cardinality = bind_data.initial_reader->file_handle->FileSize() / estimated_row_width;
	} else {
		// determined through the scientific method as the average amount of rows in a CSV file
		per_file_cardinality = 42;
	}
	return make_uniq<NodeStatistics>(bind_data.files.size() * per_file_cardinality);
}

void BufferedCSVReaderOptions::Serialize(FieldWriter &writer) const {
	// common options
	writer.WriteField<bool>(has_delimiter);
	writer.WriteString(delimiter);
	writer.WriteField<bool>(has_quote);
	writer.WriteString(quote);
	writer.WriteField<bool>(has_escape);
	writer.WriteString(escape);
	writer.WriteField<bool>(has_header);
	writer.WriteField<bool>(header);
	writer.WriteField<bool>(ignore_errors);
	writer.WriteField<idx_t>(num_cols);
	writer.WriteField<idx_t>(buffer_sample_size);
	writer.WriteString(null_str);
	writer.WriteField<FileCompressionType>(compression);
	writer.WriteField<NewLineIdentifier>(new_line);
	writer.WriteField<bool>(allow_quoted_nulls);
	// read options
	writer.WriteField<idx_t>(skip_rows);
	writer.WriteField<bool>(skip_rows_set);
	writer.WriteField<idx_t>(maximum_line_size);
	writer.WriteField<bool>(normalize_names);
	writer.WriteListNoReference<bool>(force_not_null);
	writer.WriteField<bool>(all_varchar);
	writer.WriteField<idx_t>(sample_chunk_size);
	writer.WriteField<idx_t>(sample_chunks);
	writer.WriteField<bool>(auto_detect);
	writer.WriteString(file_path);
	writer.WriteString(decimal_separator);
	writer.WriteField<bool>(null_padding);
	writer.WriteField<idx_t>(buffer_size);
	writer.WriteSerializable(file_options);
	// write options
	writer.WriteListNoReference<bool>(force_quote);
	// FIXME: serialize date_format / has_format
	vector<string> csv_formats;
	for (auto &format : date_format) {
		csv_formats.push_back(format.second.format_specifier);
	}
	writer.WriteList<string>(csv_formats);
}

void BufferedCSVReaderOptions::Deserialize(FieldReader &reader) {
	// common options
	has_delimiter = reader.ReadRequired<bool>();
	delimiter = reader.ReadRequired<string>();
	has_quote = reader.ReadRequired<bool>();
	quote = reader.ReadRequired<string>();
	has_escape = reader.ReadRequired<bool>();
	escape = reader.ReadRequired<string>();
	has_header = reader.ReadRequired<bool>();
	header = reader.ReadRequired<bool>();
	ignore_errors = reader.ReadRequired<bool>();
	num_cols = reader.ReadRequired<idx_t>();
	buffer_sample_size = reader.ReadRequired<idx_t>();
	null_str = reader.ReadRequired<string>();
	compression = reader.ReadRequired<FileCompressionType>();
	new_line = reader.ReadRequired<NewLineIdentifier>();
	allow_quoted_nulls = reader.ReadRequired<bool>();
	// read options
	skip_rows = reader.ReadRequired<idx_t>();
	skip_rows_set = reader.ReadRequired<bool>();
	maximum_line_size = reader.ReadRequired<idx_t>();
	normalize_names = reader.ReadRequired<bool>();
	force_not_null = reader.ReadRequiredList<bool>();
	all_varchar = reader.ReadRequired<bool>();
	sample_chunk_size = reader.ReadRequired<idx_t>();
	sample_chunks = reader.ReadRequired<idx_t>();
	auto_detect = reader.ReadRequired<bool>();
	file_path = reader.ReadRequired<string>();
	decimal_separator = reader.ReadRequired<string>();
	null_padding = reader.ReadRequired<bool>();
	buffer_size = reader.ReadRequired<idx_t>();
	file_options = reader.ReadRequiredSerializable<MultiFileReaderOptions, MultiFileReaderOptions>();
	// write options
	force_quote = reader.ReadRequiredList<bool>();
	auto formats = reader.ReadRequiredList<string>();
	vector<LogicalTypeId> format_types {LogicalTypeId::DATE, LogicalTypeId::TIMESTAMP};
	for (idx_t f_idx = 0; f_idx < formats.size(); f_idx++) {
		auto &format = formats[f_idx];
		auto &type = format_types[f_idx];
		if (format.empty()) {
			continue;
		}
		has_format[type] = true;
		StrTimeFormat::ParseFormatSpecifier(format, date_format[type]);
	}
}

static void CSVReaderSerialize(FieldWriter &writer, const FunctionData *bind_data_p, const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<ReadCSVData>();
	writer.WriteString(function.extra_info);
	writer.WriteList<string>(bind_data.files);
	writer.WriteRegularSerializableList<LogicalType>(bind_data.csv_types);
	writer.WriteList<string>(bind_data.csv_names);
	writer.WriteRegularSerializableList<LogicalType>(bind_data.return_types);
	writer.WriteList<string>(bind_data.return_names);
	writer.WriteField<idx_t>(bind_data.filename_col_idx);
	writer.WriteField<idx_t>(bind_data.hive_partition_col_idx);
	bind_data.options.Serialize(writer);
	writer.WriteField<bool>(bind_data.single_threaded);
	writer.WriteSerializable(bind_data.reader_bind);
	writer.WriteField<uint32_t>(bind_data.column_info.size());
	for (auto &col : bind_data.column_info) {
		col.Serialize(writer);
	}
}

static unique_ptr<FunctionData> CSVReaderDeserialize(PlanDeserializationState &state, FieldReader &reader,
                                                     TableFunction &function) {
	function.extra_info = reader.ReadRequired<string>();
	auto result_data = make_uniq<ReadCSVData>();
	result_data->files = reader.ReadRequiredList<string>();
	result_data->csv_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	result_data->csv_names = reader.ReadRequiredList<string>();
	result_data->return_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	result_data->return_names = reader.ReadRequiredList<string>();
	result_data->filename_col_idx = reader.ReadRequired<idx_t>();
	result_data->hive_partition_col_idx = reader.ReadRequired<idx_t>();
	result_data->options.Deserialize(reader);
	result_data->single_threaded = reader.ReadField<bool>(true);
	result_data->reader_bind = reader.ReadRequiredSerializable<MultiFileReaderBindData, MultiFileReaderBindData>();
	uint32_t file_number = reader.ReadRequired<uint32_t>();
	for (idx_t i = 0; i < file_number; i++) {
		result_data->column_info.emplace_back(ColumnInfo::Deserialize(reader));
	}
	return std::move(result_data);
}

TableFunction ReadCSVTableFunction::GetFunction() {
	TableFunction read_csv("read_csv", {LogicalType::VARCHAR}, ReadCSVFunction, ReadCSVBind, ReadCSVInitGlobal,
	                       ReadCSVInitLocal);
	read_csv.table_scan_progress = CSVReaderProgress;
	read_csv.pushdown_complex_filter = CSVComplexFilterPushdown;
	read_csv.serialize = CSVReaderSerialize;
	read_csv.deserialize = CSVReaderDeserialize;
	read_csv.get_batch_index = CSVReaderGetBatchIndex;
	read_csv.cardinality = CSVReaderCardinality;
	read_csv.projection_pushdown = true;
	ReadCSVAddNamedParameters(read_csv);
	return read_csv;
}

TableFunction ReadCSVTableFunction::GetAutoFunction() {
	auto read_csv_auto = ReadCSVTableFunction::GetFunction();
	read_csv_auto.name = "read_csv_auto";
	read_csv_auto.bind = ReadCSVAutoBind;
	return read_csv_auto;
}

void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(MultiFileReader::CreateFunctionSet(ReadCSVTableFunction::GetFunction()));
	set.AddFunction(MultiFileReader::CreateFunctionSet(ReadCSVTableFunction::GetAutoFunction()));
}

unique_ptr<TableRef> ReadCSVReplacement(ClientContext &context, const string &table_name, ReplacementScanData *data) {
	auto lower_name = StringUtil::Lower(table_name);
	// remove any compression
	if (StringUtil::EndsWith(lower_name, ".gz")) {
		lower_name = lower_name.substr(0, lower_name.size() - 3);
	} else if (StringUtil::EndsWith(lower_name, ".zst")) {
		lower_name = lower_name.substr(0, lower_name.size() - 4);
	}
	if (!StringUtil::EndsWith(lower_name, ".csv") && !StringUtil::Contains(lower_name, ".csv?") &&
	    !StringUtil::EndsWith(lower_name, ".tsv") && !StringUtil::Contains(lower_name, ".tsv?")) {
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("read_csv_auto", std::move(children));

	if (!FileSystem::HasGlob(table_name)) {
		table_function->alias = FileSystem::ExtractBaseName(table_name);
	}

	return std::move(table_function);
}

void BuiltinFunctions::RegisterReadFunctions() {
	CSVCopyFunction::RegisterFunction(*this);
	ReadCSVTableFunction::RegisterFunction(*this);
	auto &config = DBConfig::GetConfig(*transaction.db);
	config.replacement_scans.emplace_back(ReadCSVReplacement);
}

} // namespace duckdb
