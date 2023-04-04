#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/union_by_name.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/extension_helper.hpp"

#include <limits>

namespace duckdb {

unique_ptr<CSVFileHandle> ReadCSV::OpenCSV(const string &file_path, FileCompressionType compression,
                                           ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto opener = FileSystem::GetFileOpener(context);
	auto file_handle =
	    fs.OpenFile(file_path.c_str(), FileFlags::FILE_FLAGS_READ, FileLockType::NO_LOCK, compression, opener);
	return make_unique<CSVFileHandle>(std::move(file_handle));
}

void ReadCSVData::InitializeFiles(ClientContext &context, const vector<string> &patterns) {
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &file_pattern : patterns) {
		auto found_files = fs.GlobFiles(file_pattern, context);
		files.insert(files.end(), found_files.begin(), found_files.end());
	}
	if (files.empty()) {
		throw IOException("CSV reader needs at least one file to read");
	}
}

void ReadCSVData::FinalizeRead(ClientContext &context) {
	BaseCSVData::Finalize();
	auto &config = DBConfig::GetConfig(context);
	single_threaded = !config.options.experimental_parallel_csv_reader;
	if (options.has_parallel) {
		// Override the option set in the config
		single_threaded = !options.use_parallel;
	}
	bool null_or_empty = options.delimiter.empty() || options.escape.empty() || options.quote.empty() ||
	                     options.delimiter[0] == '\0' || options.escape[0] == '\0' || options.quote[0] == '\0';
	bool complex_options = options.delimiter.size() > 1 || options.escape.size() > 1 || options.quote.size() > 1;
	if (null_or_empty || complex_options || options.new_line == NewLineIdentifier::MIX) {
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
		                      LogicalTypeIdToString(candidate_type.id()));
	}
	return it->second;
}

static unique_ptr<FunctionData> ReadCSVBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning CSV files is disabled through configuration");
	}

	auto result = make_unique<ReadCSVData>();
	auto &options = result->options;

	vector<string> patterns;
	if (input.inputs[0].IsNull()) {
		throw ParserException("CSV reader cannot take NULL as parameter");
	}
	if (input.inputs[0].type().id() == LogicalTypeId::LIST) {
		// list of globs
		for (auto &val : ListValue::GetChildren(input.inputs[0])) {
			if (val.IsNull()) {
				throw ParserException("CSV reader cannot take NULL input as parameter");
			}
			patterns.push_back(StringValue::Get(val));
		}
	} else {
		// single glob pattern
		patterns.push_back(StringValue::Get(input.inputs[0]));
	}

	result->InitializeFiles(context, patterns);

	bool explicitly_set_columns = false;
	for (auto &kv : input.named_parameters) {
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
		} else if (loption == "filename") {
			options.include_file_name = BooleanValue::Get(kv.second);
		} else if (loption == "hive_partitioning") {
			options.include_parsed_hive_partitions = BooleanValue::Get(kv.second);
		} else {
			options.SetReadOption(loption, kv.second, names);
		}
	}
	if (!options.auto_detect && return_types.empty()) {
		throw BinderException("read_csv requires columns to be specified through the 'columns' option. Use "
		                      "read_csv_auto or set read_csv(..., "
		                      "AUTO_DETECT=TRUE) to automatically guess columns.");
	}
	if (options.auto_detect) {
		options.file_path = result->files[0];
		auto initial_reader = make_unique<BufferedCSVReader>(context, options);
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
		}
		options = initial_reader->options;
		result->sql_types = initial_reader->return_types;
		result->initial_reader = std::move(initial_reader);
	} else {
		result->sql_types = return_types;
		D_ASSERT(return_types.size() == names.size());
	}

	// union_col_names will exclude filename and hivepartition
	if (options.union_by_name) {
		case_insensitive_map_t<idx_t> union_names_map;
		vector<string> union_col_names;
		vector<LogicalType> union_col_types;

		auto dummy_readers = UnionByName<BufferedCSVReader, BufferedCSVReaderOptions>::UnionCols(
		    context, result->files, union_col_types, union_col_names, union_names_map, options);

		dummy_readers = UnionByName<BufferedCSVReader, BufferedCSVReaderOptions>::CreateUnionMap(
		    std::move(dummy_readers), union_col_types, union_col_names, union_names_map);

		std::move(dummy_readers.begin(), dummy_readers.end(), std::back_inserter(result->union_readers));
		for (auto &reader : result->union_readers) {
			reader->insert_cols_idx = reader->union_idx_map;
		}

		names.assign(union_col_names.begin(), union_col_names.end());
		return_types.assign(union_col_types.begin(), union_col_types.end());
		const idx_t first_file_index = 0;
		result->initial_reader = std::move(result->union_readers[first_file_index]);
		D_ASSERT(names.size() == return_types.size());

		if (!options.sql_types_per_column.empty()) {
			auto exception = BufferedCSVReader::ColumnTypesError(options.sql_types_per_column, names);
			if (!exception.empty()) {
				throw BinderException(exception);
			}
		}
	}

	if (result->options.include_file_name) {
		result->filename_col_idx = names.size();
		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back("filename");
	}

	if (result->options.include_parsed_hive_partitions) {
		auto partitions = HivePartitioning::Parse(result->files[0]);
		result->hive_partition_col_idx = names.size();
		for (auto &part : partitions) {
			return_types.emplace_back(LogicalType::VARCHAR);
			names.emplace_back(part.first);
		}
	}
	result->options.names = names;
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
//===--------------------------------------------------------------------===//
// Read CSV Global State
//===--------------------------------------------------------------------===//
struct ParallelCSVGlobalState : public GlobalTableFunctionState {
public:
	ParallelCSVGlobalState(ClientContext &context, unique_ptr<CSVFileHandle> file_handle_p,
	                       vector<string> &files_path_p, idx_t system_threads_p, idx_t buffer_size_p,
	                       idx_t rows_to_skip, bool force_parallelism_p)
	    : file_handle(std::move(file_handle_p)), system_threads(system_threads_p), buffer_size(buffer_size_p),
	      force_parallelism(force_parallelism_p) {
		current_file_path = files_path_p[0];
		estimated_linenr = rows_to_skip;
		file_size = file_handle->FileSize();
		first_file_size = file_size;
		bytes_read = 0;
		if (buffer_size < file_size) {
			bytes_per_local_state = buffer_size / ParallelCSVGlobalState::MaxThreads();
		} else {
			bytes_per_local_state = file_size / MaxThreads();
		}
		current_buffer = make_shared<CSVBuffer>(context, buffer_size, *file_handle, current_csv_position);
		next_buffer = current_buffer->Next(*file_handle, buffer_size, current_csv_position);
		running_threads = MaxThreads();
	}
	ParallelCSVGlobalState() {
	}

	~ParallelCSVGlobalState() override {
	}

	idx_t MaxThreads() const override;
	//! Returns buffer and index that caller thread should read.
	unique_ptr<CSVBufferRead> Next(ClientContext &context, ReadCSVData &bind_data);
	//! Verify if the CSV File was read correctly
	void Verify();

	void UpdateVerification(VerificationPositions positions);

	void IncrementThread();

	void DecrementThread();

	bool Finished();

	//! How many bytes were read up to this point
	atomic<idx_t> bytes_read;
	//! Size of current file
	idx_t file_size;

	double GetProgress(ReadCSVData &bind_data) const {
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
	unique_ptr<CSVFileHandle> prev_file_handle;
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
	//! The current estimated line number
	idx_t estimated_linenr;
	//! How many bytes we should execute per local state
	idx_t bytes_per_local_state;
	//! Size of first file
	idx_t first_file_size;
	//! Basically max number of threads in DuckDB
	idx_t system_threads;
	//! Size of the buffers
	idx_t buffer_size;
	//! Current batch index
	idx_t batch_index = 0;
	//! Forces parallelism for small CSV Files, should only be used for testing.
	bool force_parallelism;
	//! Current (Global) position of CSV
	idx_t current_csv_position = 0;
	idx_t max_tuple_end = 0;
	//! the vector stores positions where threads ended the last line they read in the CSV File, and the set stores
	//! positions where they started reading the first line.
	vector<idx_t> tuple_end;
	set<idx_t> tuple_start;
	idx_t running_threads = 0;
};

idx_t ParallelCSVGlobalState::MaxThreads() const {
	if (force_parallelism) {
		return system_threads;
	}

	idx_t one_mb = 1000000; // We initialize max one thread per Mb
	idx_t threads_per_mb = first_file_size / one_mb + 1;
	if (threads_per_mb < system_threads) {
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
	if (running_threads == 0) {
		for (auto &last_pos : tuple_end) {
			auto first_pos = tuple_start.find(last_pos);
			if (first_pos == tuple_start.end()) {
				// this might be necessary due to carriage returns outside buffer scopes.
				first_pos = tuple_start.find(last_pos + 1);
			}
			if (first_pos == tuple_start.end() && last_pos != NumericLimits<uint64_t>::Maximum()) {
				string error = "Not possible to read this CSV File with multithreading. Tuple: " + to_string(last_pos) +
				               " does not have a match\n";
				error += "End Lines: \n";
				for (auto &end_line : tuple_end) {
					error += to_string(end_line) + "\n";
				}
				error += "Start Lines: \n";
				for (auto &start_line : tuple_start) {
					error += to_string(start_line) + "\n";
				}
				throw InvalidInputException(
				    "CSV File not supported for multithreading. Please run single-threaded CSV Reading");
			}
		}
	}
}

unique_ptr<CSVBufferRead> ParallelCSVGlobalState::Next(ClientContext &context, ReadCSVData &bind_data) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (!current_buffer) {
		// This means we are done with the current file, we need to go to the next one (if exists).
		if (file_index < bind_data.files.size()) {
			current_file_path = bind_data.files[file_index++];
			file_handle = ReadCSV::OpenCSV(current_file_path, bind_data.options.compression, context);
			current_csv_position = 0;
			current_buffer = make_shared<CSVBuffer>(context, buffer_size, *file_handle, current_csv_position);
			next_buffer = current_buffer->Next(*file_handle, buffer_size, current_csv_position);
		} else {
			// We are done scanning.
			return nullptr;
		}
	}
	// set up the current buffer
	auto result = make_unique<CSVBufferRead>(current_buffer, next_buffer, next_byte, next_byte + bytes_per_local_state,
	                                         batch_index++, estimated_linenr);
	// move the byte index of the CSV reader to the next buffer
	next_byte += bytes_per_local_state;
	estimated_linenr += bytes_per_local_state / (bind_data.sql_types.size() * 5); // estimate 5 bytes per column
	if (next_byte >= current_buffer->GetBufferSize()) {
		// We replace the current buffer with the next buffer
		next_byte = 0;
		bytes_read += current_buffer->GetBufferSize();
		current_buffer = next_buffer;
		if (next_buffer) {
			// Next buffer gets the next-next buffer
			next_buffer = next_buffer->Next(*file_handle, buffer_size, current_csv_position);
		}
	}
	return result;
}
void ParallelCSVGlobalState::UpdateVerification(VerificationPositions positions) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (positions.beginning_of_first_line < positions.end_of_last_line) {
		if (positions.end_of_last_line > max_tuple_end) {
			max_tuple_end = positions.end_of_last_line;
		}
		tuple_start.insert(positions.beginning_of_first_line);
		tuple_end.push_back(positions.end_of_last_line);
	}
}

static unique_ptr<GlobalTableFunctionState> ParallelCSVInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &bind_data = (ReadCSVData &)*input.bind_data;
	if (bind_data.files.empty()) {
		// This can happen when a filename based filter pushdown has eliminated all possible files for this scan.
		return make_unique<ParallelCSVGlobalState>();
	}
	unique_ptr<CSVFileHandle> file_handle;

	bind_data.options.file_path = bind_data.files[0];
	file_handle = ReadCSV::OpenCSV(bind_data.options.file_path, bind_data.options.compression, context);
	idx_t rows_to_skip =
	    bind_data.options.skip_rows + (bind_data.options.has_header && bind_data.options.header ? 1 : 0);
	return make_unique<ParallelCSVGlobalState>(context, std::move(file_handle), bind_data.files,
	                                           context.db->NumberOfThreads(), bind_data.options.buffer_size,
	                                           rows_to_skip, ClientConfig::GetConfig(context).verify_parallelism);
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
	auto &csv_data = (ReadCSVData &)*input.bind_data;
	auto &global_state = global_state_p->Cast<ParallelCSVGlobalState>();
	auto next_local_buffer = global_state.Next(context.client, csv_data);
	unique_ptr<ParallelCSVReader> csv_reader;
	if (next_local_buffer) {
		csv_reader = make_unique<ParallelCSVReader>(context.client, csv_data.options, std::move(next_local_buffer),
		                                            csv_data.sql_types);
	} else {
		global_state.DecrementThread();
	}
	return make_unique<ParallelCSVLocalState>(std::move(csv_reader));
}

static void ParallelReadCSVFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (ReadCSVData &)*data_p.bind_data;
	auto &csv_global_state = data_p.global_state->Cast<ParallelCSVGlobalState>();
	auto &csv_local_state = data_p.local_state->Cast<ParallelCSVLocalState>();

	if (!csv_local_state.csv_reader) {
		// no csv_reader was set, this can happen when a filename-based filter has filtered out all possible files
		return;
	}

	do {
		if (output.size() != 0) {
			break;
		}
		if (csv_local_state.csv_reader->finished) {
			auto verification_updates = csv_local_state.csv_reader->GetVerificationPositions();
			if (!csv_local_state.csv_reader->buffer->next_buffer) {
				// if it's the last line of the file we mark as the maximum
				verification_updates.end_of_last_line = NumericLimits<uint64_t>::Maximum();
			}
			csv_global_state.UpdateVerification(verification_updates);
			auto next_chunk = csv_global_state.Next(context, bind_data);
			if (!next_chunk) {
				csv_global_state.DecrementThread();
				break;
			}
			csv_local_state.csv_reader->SetBufferRead(std::move(next_chunk));
		}
		csv_local_state.csv_reader->ParseCSV(output);

	} while (true);
	if (csv_global_state.Finished()) {
		csv_global_state.Verify();
	}
	if (bind_data.options.union_by_name) {
		throw InternalException("FIXME: union by name");
	}
	if (bind_data.options.include_file_name) {
		throw InternalException("FIXME: output file name");
	}
	if (bind_data.options.include_parsed_hive_partitions) {
		throw InternalException("FIXME: hive partitions");
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
	vector<LogicalType> sql_types;

	idx_t MaxThreads() const override {
		return total_files;
	}

	double GetProgress(ReadCSVData &bind_data) const {
		D_ASSERT(total_files == bind_data.files.size());
		D_ASSERT(progress_in_files <= total_files * 100);
		return (double(progress_in_files) / double(total_files));
	}

	unique_ptr<BufferedCSVReader> GetCSVReader(ClientContext &context, ReadCSVData &bind_data, idx_t &file_index,
	                                           idx_t &total_size) {
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
		if (options.union_by_name) {
			result = std::move(bind_data.union_readers[file_index]);
		} else {
			options.file_path = bind_data.files[file_index];
			result = make_unique<BufferedCSVReader>(context, std::move(options), sql_types);
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
	auto &bind_data = (ReadCSVData &)*input.bind_data;
	auto result = make_unique<SingleThreadedCSVState>(bind_data.files.size());
	if (bind_data.initial_reader) {
		result->initial_reader = std::move(bind_data.initial_reader);
	} else if (bind_data.files.empty()) {
		// This can happen when a filename based filter pushdown has eliminated all possible files for this scan.
		return std::move(result);
	} else {
		bind_data.options.file_path = bind_data.files[0];
		result->initial_reader = make_unique<BufferedCSVReader>(context, bind_data.options, bind_data.sql_types);
		if (bind_data.options.auto_detect) {
			bind_data.options = result->initial_reader->options;
		}
	}
	if (!bind_data.options.union_by_name) {
		// if we are reading multiple files - run auto-detect only on the first file
		// UNLESS union by name is turned on - in that case we assume that different files have different schemas
		// as such, we need to re-run the auto detection on each file
		bind_data.options.auto_detect = false;
	}
	result->next_file = 1;
	if (result->initial_reader) {
		result->sql_types = result->initial_reader->return_types;
	}
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> SingleThreadedReadCSVInitLocal(ExecutionContext &context,
                                                                   TableFunctionInitInput &input,
                                                                   GlobalTableFunctionState *global_state_p) {
	auto &bind_data = (ReadCSVData &)*input.bind_data;
	auto &data = (SingleThreadedCSVState &)*global_state_p;
	auto result = make_unique<SingleThreadedCSVLocalState>();
	result->csv_reader = data.GetCSVReader(context.client, bind_data, result->file_index, result->total_size);
	return std::move(result);
}

static void SingleThreadedCSVFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (ReadCSVData &)*data_p.bind_data;
	auto &data = (SingleThreadedCSVState &)*data_p.global_state;
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
			break;
		}
	} while (true);

	if (bind_data.options.union_by_name) {
		UnionByName<BufferedCSVReader, BufferedCSVReaderOptions>::SetNullUnionCols(output,
		                                                                           lstate.csv_reader->union_null_cols);
	}
	if (bind_data.options.include_file_name) {
		auto &col = output.data[bind_data.filename_col_idx];
		col.SetValue(0, Value(lstate.csv_reader->options.file_path));
		col.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	if (bind_data.options.include_parsed_hive_partitions) {
		auto partitions = HivePartitioning::Parse(lstate.csv_reader->options.file_path);

		idx_t i = bind_data.hive_partition_col_idx;

		if (partitions.size() != (bind_data.options.names.size() - bind_data.hive_partition_col_idx)) {
			throw IOException("Hive partition count mismatch, expected " +
			                  std::to_string(bind_data.options.names.size() - bind_data.hive_partition_col_idx) +
			                  " hive partitions, got " + std::to_string(partitions.size()) + "\n");
		}

		for (auto &part : partitions) {
			if (bind_data.options.names[i] != part.first) {
				throw IOException("Hive partition names mismatch, expected '" + bind_data.options.names[i] +
				                  "' but found '" + part.first + "' for file '" + lstate.csv_reader->options.file_path +
				                  "'");
			}
			auto &col = output.data[i++];
			col.SetValue(0, Value(part.second));
			col.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}
}

//===--------------------------------------------------------------------===//
// Read CSV Functions
//===--------------------------------------------------------------------===//
static unique_ptr<GlobalTableFunctionState> ReadCSVInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (ReadCSVData &)*input.bind_data;
	if (bind_data.single_threaded) {
		return SingleThreadedCSVInit(context, input);
	} else {
		return ParallelCSVInitGlobal(context, input);
	}
}

unique_ptr<LocalTableFunctionState> ReadCSVInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state_p) {
	auto &csv_data = (ReadCSVData &)*input.bind_data;
	if (csv_data.single_threaded) {
		return SingleThreadedReadCSVInitLocal(context, input, global_state_p);
	} else {
		return ParallelReadCSVInitLocal(context, input, global_state_p);
	}
}

static void ReadCSVFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (ReadCSVData &)*data_p.bind_data;
	if (bind_data.single_threaded) {
		SingleThreadedCSVFunction(context, data_p, output);
	} else {
		ParallelReadCSVFunction(context, data_p, output);
	}
}

static idx_t CSVReaderGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                    LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &bind_data = (ReadCSVData &)*bind_data_p;
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
	table_function.named_parameters["filename"] = LogicalType::BOOLEAN;
	table_function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
	table_function.named_parameters["skip"] = LogicalType::BIGINT;
	table_function.named_parameters["max_line_size"] = LogicalType::VARCHAR;
	table_function.named_parameters["maximum_line_size"] = LogicalType::VARCHAR;
	table_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	table_function.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
	table_function.named_parameters["buffer_size"] = LogicalType::UBIGINT;
	table_function.named_parameters["decimal_separator"] = LogicalType::VARCHAR;
	table_function.named_parameters["parallel"] = LogicalType::BOOLEAN;
	table_function.named_parameters["null_padding"] = LogicalType::BOOLEAN;
}

double CSVReaderProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *global_state) {
	auto &bind_data = (ReadCSVData &)*bind_data_p;
	if (bind_data.single_threaded) {
		auto &data = (SingleThreadedCSVState &)*global_state;
		return data.GetProgress(bind_data);
	} else {
		auto &data = (const ParallelCSVGlobalState &)*global_state;
		return data.GetProgress(bind_data);
	}
}

void CSVComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                              vector<unique_ptr<Expression>> &filters) {
	auto data = (ReadCSVData *)bind_data_p;

	if (data->options.include_parsed_hive_partitions || data->options.include_file_name) {
		string first_file = data->files[0];

		unordered_map<string, column_t> column_map;
		for (idx_t i = 0; i < get.column_ids.size(); i++) {
			column_map.insert({get.names[get.column_ids[i]], i});
		}

		HivePartitioning::ApplyFiltersToFileList(context, data->files, filters, column_map, get.table_index,
		                                         data->options.include_parsed_hive_partitions,
		                                         data->options.include_file_name);

		if (data->files.empty() || data->files[0] != first_file) {
			data->initial_reader.reset();
		}
	}
}

unique_ptr<NodeStatistics> CSVReaderCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (ReadCSVData &)*bind_data_p;
	idx_t per_file_cardinality = 0;
	if (bind_data.initial_reader && bind_data.initial_reader->file_handle) {
		auto estimated_row_width = (bind_data.sql_types.size() * 5);
		per_file_cardinality = bind_data.initial_reader->file_handle->FileSize() / estimated_row_width;
	} else {
		// determined through the scientific method as the average amount of rows in a CSV file
		per_file_cardinality = 42;
	}
	return make_unique<NodeStatistics>(bind_data.files.size() * per_file_cardinality);
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
	// read options
	writer.WriteList<string>(names);
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
	writer.WriteField<bool>(include_file_name);
	writer.WriteField<bool>(include_parsed_hive_partitions);
	writer.WriteString(decimal_separator);
	writer.WriteField<bool>(null_padding);
	// write options
	writer.WriteListNoReference<bool>(force_quote);
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
	// read options
	names = reader.ReadRequiredList<string>();
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
	include_file_name = reader.ReadRequired<bool>();
	include_parsed_hive_partitions = reader.ReadRequired<bool>();
	decimal_separator = reader.ReadRequired<string>();
	null_padding = reader.ReadRequired<bool>();
	// write options
	force_quote = reader.ReadRequiredList<bool>();
}

static void CSVReaderSerialize(FieldWriter &writer, const FunctionData *bind_data_p, const TableFunction &function) {
	auto &bind_data = (ReadCSVData &)*bind_data_p;
	writer.WriteList<string>(bind_data.files);
	writer.WriteRegularSerializableList<LogicalType>(bind_data.sql_types);
	writer.WriteField<idx_t>(bind_data.filename_col_idx);
	writer.WriteField<idx_t>(bind_data.hive_partition_col_idx);
	bind_data.options.Serialize(writer);
	writer.WriteField<bool>(bind_data.single_threaded);
}

static unique_ptr<FunctionData> CSVReaderDeserialize(ClientContext &context, FieldReader &reader,
                                                     TableFunction &function) {
	auto result_data = make_unique<ReadCSVData>();
	result_data->files = reader.ReadRequiredList<string>();
	result_data->sql_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	result_data->filename_col_idx = reader.ReadRequired<idx_t>();
	result_data->hive_partition_col_idx = reader.ReadRequired<idx_t>();
	result_data->options.Deserialize(reader);
	result_data->single_threaded = reader.ReadField<bool>(true);
	return std::move(result_data);
}

TableFunction ReadCSVTableFunction::GetFunction(bool list_parameter) {
	auto parameter = list_parameter ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR;
	TableFunction read_csv("read_csv", {parameter}, ReadCSVFunction, ReadCSVBind, ReadCSVInitGlobal, ReadCSVInitLocal);
	read_csv.table_scan_progress = CSVReaderProgress;
	read_csv.pushdown_complex_filter = CSVComplexFilterPushdown;
	read_csv.serialize = CSVReaderSerialize;
	read_csv.deserialize = CSVReaderDeserialize;
	read_csv.get_batch_index = CSVReaderGetBatchIndex;
	read_csv.cardinality = CSVReaderCardinality;
	ReadCSVAddNamedParameters(read_csv);
	return read_csv;
}

TableFunction ReadCSVTableFunction::GetAutoFunction(bool list_parameter) {
	auto parameter = list_parameter ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR;
	TableFunction read_csv_auto("read_csv_auto", {parameter}, ReadCSVFunction, ReadCSVAutoBind, ReadCSVInitGlobal,
	                            ReadCSVInitLocal);
	read_csv_auto.table_scan_progress = CSVReaderProgress;
	read_csv_auto.pushdown_complex_filter = CSVComplexFilterPushdown;
	read_csv_auto.serialize = CSVReaderSerialize;
	read_csv_auto.deserialize = CSVReaderDeserialize;
	read_csv_auto.get_batch_index = CSVReaderGetBatchIndex;
	read_csv_auto.cardinality = CSVReaderCardinality;
	ReadCSVAddNamedParameters(read_csv_auto);
	read_csv_auto.named_parameters["column_types"] = LogicalType::ANY;
	read_csv_auto.named_parameters["dtypes"] = LogicalType::ANY;
	read_csv_auto.named_parameters["types"] = LogicalType::ANY;
	read_csv_auto.named_parameters["names"] = LogicalType::LIST(LogicalType::VARCHAR);
	read_csv_auto.named_parameters["column_names"] = LogicalType::LIST(LogicalType::VARCHAR);
	return read_csv_auto;
}

void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet read_csv("read_csv");
	read_csv.AddFunction(ReadCSVTableFunction::GetFunction());
	read_csv.AddFunction(ReadCSVTableFunction::GetFunction(true));
	set.AddFunction(read_csv);

	TableFunctionSet read_csv_auto("read_csv_auto");
	read_csv_auto.AddFunction(ReadCSVTableFunction::GetAutoFunction());
	read_csv_auto.AddFunction(ReadCSVTableFunction::GetAutoFunction(true));
	set.AddFunction(read_csv_auto);
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
	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value(table_name)));
	table_function->function = make_unique<FunctionExpression>("read_csv_auto", std::move(children));
	return std::move(table_function);
}

void BuiltinFunctions::RegisterReadFunctions() {
	CSVCopyFunction::RegisterFunction(*this);
	ReadCSVTableFunction::RegisterFunction(*this);
	auto &config = DBConfig::GetConfig(*transaction.db);
	config.replacement_scans.emplace_back(ReadCSVReplacement);
}

} // namespace duckdb
