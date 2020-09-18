#include <string>
#include <vector>
#include <bitset>
#include <fstream>
#include <cstring>
#include <iostream>
#include <sstream>

#include "parquet-extension.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"

#include "duckdb.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

struct ParquetReadBindData : public FunctionData {
	shared_ptr<ParquetReader> initial_reader;
	vector<string> files;
	vector<column_t> column_ids;
};

struct ParquetReadOperatorData : public FunctionOperatorData {
	shared_ptr<ParquetReader> reader;
	ParquetReaderScanState scan_state;
	bool is_parallel;
	idx_t file_index;
};

struct ParquetReadParallelState : public ParallelState {
	std::mutex lock;
	shared_ptr<ParquetReader> current_reader;
	idx_t file_index;
	idx_t row_group_index;
};

class ParquetScanFunction : public TableFunction {
public:
	ParquetScanFunction()
	    : TableFunction("parquet_scan", {LogicalType::VARCHAR}, parquet_scan_function, parquet_scan_bind,
	                    parquet_scan_init, /* cleanup */ nullptr, /* dependency */ nullptr, parquet_cardinality,
	                    /* pushdown_complex_filter */ nullptr, /* to_string */ nullptr, parquet_max_threads,
	                    parquet_init_parallel_state, parquet_parallel_state_next) {
		projection_pushdown = true;
	}

	static unique_ptr<FunctionData> parquet_read_bind(ClientContext &context, CopyInfo &info,
	                                                  vector<string> &expected_names,
	                                                  vector<LogicalType> &expected_types) {
		for (auto &option : info.options) {
			throw NotImplementedException("Unsupported option for COPY FROM parquet: %s", option.first);
		}
		auto result = make_unique<ParquetReadBindData>();

		FileSystem &fs = FileSystem::GetFileSystem(context);
		result->files = fs.Glob(info.file_path);
		if (result->files.empty()) {
			throw IOException("No files found that match the pattern \"%s\"", info.file_path);
		}
		result->initial_reader = make_shared<ParquetReader>(context, result->files[0], expected_types);
		// FIXME: hacky
		for (idx_t i = 0; i < expected_types.size(); i++) {
			result->initial_reader->column_ids.push_back(i);
		}
		result->column_ids = result->initial_reader->column_ids;
		return move(result);
	}

	static unique_ptr<FunctionData> parquet_scan_bind(ClientContext &context, vector<Value> &inputs,
	                                                  unordered_map<string, Value> &named_parameters,
	                                                  vector<LogicalType> &return_types, vector<string> &names) {
		auto file_name = inputs[0].GetValue<string>();
		auto result = make_unique<ParquetReadBindData>();

		FileSystem &fs = FileSystem::GetFileSystem(context);
		result->files = fs.Glob(file_name);
		if (result->files.empty()) {
			throw IOException("No files found that match the pattern \"%s\"", file_name);
		}
		result->initial_reader = make_shared<ParquetReader>(context, result->files[0]);
		return_types = result->initial_reader->return_types;
		names = result->initial_reader->names;
		return move(result);
	}

	static unique_ptr<FunctionOperatorData>
	parquet_scan_init(ClientContext &context, const FunctionData *bind_data_, ParallelState *parallel_state_,
	                  vector<column_t> &column_ids, unordered_map<idx_t, vector<TableFilter>> &table_filters) {
		auto &bind_data = (ParquetReadBindData &)*bind_data_;
		bind_data.initial_reader->column_ids = column_ids;
		bind_data.column_ids = column_ids;

		auto result = make_unique<ParquetReadOperatorData>();

		if (parallel_state_) {
			result->is_parallel = true;
			parquet_parallel_state_next(context, bind_data_, result.get(), parallel_state_);
		} else {
			result->is_parallel = false;
			result->file_index = 0;
			// single-threaded: one thread has to read all groups
			vector<idx_t> group_ids;
			for (idx_t i = 0; i < bind_data.initial_reader->NumRowGroups(); i++) {
				group_ids.push_back(i);
			}
			result->reader = bind_data.initial_reader;
			result->reader->Initialize(result->scan_state, move(group_ids));
		}
		return move(result);
	}

	static unique_ptr<GlobalFunctionData> parquet_read_initialize(ClientContext &context, FunctionData &fdata) {
		throw NotImplementedException("FIXME: COPY ");
		// auto &bind_data = (ParquetReadBindData &)fdata;
		// auto result = make_unique<ParquetCopyFunctionData>();
		// result->scan_state = make_unique<ParquetScanStateData>(bind_data);
		// for (idx_t i = 0; i < bind_data.file_meta_data.row_groups.size(); i++) {
		// 	result->scan_state->group_idx_list.push_back(i);
		// }
		// return move(result);
	}

	static void parquet_read_function(ExecutionContext &context, GlobalFunctionData &gstate, FunctionData &bind_data,
	                                  DataChunk &output) {
		throw NotImplementedException("FIXME: COPY ");
		// auto &scan_state = (ParquetCopyFunctionData &)gstate;
		// scan_state.scan_state->ReadChunk(output);
	}

	static void parquet_scan_function(ClientContext &context, const FunctionData *bind_data_,
	                                  FunctionOperatorData *operator_state, DataChunk &output) {
		auto &data = (ParquetReadOperatorData &)*operator_state;
		do {
			data.reader->ReadChunk(data.scan_state, output);
			if (output.size() == 0 && !data.is_parallel) {
				auto &bind_data = (ParquetReadBindData &)*bind_data_;
				// check if there is another file
				if (data.file_index + 1 < bind_data.files.size()) {
					data.file_index++;
					string file = bind_data.files[data.file_index];
					// move to the next file
					data.reader = make_shared<ParquetReader>(context, file, data.reader->return_types);
					data.reader->column_ids = bind_data.column_ids;
					vector<idx_t> group_ids;
					for (idx_t i = 0; i < data.reader->NumRowGroups(); i++) {
						group_ids.push_back(i);
					}
					data.reader->Initialize(data.scan_state, move(group_ids));
				} else {
					// exhausted all the files: done
					break;
				}
			} else {
				break;
			}
		} while(true);
	}

	static idx_t parquet_cardinality(const FunctionData *bind_data) {
		auto &data = (ParquetReadBindData &)*bind_data;
		return data.initial_reader->NumRows() * data.files.size();
	}

	static idx_t parquet_max_threads(ClientContext &context, const FunctionData *bind_data) {
		auto &data = (ParquetReadBindData &)*bind_data;
		return data.initial_reader->NumRowGroups() * data.files.size();
	}

	static unique_ptr<ParallelState> parquet_init_parallel_state(ClientContext &context,
	                                                             const FunctionData *bind_data_) {
 		auto &bind_data = (ParquetReadBindData &)*bind_data_;
		auto result = make_unique<ParquetReadParallelState>();
		result->current_reader = bind_data.initial_reader;
		result->row_group_index = 0;
		result->file_index = 0;
		return move(result);
	}

	static bool parquet_parallel_state_next(ClientContext &context, const FunctionData *bind_data_,
	                                        FunctionOperatorData *state_, ParallelState *parallel_state_) {
		auto &bind_data = (ParquetReadBindData &)*bind_data_;
		auto &parallel_state = (ParquetReadParallelState &)*parallel_state_;
		auto &scan_data = (ParquetReadOperatorData &)*state_;

		lock_guard<mutex> parallel_lock(parallel_state.lock);
		if (parallel_state.row_group_index < parallel_state.current_reader->NumRowGroups()) {
			// groups remain in the current parquet file: read the next group
			scan_data.reader = parallel_state.current_reader;
			vector<idx_t> group_indexes { parallel_state.row_group_index };
			scan_data.reader->Initialize(scan_data.scan_state, group_indexes);
			parallel_state.row_group_index++;
			return true;
		} else {
			// no groups remain in the current parquet file: check if there are more files to read
			while(parallel_state.file_index + 1 < bind_data.files.size()) {
				// read the next file
				string file = bind_data.files[++parallel_state.file_index];
				parallel_state.current_reader = make_shared<ParquetReader>(context, file, parallel_state.current_reader->return_types);
				if (parallel_state.current_reader->NumRowGroups() == 0) {
					// empty parquet file, move to next file
					continue;
				}
				parallel_state.current_reader->column_ids = bind_data.column_ids;
				// set up the scan state to read the first group
				scan_data.reader = parallel_state.current_reader;
				vector<idx_t> group_indexes { 0 };
				scan_data.reader->Initialize(scan_data.scan_state, group_indexes);
				parallel_state.row_group_index = 1;
				return true;
			}
		}
		return false;
	}
};

struct ParquetWriteBindData : public FunctionData {
	vector<LogicalType> sql_types;
	string file_name;
	vector<string> column_names;
	// TODO compression flag to test the param passing stuff
};

struct ParquetWriteGlobalState : public GlobalFunctionData {
	unique_ptr<ParquetWriter> writer;
};

struct ParquetWriteLocalState : public LocalFunctionData {
	ParquetWriteLocalState() {
		buffer = make_unique<ChunkCollection>();
	}

	unique_ptr<ChunkCollection> buffer;
};

unique_ptr<FunctionData> parquet_write_bind(ClientContext &context, CopyInfo &info, vector<string> &names,
                                            vector<LogicalType> &sql_types) {
	auto bind_data = make_unique<ParquetWriteBindData>();
	bind_data->sql_types = sql_types;
	bind_data->column_names = names;
	bind_data->file_name = info.file_path;
	return move(bind_data);
}

unique_ptr<GlobalFunctionData> parquet_write_initialize_global(ClientContext &context, FunctionData &bind_data) {
	auto global_state = make_unique<ParquetWriteGlobalState>();
	auto &parquet_bind = (ParquetWriteBindData &)bind_data;

	auto &fs = FileSystem::GetFileSystem(context);
	global_state->writer = make_unique<ParquetWriter>(fs, parquet_bind.file_name, parquet_bind.sql_types, parquet_bind.column_names);
	return move(global_state);
}

void parquet_write_sink(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                        LocalFunctionData &lstate, DataChunk &input) {
	auto &global_state = (ParquetWriteGlobalState &)gstate;
	auto &local_state = (ParquetWriteLocalState &)lstate;

	// append data to the local (buffered) chunk collection
	local_state.buffer->Append(input);
	if (local_state.buffer->count > 100000) {
		// if the chunk collection exceeds a certain size we flush it to the parquet file
		global_state.writer->Flush(*local_state.buffer);
		// and reset the buffer
		local_state.buffer = make_unique<ChunkCollection>();
	}
}

void parquet_write_combine(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                           LocalFunctionData &lstate) {
	auto &global_state = (ParquetWriteGlobalState &)gstate;
	auto &local_state = (ParquetWriteLocalState &)lstate;
	// flush any data left in the local state to the file
	global_state.writer->Flush(*local_state.buffer);
}

void parquet_write_finalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &global_state = (ParquetWriteGlobalState &)gstate;
	// finalize: write any additional metadata to the file here
	global_state.writer->Finalize();
}

unique_ptr<LocalFunctionData> parquet_write_initialize_local(ClientContext &context, FunctionData &bind_data) {
	return make_unique<ParquetWriteLocalState>();
}

void ParquetExtension::Load(DuckDB &db) {
	ParquetScanFunction scan_fun;
	CreateTableFunctionInfo cinfo(scan_fun);
	cinfo.name = "read_parquet";
	CreateTableFunctionInfo pq_scan = cinfo;
	pq_scan.name = "parquet_scan";

	CopyFunction function("parquet");
	function.copy_to_bind = parquet_write_bind;
	function.copy_to_initialize_global = parquet_write_initialize_global;
	function.copy_to_initialize_local = parquet_write_initialize_local;
	function.copy_to_sink = parquet_write_sink;
	function.copy_to_combine = parquet_write_combine;
	function.copy_to_finalize = parquet_write_finalize;
	function.copy_from_bind = ParquetScanFunction::parquet_read_bind;
	function.copy_from_initialize = ParquetScanFunction::parquet_read_initialize;
	function.copy_from_get_chunk = ParquetScanFunction::parquet_read_function;

	function.extension = "parquet";
	CreateCopyFunctionInfo info(function);

	Connection conn(db);
	conn.context->transaction.BeginTransaction();
	db.catalog->CreateCopyFunction(*conn.context, &info);
	db.catalog->CreateTableFunction(*conn.context, &cinfo);
	db.catalog->CreateTableFunction(*conn.context, &pq_scan);

	conn.context->transaction.Commit();
}

}

