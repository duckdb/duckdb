#include <string>
#include <vector>
#include <fstream>
#include <iostream>

#include "parquet-extension.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "duckdb/storage/statistics/base_statistics.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#endif

namespace duckdb {

struct ParquetReadBindData : public FunctionData {
	shared_ptr<ParquetReader> initial_reader;
	vector<string> files;
	vector<column_t> column_ids;
	std::atomic<idx_t> chunk_count;
	idx_t cur_file;
};

struct ParquetReadOperatorData : public FunctionOperatorData {
	shared_ptr<ParquetReader> reader;
	ParquetReaderScanState scan_state;
	bool is_parallel;
	idx_t file_index;
	vector<column_t> column_ids;
	TableFilterSet *table_filters;
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
	    : TableFunction("parquet_scan", {LogicalType::VARCHAR}, ParquetScanImplementation, ParquetScanBind,
	                    ParquetScanInit, /* statistics */ ParquetScanStats, /* cleanup */ nullptr,
	                    /* dependency */ nullptr, ParquetCardinality,
	                    /* pushdown_complex_filter */ nullptr, /* to_string */ nullptr, ParquetScanMaxThreads,
	                    ParquetInitParallelState, ParquetScanParallelInit, ParquetParallelStateNext, true, true,
	                    ParquetProgress) {
	}

	static unique_ptr<FunctionData> ParquetReadBind(ClientContext &context, CopyInfo &info,
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
		return move(result);
	}

	static unique_ptr<BaseStatistics> ParquetScanStats(ClientContext &context, const FunctionData *bind_data_p,
	                                                   column_t column_index) {
		auto &bind_data = (ParquetReadBindData &)*bind_data_p;

		if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
			return nullptr;
		}

		// we do not want to parse the Parquet metadata for the sole purpose of getting column statistics

		// We already parsed the metadata for the first file in a glob because we need some type info.
		auto overall_stats = ParquetReader::ReadStatistics(
		    *bind_data.initial_reader, bind_data.initial_reader->return_types[column_index], column_index,
		    bind_data.initial_reader->metadata->metadata.get());

		if (!overall_stats) {
			return nullptr;
		}

		// if there is only one file in the glob (quite common case), we are done
		auto &config = DBConfig::GetConfig(context);
		if (bind_data.files.size() < 2) {
			return overall_stats;
		} else if (config.object_cache_enable) {
			auto &cache = ObjectCache::GetObjectCache(context);
			// for more than one file, we could be lucky and metadata for *every* file is in the object cache (if
			// enabled at all)
			FileSystem &fs = FileSystem::GetFileSystem(context);
			for (idx_t file_idx = 1; file_idx < bind_data.files.size(); file_idx++) {
				auto &file_name = bind_data.files[file_idx];
				auto metadata = std::dynamic_pointer_cast<ParquetFileMetadataCache>(cache.Get(file_name));
				auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);
				// but we need to check if the metadata cache entries are current
				if (!metadata || (fs.GetLastModifiedTime(*handle) >= metadata->read_time)) {
					// missing or invalid metadata entry in cache, no usable stats overall
					return nullptr;
				}
				// get and merge stats for file
				auto file_stats = ParquetReader::ReadStatistics(*bind_data.initial_reader,
				                                                bind_data.initial_reader->return_types[column_index],
				                                                column_index, metadata->metadata.get());
				if (!file_stats) {
					return nullptr;
				}
				overall_stats->Merge(*file_stats);
			}
			// success!
			return overall_stats;
		}
		// we have more than one file and no object cache so no statistics overall
		return nullptr;
	}

	static unique_ptr<FunctionData> ParquetScanBind(ClientContext &context, vector<Value> &inputs,
	                                                unordered_map<string, Value> &named_parameters,
	                                                vector<LogicalType> &input_table_types,
	                                                vector<string> &input_table_names,
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

	static unique_ptr<FunctionOperatorData> ParquetScanInit(ClientContext &context, const FunctionData *bind_data_p,
	                                                        vector<column_t> &column_ids,
	                                                        TableFilterCollection *filters) {
		auto &bind_data = (ParquetReadBindData &)*bind_data_p;
		bind_data.chunk_count = 0;
		bind_data.cur_file = 0;
		auto result = make_unique<ParquetReadOperatorData>();
		result->column_ids = column_ids;

		result->is_parallel = false;
		result->file_index = 0;
		result->table_filters = filters->table_filters;
		// single-threaded: one thread has to read all groups
		vector<idx_t> group_ids;
		for (idx_t i = 0; i < bind_data.initial_reader->NumRowGroups(); i++) {
			group_ids.push_back(i);
		}
		result->reader = bind_data.initial_reader;
		result->reader->InitializeScan(result->scan_state, column_ids, move(group_ids), filters->table_filters);
		return move(result);
	}

	static int ParquetProgress(ClientContext &context, const FunctionData *bind_data_p) {
		auto &bind_data = (ParquetReadBindData &)*bind_data_p;
		if (bind_data.initial_reader->NumRows() == 0) {
			return (100 * (bind_data.cur_file + 1)) / bind_data.files.size();
		}
		auto percentage = (bind_data.chunk_count * STANDARD_VECTOR_SIZE * 100 / bind_data.initial_reader->NumRows()) /
		                  bind_data.files.size();
		percentage += 100 * bind_data.cur_file / bind_data.files.size();
		return percentage;
	}

	static unique_ptr<FunctionOperatorData>
	ParquetScanParallelInit(ClientContext &context, const FunctionData *bind_data_p, ParallelState *parallel_state_p,
	                        vector<column_t> &column_ids, TableFilterCollection *filters) {
		auto result = make_unique<ParquetReadOperatorData>();
		result->column_ids = column_ids;
		result->is_parallel = true;
		result->table_filters = filters->table_filters;
		if (!ParquetParallelStateNext(context, bind_data_p, result.get(), parallel_state_p)) {
			return nullptr;
		}
		return move(result);
	}

	static void ParquetScanImplementation(ClientContext &context, const FunctionData *bind_data_p,
	                                      FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
		auto &data = (ParquetReadOperatorData &)*operator_state;
		auto &bind_data = (ParquetReadBindData &)*bind_data_p;

		do {
			data.reader->Scan(data.scan_state, output);
			bind_data.chunk_count++;
			if (output.size() == 0 && !data.is_parallel) {
				auto &bind_data = (ParquetReadBindData &)*bind_data_p;
				// check if there is another file
				if (data.file_index + 1 < bind_data.files.size()) {
					data.file_index++;
					bind_data.cur_file++;
					bind_data.chunk_count = 0;
					string file = bind_data.files[data.file_index];
					// move to the next file
					data.reader =
					    make_shared<ParquetReader>(context, file, data.reader->return_types, bind_data.files[0]);
					vector<idx_t> group_ids;
					for (idx_t i = 0; i < data.reader->NumRowGroups(); i++) {
						group_ids.push_back(i);
					}
					data.reader->InitializeScan(data.scan_state, data.column_ids, move(group_ids), data.table_filters);
				} else {
					// exhausted all the files: done
					break;
				}
			} else {
				break;
			}
		} while (true);
	}

	static unique_ptr<NodeStatistics> ParquetCardinality(ClientContext &context, const FunctionData *bind_data) {
		auto &data = (ParquetReadBindData &)*bind_data;
		return make_unique<NodeStatistics>(data.initial_reader->NumRows() * data.files.size());
	}

	static idx_t ParquetScanMaxThreads(ClientContext &context, const FunctionData *bind_data) {
		auto &data = (ParquetReadBindData &)*bind_data;
		return data.initial_reader->NumRowGroups() * data.files.size();
	}

	static unique_ptr<ParallelState> ParquetInitParallelState(ClientContext &context, const FunctionData *bind_data_p) {
		auto &bind_data = (ParquetReadBindData &)*bind_data_p;
		auto result = make_unique<ParquetReadParallelState>();
		result->current_reader = bind_data.initial_reader;
		result->row_group_index = 0;
		result->file_index = 0;
		return move(result);
	}

	static bool ParquetParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	                                     FunctionOperatorData *state_p, ParallelState *parallel_state_p) {
		auto &bind_data = (ParquetReadBindData &)*bind_data_p;
		auto &parallel_state = (ParquetReadParallelState &)*parallel_state_p;
		auto &scan_data = (ParquetReadOperatorData &)*state_p;

		lock_guard<mutex> parallel_lock(parallel_state.lock);
		if (parallel_state.row_group_index < parallel_state.current_reader->NumRowGroups()) {
			// groups remain in the current parquet file: read the next group
			scan_data.reader = parallel_state.current_reader;
			vector<idx_t> group_indexes {parallel_state.row_group_index};
			scan_data.reader->InitializeScan(scan_data.scan_state, scan_data.column_ids, group_indexes,
			                                 scan_data.table_filters);
			parallel_state.row_group_index++;
			return true;
		} else {
			// no groups remain in the current parquet file: check if there are more files to read
			while (parallel_state.file_index + 1 < bind_data.files.size()) {
				// read the next file
				string file = bind_data.files[++parallel_state.file_index];
				parallel_state.current_reader =
				    make_shared<ParquetReader>(context, file, parallel_state.current_reader->return_types);
				if (parallel_state.current_reader->NumRowGroups() == 0) {
					// empty parquet file, move to next file
					continue;
				}
				// set up the scan state to read the first group
				scan_data.reader = parallel_state.current_reader;
				vector<idx_t> group_indexes {0};
				scan_data.reader->InitializeScan(scan_data.scan_state, scan_data.column_ids, group_indexes,
				                                 scan_data.table_filters);
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
	duckdb_parquet::format::CompressionCodec::type codec = duckdb_parquet::format::CompressionCodec::SNAPPY;
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

unique_ptr<FunctionData> ParquetWriteBind(ClientContext &context, CopyInfo &info, vector<string> &names,
                                          vector<LogicalType> &sql_types) {
	auto bind_data = make_unique<ParquetWriteBindData>();
	for (auto &option : info.options) {
		auto loption = StringUtil::Lower(option.first);
		if (loption == "compression" || loption == "codec") {
			if (!option.second.empty()) {
				auto roption = StringUtil::Lower(option.second[0].ToString());
				if (roption == "uncompressed") {
					bind_data->codec = duckdb_parquet::format::CompressionCodec::UNCOMPRESSED;
					continue;
				} else if (roption == "snappy") {
					bind_data->codec = duckdb_parquet::format::CompressionCodec::SNAPPY;
					continue;
				} else if (roption == "gzip") {
					bind_data->codec = duckdb_parquet::format::CompressionCodec::GZIP;
					continue;
				} else if (roption == "zstd") {
					bind_data->codec = duckdb_parquet::format::CompressionCodec::ZSTD;
					continue;
				}
			}
			throw ParserException("Expected %s argument to be either [uncompressed, snappy, gzip or zstd]", loption);
		} else {
			throw NotImplementedException("Unrecognized option for PARQUET: %s", option.first.c_str());
		}
	}
	bind_data->sql_types = sql_types;
	bind_data->column_names = names;
	bind_data->file_name = info.file_path;
	return move(bind_data);
}

unique_ptr<GlobalFunctionData> ParquetWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data) {
	auto global_state = make_unique<ParquetWriteGlobalState>();
	auto &parquet_bind = (ParquetWriteBindData &)bind_data;

	auto &fs = FileSystem::GetFileSystem(context);
	global_state->writer = make_unique<ParquetWriter>(fs, parquet_bind.file_name, parquet_bind.sql_types,
	                                                  parquet_bind.column_names, parquet_bind.codec);
	return move(global_state);
}

void ParquetWriteSink(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                      LocalFunctionData &lstate, DataChunk &input) {
	auto &global_state = (ParquetWriteGlobalState &)gstate;
	auto &local_state = (ParquetWriteLocalState &)lstate;

	// append data to the local (buffered) chunk collection
	local_state.buffer->Append(input);
	if (local_state.buffer->Count() > 100000) {
		// if the chunk collection exceeds a certain size we flush it to the parquet file
		global_state.writer->Flush(*local_state.buffer);
		// and reset the buffer
		local_state.buffer = make_unique<ChunkCollection>();
	}
}

void ParquetWriteCombine(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                         LocalFunctionData &lstate) {
	auto &global_state = (ParquetWriteGlobalState &)gstate;
	auto &local_state = (ParquetWriteLocalState &)lstate;
	// flush any data left in the local state to the file
	global_state.writer->Flush(*local_state.buffer);
}

void ParquetWriteFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &global_state = (ParquetWriteGlobalState &)gstate;
	// finalize: write any additional metadata to the file here
	global_state.writer->Finalize();
}

unique_ptr<LocalFunctionData> ParquetWriteInitializeLocal(ClientContext &context, FunctionData &bind_data) {
	return make_unique<ParquetWriteLocalState>();
}

unique_ptr<TableFunctionRef> ParquetScanReplacement(const string &table_name, void *data) {
	if (!StringUtil::EndsWith(table_name, ".parquet")) {
		return nullptr;
	}
	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value(table_name)));
	table_function->function = make_unique<FunctionExpression>("parquet_scan", children);
	return table_function;
}

void ParquetExtension::Load(DuckDB &db) {
	ParquetScanFunction scan_fun;
	CreateTableFunctionInfo cinfo(scan_fun);
	cinfo.name = "read_parquet";
	CreateTableFunctionInfo pq_scan = cinfo;
	pq_scan.name = "parquet_scan";

	CopyFunction function("parquet");
	function.copy_to_bind = ParquetWriteBind;
	function.copy_to_initialize_global = ParquetWriteInitializeGlobal;
	function.copy_to_initialize_local = ParquetWriteInitializeLocal;
	function.copy_to_sink = ParquetWriteSink;
	function.copy_to_combine = ParquetWriteCombine;
	function.copy_to_finalize = ParquetWriteFinalize;
	function.copy_from_bind = ParquetScanFunction::ParquetReadBind;
	function.copy_from_function = scan_fun;

	function.extension = "parquet";
	CreateCopyFunctionInfo info(function);

	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateCopyFunction(context, &info);
	catalog.CreateTableFunction(context, &cinfo);
	catalog.CreateTableFunction(context, &pq_scan);
	con.Commit();

	auto &config = DBConfig::GetConfig(*db.instance);
	config.replacement_scans.emplace_back(ParquetScanReplacement);
}

} // namespace duckdb
