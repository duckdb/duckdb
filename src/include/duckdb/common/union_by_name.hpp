//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/union_by_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

template <class READER_TYPE, class OPTION_TYPE>
class UnionByReaderTask : public BaseExecutorTask {
public:
	UnionByReaderTask(TaskExecutor &executor, ClientContext &context, const string &file, idx_t file_idx,
	                  vector<typename READER_TYPE::UNION_READER_DATA> &readers, OPTION_TYPE &options)
	    : BaseExecutorTask(executor), context(context), file_name(file), file_idx(file_idx), readers(readers),
	      options(options) {
	}

	void ExecuteTask() override {
		auto reader = make_uniq<READER_TYPE>(context, file_name, options);
		readers[file_idx] = READER_TYPE::StoreUnionReader(std::move(reader), file_idx);
	}

private:
	ClientContext &context;
	const string &file_name;
	idx_t file_idx;
	vector<typename READER_TYPE::UNION_READER_DATA> &readers;
	OPTION_TYPE &options;
};

class UnionByName {
public:
	static void CombineUnionTypes(const vector<string> &new_names, const vector<LogicalType> &new_types,
	                              vector<LogicalType> &union_col_types, vector<string> &union_col_names,
	                              case_insensitive_map_t<idx_t> &union_names_map);

	//! Union all files(readers) by their col names
	template <class READER_TYPE, class OPTION_TYPE>
	static vector<typename READER_TYPE::UNION_READER_DATA>
	UnionCols(ClientContext &context, const vector<string> &files, vector<LogicalType> &union_col_types,
	          vector<string> &union_col_names, OPTION_TYPE &options) {
		if (!options.file_options.cache_union_readers) {
			// Each worker thread sequentially unifies schemas for a subset of files
			// without keeping readers in memory
			idx_t max_threads = static_cast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
			vector<vector<LogicalType>> union_col_types_per_thread(max_threads);
			vector<vector<string>> union_col_names_per_thread(max_threads);

			TaskExecutor executor(context);
			idx_t files_per_thread = (files.size() + max_threads - 1) / max_threads;
			for (idx_t thread_idx = 0; thread_idx < max_threads; ++thread_idx) {
				idx_t start_idx = thread_idx * files_per_thread;
				idx_t end_idx = MinValue<idx_t>(start_idx + files_per_thread, files.size());
				executor.ScheduleTask(make_uniq<UnionNoCacheReaderTask<READER_TYPE, OPTION_TYPE>>(
				    executor, context, files, start_idx, end_idx, union_col_types_per_thread,
				    union_col_names_per_thread, options, thread_idx));
			}
			executor.WorkOnTasks();

			// Unify schemas from all threads
			case_insensitive_map_t<idx_t> union_names_map;
			for (idx_t thread_idx = 0; thread_idx < max_threads; ++thread_idx) {
				CombineUnionTypes(union_col_names_per_thread[thread_idx], union_col_types_per_thread[thread_idx],
				                  union_col_types, union_col_names, union_names_map);
			}

			return {};
		} else {
			// Create readers for all files in memory before unifying schemas
			vector<typename READER_TYPE::UNION_READER_DATA> union_readers;
			union_readers.resize(files.size());

			TaskExecutor executor(context);
			// schedule tasks for all files
			for (idx_t file_idx = 0; file_idx < files.size(); ++file_idx) {
				auto task = make_uniq<UnionByReaderTask<READER_TYPE, OPTION_TYPE>>(executor, context, files[file_idx],
				                                                                   file_idx, union_readers, options);
				executor.ScheduleTask(std::move(task));
			}
			// complete all tasks
			executor.WorkOnTasks();

			// now combine the result schemas
			case_insensitive_map_t<idx_t> union_names_map;
			for (auto &reader : union_readers) {
				auto &col_names = reader->names;
				auto &sql_types = reader->types;
				CombineUnionTypes(col_names, sql_types, union_col_types, union_col_names, union_names_map);
			}
			return union_readers;
		}
	}

	template <class READER_TYPE, class OPTION_TYPE>
	class UnionNoCacheReaderTask : public BaseExecutorTask {
	public:
		UnionNoCacheReaderTask(TaskExecutor &executor, ClientContext &context, const vector<string> &files,
		                       idx_t start_idx, idx_t end_idx, vector<vector<LogicalType>> &union_col_types_per_thread,
		                       vector<vector<string>> &union_col_names_per_thread, OPTION_TYPE &options,
		                       idx_t thread_idx)
		    : BaseExecutorTask(executor), context(context), files(files), start_idx(start_idx), end_idx(end_idx),
		      union_col_types_per_thread(union_col_types_per_thread),
		      union_col_names_per_thread(union_col_names_per_thread), options(options), thread_idx(thread_idx) {
		}

		void ExecuteTask() override {
			case_insensitive_map_t<idx_t> union_names_map;
			for (idx_t file_idx = start_idx; file_idx < end_idx; ++file_idx) {
				auto reader = make_uniq<READER_TYPE>(context, files[file_idx], options);
				auto union_reader = READER_TYPE::StoreUnionReader(std::move(reader), file_idx);
				auto &col_names = union_reader->names;
				auto &sql_types = union_reader->types;
				CombineUnionTypes(col_names, sql_types, union_col_types_per_thread[thread_idx],
				                  union_col_names_per_thread[thread_idx], union_names_map);
			}
		}

	private:
		ClientContext &context;
		const vector<string> &files;
		idx_t start_idx;
		idx_t end_idx;
		vector<vector<LogicalType>> &union_col_types_per_thread;
		vector<vector<string>> &union_col_names_per_thread;
		OPTION_TYPE &options;
		idx_t thread_idx;
	};
};

} // namespace duckdb
