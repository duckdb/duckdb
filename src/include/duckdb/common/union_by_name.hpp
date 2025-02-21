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
#include "duckdb/common/base_file_reader.hpp"
#include "duckdb/common/multi_file_reader_options.hpp"

namespace duckdb {

template <class OP, class OPTIONS_TYPE>
class UnionByReaderTask : public BaseExecutorTask {
public:
	UnionByReaderTask(TaskExecutor &executor, ClientContext &context, const string &file, idx_t file_idx,
	                  vector<shared_ptr<BaseUnionData>> &readers, OPTIONS_TYPE &options,
	                  MultiFileReaderOptions &file_options)
	    : BaseExecutorTask(executor), context(context), file_name(file), file_idx(file_idx), readers(readers),
	      options(options), file_options(file_options) {
	}

	void ExecuteTask() override {
		auto reader = OP::CreateReader(context, file_name, options, file_options);
		readers[file_idx] = OP::GetUnionData(std::move(reader), file_idx);
	}

private:
	ClientContext &context;
	const string &file_name;
	idx_t file_idx;
	vector<shared_ptr<BaseUnionData>> &readers;
	OPTIONS_TYPE &options;
	MultiFileReaderOptions &file_options;
};

class UnionByName {
public:
	static void CombineUnionTypes(const vector<string> &new_names, const vector<LogicalType> &new_types,
	                              vector<LogicalType> &union_col_types, vector<string> &union_col_names,
	                              case_insensitive_map_t<idx_t> &union_names_map);

	//! Union all files(readers) by their col names
	template <class OP, class OPTIONS_TYPE>
	static vector<shared_ptr<BaseUnionData>>
	UnionCols(ClientContext &context, const vector<string> &files, vector<LogicalType> &union_col_types,
	          vector<string> &union_col_names, OPTIONS_TYPE &options, MultiFileReaderOptions &file_options) {
		vector<shared_ptr<BaseUnionData>> union_readers;
		union_readers.resize(files.size());

		TaskExecutor executor(context);
		// schedule tasks for all files
		for (idx_t file_idx = 0; file_idx < files.size(); ++file_idx) {
			auto task = make_uniq<UnionByReaderTask<OP, OPTIONS_TYPE>>(executor, context, files[file_idx], file_idx,
			                                                           union_readers, options, file_options);
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
};

} // namespace duckdb
