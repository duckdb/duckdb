#include "duckdb/common/multi_file/union_by_name.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"

namespace duckdb {

class UnionByReaderTask : public BaseExecutorTask {
public:
	UnionByReaderTask(TaskExecutor &executor, ClientContext &context, const OpenFileInfo &file, idx_t file_idx,
	                  vector<shared_ptr<BaseUnionData>> &readers, BaseFileReaderOptions &options,
	                  MultiFileOptions &file_options, MultiFileReader &multi_file_reader,
	                  MultiFileReaderInterface &interface)
	    : BaseExecutorTask(executor), context(context), file(file), file_idx(file_idx), readers(readers),
	      options(options), file_options(file_options), multi_file_reader(multi_file_reader), interface(interface) {
	}

	void ExecuteTask() override {
		auto reader = multi_file_reader.CreateReader(context, file, options, file_options, interface);
		readers[file_idx] = reader->GetUnionData(file_idx);
	}

	string TaskType() const override {
		return "UnionByReaderTask";
	}

private:
	ClientContext &context;
	const OpenFileInfo &file;
	idx_t file_idx;
	vector<shared_ptr<BaseUnionData>> &readers;
	BaseFileReaderOptions &options;
	MultiFileOptions &file_options;
	MultiFileReader &multi_file_reader;
	MultiFileReaderInterface &interface;
};

vector<shared_ptr<BaseUnionData>>
UnionByName::UnionCols(ClientContext &context, const vector<OpenFileInfo> &files, vector<LogicalType> &union_col_types,
                       vector<string> &union_col_names, BaseFileReaderOptions &options, MultiFileOptions &file_options,
                       MultiFileReader &multi_file_reader, MultiFileReaderInterface &interface) {
	vector<shared_ptr<BaseUnionData>> union_readers;
	union_readers.resize(files.size());

	TaskExecutor executor(context);
	// schedule tasks for all files
	for (idx_t file_idx = 0; file_idx < files.size(); ++file_idx) {
		auto task = make_uniq<UnionByReaderTask>(executor, context, files[file_idx], file_idx, union_readers, options,
		                                         file_options, multi_file_reader, interface);
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

} // namespace duckdb
