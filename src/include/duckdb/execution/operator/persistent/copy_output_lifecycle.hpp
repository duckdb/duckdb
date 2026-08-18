//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/copy_output_lifecycle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

class ClientContext;

class CopyOutputLifecycle {
public:
	explicit CopyOutputLifecycle(ClientContext &context);
	~CopyOutputLifecycle();

public:
	idx_t RegisterFile(string path);
	void UpdateFile(idx_t file_index, string path);
	void MarkFileCommitted(idx_t file_index);
	string GetTemporaryFileTarget(const string &temporary_path) const;
	void CommitTemporaryFile(const string &temporary_path);
	void RegisterCreatedDirectory(string path);
	void MarkSuccessful();

private:
	void Cleanup();

	struct FileEntry {
		string path;
		bool committed = false;
	};

private:
	ClientContext &context;
	mutex lock;
	vector<FileEntry> files;
	vector<string> created_directories;
	bool successful = false;
};

struct GlobalFileState {
public:
	GlobalFileState(unique_ptr<GlobalFunctionData> data, string path, ClientContext &context, FunctionData &bind_data,
	                copy_to_abort_t abort, CopyOutputLifecycle &output_lifecycle, idx_t lifecycle_file_index);
	~GlobalFileState();

public:
	void Finalize(copy_to_finalize_t finalize);

public:
	annotated_mutex lock;
	unique_ptr<GlobalFunctionData> data;
	const string path;
	idx_t num_batches DUCKDB_GUARDED_BY(lock) = 0;

private:
	ClientContext &context;
	FunctionData &bind_data;
	copy_to_abort_t abort;
	CopyOutputLifecycle &output_lifecycle;
	idx_t lifecycle_file_index;
};

} // namespace duckdb
