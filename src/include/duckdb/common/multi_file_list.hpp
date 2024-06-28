//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/multi_file_reader_options.hpp"

namespace duckdb {
class MultiFileList;

enum class FileExpandResult : uint8_t { NO_FILES, SINGLE_FILE, MULTIPLE_FILES };

struct MultiFileListScanData {
	idx_t current_file_idx = DConstants::INVALID_INDEX;
};

class MultiFileListIterationHelper {
public:
	DUCKDB_API explicit MultiFileListIterationHelper(MultiFileList &collection);

private:
	MultiFileList &file_list;

private:
	class MultiFileListIterator;

	class MultiFileListIterator {
	public:
		DUCKDB_API explicit MultiFileListIterator(MultiFileList *file_list);

		optional_ptr<MultiFileList> file_list;
		MultiFileListScanData file_scan_data;
		string current_file;

	public:
		DUCKDB_API void Next();

		DUCKDB_API MultiFileListIterator &operator++();
		DUCKDB_API bool operator!=(const MultiFileListIterator &other) const;
		DUCKDB_API const string &operator*() const;
	};

public:
	MultiFileListIterator begin(); // NOLINT: match stl API
	MultiFileListIterator end();   // NOLINT: match stl API
};

//! Abstract class for lazily generated list of file paths/globs
//! NOTE: subclasses are responsible for ensuring thread-safety
class MultiFileList {
public:
	explicit MultiFileList(vector<string> paths, FileGlobOptions options);
	virtual ~MultiFileList();

	//! Returns the raw, unexpanded paths, pre-filter
	const vector<string> GetPaths() const;

	//! Get Iterator over the files for pretty for loops
	MultiFileListIterationHelper Files();

	//! Initialize a sequential scan over a file list
	void InitializeScan(MultiFileListScanData &iterator);
	//! Scan the next file into result_file, returns false when out of files
	bool Scan(MultiFileListScanData &iterator, string &result_file);

	//! Returns the first file or an empty string if GetTotalFileCount() == 0
	string GetFirstFile();
	//! Syntactic sugar for GetExpandResult() == FileExpandResult::NO_FILES
	bool IsEmpty();

	//! Virtual functions for subclasses
public:
	virtual unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context,
	                                                        const MultiFileReaderOptions &options, LogicalGet &get,
	                                                        vector<unique_ptr<Expression>> &filters);

	virtual vector<string> GetAllFiles() = 0;
	virtual FileExpandResult GetExpandResult() = 0;
	virtual idx_t GetTotalFileCount() = 0;

	virtual unique_ptr<NodeStatistics> GetCardinality(ClientContext &context);

protected:
	//! Get the i-th expanded file
	virtual string GetFile(idx_t i) = 0;

protected:
	//! The unexpanded input paths
	const vector<string> paths;
	//! Whether paths can expand to 0 files
	const FileGlobOptions glob_options;
};

//! MultiFileList that takes a list of files and produces the same list of paths. Useful for quickly wrapping
//! existing vectors of paths in a MultiFileList without changing any code
class SimpleMultiFileList : public MultiFileList {
public:
	//! Construct a SimpleMultiFileList from a list of already expanded files
	explicit SimpleMultiFileList(vector<string> paths);
	//! Copy `paths` to `filtered_files` and apply the filters
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options,
	                                                LogicalGet &get, vector<unique_ptr<Expression>> &filters) override;

	//! Main MultiFileList API
	vector<string> GetAllFiles() override;
	FileExpandResult GetExpandResult() override;
	idx_t GetTotalFileCount() override;

protected:
	//! Main MultiFileList API
	string GetFile(idx_t i) override;
};

//! MultiFileList that takes a list of paths and produces a list of files with all globs expanded
class GlobMultiFileList : public MultiFileList {
public:
	GlobMultiFileList(ClientContext &context, vector<string> paths, FileGlobOptions options);
	//! Calls ExpandAll, then prunes the expanded_files using the hive/filename filters
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options,
	                                                LogicalGet &get, vector<unique_ptr<Expression>> &filters) override;

	//! Main MultiFileList API
	vector<string> GetAllFiles() override;
	FileExpandResult GetExpandResult() override;
	idx_t GetTotalFileCount() override;

protected:
	//! Main MultiFileList API
	string GetFile(idx_t i) override;

	//! Get the i-th expanded file
	string GetFileInternal(idx_t i);
	//! Grabs the next path and expands it into Expanded paths: returns false if no more files to expand
	bool ExpandPathInternal();
	//! Whether all files have been expanded
	bool IsFullyExpanded();

	//! The ClientContext for globbing
	ClientContext &context;
	//! The current path to expand
	idx_t current_path;
	//! The expanded files
	vector<string> expanded_files;

	mutex lock;
};

} // namespace duckdb
