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

//#include "duckdb/common/types/value.hpp"
//#include "duckdb/common/enums/file_glob_options.hpp"
//#include "duckdb/common/optional_ptr.hpp"
//#include "duckdb/common/union_by_name.hpp"

namespace duckdb {
class MultiFileList;
// class TableFunctionSet;
// class TableFilterSet;
// class LogicalGet;
// class Expression;
// class ClientContext;
// class DataChunk;

enum class FileExpandResult : uint8_t { NO_FILES, SINGLE_FILE, MULTIPLE_FILES };

struct MultiFileListScanData {
	idx_t current_file_idx = DConstants::INVALID_INDEX;
};

class MultiFileListIterationHelper {
public:
	DUCKDB_API MultiFileListIterationHelper(MultiFileList &collection);

private:
	MultiFileList &file_list;

private:
	class MultiFileListIterator;

	class MultiFileListIterator {
	public:
		DUCKDB_API explicit MultiFileListIterator(MultiFileList *file_list);

		MultiFileList *file_list;
		MultiFileListScanData file_scan_data;
		string current_file;

	public:
		DUCKDB_API void Next();

		DUCKDB_API MultiFileListIterator &operator++();
		DUCKDB_API bool operator!=(const MultiFileListIterator &other) const;
		DUCKDB_API const string &operator*() const;
	};

public:
	MultiFileListIterator begin();
	MultiFileListIterator end();
};

//! Abstract base class for lazily generated list of file paths/globs
//! note: most methods are NOT threadsafe
class MultiFileList {
public:
	MultiFileList();
	virtual ~MultiFileList();

	//! Returns the raw, unexpanded paths
	vector<string> GetPaths();

	//! Get Iterator over the files for pretty for loops
	MultiFileListIterationHelper Files();

	//! Initialize a sequential scan over a filelist
	void InitializeScan(MultiFileListScanData &iterator);
	//! Scan the next file into result_file, returns false when out of files
	bool Scan(MultiFileListScanData &iterator, string &result_file);

	//! Checks whether the MultiFileList is empty
	bool IsEmpty();
	//! Returns the first file or an empty string if GetTotalFileCount() == 0
	string GetFirstFile();
	//! Returns a FileExpandResult to give an indication of the total count. Calls ExpandTo(2).
	FileExpandResult GetExpandResult();

	//! Returns the current size of the expanded size
	idx_t GetCurrentFileCount();
	//! Expand the file list to n files
	void ExpandTo(idx_t n);

	//! Completely expands the list (potentially expensive for big datasets!)
	void ExpandAll();
	//! Calls ExpandAll() and returns the resulting size
	virtual idx_t GetTotalFileCount();
	//! Calls ExpandAll() and returns a reference to the fully expanded files
	virtual const vector<string> &GetAllFiles();

	//! Push down filters into the MultiFileList; sometimes the filters can be used to skip files completely
	virtual bool ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options, LogicalGet &get,
	                                   vector<unique_ptr<Expression>> &filters);

	//! Moves the vector out of the MultiFileList, caller is responsible to not use the MultiFileList after calling this
	//! DEPRECATED: should be removed once all DuckDB code can properly handle MultiFileLists
	vector<string> ToStringVector();

	//! Default copy method: CallsExpandAll() then creates a SimpleMultiFileList from expanded_files
	virtual unique_ptr<MultiFileList> Copy();

	//! API to implement for subclasses
protected:
	//! Get the i-th expanded file
	virtual string GetFileInternal(idx_t i) = 0;
	//! Get the raw unexpanded paths
	virtual vector<string> GetPathsInternal() = 0;

protected:
	//! The generated files
	vector<string> expanded_files;
	bool fully_expanded = false;
};

//! Simplest implementation of a MultiFileList which is fully expanded on creation
class SimpleMultiFileList : public MultiFileList {
public:
	//! Construct a SimpleMultiFileList from a list of already expanded files
	explicit SimpleMultiFileList(vector<string> files);
	//! Pruned the expanded_files using the hive/filename filters
	bool ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options, LogicalGet &get,
	                           vector<unique_ptr<Expression>> &filters) override;

protected:
	//! MultiFileList abstract interface implementation
	string GetFileInternal(idx_t i) override;
	vector<string> GetPathsInternal() override;
};

//! MultiFileList that will expand globs into files
class GlobMultiFileList : public MultiFileList {
public:
	GlobMultiFileList(ClientContext &context, vector<string> paths);
	//! Calls ExpandAll, then prunes the expanded_files using the hive/filename filters
	bool ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options, LogicalGet &get,
	                           vector<unique_ptr<Expression>> &filters) override;
	unique_ptr<MultiFileList> Copy() override;

protected:
	//! MultiFileList abstract interface implementation
	string GetFileInternal(idx_t i) override;
	vector<string> GetPathsInternal() override;

	//! Grabs the next path and expands it into Expanded paths:
	bool ExpandPathInternal();

	//! The ClientContext for globbing
	ClientContext &context;
	//! The input paths/globs
	vector<string> paths;
	//! The current path to expand
	idx_t current_path;
};

} // namespace duckdb
