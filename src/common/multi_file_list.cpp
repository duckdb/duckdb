#include "duckdb/common/multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>

namespace duckdb {

// Helper method to do Filter Pushdown into a MultiFileList
bool PushdownInternal(ClientContext &context, const MultiFileReaderOptions &options, LogicalGet &get,
                      vector<unique_ptr<Expression>> &filters, vector<string> &expanded_files) {
	unordered_map<string, column_t> column_map;
	for (idx_t i = 0; i < get.column_ids.size(); i++) {
		if (!IsRowIdColumnId(get.column_ids[i])) {
			column_map.insert({get.names[get.column_ids[i]], i});
		}
	}

	auto start_files = expanded_files.size();
	HivePartitioning::ApplyFiltersToFileList(context, expanded_files, filters, column_map, get,
	                                         options.hive_partitioning, options.filename);

	if (expanded_files.size() != start_files) {
		return true;
	}

	return false;
}

//===--------------------------------------------------------------------===//
// MultiFileListIterator
//===--------------------------------------------------------------------===//
MultiFileListIterationHelper MultiFileList::Files() {
	return MultiFileListIterationHelper(*this);
}

MultiFileListIterationHelper::MultiFileListIterationHelper(MultiFileList &file_list_p) : file_list(file_list_p) {
}

MultiFileListIterationHelper::MultiFileListIterator::MultiFileListIterator(MultiFileList *file_list_p)
    : file_list(file_list_p) {
	if (!file_list) {
		return;
	}

	file_list->InitializeScan(file_scan_data);
	if (!file_list->Scan(file_scan_data, current_file)) {
		// There is no first file: move iterator to nop state
		file_list = nullptr;
		file_scan_data.current_file_idx = DConstants::INVALID_INDEX;
	}
}

void MultiFileListIterationHelper::MultiFileListIterator::Next() {
	if (!file_list) {
		return;
	}

	if (!file_list->Scan(file_scan_data, current_file)) {
		// exhausted collection: move iterator to nop state
		file_list = nullptr;
		file_scan_data.current_file_idx = DConstants::INVALID_INDEX;
	}
}

MultiFileListIterationHelper::MultiFileListIterator MultiFileListIterationHelper::begin() { // NOLINT: match stl API
	return MultiFileListIterationHelper::MultiFileListIterator(
	    file_list.GetExpandResult() == FileExpandResult::NO_FILES ? nullptr : &file_list);
}
MultiFileListIterationHelper::MultiFileListIterator MultiFileListIterationHelper::end() { // NOLINT: match stl API
	return MultiFileListIterationHelper::MultiFileListIterator(nullptr);
}

MultiFileListIterationHelper::MultiFileListIterator &MultiFileListIterationHelper::MultiFileListIterator::operator++() {
	Next();
	return *this;
}

bool MultiFileListIterationHelper::MultiFileListIterator::operator!=(const MultiFileListIterator &other) const {
	return file_list != other.file_list || file_scan_data.current_file_idx != other.file_scan_data.current_file_idx;
}

const string &MultiFileListIterationHelper::MultiFileListIterator::operator*() const {
	return current_file;
}

//===--------------------------------------------------------------------===//
// MultiFileList
//===--------------------------------------------------------------------===//
MultiFileList::MultiFileList(vector<string> paths, FileGlobOptions options)
    : paths(std::move(paths)), glob_options(options) {
}

MultiFileList::~MultiFileList() {
}

const vector<string> MultiFileList::GetPaths() const {
	return paths;
}

void MultiFileList::InitializeScan(MultiFileListScanData &iterator) {
	iterator.current_file_idx = 0;
}

bool MultiFileList::Scan(MultiFileListScanData &iterator, string &result_file) {
	D_ASSERT(iterator.current_file_idx != DConstants::INVALID_INDEX);
	auto maybe_file = GetFile(iterator.current_file_idx);

	if (maybe_file.empty()) {
		D_ASSERT(iterator.current_file_idx >= GetTotalFileCount());
		return false;
	}

	result_file = maybe_file;
	iterator.current_file_idx++;
	return true;
}

unique_ptr<MultiFileList> MultiFileList::ComplexFilterPushdown(ClientContext &context,
                                                               const MultiFileReaderOptions &options, LogicalGet &get,
                                                               vector<unique_ptr<Expression>> &filters) {
	// By default the filter pushdown into a multifilelist does nothing
	return nullptr;
}

unique_ptr<NodeStatistics> MultiFileList::GetCardinality(ClientContext &context) {
	return nullptr;
}

string MultiFileList::GetFirstFile() {
	return GetFile(0);
}

bool MultiFileList::IsEmpty() {
	return GetExpandResult() == FileExpandResult::NO_FILES;
}

//===--------------------------------------------------------------------===//
// SimpleMultiFileList
//===--------------------------------------------------------------------===//
SimpleMultiFileList::SimpleMultiFileList(vector<string> paths_p)
    : MultiFileList(std::move(paths_p), FileGlobOptions::ALLOW_EMPTY) {
}

unique_ptr<MultiFileList> SimpleMultiFileList::ComplexFilterPushdown(ClientContext &context_p,
                                                                     const MultiFileReaderOptions &options,
                                                                     LogicalGet &get,
                                                                     vector<unique_ptr<Expression>> &filters) {
	if (!options.hive_partitioning && !options.filename) {
		return nullptr;
	}

	// FIXME: don't copy list until first file is filtered
	auto file_copy = paths;
	auto res = PushdownInternal(context_p, options, get, filters, file_copy);

	if (res) {
		return make_uniq<SimpleMultiFileList>(file_copy);
	}

	return nullptr;
}

vector<string> SimpleMultiFileList::GetAllFiles() {
	return paths;
}

FileExpandResult SimpleMultiFileList::GetExpandResult() {
	if (paths.size() > 1) {
		return FileExpandResult::MULTIPLE_FILES;
	} else if (paths.size() == 1) {
		return FileExpandResult::SINGLE_FILE;
	}

	return FileExpandResult::NO_FILES;
}

string SimpleMultiFileList::GetFile(idx_t i) {
	if (paths.empty() || i >= paths.size()) {
		return "";
	}

	return paths[i];
}

idx_t SimpleMultiFileList::GetTotalFileCount() {
	return paths.size();
}

//===--------------------------------------------------------------------===//
// GlobMultiFileList
//===--------------------------------------------------------------------===//
GlobMultiFileList::GlobMultiFileList(ClientContext &context_p, vector<string> paths_p, FileGlobOptions options)
    : MultiFileList(std::move(paths_p), options), context(context_p), current_path(0) {
}

unique_ptr<MultiFileList> GlobMultiFileList::ComplexFilterPushdown(ClientContext &context_p,
                                                                   const MultiFileReaderOptions &options,
                                                                   LogicalGet &get,
                                                                   vector<unique_ptr<Expression>> &filters) {
	lock_guard<mutex> lck(lock);

	// Expand all
	// FIXME: lazy expansion
	// FIXME: push down filters into glob
	while (ExpandPathInternal()) {
	}

	if (!options.hive_partitioning && !options.filename) {
		return nullptr;
	}
	auto res = PushdownInternal(context, options, get, filters, expanded_files);

	if (res) {
		return make_uniq<SimpleMultiFileList>(expanded_files);
	}

	return nullptr;
}

vector<string> GlobMultiFileList::GetAllFiles() {
	lock_guard<mutex> lck(lock);
	while (ExpandPathInternal()) {
	}
	return expanded_files;
}

idx_t GlobMultiFileList::GetTotalFileCount() {
	lock_guard<mutex> lck(lock);
	while (ExpandPathInternal()) {
	}
	return expanded_files.size();
}

FileExpandResult GlobMultiFileList::GetExpandResult() {
	// GetFile(1) will ensure at least the first 2 files are expanded if they are available
	GetFile(1);

	if (expanded_files.size() > 1) {
		return FileExpandResult::MULTIPLE_FILES;
	} else if (expanded_files.size() == 1) {
		return FileExpandResult::SINGLE_FILE;
	}

	return FileExpandResult::NO_FILES;
}

string GlobMultiFileList::GetFile(idx_t i) {
	lock_guard<mutex> lck(lock);
	return GetFileInternal(i);
}

string GlobMultiFileList::GetFileInternal(idx_t i) {
	while (expanded_files.size() <= i) {
		if (!ExpandPathInternal()) {
			return "";
		}
	}
	D_ASSERT(expanded_files.size() > i);
	return expanded_files[i];
}

bool GlobMultiFileList::ExpandPathInternal() {
	if (IsFullyExpanded()) {
		return false;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto glob_files = fs.GlobFiles(paths[current_path], context, glob_options);
	std::sort(glob_files.begin(), glob_files.end());
	expanded_files.insert(expanded_files.end(), glob_files.begin(), glob_files.end());

	current_path++;

	return true;
}

bool GlobMultiFileList::IsFullyExpanded() {
	return current_path == paths.size();
}

} // namespace duckdb
