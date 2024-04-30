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
static bool PushdownInternal(ClientContext &context, const MultiFileReaderOptions &options, LogicalGet &get,
                             vector<unique_ptr<Expression>> &filters, vector<string> &expanded_files) {
	if (!options.hive_partitioning && !options.filename) {
		return false;
	}

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

MultiFileListIterationHelper::MultiFileListIterator MultiFileListIterationHelper::begin() { // NOLINT
	return MultiFileListIterationHelper::MultiFileListIterator(
	    file_list.GetExpandResult() == FileExpandResult::NO_FILES ? nullptr : &file_list);
}
MultiFileListIterationHelper::MultiFileListIterator MultiFileListIterationHelper::end() { // NOLINT
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
MultiFileList::MultiFileList() : expanded_files(), fully_expanded(false) {
}

MultiFileList::~MultiFileList() {
}

vector<string> MultiFileList::GetPaths() {
	return GetPathsInternal();
}

bool MultiFileList::ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options,
                                          LogicalGet &get, vector<unique_ptr<Expression>> &filters) {
	// By default the filter pushdown into a multifilelist does nothing
	return false;
}

void MultiFileList::InitializeScan(MultiFileListScanData &iterator) {
	iterator.current_file_idx = 0;
}

bool MultiFileList::Scan(MultiFileListScanData &iterator, string &result_file) {
	D_ASSERT(iterator.current_file_idx != DConstants::INVALID_INDEX);
	ExpandTo(iterator.current_file_idx);

	if (iterator.current_file_idx >= expanded_files.size()) {
		return false;
	}

	result_file = expanded_files[iterator.current_file_idx++];
	return true;
}

bool MultiFileList::IsEmpty() {
	return GetFirstFile().empty();
}

string MultiFileList::GetFirstFile() {
	ExpandTo(1);
	if (!expanded_files.empty()) {
		return expanded_files[0];
	}
	return "";
}

FileExpandResult MultiFileList::GetExpandResult() {
	ExpandTo(2);

	if (GetCurrentFileCount() >= 2) {
		return FileExpandResult::MULTIPLE_FILES;
	} else if (GetCurrentFileCount() == 1) {
		return FileExpandResult::SINGLE_FILE;
	}

	return FileExpandResult::NO_FILES;
}

idx_t MultiFileList::GetCurrentFileCount() {
	return expanded_files.size();
}

void MultiFileList::ExpandAll() {
	ExpandTo(NumericLimits<idx_t>::Maximum());
}

void MultiFileList::ExpandTo(idx_t n) {
	if (fully_expanded) {
		return;
	}

	idx_t i = expanded_files.size();
	while (i < n) {
		auto next_file = GetFileInternal(i);
		if (next_file.empty()) {
			fully_expanded = true;
			break;
		}
		expanded_files[i] = next_file;
		i++;
	}
}

idx_t MultiFileList::GetTotalFileCount() {
	if (!fully_expanded) {
		ExpandAll();
	}
	return expanded_files.size();
}

const vector<string> &MultiFileList::GetAllFiles() {
	if (!fully_expanded) {
		ExpandAll();
	}
	return expanded_files;
}

vector<string> MultiFileList::ToStringVector() {
	if (!fully_expanded) {
		ExpandAll();
	}
	return std::move(expanded_files);
}

unique_ptr<MultiFileList> MultiFileList::Copy() {
	ExpandAll();
	auto res = make_uniq<SimpleMultiFileList>(std::move(expanded_files));
	expanded_files = res->expanded_files;
	return res;
}

//===--------------------------------------------------------------------===//
// SimpleMultiFileList
//===--------------------------------------------------------------------===//
SimpleMultiFileList::SimpleMultiFileList(vector<string> files) : MultiFileList() {
	expanded_files = std::move(files);
	fully_expanded = true;
}

vector<string> SimpleMultiFileList::GetPathsInternal() {
	return expanded_files;
}

string SimpleMultiFileList::GetFileInternal(idx_t i) {
	if (expanded_files.size() <= i) {
		return "";
	}
	return expanded_files[i];
}

bool SimpleMultiFileList::ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options,
                                                LogicalGet &get, vector<unique_ptr<Expression>> &filters) {
	return PushdownInternal(context, options, get, filters, expanded_files);
}

//===--------------------------------------------------------------------===//
// GlobMultiFileList
//===--------------------------------------------------------------------===//
GlobMultiFileList::GlobMultiFileList(ClientContext &context_p, vector<string> paths_p)
    : MultiFileList(), context(context_p), paths(std::move(paths_p)), current_path(0) {
}

vector<string> GlobMultiFileList::GetPathsInternal() {
	return paths;
}

bool GlobMultiFileList::ComplexFilterPushdown(ClientContext &context_p, const MultiFileReaderOptions &options,
                                              LogicalGet &get, vector<unique_ptr<Expression>> &filters) {
	// TODO: implement special glob that makes use of hive partition filters to do more efficient globbing
	ExpandAll();
	return PushdownInternal(context, options, get, filters, expanded_files);
}

string GlobMultiFileList::GetFileInternal(idx_t i) {
	while (GetCurrentFileCount() <= i) {
		if (!ExpandPathInternal()) {
			return "";
		}
	}

	D_ASSERT(GetCurrentFileCount() > i);
	return expanded_files[i];
}

unique_ptr<MultiFileList> GlobMultiFileList::Copy() {
	auto res = make_uniq<GlobMultiFileList>(context, std::move(paths));
	res->current_path = current_path;
	res->expanded_files = std::move(expanded_files);
	res->fully_expanded = fully_expanded;

	current_path = res->current_path;
	expanded_files = res->expanded_files;
	paths = res->paths;

	return std::move(res);
}

bool GlobMultiFileList::ExpandPathInternal() {
	if (fully_expanded || current_path >= paths.size()) {
		return false;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto glob_files = fs.GlobFiles(paths[current_path], context, FileGlobOptions::DISALLOW_EMPTY);
	std::sort(glob_files.begin(), glob_files.end());
	expanded_files.insert(expanded_files.end(), glob_files.begin(), glob_files.end());

	current_path++;

	if (current_path >= paths.size()) {
		fully_expanded = true;
	}

	return true;
}

} // namespace duckdb
