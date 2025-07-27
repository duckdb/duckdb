#include "duckdb/common/multi_file/multi_file_reader.hpp"

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

MultiFilePushdownInfo::MultiFilePushdownInfo(LogicalGet &get)
    : table_index(get.table_index), column_names(get.names), column_indexes(get.GetColumnIds()),
      extra_info(get.extra_info) {
	for (auto &col_id : column_indexes) {
		column_ids.push_back(col_id.GetPrimaryIndex());
	}
}

MultiFilePushdownInfo::MultiFilePushdownInfo(idx_t table_index, const vector<string> &column_names,
                                             const vector<column_t> &column_ids, ExtraOperatorInfo &extra_info)
    : table_index(table_index), column_names(column_names), column_ids(column_ids), extra_info(extra_info) {
}

// Helper method to do Filter Pushdown into a MultiFileList
bool PushdownInternal(ClientContext &context, const MultiFileOptions &options, MultiFilePushdownInfo &info,
                      vector<unique_ptr<Expression>> &filters, vector<OpenFileInfo> &expanded_files) {
	auto start_files = expanded_files.size();
	HivePartitioning::ApplyFiltersToFileList(context, expanded_files, filters, options, info);

	if (expanded_files.size() != start_files) {
		return true;
	}

	return false;
}

bool PushdownInternal(ClientContext &context, const MultiFileOptions &options, const vector<string> &names,
                      const vector<LogicalType> &types, const vector<column_t> &column_ids,
                      const TableFilterSet &filters, vector<OpenFileInfo> &expanded_files) {
	idx_t table_index = 0;
	ExtraOperatorInfo extra_info;

	// construct the pushdown info
	MultiFilePushdownInfo info(table_index, names, column_ids, extra_info);

	// construct the set of expressions from the table filters
	vector<unique_ptr<Expression>> filter_expressions;
	for (auto &entry : filters.filters) {
		idx_t local_index = entry.first;
		idx_t column_idx = column_ids[local_index];
		if (IsVirtualColumn(column_idx)) {
			continue;
		}
		auto column_ref =
		    make_uniq<BoundColumnRefExpression>(types[column_idx], ColumnBinding(table_index, entry.first));
		auto filter_expr = entry.second->ToExpression(*column_ref);
		filter_expressions.push_back(std::move(filter_expr));
	}

	// call the original PushdownInternal method
	return PushdownInternal(context, options, info, filter_expressions, expanded_files);
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

const OpenFileInfo &MultiFileListIterationHelper::MultiFileListIterator::operator*() const {
	return current_file;
}

//===--------------------------------------------------------------------===//
// MultiFileList
//===--------------------------------------------------------------------===//
MultiFileList::MultiFileList(vector<OpenFileInfo> paths, FileGlobOptions options)
    : paths(std::move(paths)), glob_options(options) {
}

MultiFileList::~MultiFileList() {
}

const vector<OpenFileInfo> MultiFileList::GetPaths() const {
	return paths;
}

void MultiFileList::InitializeScan(MultiFileListScanData &iterator) {
	iterator.current_file_idx = 0;
}

bool MultiFileList::Scan(MultiFileListScanData &iterator, OpenFileInfo &result_file) {
	D_ASSERT(iterator.current_file_idx != DConstants::INVALID_INDEX);
	auto maybe_file = GetFile(iterator.current_file_idx);

	if (maybe_file.path.empty()) {
		D_ASSERT(iterator.current_file_idx >= GetTotalFileCount());
		return false;
	}

	result_file = maybe_file;
	iterator.current_file_idx++;
	return true;
}

unique_ptr<MultiFileList> MultiFileList::ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                                               MultiFilePushdownInfo &info,
                                                               vector<unique_ptr<Expression>> &filters) {
	// By default the filter pushdown into a multifilelist does nothing
	return nullptr;
}

unique_ptr<MultiFileList> MultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                                               const vector<string> &names,
                                                               const vector<LogicalType> &types,
                                                               const vector<column_t> &column_ids,
                                                               TableFilterSet &filters) const {
	// By default the filter pushdown into a multifilelist does nothing
	return nullptr;
}

unique_ptr<NodeStatistics> MultiFileList::GetCardinality(ClientContext &context) {
	return nullptr;
}

OpenFileInfo MultiFileList::PeekFile(idx_t i) {
	return GetFile(i);
}

OpenFileInfo MultiFileList::GetFirstFile() {
	return GetFile(0);
}

OpenFileInfo MultiFileList::PeekFirstFile() {
	return GetFirstFile();
}

unique_ptr<MultiFileList> MultiFileList::GetFirstFileList(idx_t max_files) {
	vector<OpenFileInfo> files;
	for (idx_t i = 0; i < max_files; i++) {
		auto file = PeekFile(i);
		if (file.path.empty()) {
			break;
		}
		files.push_back(file);
	}
	return make_uniq<SimpleMultiFileList>(files);
}

bool MultiFileList::IsEmpty() {
	return GetExpandResult() == FileExpandResult::NO_FILES;
}

unique_ptr<MultiFileList> MultiFileList::Copy() {
	return make_uniq<SimpleMultiFileList>(GetAllFiles());
}

//===--------------------------------------------------------------------===//
// SimpleMultiFileList
//===--------------------------------------------------------------------===//
SimpleMultiFileList::SimpleMultiFileList(vector<OpenFileInfo> paths_p)
    : MultiFileList(std::move(paths_p), FileGlobOptions::ALLOW_EMPTY) {
}

unique_ptr<MultiFileList> SimpleMultiFileList::ComplexFilterPushdown(ClientContext &context_p,
                                                                     const MultiFileOptions &options,
                                                                     MultiFilePushdownInfo &info,
                                                                     vector<unique_ptr<Expression>> &filters) {
	if (!options.hive_partitioning && !options.filename) {
		return nullptr;
	}

	// FIXME: don't copy list until first file is filtered
	auto file_copy = paths;
	auto res = PushdownInternal(context_p, options, info, filters, file_copy);

	if (res) {
		return make_uniq<SimpleMultiFileList>(file_copy);
	}

	return nullptr;
}

unique_ptr<MultiFileList>
SimpleMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                           const vector<string> &names, const vector<LogicalType> &types,
                                           const vector<column_t> &column_ids, TableFilterSet &filters) const {
	if (!options.hive_partitioning && !options.filename) {
		return nullptr;
	}

	// FIXME: don't copy list until first file is filtered
	auto file_copy = paths;
	auto res = PushdownInternal(context, options, names, types, column_ids, filters, file_copy);
	if (res) {
		return make_uniq<SimpleMultiFileList>(file_copy);
	}

	return nullptr;
}

vector<OpenFileInfo> SimpleMultiFileList::GetAllFiles() {
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

OpenFileInfo SimpleMultiFileList::GetFile(idx_t i) {
	if (paths.empty() || i >= paths.size()) {
		return OpenFileInfo("");
	}

	return paths[i];
}

idx_t SimpleMultiFileList::GetTotalFileCount() {
	return paths.size();
}

//===--------------------------------------------------------------------===//
// GlobMultiFileList
//===--------------------------------------------------------------------===//
GlobMultiFileList::GlobMultiFileList(ClientContext &context_p, vector<OpenFileInfo> paths_p, FileGlobOptions options)
    : MultiFileList(std::move(paths_p), options), context(context_p), current_path(0), first_files_current_path(0) {
}

unique_ptr<MultiFileList> GlobMultiFileList::ComplexFilterPushdown(ClientContext &context_p,
                                                                   const MultiFileOptions &options,
                                                                   MultiFilePushdownInfo &info,
                                                                   vector<unique_ptr<Expression>> &filters) {
	lock_guard<mutex> lck(lock);

	// Expand all
	if (options.hive_lazy_listing) {
		HiveFilterParams hive_filter_params(context, filters, options, info);
		while (ExpandNextPath(std::numeric_limits<idx_t>::max(), false, &hive_filter_params)) {
		}
	} else {
		auto res = PushdownInternal(context, options, info, filters, expanded_files);
		if (res) {
			return make_uniq<SimpleMultiFileList>(expanded_files);
		}
	}

	if (!options.hive_partitioning && !options.filename) {
		return nullptr;
	}

	return make_uniq<SimpleMultiFileList>(expanded_files);

	// return nullptr;
}

unique_ptr<MultiFileList>
GlobMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                         const vector<string> &names, const vector<LogicalType> &types,
                                         const vector<column_t> &column_ids, TableFilterSet &filters) const {
	if (!options.hive_partitioning && !options.filename) {
		return nullptr;
	}
	lock_guard<mutex> lck(lock);

	// Expand all paths into a copy
	idx_t path_index = current_path;
	auto file_list = expanded_files;
	while (ExpandPathInternal(path_index, file_list)) {
	}

	auto res = PushdownInternal(context, options, names, types, column_ids, filters, file_list);
	if (res) {
		return make_uniq<SimpleMultiFileList>(file_list);
	}

	return nullptr;
}

vector<OpenFileInfo> GlobMultiFileList::GetAllFiles() {
	lock_guard<mutex> lck(lock);
	while (ExpandNextPath(std::numeric_limits<idx_t>::max())) {
	}
	return expanded_files;
}

idx_t GlobMultiFileList::GetTotalFileCount() {
	lock_guard<mutex> lck(lock);
	while (ExpandNextPath(std::numeric_limits<idx_t>::max())) {
	}
	return expanded_files.size();
}

FileExpandResult GlobMultiFileList::GetExpandResult() {
	// return FileExpandResult::MULTIPLE_FILES;
	// Ensure at least the first 2 files are expanded if they are available
	lock_guard<mutex> lck(lock);
	GetFileInternal(1, true);

	if (first_expanded_files.size() > 1) {
		return FileExpandResult::MULTIPLE_FILES;
	} else if (first_expanded_files.size() == 1) {
		return FileExpandResult::SINGLE_FILE;
	}

	return FileExpandResult::NO_FILES;
}

unique_ptr<MultiFileList> GlobMultiFileList::GetFirstFileList(idx_t max_files) {
	if (first_expanded_files.size() < max_files) {
		ClearPeek();
		PeekFile(max_files - 1);
	}

	auto list = make_uniq<SimpleMultiFileList>(
	    vector<OpenFileInfo>(first_expanded_files.begin(),
	                         first_expanded_files.begin() +
	                             (first_expanded_files.size() < max_files ? first_expanded_files.size() : max_files)));

	return list;
}

OpenFileInfo GlobMultiFileList::GetFile(idx_t i) {
	lock_guard<mutex> lck(lock);
	return GetFileInternal(i, false);
}

OpenFileInfo GlobMultiFileList::PeekFile(idx_t i) {
	lock_guard<mutex> lck(lock);
	return GetFileInternal(i, true);
}

OpenFileInfo GlobMultiFileList::PeekFirstFile() {
	lock_guard<mutex> lck(lock);
	// Ensure at least the first 2 files are expanded if they are available
	GetFileInternal(1, true);
	// Just return the first file
	return GetFileInternal(0, true);
}

OpenFileInfo GlobMultiFileList::GetFileInternal(idx_t i, bool first_files) {
	auto &files = first_files ? first_expanded_files : expanded_files;
	while (files.size() <= i) {
		if (!ExpandNextPath(first_files ? i + 1 : std::numeric_limits<idx_t>::max(), first_files)) {
			return OpenFileInfo("");
		}
	}
	D_ASSERT(files.size() > i);
	return files[i];
}

void GlobMultiFileList::Clear() {
	lock_guard<mutex> lck(lock);
	ClearInternal();
}

void GlobMultiFileList::ClearInternal() {
	expanded_files.clear();
	current_path = 0;
}

void GlobMultiFileList::ClearPeek() {
	lock_guard<mutex> lck(lock);
	ClearPeekInternal();
}

void GlobMultiFileList::ClearPeekInternal() {
	first_expanded_files.clear();
	first_files_current_path = 0;
}

bool GlobMultiFileList::ExpandPathInternal(idx_t &current_path, vector<OpenFileInfo> &result, idx_t max_files,
                                           optional_ptr<HiveFilterParams> hive_filter_params) const {
	if (current_path >= paths.size()) {
		return false;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto glob_files = fs.GlobFiles(paths[current_path].path, context, glob_options, max_files, hive_filter_params);
	std::sort(glob_files.begin(), glob_files.end());
	result.insert(result.end(), glob_files.begin(), glob_files.end());

	current_path++;
	return true;
}

bool GlobMultiFileList::ExpandNextPath(idx_t max_files, bool first_files,
                                       optional_ptr<HiveFilterParams> hive_filter_params) {
	return ExpandPathInternal(first_files ? first_files_current_path : current_path,
	                          first_files ? first_expanded_files : expanded_files, max_files, hive_filter_params);
}

bool GlobMultiFileList::IsFullyExpanded() const {
	return current_path == paths.size();
}

} // namespace duckdb
