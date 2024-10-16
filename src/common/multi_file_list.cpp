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

MultiFilePushdownInfo::MultiFilePushdownInfo(LogicalGet &get)
    : table_index(get.table_index), column_names(get.names), column_ids(get.GetColumnIds()),
      extra_info(get.extra_info) {
}

MultiFilePushdownInfo::MultiFilePushdownInfo(idx_t table_index, const vector<string> &column_names,
                                             const vector<column_t> &column_ids, ExtraOperatorInfo &extra_info)
    : table_index(table_index), column_names(column_names), column_ids(column_ids), extra_info(extra_info) {
}

// Helper method to do Filter Pushdown into a MultiFileList
bool PushdownInternal(ClientContext &context, const MultiFileReaderOptions &options, MultiFilePushdownInfo &info,
                      vector<unique_ptr<Expression>> &filters, vector<string> &expanded_files) {
	HivePartitioningFilterInfo filter_info;
	for (idx_t i = 0; i < info.column_ids.size(); i++) {
		if (!IsRowIdColumnId(info.column_ids[i])) {
			filter_info.column_map.insert({info.column_names[info.column_ids[i]], i});
		}
	}
	filter_info.hive_enabled = options.hive_partitioning;
	filter_info.filename_enabled = options.filename;

	auto start_files = expanded_files.size();
	HivePartitioning::ApplyFiltersToFileList(context, expanded_files, filters, filter_info, info);

	if (expanded_files.size() != start_files) {
		return true;
	}

	return false;
}

bool PushdownInternal(ClientContext &context, const MultiFileReaderOptions &options, const vector<string> &names,
                      const vector<LogicalType> &types, const vector<column_t> &column_ids,
                      const TableFilterSet &filters, vector<string> &expanded_files) {
	idx_t table_index = 0;
	ExtraOperatorInfo extra_info;

	// construct the pushdown info
	MultiFilePushdownInfo info(table_index, names, column_ids, extra_info);

	// construct the set of expressions from the table filters
	vector<unique_ptr<Expression>> filter_expressions;
	for (auto &entry : filters.filters) {
		auto column_idx = column_ids[entry.first];
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
                                                               const MultiFileReaderOptions &options,
                                                               MultiFilePushdownInfo &info,
                                                               vector<unique_ptr<Expression>> &filters) {
	// By default the filter pushdown into a multifilelist does nothing
	return nullptr;
}

unique_ptr<MultiFileList>
MultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options,
                                     const vector<string> &names, const vector<LogicalType> &types,
                                     const vector<column_t> &column_ids, TableFilterSet &filters) const {
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
SimpleMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options,
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
                                                                   MultiFilePushdownInfo &info,
                                                                   vector<unique_ptr<Expression>> &filters) {
	lock_guard<mutex> lck(lock);

	// Expand all
	// FIXME: lazy expansion
	// FIXME: push down filters into glob
	while (ExpandNextPath()) {
	}

	if (!options.hive_partitioning && !options.filename) {
		return nullptr;
	}
	auto res = PushdownInternal(context, options, info, filters, expanded_files);
	if (res) {
		return make_uniq<SimpleMultiFileList>(expanded_files);
	}

	return nullptr;
}

unique_ptr<MultiFileList>
GlobMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options,
                                         const vector<string> &names, const vector<LogicalType> &types,
                                         const vector<column_t> &column_ids, TableFilterSet &filters) const {
	if (!options.hive_partitioning && !options.filename) {
		return nullptr;
	}
	lock_guard<mutex> lck(lock);

	// Expand all paths into a copy
	// FIXME: lazy expansion and push filters into glob
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

vector<string> GlobMultiFileList::GetAllFiles() {
	lock_guard<mutex> lck(lock);
	while (ExpandNextPath()) {
	}
	return expanded_files;
}

idx_t GlobMultiFileList::GetTotalFileCount() {
	lock_guard<mutex> lck(lock);
	while (ExpandNextPath()) {
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
		if (!ExpandNextPath()) {
			return "";
		}
	}
	D_ASSERT(expanded_files.size() > i);
	return expanded_files[i];
}

bool GlobMultiFileList::ExpandPathInternal(idx_t &current_path, vector<string> &result) const {
	if (current_path >= paths.size()) {
		return false;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto glob_files = fs.GlobFiles(paths[current_path], context, glob_options);
	std::sort(glob_files.begin(), glob_files.end());
	result.insert(result.end(), glob_files.begin(), glob_files.end());

	current_path++;
	return true;
}

bool GlobMultiFileList::ExpandNextPath() {
	return ExpandPathInternal(current_path, expanded_files);
}

bool GlobMultiFileList::IsFullyExpanded() const {
	return current_path == paths.size();
}

} // namespace duckdb
