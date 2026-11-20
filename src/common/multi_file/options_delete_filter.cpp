#include "duckdb/common/multi_file/options_delete_filter.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

static const char *DELETED_ROWS_OPTION = "deleted_rows";
static const char *SCAN_ROWS_OPTION = "scan_rows";
static const char *DELETE_FILE_OPTION = "delete_file";
static const char *DELETE_FILE_FUNCTION_OPTION = "delete_file_function";
static const char *SNAPSHOT_FILTER_OPTION = "delete_snapshot_filter";
static const char *MAX_ROW_COUNT_OPTION = "max_row_count";
static const char *SNAPSHOT_COLUMN = "_ducklake_internal_snapshot_id";

static bool HasOption(const OpenFileInfo &file, const char *name) {
	auto entry = file.extended_info->options.find(name);
	return entry != file.extended_info->options.end() && !entry->second.IsNull();
}

bool OptionsDeleteFilter::HasDeleteOptions(const OpenFileInfo &file) {
	return file.extended_info && (HasOption(file, DELETED_ROWS_OPTION) || HasOption(file, DELETE_FILE_OPTION) ||
	                              HasOption(file, SCAN_ROWS_OPTION) || HasOption(file, MAX_ROW_COUNT_OPTION));
}

unique_ptr<DeleteFilter> OptionsDeleteFilter::TryCreate(ClientContext &context, const OpenFileInfo &file) {
	if (!HasDeleteOptions(file)) {
		return nullptr;
	}
	auto &options = file.extended_info->options;
	bool has_deleted_rows = HasOption(file, DELETED_ROWS_OPTION);
	bool has_delete_file = HasOption(file, DELETE_FILE_OPTION);
	auto deleted_rows_entry = options.find(DELETED_ROWS_OPTION);
	auto delete_file_entry = options.find(DELETE_FILE_OPTION);
	auto result = make_uniq<OptionsDeleteFilter>();
	idx_t value;
	if (file.extended_info->TryGetOption(MAX_ROW_COUNT_OPTION, value)) {
		result->max_row_count = value;
	}
	if (file.extended_info->TryGetOption(SNAPSHOT_FILTER_OPTION, value)) {
		result->snapshot_filter = value;
	}
	if (has_deleted_rows) {
		for (auto &position : ListValue::GetChildren(deleted_rows_entry->second)) {
			result->deleted_rows.push_back(UBigIntValue::Get(position.DefaultCastAs(LogicalType::UBIGINT)));
		}
	}
	if (HasOption(file, SCAN_ROWS_OPTION)) {
		result->has_scan_rows = true;
		for (auto &position : ListValue::GetChildren(options.at(SCAN_ROWS_OPTION))) {
			result->scan_rows.push_back(UBigIntValue::Get(position.DefaultCastAs(LogicalType::UBIGINT)));
		}
		sort(result->scan_rows.begin(), result->scan_rows.end());
	}
	if (has_delete_file) {
		string function_name = "read_parquet";
		file.extended_info->TryGetOption(DELETE_FILE_FUNCTION_OPTION, function_name);
		for (auto c : function_name) {
			if (!StringUtil::CharacterIsAlphaNumeric(c) && c != '_') {
				throw InvalidInputException("Invalid delete file function \"%s\"", function_name);
			}
		}
		result->AddDeleteFile(context, delete_file_entry->second, function_name);
	}
	if (result->snapshot_ids.empty() && !std::is_sorted(result->deleted_rows.begin(), result->deleted_rows.end())) {
		sort(result->deleted_rows.begin(), result->deleted_rows.end());
	}
	return std::move(result);
}

void OptionsDeleteFilter::AddDeleteFile(ClientContext &context, const Value &delete_file, const string &function_name) {
	// read the delete file on a connection of its own - this runs while the scan holds the lock of its context
	Connection connection(DatabaseInstance::GetDatabase(context));
	auto file = OpenFileInfo::FromValue(delete_file, Identifier("delete_file"));
	auto file_value = file.ToValue();
	auto result = connection.Query("SELECT * FROM " + function_name + "([?])", file_value);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to read delete file " + file.path + ": ");
	}
	optional_idx pos_column;
	optional_idx snapshot_column;
	for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
		auto &name = result->ColumnName(col_idx).GetIdentifierName();
		if (name == "pos") {
			pos_column = col_idx;
		} else if (name == SNAPSHOT_COLUMN) {
			snapshot_column = col_idx;
		}
	}
	if (!pos_column.IsValid()) {
		throw InvalidInputException("Delete file %s must have a \"pos\" column", file.path);
	}
	if (!deleted_rows.empty() && snapshot_column.IsValid()) {
		throw InvalidInputException("Delete file %s with snapshot ids cannot be combined with deleted_rows", file.path);
	}
	vector<pair<idx_t, idx_t>> entries;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			auto position = chunk->GetValue(pos_column.GetIndex(), row_idx);
			if (position.IsNull()) {
				continue;
			}
			idx_t snapshot_id = 0;
			if (snapshot_column.IsValid()) {
				auto snapshot = chunk->GetValue(snapshot_column.GetIndex(), row_idx);
				snapshot_id = snapshot.IsNull() ? 0 : UBigIntValue::Get(snapshot.DefaultCastAs(LogicalType::UBIGINT));
			}
			entries.emplace_back(UBigIntValue::Get(position.DefaultCastAs(LogicalType::UBIGINT)), snapshot_id);
		}
	}
	// positions that were listed without a snapshot are deleted in every snapshot
	bool keep_snapshots = snapshot_column.IsValid() && deleted_rows.empty();
	for (auto &entry : entries) {
		deleted_rows.push_back(entry.first);
		if (keep_snapshots) {
			snapshot_ids.push_back(entry.second);
		}
	}
	if (!keep_snapshots) {
		sort(deleted_rows.begin(), deleted_rows.end());
	}
}

idx_t OptionsDeleteFilter::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	auto start = NumericCast<idx_t>(start_row_index);
	if (max_row_count.IsValid()) {
		auto max_count = max_row_count.GetIndex();
		if (max_count <= start) {
			return 0;
		}
		count = MinValue<idx_t>(max_count - start, count);
	}
	if (has_scan_rows) {
		// only the listed rows are produced - and those that are not deleted
		auto scan_entry = std::lower_bound(scan_rows.begin(), scan_rows.end(), start);
		idx_t result_count = 0;
		result_sel.Initialize(STANDARD_VECTOR_SIZE);
		for (; scan_entry != scan_rows.end() && *scan_entry < start + count; scan_entry++) {
			if (!std::binary_search(deleted_rows.begin(), deleted_rows.end(), *scan_entry)) {
				result_sel.set_index(result_count++, *scan_entry - start);
			}
		}
		return result_count;
	}
	auto entry = std::lower_bound(deleted_rows.begin(), deleted_rows.end(), start);
	if (entry == deleted_rows.end() || *entry >= start + count) {
		// nothing in this range is deleted
		return count;
	}
	auto delete_idx = NumericCast<idx_t>(entry - deleted_rows.begin());
	bool check_snapshots = snapshot_filter.IsValid() && !snapshot_ids.empty();
	result_sel.Initialize(STANDARD_VECTOR_SIZE);
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; i++) {
		// skip duplicates of positions we already passed
		while (delete_idx < deleted_rows.size() && deleted_rows[delete_idx] < start + i) {
			delete_idx++;
		}
		bool is_deleted = false;
		// a position can be listed more than once - it is deleted if any of its entries applies
		for (idx_t cur = delete_idx; cur < deleted_rows.size() && deleted_rows[cur] == start + i; cur++) {
			if (!check_snapshots || snapshot_ids[cur] <= snapshot_filter.GetIndex()) {
				is_deleted = true;
				break;
			}
		}
		if (!is_deleted) {
			result_sel.set_index(result_count++, i);
		}
	}
	return result_count;
}

} // namespace duckdb
