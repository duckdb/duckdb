#include "duckdb/common/multi_file/table_function_delete_filter.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

static const char *SCAN_DELETED_OPTION = "scan_deleted_rows";
static const char *DELETE_QUERY_OPTION = "delete_query";
static const char *MAX_ROW_COUNT_OPTION = "max_row_count";

static bool HasOption(const OpenFileInfo &file, const char *name) {
	auto entry = file.extended_info->options.find(name);
	return entry != file.extended_info->options.end() && !entry->second.IsNull();
}

bool TableFunctionDeleteFilter::HasDeleteOptions(const OpenFileInfo &file) {
	return file.extended_info && (HasOption(file, DELETE_QUERY_OPTION) || HasOption(file, SCAN_DELETED_OPTION) ||
	                              HasOption(file, MAX_ROW_COUNT_OPTION));
}

unique_ptr<DeleteFilter> TableFunctionDeleteFilter::TryCreate(ClientContext &context, const OpenFileInfo &file) {
	if (!HasDeleteOptions(file)) {
		return nullptr;
	}
	auto &options = file.extended_info->options;
	auto result = make_uniq<TableFunctionDeleteFilter>();
	idx_t value;
	if (file.extended_info->TryGetOption(MAX_ROW_COUNT_OPTION, value)) {
		result->max_row_count = value;
	}
	file.extended_info->TryGetOption(SCAN_DELETED_OPTION, result->scan_deleted_rows);
	if (result->scan_deleted_rows && !HasOption(file, DELETE_QUERY_OPTION)) {
		throw InvalidInputException("Option \"%s\" of file %s requires a \"%s\"", SCAN_DELETED_OPTION, file.path,
		                            DELETE_QUERY_OPTION);
	}
	if (HasOption(file, DELETE_QUERY_OPTION)) {
		result->AddDeletes(context, StringValue::Get(options.at(DELETE_QUERY_OPTION)), file.path);
	}
	return std::move(result);
}

void TableFunctionDeleteFilter::AddDeletes(ClientContext &context, const string &query, const string &path) {
	// run the query on a connection of its own - this runs while the scan holds the lock of its context
	Connection connection(DatabaseInstance::GetDatabase(context));
	auto result = connection.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to read the deletes of file " + path + ": ");
	}
	optional_idx pos_column;
	for (idx_t col_idx = 0; col_idx < result->ColumnCount(); col_idx++) {
		if (result->ColumnName(col_idx).GetIdentifierName() == "pos") {
			pos_column = col_idx;
		}
	}
	if (!pos_column.IsValid()) {
		throw InvalidInputException("The delete query of file %s must return a \"pos\" column", path);
	}
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			auto position = chunk->GetValue(pos_column.GetIndex(), row_idx);
			if (!position.IsNull()) {
				deleted_rows.push_back(UBigIntValue::Get(position.DefaultCastAs(LogicalType::UBIGINT)));
			}
		}
	}
	sort(deleted_rows.begin(), deleted_rows.end());
	deleted_rows.erase(std::unique(deleted_rows.begin(), deleted_rows.end()), deleted_rows.end());
}

idx_t TableFunctionDeleteFilter::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	auto start = NumericCast<idx_t>(start_row_index);
	if (max_row_count.IsValid()) {
		auto max_count = max_row_count.GetIndex();
		if (max_count <= start) {
			return 0;
		}
		count = MinValue<idx_t>(max_count - start, count);
	}
	if (scan_deleted_rows) {
		// only the rows of the delete query are produced
		auto scan_entry = std::lower_bound(deleted_rows.begin(), deleted_rows.end(), start);
		idx_t result_count = 0;
		result_sel.Initialize(STANDARD_VECTOR_SIZE);
		for (; scan_entry != deleted_rows.end() && *scan_entry < start + count; scan_entry++) {
			result_sel.set_index(result_count++, *scan_entry - start);
		}
		return result_count;
	}
	auto entry = std::lower_bound(deleted_rows.begin(), deleted_rows.end(), start);
	if (entry == deleted_rows.end() || *entry >= start + count) {
		// nothing in this range is deleted
		return count;
	}
	result_sel.Initialize(STANDARD_VECTOR_SIZE);
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; i++) {
		if (entry != deleted_rows.end() && *entry == start + i) {
			entry++;
			continue;
		}
		result_sel.set_index(result_count++, i);
	}
	return result_count;
}

} // namespace duckdb
