#include "duckdb/common/multi_file/table_function_row_id_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

static const char *ROW_ID_QUERY_OPTION = "row_id_query";

bool TableFunctionRowIdFilter::HasRowIdQuery(const OpenFileInfo &file) {
	if (!file.extended_info) {
		return false;
	}
	auto entry = file.extended_info->options.find(ROW_ID_QUERY_OPTION);
	return entry != file.extended_info->options.end() && !entry->second.IsNull();
}

unique_ptr<DeleteFilter> TableFunctionRowIdFilter::TryCreate(ClientContext &context, BaseFileReader &reader) {
	if (!HasRowIdQuery(reader.file)) {
		return nullptr;
	}
	auto row_count = reader.TryGetRowCount(context);
	if (!row_count.IsValid()) {
		throw NotImplementedException("The row id query of file %s needs the number of rows of the file",
		                              reader.file.path);
	}
	auto result = make_uniq<TableFunctionRowIdFilter>();
	result->row_count = row_count.GetIndex();
	result->scan_rows.Initialize(result->row_count);
	result->scan_rows.SetAllInvalid(result->row_count);
	auto query = StringValue::Get(reader.file.extended_info->options.at(ROW_ID_QUERY_OPTION));
	result->ReadRowIds(context, query, reader.file.path);
	return std::move(result);
}

void TableFunctionRowIdFilter::ReadRowIds(ClientContext &context, const string &query, const string &path) {
	// run the query on a connection of its own - this runs while the scan holds the lock of its context
	Connection connection(DatabaseInstance::GetDatabase(context));
	auto view = connection.Query("CREATE TEMP VIEW __file_row_ids AS SELECT range::BIGINT AS row_id FROM range(" +
	                             to_string(row_count) + ")");
	if (view->HasError()) {
		view->GetErrorObject().Throw("Failed to read the row ids of file " + path + ": ");
	}
	auto result = connection.Query("SELECT CAST(row_id AS UBIGINT) FROM (" + query + ")");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to read the row ids of file " + path + ": ");
	}
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (auto entry : chunk->data[0].ValidValues<uint64_t>()) {
			if (entry.GetValue() < row_count) {
				scan_rows.SetValid(entry.GetValue());
			}
		}
	}
}

idx_t TableFunctionRowIdFilter::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	auto start = NumericCast<idx_t>(start_row_index);
	result_sel.Initialize(STANDARD_VECTOR_SIZE);
	idx_t result_count = 0;
	for (idx_t i = 0; i < count && start + i < row_count; i++) {
		if (scan_rows.RowIsValid(start + i)) {
			result_sel.set_index(result_count++, i);
		}
	}
	return result_count;
}

} // namespace duckdb
