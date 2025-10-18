#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/main/result_set_manager.hpp"

namespace duckdb {

MaterializedQueryResult::MaterializedQueryResult(StatementType statement_type, StatementProperties properties,
                                                 vector<string> names_p, shared_ptr<ManagedResultSet> result_set_p,
                                                 ClientProperties client_properties)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, statement_type, std::move(properties),
                  result_set_p->Pin()->collection.Types(), std::move(names_p), std::move(client_properties)),
      result_set(std::move(result_set_p)), scan_initialized(false) {
}

MaterializedQueryResult::MaterializedQueryResult(ErrorData error)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, std::move(error)), scan_initialized(false) {
}

string MaterializedQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		auto pinned_result_set = Pin();
		auto &collection = pinned_result_set->collection;
		result += "[ Rows: " + to_string(collection.Count()) + "]\n";
		for (auto &row : collection.Rows()) {
			for (idx_t col_idx = 0; col_idx < collection.ColumnCount(); col_idx++) {
				if (col_idx > 0) {
					result += "\t";
				}
				auto val = row.GetValue(col_idx);
				result += val.IsNull() ? "NULL" : StringUtil::Replace(val.ToString(), string("\0", 1), "\\0");
			}
			result += "\n";
		}
		result += "\n";
	} else {
		result = GetError() + "\n";
	}
	return result;
}

string MaterializedQueryResult::ToBox(ClientContext &context, const BoxRendererConfig &config) {
	if (!success) {
		return GetError() + "\n";
	}
	if (!HasManagedResult()) {
		return "Internal error - result was successful but there was no collection";
	}
	auto pinned_result_set = Pin();
	BoxRenderer renderer(config);
	return renderer.ToString(context, names, pinned_result_set->collection);
}

Value MaterializedQueryResult::GetValue(idx_t column, idx_t index) {
	auto pinned_result_set = Pin();
	if (!row_collection) {
		row_collection = make_uniq<ColumnDataRowCollection>(pinned_result_set->collection.GetRows());
	}
	return row_collection->GetValue(column, index);
}

idx_t MaterializedQueryResult::RowCount() const {
	if (!HasManagedResult()) {
		return 0;
	}
	auto pinned_result_set = Pin();
	return pinned_result_set->collection.Count();
}

bool MaterializedQueryResult::HasManagedResult() const {
	return result_set.get();
}

void MaterializedQueryResult::ValidateManagedResultInternal() const {
	if (HasError()) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	if (!HasManagedResult()) {
		throw InternalException("Missing collection from materialized query result");
	}
}

unique_ptr<PinnedResultSet> MaterializedQueryResult::Pin() const {
	ValidateManagedResultInternal();
	return result_set->Pin();
}

shared_ptr<ManagedResultSet> MaterializedQueryResult::GetManagedResultSet() {
	ValidateManagedResultInternal();
	return result_set;
}

unique_ptr<DataChunk> MaterializedQueryResult::Fetch() {
	return FetchRaw();
}

unique_ptr<DataChunk> MaterializedQueryResult::FetchRaw() {
	auto pinned_result_set = Pin();
	auto &collection = pinned_result_set->collection;
	auto &scan_state = pinned_result_set->scan_state;
	auto result = make_uniq<DataChunk>();
	// Use default allocator so the chunk is independently usable even after the DB allocator is destroyed
	collection.InitializeScanChunk(Allocator::DefaultAllocator(), *result);
	if (!scan_initialized) {
		// we disallow zero copy so the chunk is independently usable even after the result is destroyed
		collection.InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
		scan_initialized = true;
	}
	collection.Scan(scan_state, *result);
	if (result->size() == 0) {
		return nullptr;
	}
	return result;
}

} // namespace duckdb
