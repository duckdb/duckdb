#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/main/query_result_manager.hpp"

namespace duckdb {

MaterializedQueryResult::MaterializedQueryResult(StatementType statement_type, StatementProperties properties,
                                                 vector<string> names_p,
                                                 shared_ptr<ManagedQueryResult> managed_result_p,
                                                 ClientProperties client_properties)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, statement_type, std::move(properties),
                  managed_result_p->Collection().Types(), std::move(names_p), std::move(client_properties)),
      managed_result(std::move(managed_result_p)), scan_initialized(false) {
}

MaterializedQueryResult::MaterializedQueryResult(ErrorData error)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, std::move(error)), scan_initialized(false) {
}

string MaterializedQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		auto &collection = Collection();
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
	if (!HasCollection()) {
		return "Internal error - result was successful but there was no collection";
	}
	BoxRenderer renderer(config);
	return renderer.ToString(context, names, Collection());
}

Value MaterializedQueryResult::GetValue(idx_t column, idx_t index) {
	if (!row_collection) {
		row_collection = make_uniq<ColumnDataRowCollection>(Collection().GetRows());
	}
	return row_collection->GetValue(column, index);
}

idx_t MaterializedQueryResult::RowCount() const {
	return HasCollection() ? Collection().Count() : 0;
}

bool MaterializedQueryResult::HasCollection() const {
	return managed_result.get();
}

void MaterializedQueryResult::ValidateManagedResultInternal() const {
	if (HasError()) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	if (!HasCollection()) {
		throw InternalException("Missing collection from materialized query result");
	}
}

ColumnDataCollection &MaterializedQueryResult::Collection() const {
	ValidateManagedResultInternal();
	return managed_result->Collection();
}

shared_ptr<ManagedQueryResult> MaterializedQueryResult::GetManagedResult() {
	ValidateManagedResultInternal();
	return managed_result;
}

unique_ptr<DataChunk> MaterializedQueryResult::Fetch() {
	return FetchRaw();
}

unique_ptr<DataChunk> MaterializedQueryResult::FetchRaw() {
	auto &collection = Collection();
	auto &scan_state = managed_result->ScanState();
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
