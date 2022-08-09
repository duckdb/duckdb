#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

MaterializedQueryResult::MaterializedQueryResult(StatementType statement_type, StatementProperties properties,
                                                 vector<string> names_p, unique_ptr<ColumnDataCollection> collection_p,
                                                 ClientProperties client_properties)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, statement_type, properties, collection_p->Types(),
                  move(names_p), move(client_properties)),
      collection(move(collection_p)), scan_initialized(false) {
}

MaterializedQueryResult::MaterializedQueryResult(string error)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, move(error)), scan_initialized(false) {
}

string MaterializedQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[ Rows: " + to_string(collection->Count()) + "]\n";
		auto &coll = Collection();
		for (auto &row : coll.Rows()) {
			for (idx_t col_idx = 0; col_idx < coll.ColumnCount(); col_idx++) {
				if (col_idx > 0) {
					result += "\t";
				}
				auto val = row.GetValue(col_idx);
				result += val.IsNull() ? "NULL" : val.ToString();
			}
			result += "\n";
		}
		result += "\n";
	} else {
		result = error + "\n";
	}
	return result;
}

Value MaterializedQueryResult::GetValue(idx_t column, idx_t index) {
	if (!row_collection) {
		row_collection = make_unique<ColumnDataRowCollection>(collection->GetRows());
	}
	return row_collection->GetValue(column, index);
}

idx_t MaterializedQueryResult::RowCount() const {
	return collection ? collection->Count() : 0;
}

ColumnDataCollection &MaterializedQueryResult::Collection() {
	if (!success) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            error);
	}
	if (!collection) {
		throw InternalException("Missing collection from materialized query result");
	}
	return *collection;
}

unique_ptr<DataChunk> MaterializedQueryResult::Fetch() {
	return FetchRaw();
}

unique_ptr<DataChunk> MaterializedQueryResult::FetchRaw() {
	if (!success) {
		throw InvalidInputException("Attempting to fetch from an unsuccessful query result\nError: %s", error);
	}
	auto result = make_unique<DataChunk>();
	collection->InitializeScanChunk(*result);
	if (!scan_initialized) {
		// we disallow zero copy so the chunk is independently usable even after the result is destroyed
		collection->InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
		scan_initialized = true;
	}
	collection->Scan(scan_state, *result);
	if (result->size() == 0) {
		return nullptr;
	}
	return result;
}

} // namespace duckdb
