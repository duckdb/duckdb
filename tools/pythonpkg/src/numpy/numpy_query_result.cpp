#include "duckdb_python/numpy/numpy_query_result.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

NumpyQueryResult::NumpyQueryResult(StatementType statement_type, StatementProperties properties, vector<string> names_p,
                                   unique_ptr<NumpyResultConversion> collection_p, ClientProperties client_properties)
    : QueryResult(QueryResultType::NUMPY_RESULT, statement_type, std::move(properties), collection_p->Types(),
                  std::move(names_p), std::move(client_properties)),
      collection(std::move(collection_p)) {
}

NumpyQueryResult::NumpyQueryResult(PreservedError error)
    : QueryResult(QueryResultType::NUMPY_RESULT, std::move(error)) {
}

unique_ptr<DataChunk> NumpyQueryResult::Fetch() {
	throw NotImplementedException("Can't 'Fetch' from NumpyQueryResult");
}
unique_ptr<DataChunk> NumpyQueryResult::FetchRaw() {
	throw NotImplementedException("Can't 'FetchRaw' from NumpyQueryResult");
}

string NumpyQueryResult::ToString() {
	//throw NotImplementedException("Can't convert NumpyQueryResult to string");
	return "";
}

string NumpyQueryResult::ToBox(ClientContext &context, const BoxRendererConfig &config) {
	//throw NotImplementedException("Can't convert NumpyQueryResult to a box-rendered string");
	return "";
}

idx_t NumpyQueryResult::RowCount() const {
	return collection ? collection->Count() : 0;
}

NumpyResultConversion &NumpyQueryResult::Collection() {
	if (HasError()) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	if (!collection) {
		throw InternalException("Missing result from numpy query result");
	}
	return *collection;
}

} // namespace duckdb
