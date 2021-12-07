#include "duckdb/main/pending_query_result.hpp"

namespace duckdb {

PendingQueryResult::PendingQueryResult(StatementType statement_type) :
	BaseQueryResult(QueryResultType::PENDING_RESULT, statement_type) {}

PendingQueryResult::PendingQueryResult(StatementType statement_type, vector<LogicalType> types, vector<string> names) :
	BaseQueryResult(QueryResultType::PENDING_RESULT, statement_type, move(types), move(names)) {}

PendingQueryResult::PendingQueryResult(string error) :
	BaseQueryResult(QueryResultType::PENDING_RESULT, move(error)) {

}

PendingQueryResult::~PendingQueryResult() {}

PendingExecutionResult PendingQueryResult::ExecuteTask() {
	throw InternalException("FIXME: execute task");
}

unique_ptr<QueryResult> PendingQueryResult::Execute(bool allow_streaming_result) {
	throw InternalException("FIXME: execute");
}


}
