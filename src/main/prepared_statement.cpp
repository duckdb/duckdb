#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

PreparedStatement::~PreparedStatement() {
	if (!is_invalidated && success) {
		assert(context);
		context->RemovePreparedStatement(this);
	}
}

unique_ptr<QueryResult> PreparedStatement::Execute(vector<Value> &values, bool allow_stream_result) {
	if (!success) {
		return make_unique<MaterializedQueryResult>("Attempting to execute an unsuccessfully prepared statement!");
	}
	if (is_invalidated) {
		return make_unique<MaterializedQueryResult>(
		    "Cannot execute prepared statement: underlying database or connection has been destroyed");
	}
	assert(context);
	return context->Execute(name, values, allow_stream_result);
}
