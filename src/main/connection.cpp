#include "main/connection.hpp"

#include "main/client_context.hpp"
#include "main/connection_manager.hpp"
#include "main/database.hpp"

using namespace duckdb;
using namespace std;

Connection::Connection(DuckDB &database) : db(database), context(make_unique<ClientContext>(database)) {
	db.connection_manager->AddConnection(this);
#ifdef DEBUG
	EnableProfiling();
#endif
}

Connection::~Connection() {
	if (!context->is_invalidated) {
		context->Cleanup();
		db.connection_manager->RemoveConnection(this);
	}
}

string Connection::GetProfilingInformation() {
	if (context->is_invalidated) {
		return "Context is invalidated.";
	}
	return context->profiler.ToString();
}

void Connection::Interrupt() {
	context->Interrupt();
}

void Connection::EnableProfiling() {
	context->EnableProfiling();
}

void Connection::DisableProfiling() {
	context->DisableProfiling();
}

void Connection::EnableQueryVerification() {
#ifdef DEBUG
	context->query_verification_enabled = true;
#endif
}

unique_ptr<QueryResult> Connection::SendQuery(string query) {
	return context->Query(query, true);
}

unique_ptr<MaterializedQueryResult> Connection::Query(string query) {
	auto result = context->Query(query, false);
	assert(result->type == QueryResultType::MATERIALIZED_RESULT);
	return unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
}
