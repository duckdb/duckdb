#include "duckdb/verification/deserialized_statement_verifier.hpp"

#include "duckdb/common/serializer/buffered_deserializer.hpp"

namespace duckdb {

DeserializedStatementVerifier::DeserializedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier("Deserialized", move(statement_p)) {
}

StatementVerifier DeserializedStatementVerifier::Create(const SQLStatement &statement) {
	auto &select_stmt = (SelectStatement &)statement;
	BufferedSerializer serializer;
	select_stmt.Serialize(serializer);
	BufferedDeserializer source(serializer);
	return DeserializedStatementVerifier(SelectStatement::Deserialize(source));
}

} // namespace duckdb
