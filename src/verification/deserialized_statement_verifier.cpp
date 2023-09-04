#include "duckdb/verification/deserialized_statement_verifier.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
namespace duckdb {

DeserializedStatementVerifier::DeserializedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::DESERIALIZED, "Deserialized", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> DeserializedStatementVerifier::Create(const SQLStatement &statement) {

	auto &select_stmt = statement.Cast<SelectStatement>();

	BufferedSerializer sink;
	BinarySerializer::Serialize(select_stmt, sink);
	auto blob = sink.GetData();

	BufferedDeserializer source(blob.data.get(), blob.size);
	BinaryDeserializer deserializer(source);
	return make_uniq<DeserializedStatementVerifier>(deserializer.Deserialize<SelectStatement>());
}

} // namespace duckdb
