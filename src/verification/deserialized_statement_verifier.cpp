#include "duckdb/verification/deserialized_statement_verifier.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
namespace duckdb {

DeserializedStatementVerifier::DeserializedStatementVerifier(
    unique_ptr<SQLStatement> statement_p, optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters)
    : StatementVerifier(VerificationType::DESERIALIZED, "Deserialized", std::move(statement_p), parameters) {
}

unique_ptr<StatementVerifier>
DeserializedStatementVerifier::Create(const SQLStatement &statement,
                                      optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters) {

	auto &select_stmt = statement.Cast<SelectStatement>();
	MemoryStream stream;
	BinarySerializer::Serialize(select_stmt, stream);
	stream.Rewind();
	auto result = BinaryDeserializer::Deserialize<SelectStatement>(stream);

	return make_uniq<DeserializedStatementVerifier>(std::move(result), parameters);
}

} // namespace duckdb
