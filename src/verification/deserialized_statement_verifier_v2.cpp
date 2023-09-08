#include "duckdb/verification/deserialized_statement_verifier_v2.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
namespace duckdb {

DeserializedStatementVerifierV2::DeserializedStatementVerifierV2(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::DESERIALIZED_V2, "Deserialized V2", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> DeserializedStatementVerifierV2::Create(const SQLStatement &statement) {
	auto &select_stmt = statement.Cast<SelectStatement>();

	auto blob = BinarySerializer::Serialize(select_stmt, true);
	auto result = BinaryDeserializer::Deserialize<SelectStatement>(blob.data(), blob.size());

	return make_uniq<DeserializedStatementVerifierV2>(std::move(result));
}

DeserializedStatementVerifierNoDefaultV2::DeserializedStatementVerifierNoDefaultV2(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::DESERIALIZED_V2_NO_DEFAULT, "Deserialized V2 without default values",
                        std::move(statement_p)) {
}

unique_ptr<StatementVerifier> DeserializedStatementVerifierNoDefaultV2::Create(const SQLStatement &statement) {
	auto &select_stmt = statement.Cast<SelectStatement>();

	auto blob = BinarySerializer::Serialize(select_stmt, false);
	auto result = BinaryDeserializer::Deserialize<SelectStatement>(blob.data(), blob.size());

	return make_uniq<DeserializedStatementVerifierNoDefaultV2>(std::move(result));
}

} // namespace duckdb
