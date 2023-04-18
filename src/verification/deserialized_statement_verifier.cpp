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
	/*BufferedSerializer serializer;
	select_stmt.Serialize(serializer);
	BufferedDeserializer source(serializer);
	 */

	BinarySerializer serializer;
	select_stmt.FormatSerialize(serializer);

	BufferedDeserializer buffer(serializer.GetRootBlobData(), serializer.GetRootBlobSize());
	BinaryDeserializer deserializer(buffer);

	return make_uniq<DeserializedStatementVerifier>(SelectStatement::FormatDeserialize(deserializer));
}

} // namespace duckdb
