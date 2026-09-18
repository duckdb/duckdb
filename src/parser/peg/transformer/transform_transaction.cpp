#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformBeginTransaction(PEGTransformer &transformer, const bool &has_result,
                                                 const optional<TransactionModifierType> &read_or_write) {
	auto info = make_uniq<TransactionInfo>(TransactionType::BEGIN_TRANSACTION);
	if (read_or_write) {
		info->modifier = *read_or_write;
	}
	return make_uniq<TransactionStatement>(std::move(info));
}

TransactionModifierType
PEGTransformerFactory::TransformReadOrWrite(PEGTransformer &transformer,
                                            const TransactionModifierType &read_only_or_read_write) {
	return read_only_or_read_write;
}

TransactionModifierType PEGTransformerFactory::TransformReadOnly(PEGTransformer &transformer) {
	return TransactionModifierType::TRANSACTION_READ_ONLY;
}

TransactionModifierType PEGTransformerFactory::TransformReadWrite(PEGTransformer &transformer) {
	return TransactionModifierType::TRANSACTION_READ_WRITE;
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCommitTransaction(PEGTransformer &transformer,
                                                                           const bool &has_result) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::COMMIT));
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformRollbackTransaction(PEGTransformer &transformer,
                                                                             const bool &has_result) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::ROLLBACK));
}
} // namespace duckdb
