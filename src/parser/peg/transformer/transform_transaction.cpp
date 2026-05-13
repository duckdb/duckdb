#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformBeginTransaction(PEGTransformer &transformer,
                                                 const TransactionModifierType &read_or_write) {
	auto info = make_uniq<TransactionInfo>(TransactionType::BEGIN_TRANSACTION);
	info->modifier = read_or_write;
	return make_uniq<TransactionStatement>(std::move(info));
}

TransactionModifierType
PEGTransformerFactory::TransformReadOrWrite(PEGTransformer &transformer,
                                            const TransactionModifierType &read_only_or_read_write) {
	return read_only_or_read_write;
}

TransactionModifierType PEGTransformerFactory::TransformReadOnlyOrReadWrite(PEGTransformer &transformer,
                                                                            ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<TransactionModifierType>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

TransactionModifierType PEGTransformerFactory::TransformReadOnly(PEGTransformer &transformer) {
	return TransactionModifierType::TRANSACTION_READ_ONLY;
}

TransactionModifierType PEGTransformerFactory::TransformReadWrite(PEGTransformer &transformer) {
	return TransactionModifierType::TRANSACTION_READ_WRITE;
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCommitTransaction(PEGTransformer &transformer) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::COMMIT));
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformRollbackTransaction(PEGTransformer &transformer) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::ROLLBACK));
}
} // namespace duckdb
