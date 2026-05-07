#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformBeginTransaction(TransactionModifierType read_or_write) {
	auto info = make_uniq<TransactionInfo>(TransactionType::BEGIN_TRANSACTION);
	info->modifier = read_or_write;
	return make_uniq<TransactionStatement>(std::move(info));
}

TransactionModifierType PEGTransformerFactory::TransformReadOrWrite(TransactionModifierType read_only_or_read_write) {
	return read_only_or_read_write;
}

TransactionModifierType PEGTransformerFactory::TransformReadOnlyOrReadWrite(PEGTransformer &transformer,
                                                                            ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<TransactionModifierType>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCommitTransaction() {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::COMMIT));
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformRollbackTransaction() {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::ROLLBACK));
}
} // namespace duckdb
