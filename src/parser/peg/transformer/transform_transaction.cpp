#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformBeginTransaction(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto info = make_uniq<TransactionInfo>(TransactionType::BEGIN_TRANSACTION);
	auto &read_or_write = list_pr.Child<OptionalParseResult>(2);
	if (read_or_write.HasResult()) {
		info->modifier = transformer.Transform<TransactionModifierType>(read_or_write.GetResult());
	} else {
		info->modifier = TransactionModifierType::TRANSACTION_DEFAULT_MODIFIER;
	}
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

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCommitTransaction(PEGTransformer &, ParseResult &) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::COMMIT));
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformRollbackTransaction(PEGTransformer &, ParseResult &) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::ROLLBACK));
}
} // namespace duckdb
