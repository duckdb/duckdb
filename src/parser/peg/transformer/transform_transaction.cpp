#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformTransactionStatement(PEGTransformer &transformer,
                                                                              ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.Transform<unique_ptr<TransactionStatement>>(choice_pr.GetResult());
}

unique_ptr<TransactionStatement> PEGTransformerFactory::TransformBeginTransaction(PEGTransformer &transformer,
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

TransactionModifierType PEGTransformerFactory::TransformReadOrWrite(PEGTransformer &transformer,
                                                                    ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<TransactionModifierType>(list_pr.Child<ListParseResult>(1));
}

TransactionModifierType PEGTransformerFactory::TransformReadOnlyOrReadWrite(PEGTransformer &transformer,
                                                                            ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<TransactionModifierType>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

unique_ptr<TransactionStatement> PEGTransformerFactory::TransformCommitTransaction(PEGTransformer &, ParseResult &) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::COMMIT));
}

unique_ptr<TransactionStatement> PEGTransformerFactory::TransformRollbackTransaction(PEGTransformer &, ParseResult &) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::ROLLBACK));
}

unique_ptr<TransactionStatement> PEGTransformerFactory::TransformSetTransactionSnapshot(PEGTransformer &transformer,
                                                                                        ParseResult &parse_result) {
	// SetTransactionSnapshot <- 'SET' Transaction 'SNAPSHOT' StringLiteral
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto info = make_uniq<TransactionInfo>(TransactionType::SET_SNAPSHOT);
	info->snapshot_id = transformer.Transform<string>(list_pr.Child<StringLiteralParseResult>(3));
	return make_uniq<TransactionStatement>(std::move(info));
}
} // namespace duckdb
