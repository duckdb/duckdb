#include "duckdb/parser/statement/transaction_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformTransactionStatement(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.Transform<unique_ptr<TransactionStatement>>(choice_pr.result);
}

unique_ptr<TransactionStatement>
PEGTransformerFactory::TransformBeginTransaction(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto info = make_uniq<TransactionInfo>(TransactionType::BEGIN_TRANSACTION);
	auto &read_or_write = list_pr.Child<OptionalParseResult>(2);
	if (read_or_write.HasResult()) {
		info->modifier = transformer.Transform<TransactionModifierType>(read_or_write.optional_result);
	} else {
		info->modifier = TransactionModifierType::TRANSACTION_DEFAULT_MODIFIER;
	}
	return make_uniq<TransactionStatement>(std::move(info));
}

TransactionModifierType PEGTransformerFactory::TransformReadOrWrite(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &only_or_write = list_pr.Child<ListParseResult>(1);
	auto &only_or_write_choice = only_or_write.Child<ChoiceParseResult>(0);
	return transformer.TransformEnum<TransactionModifierType>(only_or_write_choice.result);
}

unique_ptr<TransactionStatement> PEGTransformerFactory::TransformCommitTransaction(PEGTransformer &,
                                                                                   optional_ptr<ParseResult>) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::COMMIT));
}

unique_ptr<TransactionStatement> PEGTransformerFactory::TransformRollbackTransaction(PEGTransformer &,
                                                                                     optional_ptr<ParseResult>) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::ROLLBACK));
}
} // namespace duckdb
