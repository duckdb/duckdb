#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformBeginTransaction(PEGTransformer &transformer,
                                                 const TransactionIsolationLevel &isolation_level_clause,
                                                 const TransactionModifierType &read_or_write) {
	auto info = make_uniq<TransactionInfo>(TransactionType::BEGIN_TRANSACTION);
	info->modifier = read_or_write;
	info->isolation_level = isolation_level_clause;
	return make_uniq<TransactionStatement>(std::move(info));
}

// IsolationLevelClause <- 'ISOLATION' 'LEVEL' IsolationLevel
TransactionIsolationLevel
PEGTransformerFactory::TransformIsolationLevelClause(PEGTransformer &transformer,
                                                     const TransactionIsolationLevel &isolation_level) {
	return isolation_level;
}

// IsolationLevel <- ('READ' 'COMMITTED') / ('READ' 'UNCOMMITTED') / ('REPEATABLE' 'READ') / 'SERIALIZABLE'
TransactionIsolationLevel PEGTransformerFactory::TransformIsolationLevel(PEGTransformer &transformer,
                                                                         ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice = list_pr.Child<ChoiceParseResult>(0);
	const auto &name = choice.GetResult().name;
	// Each alternative is matched by its name; choice 0/1 are READ X pairs,
	// choice 2 is REPEATABLE READ, choice 3 is SERIALIZABLE. We dispatch on
	// the keyword sequence text since the alternatives themselves are unnamed.
	auto &alt = choice.GetResult();
	if (alt.type == ParseResultType::LIST) {
		auto &kw_list = alt.Cast<ListParseResult>();
		auto &kw0 = kw_list.Child<KeywordParseResult>(0);
		if (StringUtil::CIEquals(kw0.keyword, "READ")) {
			auto &kw1 = kw_list.Child<KeywordParseResult>(1);
			if (StringUtil::CIEquals(kw1.keyword, "COMMITTED")) {
				return TransactionIsolationLevel::READ_COMMITTED;
			}
			return TransactionIsolationLevel::READ_UNCOMMITTED;
		}
		return TransactionIsolationLevel::REPEATABLE_READ;
	}
	return TransactionIsolationLevel::SERIALIZABLE;
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

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCommitTransaction(PEGTransformer &transformer) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::COMMIT));
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformRollbackTransaction(PEGTransformer &transformer) {
	return make_uniq<TransactionStatement>(make_uniq<TransactionInfo>(TransactionType::ROLLBACK));
}
} // namespace duckdb
