#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/multi_statement.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformDeleteStatement(
    PEGTransformer &transformer, CommonTableExpressionMap with_clause, unique_ptr<BaseTableRef> target_opt_alias,
    vector<unique_ptr<TableRef>> delete_using_clause, unique_ptr<ParsedExpression> where_clause,
    vector<unique_ptr<ParsedExpression>> returning_clause) {
	auto result = make_uniq<DeleteStatement>();
	auto &node = *result->node;
	if (!with_clause.map.empty()) {
		node.cte_map = std::move(with_clause);
	}
	node.table = std::move(target_opt_alias);
	node.using_clauses = std::move(delete_using_clause);
	node.condition = std::move(where_clause);
	node.returning_list = std::move(returning_clause);
	return std::move(result);
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformTargetOptAlias(PEGTransformer &transformer,
                                                                        unique_ptr<BaseTableRef> base_table_name,
                                                                        const string &col_id) {
	if (!col_id.empty()) {
		base_table_name->alias = col_id;
	}
	return base_table_name;
}

vector<unique_ptr<TableRef>> PEGTransformerFactory::TransformDeleteUsingClause(PEGTransformer &transformer,
                                                                               vector<unique_ptr<TableRef>> table_ref) {
	return table_ref;
}

// TRUNCATE [TABLE] [ONLY] t1 [*], [ONLY] t2 [*], ... [RESTART | CONTINUE IDENTITY] [CASCADE | RESTRICT]
// Lowers to one DeleteStatement per relation with is_truncate=true on the
// DeleteQueryNode. Multi-relation produces a MultiStatement. ONLY / * /
// RESTART | CONTINUE IDENTITY / CASCADE | RESTRICT are accepted for PG-syntax
// compat but ignored: no inheritance, no FK enforcement, no OWNED-BY identity
// sequences.
unique_ptr<SQLStatement> PEGTransformerFactory::TransformTruncateStatement(PEGTransformer &transformer,
                                                                           ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto target_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));

	auto build_one = [&](ParseResult &target_pr) -> unique_ptr<DeleteStatement> {
		auto del = make_uniq<DeleteStatement>();
		del->node->table = transformer.Transform<unique_ptr<BaseTableRef>>(target_pr);
		del->node->is_truncate = true;
		return del;
	};

	if (target_list.size() == 1) {
		return build_one(target_list[0].get());
	}

	auto multi = make_uniq<MultiStatement>();
	for (auto target : target_list) {
		multi->statements.push_back(build_one(target.get()));
	}
	return std::move(multi);
}

// TruncateTarget <- TruncateOnly? BaseTableName TruncateStar?
// ONLY and * are accepted but discarded.
unique_ptr<BaseTableRef> PEGTransformerFactory::TransformTruncateTarget(PEGTransformer &transformer,
                                                                        ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(1));
}

bool PEGTransformerFactory::TransformTruncateOnly(PEGTransformer &transformer, ParseResult &parse_result) {
	return true;
}

bool PEGTransformerFactory::TransformTruncateStar(PEGTransformer &transformer, ParseResult &parse_result) {
	return true;
}

bool PEGTransformerFactory::TransformTruncateIdentityClause(PEGTransformer &transformer, ParseResult &parse_result) {
	return true;
}

bool PEGTransformerFactory::TransformTruncateRestart(PEGTransformer &transformer, ParseResult &parse_result) {
	return true;
}

bool PEGTransformerFactory::TransformTruncateContinue(PEGTransformer &transformer, ParseResult &parse_result) {
	return true;
}

} // namespace duckdb
