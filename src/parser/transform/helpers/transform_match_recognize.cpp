#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/tableref/match_recognize_ref.hpp"
#include "duckdb/parser/expression/pattern_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformPattern(duckdb_libpgquery::PGMatchRecognizePattern *pattern) {
	D_ASSERT(pattern);
	switch (pattern->type) {
	case duckdb_libpgquery::PGMatchRecognizePatternLabel: {
		auto name = PGPointerCast<duckdb_libpgquery::PGValue>(pattern->child_left);
		auto child = make_uniq_base<ParsedExpression, ColumnRefExpression>(name.get()->val.str);
		auto quantifiers = ParseQuantifier(pattern->min_count, pattern->max_count);
		return make_uniq_base<ParsedExpression, QuantifiedExpression>(std::move(child), quantifiers.first,
		                                                              quantifiers.second);
	}
	case duckdb_libpgquery::PGMatchRecognizePatternGrouping: {
		auto child = TransformPatternList(PGPointerCast<duckdb_libpgquery::PGList>(pattern->child_left).get());
		auto quantifiers = ParseQuantifier(pattern->min_count, pattern->max_count);
		return make_uniq_base<ParsedExpression, QuantifiedExpression>(std::move(child), quantifiers.first,
		                                                              quantifiers.second);
	}
	case duckdb_libpgquery::PGMatchRecognizePatternAlternation: {
		auto child_left = TransformPatternList(PGPointerCast<duckdb_libpgquery::PGList>(pattern->child_left).get());
		auto child_right = TransformPatternList(PGPointerCast<duckdb_libpgquery::PGList>(pattern->child_right).get());

		return make_uniq_base<ParsedExpression, AlternationExpression>(std::move(child_left), std::move(child_right));
	}
	default:
		throw NotImplementedException("Unknown pattern type %d", pattern->type);
	}
}

unique_ptr<ParsedExpression> Transformer::TransformPatternList(duckdb_libpgquery::PGList *pattern_list) {
	D_ASSERT(pattern_list);
	D_ASSERT(pattern_list->length > 0);
	if (pattern_list->length == 1) { // special case we can short-cut this
		return TransformPattern(
		    PGPointerCast<duckdb_libpgquery::PGMatchRecognizePattern>(pattern_list->head->data.ptr_value).get());
	}
	vector<unique_ptr<ParsedExpression>> children;
	for (auto node = pattern_list->head; node != nullptr; node = node->next) {
		auto child_pattern = PGPointerCast<duckdb_libpgquery::PGMatchRecognizePattern>(node->data.ptr_value);
		auto expr = TransformPattern(child_pattern.get());
		children.push_back(std::move(expr));
	}
	return make_uniq_base<ParsedExpression, ConcatenationExpression>(std::move(children));
}

unique_ptr<TableRef> Transformer::TransformMatchRecognize(unique_ptr<TableRef> table,
                                                          optional_ptr<duckdb_libpgquery::PGNode> options) {
	D_ASSERT(options);
	auto match_recognize_stmt = PGPointerCast<duckdb_libpgquery::PGMatchRecognizeStmt>(options.get());
	auto match_recognize_config = make_uniq<MatchRecognizeConfig>();
	// optional partition
	if (match_recognize_stmt->partition_clause) {
		TransformExpressionList(*match_recognize_stmt->partition_clause, match_recognize_config->partition_expressions);
	}

	// optional ordering
	if (match_recognize_stmt->order_clause) {
		TransformOrderBy(match_recognize_stmt->order_clause, match_recognize_config->order_by_expressions);
	}

	// measures, I guess this is required?
	D_ASSERT(match_recognize_stmt->measures_clause);
	TransformExpressionList(*match_recognize_stmt->measures_clause, match_recognize_config->measures_expression_list);

	// rows per match
	if (match_recognize_stmt->rows_per_match == duckdb_libpgquery::PGMatchRecognizeRowsPerMatchDefault) {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
		match_recognize_config->rows_per_match = MatchRecognizeRows::MATCH_RECOGNIZE_ROWS_ONE;
#else
		match_recognize_config->rows_per_match = MatchRecognizeRows::MATCH_RECOGNIZE_ROWS_DEFAULT;
#endif
	} else if (match_recognize_stmt->rows_per_match == duckdb_libpgquery::PGMatchRecognizeRowsPerMatchOneRow) {
		match_recognize_config->rows_per_match = MatchRecognizeRows::MATCH_RECOGNIZE_ROWS_ONE;
	} else if (match_recognize_stmt->rows_per_match == duckdb_libpgquery::PGMatchRecognizeRowsPerMatchAllRows) {
		match_recognize_config->rows_per_match = MatchRecognizeRows::MATCH_RECOGNIZE_ROWS_ALL;
	}

	// after match skip option
	D_ASSERT(match_recognize_stmt->after_match_clause);
	if (match_recognize_stmt->after_match_clause->after_match == duckdb_libpgquery::PGMatchRecognizeAfterMatchDefault) {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
		match_recognize_config->after_match = MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_LAST_ROW;
#else
		match_recognize_config->after_match = MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_DEFAULT;
#endif
	} else if (match_recognize_stmt->after_match_clause->after_match ==
	           duckdb_libpgquery::PGMatchRecognizeAfterMatchNextRow) {
		match_recognize_config->after_match = MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_NEXT_ROW;
	} else if (match_recognize_stmt->after_match_clause->after_match ==
	           duckdb_libpgquery::PGMatchRecognizeAfterMatchLastRow) {
		match_recognize_config->after_match = MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_LAST_ROW;
	} else if (match_recognize_stmt->after_match_clause->after_match ==
	           duckdb_libpgquery::PGMatchRecognizeAfterMatchFirstVar) {
		match_recognize_config->after_match = MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_FIRST_VAR;
		match_recognize_config->after_match_variable =
		    TransformValue(*match_recognize_stmt->after_match_clause->variable);
	} else if (match_recognize_stmt->after_match_clause->after_match ==
	           duckdb_libpgquery::PGMatchRecognizeAfterMatchLastVar) {
		match_recognize_config->after_match = MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_LAST_VAR;
		match_recognize_config->after_match_variable =
		    TransformValue(*match_recognize_stmt->after_match_clause->variable);
	}

	// pattern
	if (!match_recognize_stmt->pattern_clause || list_length(match_recognize_stmt->pattern_clause) < 1) {
		throw ParserException("MATCH_RECOGNIZE needs a non-empty pattern");
	}
	auto pattern = TransformPatternList(match_recognize_stmt->pattern_clause);
	match_recognize_config->pattern = std::move(pattern);

	// defines, this is required?
	D_ASSERT(match_recognize_stmt->defines_clause);
	TransformExpressionList(*match_recognize_stmt->defines_clause, match_recognize_config->defines_expression_list);

	auto result = make_uniq<MatchRecognizeRef>(std::move(table), std::move(match_recognize_config));

	if (match_recognize_stmt->alias) {
		result->alias = TransformAlias(match_recognize_stmt->alias, result->column_name_alias);
	}

	return std::move(result);
}

} // namespace duckdb
