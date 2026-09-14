#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/common/query_location.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/token_iterator.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/enums/merge_action_type.hpp"
#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformStatement(PEGTransformer &transformer,
                                                                   ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	auto result = transformer.Transform<unique_ptr<SQLStatement>>(choice_pr.GetResult());
	if (!transformer.named_parameter_map.empty()) {
		// Avoid overriding a previous move with nothing
		result->named_param_map = transformer.named_parameter_map;
	}
	result->has_anonymous_parameters = transformer.has_anonymous_parameters;
	return result;
}

static unique_ptr<SQLStatement> ExtractAndTransformStatement(PEGTransformer &transformer,
                                                             const TokenIterator &token_iterator, ParseResult &stmt_pr,
                                                             optional_idx terminator_offset) {
	auto stmt = transformer.Transform<unique_ptr<SQLStatement>>(stmt_pr);

	if (!transformer.named_parameter_map.empty()) {
		stmt->named_param_map = transformer.named_parameter_map;
	}
	if (!transformer.pivot_entries.empty()) {
		stmt = transformer.CreatePivotStatement(std::move(stmt));
	}
	transformer.Clear();

	// Calculate location and length cleanly
	if (stmt_pr.offset.IsValid()) {
		auto start = stmt_pr.offset.GetIndex();
		idx_t end_index = terminator_offset.IsValid() ? terminator_offset.GetIndex() : token_iterator.EndOffset();
		stmt->stmt_location = QueryLocation(start, end_index - start);
	}

	return stmt;
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformTopLevelStatement(TokenIterator &token_iterator,
                                                                           ParserOptions &options,
                                                                           const CompiledGrammar &grammar) {
	if (!token_iterator.Current()) {
		return nullptr;
	}
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_result_allocator;
	ParserPackratCache packrat_cache;
	idx_t max_token_index = token_iterator.Position();
	ArenaAllocator process_allocator(Allocator::DefaultAllocator());
	MatchContext match_context(suggestions, parse_result_allocator, process_allocator, max_token_index,
	                           MatchMode::BUILD_PARSE_RESULT, options.identifier_case_mode, options.heap_based_parser,
	                           &packrat_cache);
	MatchState state(token_iterator, match_context);
	auto match_result = grammar.TopLevelStatementMatcher().MatchParseResult(state);
	process_allocator.FreeAll();
	if (!match_result.IsSuccess()) {
		// syntax error — surface as a parser exception in the same shape as Transform()
		auto token_stream = token_iterator.ToString();
		idx_t error_token_idx = state.GetMaxTokenIndex();
		if (error_token_idx >= token_iterator.Size()) {
			error_token_idx = token_iterator.Size() - 1;
		}
		// Walk back past the EOI sentinel so the error message names a real token.
		if (error_token_idx > 0 &&
		    (token_iterator.GetToken(error_token_idx).type == TokenType::END_OF_INPUT ||
		     token_iterator.GetToken(error_token_idx).type == TokenType::END_OF_INPUT_AUTOCOMPLETE)) {
			error_token_idx--;
		}
		auto &error_token = token_iterator.GetToken(error_token_idx);
		auto error_message = "syntax error at or near \"" + error_token.text + "\"";
		throw ParserException::SyntaxError(token_stream, error_message,
		                                   QueryLocation(error_token.offset, error_token.length));
	}
	D_ASSERT(match_result.HasParseResult());

	// Advance the caller's cursor past the consumed tokens.
	token_iterator.SetPosition(state.token_iterator);

	// TopLevelStatement <- Statement? (';'+ / EndOfInput)
	//   child 0: Optional<Statement>
	//   child 1: bracket-wrapper list around Choice<';'+ | EndOfInput>
	auto &tls = match_result.GetParseResult()->Cast<ListParseResult>();
	auto &stmt_opt = tls.Child<OptionalParseResult>(0);
	if (!stmt_opt.HasResult()) {
		// separator-only or EOI-only TopLevelStatement — no statement to yield
		return nullptr;
	}
	auto &term_wrapper = tls.Child<ListParseResult>(1);
	auto &term_inner = term_wrapper.Child<ChoiceParseResult>(0).GetResult();
	optional_idx terminator_offset;
	if (term_inner.type != ParseResultType::END_OF_INPUT) {
		auto semi_children = term_inner.Cast<RepeatParseResult>().GetChildren();
		if (!semi_children.empty()) {
			terminator_offset = semi_children[0].get().offset;
		}
	}

	ArenaAllocator transformer_allocator(Allocator::DefaultAllocator());
	PEGTransformer transformer(transformer_allocator, token_iterator, options, grammar);

	return ExtractAndTransformStatement(transformer, token_iterator, stmt_opt.GetResult(), terminator_offset);
}

#define REGISTER_TRANSFORM(FUNCTION) Register(string(#FUNCTION).substr(9), &FUNCTION)

void PEGTransformerFactory::RegisterCommon() {
	// common.gram
	REGISTER_TRANSFORM(TransformNumberLiteral);
	REGISTER_TRANSFORM(TransformStringLiteral);
	REGISTER_TRANSFORM(TransformIntervalToIntervalAsType);
}

void PEGTransformerFactory::RegisterCreateTable() {
	// create_table.gram
	REGISTER_TRANSFORM(TransformIdentifier);
}

void PEGTransformerFactory::RegisterExpression() {
	// expression.gram
	REGISTER_TRANSFORM(TransformPrefixExpression);
	REGISTER_TRANSFORM(TransformOverClause);
}

void PEGTransformerFactory::RegisterPivot() {
	// PivotStatement and UnpivotStatement measure parameter usage while transforming
	// the source table, so their top-level wrappers remain manual.
	REGISTER_TRANSFORM(TransformPivotStatement);
	REGISTER_TRANSFORM(TransformUnpivotStatement);
}

void PEGTransformerFactory::RegisterSelect() {
	// select.gram rules that remain manual after generated wrappers are registered.
	Register("SelectStatementInternal", &TransformSelectStatementInternalRule);
	REGISTER_TRANSFORM(TransformSimpleSelect);
	REGISTER_TRANSFORM(TransformTableRef);
	REGISTER_TRANSFORM(TransformWithClause);
	REGISTER_TRANSFORM(TransformWindowDefinition);
}

void PEGTransformerFactory::RegisterKeywordsAndIdentifiers() {
	Register("PragmaName", &TransformIdentifierOrKeyword);
	Register("TypeName", &TransformIdentifierOrKeyword);
	Register("PlainIdentifier", &TransformIdentifierOrKeyword);
	Register("QuotedIdentifier", &TransformIdentifierOrKeyword);
	Register("ReservedKeyword", &TransformIdentifierOrKeyword);
	Register("UnreservedKeyword", &TransformIdentifierOrKeyword);
	Register("ColumnNameKeyword", &TransformIdentifierOrKeyword);
	Register("FuncNameKeyword", &TransformIdentifierOrKeyword);
	Register("TypeNameKeyword", &TransformIdentifierOrKeyword);
	Register("SettingName", &TransformIdentifierOrKeyword);
}

PEGTransformerFactory::PEGTransformerFactory(ParsedGrammar &grammar_p) : grammar(grammar_p) {
	RegisterGenerated();
	REGISTER_TRANSFORM(TransformStatement);
	RegisterCommon();
	RegisterCreateTable();
	RegisterExpression();
	RegisterPivot();
	RegisterSelect();
	RegisterKeywordsAndIdentifiers();
	for (auto &entry : GeneratedTransformFrameOps()) {
		auto process_info = entry.second;
		grammar.SetTransformProcess(
		    entry.first,
		    [process_info](PEGTransformer &transformer, ParseResult &parse_result) -> unique_ptr<TransformProcess> {
			    return make_uniq<GeneratedTransformProcess>(transformer, TransformInput {parse_result}, *process_info);
		    });
	}
}

void PEGTransformerFactory::RegisterDefaultTransforms(ParsedGrammar &grammar) {
	PEGTransformerFactory factory(grammar);
}

vector<reference<ParseResult>> PEGTransformerFactory::ExtractParseResultsFromList(ParseResult &parse_result) {
	// List(D) <- D (',' D)* ','?
	vector<reference<ParseResult>> result;
	auto &list_pr = parse_result.Cast<ListParseResult>();
	result.push_back(list_pr.GetChild(0));
	auto &opt_child = list_pr.Child<OptionalParseResult>(1);
	if (opt_child.HasResult()) {
		auto &repeat_result = opt_child.GetResult().Cast<RepeatParseResult>();
		for (auto &child : repeat_result.GetChildren()) {
			auto &list_child = child.get().Cast<ListParseResult>();
			result.push_back(list_child.GetChild(1));
		}
	}
	return result;
}

ParseResult &PEGTransformerFactory::ExtractResultFromParens(ParseResult &parse_result) {
	// Parens(D) <- '(' D ')'
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return list_pr.GetChild(1);
}

bool PEGTransformerFactory::ExpressionIsEmptyStar(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::STAR) {
		return false;
	}
	auto &star = expr.Cast<StarExpression>();
	if (!star.IsColumns() && star.ExcludeList().empty() && star.ReplaceList().empty()) {
		return true;
	}
	return false;
}

QualifiedName PEGTransformerFactory::StringToQualifiedName(vector<string> input) {
	if (input.empty()) {
		throw InternalException("QualifiedName cannot be made with an empty input.");
	}
	if (input.size() == 1) {
		return QualifiedName(Identifier(input[0]));
	} else if (input.size() == 2) {
		return QualifiedName({Identifier(input[0])}, Identifier(input[1]));
	} else if (input.size() == 3) {
		return QualifiedName(Identifier(input[0]), Identifier(input[1]), Identifier(input[2]));
	} else {
		throw ParserException("Too many qualifications found - expected [catalog.schema.name] or [schema.name]");
	}
}

QualifiedColumnName PEGTransformerFactory::StringToQualifiedColumnName(const vector<string> &input) {
	if (input.empty()) {
		throw InternalException("QualifiedColumnName cannot be made with an empty input.");
	}
	auto identifiers = StringsToIdentifiers(input);
	if (identifiers.size() == 1) {
		return QualifiedColumnName(std::move(identifiers[0]));
	} else if (identifiers.size() == 2) {
		return QualifiedColumnName(std::move(identifiers[0]), std::move(identifiers[1]));
	} else if (identifiers.size() == 3) {
		QualifiedColumnName result;
		result.schema = std::move(identifiers[0]);
		result.table = std::move(identifiers[1]);
		result.column = std::move(identifiers[2]);
		return result;
	} else if (identifiers.size() == 4) {
		QualifiedColumnName result;
		result.catalog = std::move(identifiers[0]);
		result.schema = std::move(identifiers[1]);
		result.table = std::move(identifiers[2]);
		result.column = std::move(identifiers[3]);
		return result;
	}
	throw ParserException("Expected at most 4 entries (catalog.schema.table.column), but found %zu entries (input: %s)",
	                      input.size(), StringUtil::Join(input, "."));
}

LogicalType PEGTransformerFactory::GetIntervalTargetType(DatePartSpecifier date_part) {
	switch (date_part) {
	case DatePartSpecifier::YEAR:
	case DatePartSpecifier::MONTH:
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::QUARTER:
	case DatePartSpecifier::DECADE:
	case DatePartSpecifier::CENTURY:
	case DatePartSpecifier::MILLENNIUM:
		return LogicalType::INTEGER;
	case DatePartSpecifier::HOUR:
	case DatePartSpecifier::MINUTE:
	case DatePartSpecifier::MICROSECONDS:
		return LogicalType::BIGINT;
	case DatePartSpecifier::MILLISECONDS:
	case DatePartSpecifier::SECOND:
		return LogicalType::DOUBLE;
	default:
		throw InternalException("Unsupported interval post-fix");
	}
}

} // namespace duckdb
