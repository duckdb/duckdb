#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/common/query_location.hpp"
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
	if (transformer.options.heap_based_parser && !transformer.grammar.HasGrammarChanges()) {
		return TransformStatementTrampoline(transformer, parse_result);
	}
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

unique_ptr<SQLStatement> PEGTransformerFactory::TransformStatementTrampoline(PEGTransformer &transformer,
                                                                             ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	auto &choice_result = choice_pr.GetResult();

	TransformStack stack(transformer);
	auto result = stack.Execute<unique_ptr<SQLStatement>>(choice_result, GetTrampolineOps(choice_result));
	if (!transformer.named_parameter_map.empty()) {
		result->named_param_map = transformer.named_parameter_map;
	}
	result->has_anonymous_parameters = transformer.has_anonymous_parameters;
	return result;
}

const TransformFrameOps &PEGTransformerFactory::GetTrampolineOps(const ParseResult &parse_result) {
	auto &ops_map = GeneratedTrampolineOps();
	auto ops_entry = ops_map.find(parse_result.name);
	if (ops_entry == ops_map.end()) {
		throw NotImplementedException("No trampoline transformer for rule '%s'", parse_result.name);
	}
	return *ops_entry->second;
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
	const bool use_heap_based_parser = options.heap_based_parser && !grammar.HasGrammarChanges();
	MatchState state(token_iterator, suggestions, parse_result_allocator, max_token_index,
	                 MatchMode::BUILD_PARSE_RESULT, options.identifier_case_mode, use_heap_based_parser,
	                 &packrat_cache);
	auto match_result = grammar.TopLevelStatementMatcher().MatchParseResult(state);
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

bool PEGTransformerFactory::ConstructConstantFromExpression(const ParsedExpression &expr, Value &value) {
	// We have to construct it like this because we don't have the ClientContext for binding/executing the expr here
	switch (expr.GetExpressionType()) {
	case ExpressionType::FUNCTION: {
		auto &function = expr.Cast<FunctionExpression>();
		if (function.FunctionName() == "struct_pack") {
			identifier_set_t unique_names;
			child_list_t<Value> values;
			values.reserve(function.GetArguments().size());
			for (const auto &child : function.GetArguments()) {
				if (!unique_names.insert(child.GetExpression().GetAlias()).second) {
					throw BinderException("Duplicate struct entry name %s", child.GetExpression().GetAlias());
				}
				Value child_value;
				if (!ConstructConstantFromExpression(child.GetExpression(), child_value)) {
					return false;
				}
				values.emplace_back(child.GetExpression().GetAlias(), std::move(child_value));
			}
			value = Value::STRUCT(std::move(values));
			return true;
		} else if (function.FunctionName() == "list_value") {
			vector<Value> values;
			values.reserve(function.GetArguments().size());
			for (const auto &child : function.GetArguments()) {
				Value child_value;
				if (!ConstructConstantFromExpression(child.GetExpression(), child_value)) {
					return false;
				}
				values.emplace_back(std::move(child_value));
			}

			// figure out child type
			LogicalType child_type(LogicalTypeId::SQLNULL);
			for (auto &child_value : values) {
				child_type = LogicalType::DefaultForceMaxLogicalType(child_type, child_value.type());
			}

			// finally create the list
			value = Value::LIST(child_type, values);
			return true;
		} else if (function.FunctionName() == "map") {
			Value keys;
			if (!ConstructConstantFromExpression(function.GetArguments()[0].GetExpression(), keys)) {
				return false;
			}

			Value values;
			if (!ConstructConstantFromExpression(function.GetArguments()[1].GetExpression(), values)) {
				return false;
			}

			vector<Value> keys_unpacked = ListValue::GetChildren(keys);
			vector<Value> values_unpacked = ListValue::GetChildren(values);

			value = Value::MAP(ListType::GetChildType(keys.type()), ListType::GetChildType(values.type()),
			                   keys_unpacked, values_unpacked);
			return true;
		} else {
			return false;
		}
	}
	case ExpressionType::VALUE_CONSTANT: {
		auto &constant = expr.Cast<ConstantExpression>();
		value = constant.GetValue();
		return true;
	}
	case ExpressionType::OPERATOR_CAST: {
		auto &cast = expr.Cast<CastExpression>();
		Value dummy_value;
		if (!ConstructConstantFromExpression(cast.Child(), dummy_value)) {
			return false;
		}

		auto cast_type = UnboundType::TryDefaultBind(cast.TargetType());
		if (cast_type.id() == LogicalTypeId::INVALID || cast_type.id() == LogicalTypeId::UNBOUND) {
			return false;
		}

		string error_message;
		auto cast_value = dummy_value.DefaultTryCastAs(cast_type, &error_message);
		if (!cast_value) {
			throw ConversionException("Unable to cast %s to %s", dummy_value.ToString(),
			                          EnumUtil::ToString(cast_type.id()));
		}
		value = std::move(*cast_value);
		return true;
	}
	default:
		return false;
	}
}

} // namespace duckdb
