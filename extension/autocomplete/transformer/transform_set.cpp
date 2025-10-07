#include "transformer/peg_transformer.hpp"

namespace duckdb {

// ResetStatement <- 'RESET' (SetVariable / SetSetting)
unique_ptr<SQLStatement> PEGTransformerFactory::TransformResetStatement(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &child_pr = list_pr.Child<ListParseResult>(1);
	auto &choice_pr = child_pr.Child<ChoiceParseResult>(0);

	SettingInfo setting_info = transformer.Transform<SettingInfo>(choice_pr.result);
	return make_uniq<ResetVariableStatement>(setting_info.name, setting_info.scope);
}

// SetAssignment <- VariableAssign VariableList
vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSetAssignment(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr, 1);
}

// SetSetting <- SettingScope? SettingName
SettingInfo PEGTransformerFactory::TransformSetSetting(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &optional_scope_pr = list_pr.Child<OptionalParseResult>(0);

	SettingInfo result;
	result.name = list_pr.Child<IdentifierParseResult>(1).identifier;
	if (optional_scope_pr.optional_result) {
		auto setting_scope = optional_scope_pr.optional_result->Cast<ListParseResult>();
		auto scope_value = setting_scope.Child<ChoiceParseResult>(0);
		result.scope = transformer.TransformEnum<SetScope>(scope_value);
	}
	return result;
}

// SetStatement <- 'SET' (StandardAssignment / SetTimeZone)
unique_ptr<SQLStatement> PEGTransformerFactory::TransformSetStatement(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &child_pr = list_pr.Child<ListParseResult>(1);
	auto &assignment_or_timezone = child_pr.Child<ChoiceParseResult>(0);
	return transformer.Transform<unique_ptr<SQLStatement>>(assignment_or_timezone);
}

// SetTimeZone <- 'TIME' 'ZONE' Expression
unique_ptr<SQLStatement> PEGTransformerFactory::TransformSetTimeZone(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	throw NotImplementedException("Rule 'SetTimeZone' has not been implemented yet");
}

// SetVariable <- VariableScope Identifier
SettingInfo PEGTransformerFactory::TransformSetVariable(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	SettingInfo result;
	result.scope = transformer.TransformEnum<SetScope>(list_pr.Child<ListParseResult>(0));
	result.name = list_pr.Child<IdentifierParseResult>(1).identifier;
	return result;
}

// StandardAssignment <- (SetVariable / SetSetting) SetAssignment
unique_ptr<SQLStatement> PEGTransformerFactory::TransformStandardAssignment(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &choice_pr = parse_result->Cast<ChoiceParseResult>();
	auto &list_pr = choice_pr.result->Cast<ListParseResult>();
	auto &first_sub_rule = list_pr.Child<ListParseResult>(0);

	auto &setting_or_var_pr = first_sub_rule.Child<ChoiceParseResult>(0);
	SettingInfo setting_info = transformer.Transform<SettingInfo>(setting_or_var_pr.result);

	auto &set_assignment_pr = list_pr.Child<ListParseResult>(1);
	auto value = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(set_assignment_pr);
	// TODO(dtenwolde) Needs to throw error if more than 1 value (e.g. set threads=1,2;)
	return make_uniq<SetVariableStatement>(setting_info.name, std::move(value[0]), setting_info.scope);
}

// VariableList <- List(Expression)
vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformVariableList(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	vector<unique_ptr<ParsedExpression>> expressions;
	for (auto &expr : expr_list) {
		expressions.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	return expressions;
}
} // namespace duckdb
