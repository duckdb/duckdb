
#include "ast/setting_info.hpp"
namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformUseStatement(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	// Rule: UseStatement <- 'USE'i UseTarget
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto qn = transformer.Transform<QualifiedName>(list_pr.Child<ChoiceParseResult>(1).result);

	string value_str;
	if (qn.schema.empty()) {
		value_str = KeywordHelper::WriteOptionallyQuoted(qn.name, '"');
	} else {
		value_str = KeywordHelper::WriteOptionallyQuoted(qn.schema, '"') + "." +
		            KeywordHelper::WriteOptionallyQuoted(qn.name, '"');
	}

	auto value_expr = make_uniq<ConstantExpression>(Value(value_str));
	return make_uniq<SetVariableStatement>("schema", std::move(value_expr), SetScope::AUTOMATIC);
}

QualifiedName PEGTransformerFactory::TransformUseTarget(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	string qualified_name;
	if (choice_pr.result->type == ParseResultType::LIST) {
		auto use_target_children = choice_pr.result->Cast<ListParseResult>();
		for (auto &child : use_target_children.children) {
			if (child->type == ParseResultType::IDENTIFIER) {
				qualified_name += child->Cast<IdentifierParseResult>().identifier;
			} else if (child->type == ParseResultType::KEYWORD) {
				qualified_name += child->Cast<KeywordParseResult>().keyword;
			}
		}
	} else {
		qualified_name = choice_pr.result->Cast<IdentifierParseResult>().identifier;
	}
	auto result = QualifiedName::Parse(qualified_name);
	return result;
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformSetStatement(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &child_pr = list_pr.Child<ListParseResult>(1);
	auto &assignment_or_timezone = child_pr.Child<ChoiceParseResult>(0);
	return transformer.Transform<unique_ptr<SQLStatement>>(assignment_or_timezone);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformResetStatement(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	// Composer: 'RESET' (SetVariable / SetSetting)
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &child_pr = list_pr.Child<ListParseResult>(1);
	auto &choice_pr = child_pr.Child<ChoiceParseResult>(0);

	SettingInfo setting_info = transformer.Transform<SettingInfo>(choice_pr.result);
	return make_uniq<ResetVariableStatement>(setting_info.name, setting_info.scope);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformStandardAssignment(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	// Composer: (SetVariable / SetSetting) SetAssignment
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

SettingInfo PEGTransformerFactory::TransformSettingOrVariable(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	// Dispatcher: SetVariable / SetSetting
	auto &choice_pr = parse_result->Cast<ChoiceParseResult>();
	auto &matched_child = choice_pr.result;
	return transformer.Transform<SettingInfo>(matched_child);
}

SettingInfo PEGTransformerFactory::TransformSetSetting(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	// Leaf: SettingScope? SettingName
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

SettingInfo PEGTransformerFactory::TransformSetVariable(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	// Leaf: VariableScope SettingName
	auto &list_pr = parse_result->Cast<ListParseResult>();

	SettingInfo result;
	result.scope = transformer.TransformEnum<SetScope>(list_pr.Child<ListParseResult>(0));
	result.name = list_pr.Child<IdentifierParseResult>(1).identifier;
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSetAssignment(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &variable_list_pr = list_pr.children[1];
	return transformer.Transform<vector<unique_ptr<ParsedExpression>>>(variable_list_pr);
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformVariableList(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &variable_list = parse_result->Cast<ListParseResult>();
	auto &list_pr = variable_list.Child<ListParseResult>(0);
	vector<unique_ptr<ParsedExpression>> expressions;
	expressions.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.children[0]));
	idx_t child_idx = 1;
	while (!list_pr.children[child_idx]->name.empty() && list_pr.children[child_idx]->type == ParseResultType::LIST) {
		expressions.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.children[child_idx]));
		child_idx++;
	}
	return expressions;
}

} // namespace duckdb
