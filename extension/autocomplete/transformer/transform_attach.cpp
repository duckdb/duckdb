#include "ast/generic_copy_option.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/statement/attach_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformAttachStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto result = make_uniq<AttachStatement>();
	auto info = make_uniq<AttachInfo>();

	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto or_replace = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto if_not_exists = list_pr.Child<OptionalParseResult>(2).HasResult();

	if (or_replace && if_not_exists) {
		throw ParserException("Cannot specify both OR REPLACE and IF NOT EXISTS at the same time");
	}

	if (or_replace) {
		info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	} else if (if_not_exists) {
		info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	} else {
		info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	}

	info->path = list_pr.Child<ListParseResult>(4).Child<StringLiteralParseResult>(0).result;
	transformer.TransformOptional<string>(list_pr, 5, info->name);
	transformer.TransformOptional<unordered_map<string, Value>>(list_pr, 6, info->options);
	result->info = std::move(info);
	return result;
}

string PEGTransformerFactory::TransformAttachAlias(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &col_id = list_pr.Child<ListParseResult>(1).Child<ChoiceParseResult>(0);
	if (col_id.result->type == ParseResultType::IDENTIFIER) {
		return col_id.result->Cast<IdentifierParseResult>().identifier;
	}
	return transformer.Transform<string>(col_id.result);
}

unordered_map<string, Value> PEGTransformerFactory::TransformAttachOptions(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &parens = list_pr.Child<ListParseResult>(0);
	auto &generic_copy_option_list = parens.Child<ListParseResult>(1);

	auto generic_options = transformer.Transform<unordered_map<string, vector<Value>>>(generic_copy_option_list);
	unordered_map<string, Value> option_result;
	for (auto &option : generic_options) {
		if (option.second.empty()) {
			option_result[option.first] = Value(true);
		} else {
			option_result[option.first] = option.second[0];
		}
	}
	return option_result;
}

unordered_map<string, vector<Value>>
PEGTransformerFactory::TransformGenericCopyOptionList(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	unordered_map<string, vector<Value>> result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &list = list_pr.Child<ListParseResult>(0);
	auto &first_element = list.Child<ListParseResult>(0);
	GenericCopyOption copy_option = transformer.Transform<GenericCopyOption>(first_element);
	result[copy_option.name] = copy_option.children;
	auto &extra_elements = list.Child<OptionalParseResult>(1);
	if (extra_elements.HasResult()) {
		auto &repeat_pr = extra_elements.optional_result->Cast<RepeatParseResult>();
		for (auto &element : repeat_pr.children) {
			auto &child = element->Cast<ListParseResult>();
			GenericCopyOption extra_copy_option =
			    transformer.Transform<GenericCopyOption>(child.Child<ListParseResult>(1));
			result[extra_copy_option.name] = extra_copy_option.children;
		}
	}
	return result;
}

GenericCopyOption PEGTransformerFactory::TransformGenericCopyOption(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	GenericCopyOption copy_option;

	auto &list_pr = parse_result->Cast<ListParseResult>();
	copy_option.name = StringUtil::Lower(list_pr.Child<IdentifierParseResult>(0).identifier);
	auto &optional_expression = list_pr.Child<OptionalParseResult>(1);
	if (optional_expression.HasResult()) {
		auto expression = transformer.Transform<unique_ptr<ParsedExpression>>(optional_expression.optional_result);
		if (expression->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			copy_option.children.push_back(Value(expression->Cast<ConstantExpression>().value));
		} else if (expression->GetExpressionType() == ExpressionType::COLUMN_REF) {
			copy_option.children.push_back(Value(expression->Cast<ColumnRefExpression>().GetColumnName()));
		} else if (expression->GetExpressionType() == ExpressionType::PLACEHOLDER) {
			auto &op_expr = expression->Cast<OperatorExpression>();
			for (auto &child : op_expr.children) {
				if (child->GetExpressionClass() == ExpressionClass::CONSTANT) {
					copy_option.children.push_back(Value(child->Cast<ConstantExpression>().value));
				} else if (child->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
					copy_option.children.push_back(Value(child->Cast<ColumnRefExpression>().GetColumnName()));
				} else {
					throw InternalException("Unexpected expression type %s encountered for GenericCopyOption",
					                        ExpressionClassToString(child->GetExpressionClass()));
				}
			}
		} else {
			throw NotImplementedException("Unrecognized expression type %s",
			                              ExpressionTypeToString(expression->GetExpressionType()));
		}
	}
	return copy_option;
}

} // namespace duckdb
