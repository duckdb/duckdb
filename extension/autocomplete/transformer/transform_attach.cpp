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
	vector<GenericCopyOption> copy_options;
	transformer.TransformOptional<vector<GenericCopyOption>>(list_pr, 6, copy_options);
	for (auto &copy_option : copy_options) {
		if (copy_option.expression) {
			info->parsed_options[copy_option.name] = std::move(copy_option.expression);
			continue;
		}
		if (copy_option.children.empty()) {
			info->options[copy_option.name] = Value(true);
		} else if (copy_option.children.size() == 1) {
			auto val = copy_option.children[0];
			if (val.IsNull()) {
				throw BinderException("NULL is not supported as a valid option for ATTACH option \"%s\"",
				                      copy_option.name);
			}
			info->options[copy_option.name] = std::move(copy_option.children[0]);
		} else {
			throw ParserException("Option %s can only have one argument", copy_option.name);
		}
	}
	result->info = std::move(info);
	return std::move(result);
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

vector<GenericCopyOption> PEGTransformerFactory::TransformAttachOptions(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &parens = list_pr.Child<ListParseResult>(0);
	auto &generic_copy_option_list = parens.Child<ListParseResult>(1);

	return transformer.Transform<vector<GenericCopyOption>>(generic_copy_option_list);
}

vector<GenericCopyOption>
PEGTransformerFactory::TransformGenericCopyOptionList(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	vector<GenericCopyOption> result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &list = list_pr.Child<ListParseResult>(0);
	auto &first_element = list.Child<ListParseResult>(0);
	result.push_back(transformer.Transform<GenericCopyOption>(first_element));
	auto &extra_elements = list.Child<OptionalParseResult>(1);
	if (extra_elements.HasResult()) {
		auto &repeat_pr = extra_elements.optional_result->Cast<RepeatParseResult>();
		for (auto &element : repeat_pr.children) {
			auto &child = element->Cast<ListParseResult>();
			result.push_back(transformer.Transform<GenericCopyOption>(child.Child<ListParseResult>(1)));
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
		} else if (expression->GetExpressionType() == ExpressionType::FUNCTION) {
			copy_option.expression = std::move(expression);
		} else {
			throw NotImplementedException("Unrecognized expression type %s",
			                              ExpressionTypeToString(expression->GetExpressionType()));
		}
	}
	return copy_option;
}

} // namespace duckdb
