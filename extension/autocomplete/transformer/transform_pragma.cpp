#include "duckdb/parser/statement/pragma_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {
unique_ptr<SQLStatement> PEGTransformerFactory::TransformPragmaStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	// Rule: PragmaStatement <- 'PRAGMA'i (PragmaAssign / PragmaFunction)
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &matched_child = list_pr.Child<ListParseResult>(1).Child<ChoiceParseResult>(0);
	return transformer.Transform<unique_ptr<SQLStatement>>(matched_child.result);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformPragmaAssign(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	// Rule: PragmaAssign <- SettingName '=' Expression
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<PragmaStatement>();
	auto &info = *result->info;
	info.name = list_pr.Child<IdentifierParseResult>(0).identifier;
	auto value_list = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(2));
	if (value_list.size() > 1) {
		throw ParserException("PRAGMA statement with assignment should contain exactly one parameter");
	}
	auto &expr = value_list[0];
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = value_list[0]->Cast<ColumnRefExpression>();
		if (!colref.IsQualified()) {
			info.parameters.emplace_back(make_uniq<ConstantExpression>(Value(colref.GetColumnName())));
		} else {
			info.parameters.emplace_back(make_uniq<ConstantExpression>(Value(expr->ToString())));
		}
	} else {
		info.parameters.emplace_back(std::move(expr));
	}
	// SQLite does not distinguish between:
	// "PRAGMA table_info='integers'"
	// "PRAGMA table_info('integers')"
	// for compatibility, any pragmas that match the SQLite ones are parsed as calls
	case_insensitive_set_t sqlite_compat_pragmas {"table_info"};
	if (sqlite_compat_pragmas.find(info.name) != sqlite_compat_pragmas.end()) {
		return std::move(result);
	}
	auto set_statement = make_uniq<SetVariableStatement>(info.name, std::move(info.parameters[0]), SetScope::AUTOMATIC);

	return std::move(set_statement);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformPragmaFunction(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	// Rule: PragmaFunction <- PragmaName PragmaParameters?
	auto result = make_uniq<PragmaStatement>();
	auto &list_pr = parse_result->Cast<ListParseResult>();
	result->info->name = list_pr.Child<IdentifierParseResult>(0).identifier;
	auto &optional_parameters_pr = list_pr.Child<OptionalParseResult>(1);
	if (optional_parameters_pr.HasResult()) {
		auto parameters =
		    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(optional_parameters_pr.optional_result);
		for (auto &parameter : parameters) {
			if (parameter->GetExpressionType() == ExpressionType::COLUMN_REF) {
				auto &colref = parameter->Cast<ColumnRefExpression>();
				if (!colref.IsQualified()) {
					result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(colref.GetColumnName())));
				} else {
					result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(parameter->ToString())));
				}
			} else {
				result->info->parameters.emplace_back(std::move(parameter));
			}
		}
	}
	return std::move(result);
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformPragmaParameters(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	// TODO(Dtenwolde) Check about named parameters
	// PragmaParameters <- List(Expression)
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto expr_list = ExtractParseResultsFromList(extract_parens);
	vector<unique_ptr<ParsedExpression>> parameters;
	for (auto expr : expr_list) {
		parameters.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	return parameters;
}

} // namespace duckdb
