#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/function/scalar_macro_function.hpp"

namespace duckdb {
unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateMacroStmt(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateMacroInfo>(CatalogType::MACRO_ENTRY);

	auto if_not_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto qualified_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(2));
	if (qualified_name.schema.empty()) {
		info->schema = qualified_name.catalog;
	} else {
		info->catalog = qualified_name.catalog;
		info->schema = qualified_name.schema;
	}
	info->name = qualified_name.name;
	auto macro_definition_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(3));

	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	for (auto macro_definition : macro_definition_list) {
		info->macros.push_back(transformer.Transform<unique_ptr<MacroFunction>>(macro_definition));
	}
	result->info = std::move(info);
	return result;
}

unique_ptr<MacroFunction> PEGTransformerFactory::TransformMacroDefinition(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto nested_list = list_pr.Child<ListParseResult>(2);

	auto macro_function =
	    transformer.Transform<unique_ptr<MacroFunction>>(nested_list.Child<ChoiceParseResult>(0).result);
	auto parameters_pr = ExtractResultFromParens(list_pr.Child<ListParseResult>(0))->Cast<OptionalParseResult>();
	vector<unique_ptr<Expression>> parameters;
	if (parameters_pr.HasResult()) {
		macro_function->parameters =
		    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(parameters_pr.optional_result);
	}

	return macro_function;
}

unique_ptr<MacroFunction> PEGTransformerFactory::TransformTableMacroDefinition(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<TableMacroFunction>();
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(1));
	result->query_node = std::move(select_statement->node);
	return result;
}

unique_ptr<MacroFunction>
PEGTransformerFactory::TransformScalarMacroDefinition(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<ScalarMacroFunction>();
	result->expression = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformMacroParameters(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto parameter_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	vector<unique_ptr<ParsedExpression>> parameters;
	for (auto parameter : parameter_list) {
		parameters.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(std::move(parameter)));
	}
	return parameters;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformMacroParameter(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformSimpleParameter(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto parameter = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	return make_uniq<ColumnRefExpression>(parameter);
}

} // namespace duckdb
