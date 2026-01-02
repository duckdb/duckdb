#include "ast/macro_parameter.hpp"
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
	if (parameters_pr.HasResult()) {
		bool default_value_found = false;
		auto parameters = transformer.Transform<vector<MacroParameter>>(parameters_pr.optional_result);
		case_insensitive_string_set_t parameter_names;
		for (auto &parameter : parameters) {
			D_ASSERT(!parameter.name.empty());
			if (parameter_names.find(parameter.name) != parameter_names.end()) {
				throw ParserException("Duplicate parameter '%s' in macro definition", parameter.name);
			}
			parameter_names.insert(parameter.name);
			if (parameter.is_default) {
				Value default_value;
				if (!ConstructConstantFromExpression(*parameter.expression, default_value)) {
					throw ParserException("Invalid default value for parameter '%s': %s", parameter.name,
					                      parameter.expression->ToString());
				}
				auto default_expr = make_uniq<ConstantExpression>(std::move(default_value));
				default_expr->SetAlias(parameter.name);
				macro_function->default_parameters[parameter.name] = std::move(default_expr);
				macro_function->parameters.push_back(make_uniq<ColumnRefExpression>(parameter.name));
				macro_function->default_parameters.insert(parameter.name, std::move(default_expr));
				default_value_found = true;
			} else {
				if (default_value_found) {
					throw ParserException("Parameter without a default follows parameter with a default");
				}
				macro_function->parameters.push_back(std::move(parameter.expression));
			}
			macro_function->types.push_back(parameter.type);
		}
	}

	return macro_function;
}

unique_ptr<MacroFunction> PEGTransformerFactory::TransformTableMacroDefinition(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<TableMacroFunction>();
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(1));
	result->query_node = std::move(select_statement->node);
	return std::move(result);
}

unique_ptr<MacroFunction>
PEGTransformerFactory::TransformScalarMacroDefinition(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<ScalarMacroFunction>();
	result->expression = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	return std::move(result);
}

vector<MacroParameter> PEGTransformerFactory::TransformMacroParameters(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto parameter_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	vector<MacroParameter> parameters;
	for (auto parameter : parameter_list) {
		parameters.push_back(transformer.Transform<MacroParameter>(parameter));
	}
	return parameters;
}

MacroParameter PEGTransformerFactory::TransformMacroParameter(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return transformer.Transform<MacroParameter>(choice_pr);
}

MacroParameter PEGTransformerFactory::TransformSimpleParameter(PEGTransformer &transformer,
                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto parameter = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	MacroParameter result;
	result.name = parameter;
	result.expression = make_uniq<ColumnRefExpression>(parameter);
	transformer.TransformOptional<LogicalType>(list_pr, 1, result.type);
	result.is_default = false;
	return result;
}

} // namespace duckdb
