#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

// Convert a QualifiedName back into a dotted dictionary name. Mirrors the old
// TransformDefineStmt that joined defnames with '.'.
static string QualifiedNameToDottedString(const QualifiedName &name) {
	string result;
	if (!name.catalog.empty() && name.catalog != INVALID_CATALOG) {
		result += name.catalog;
		result += ".";
	}
	if (!name.schema.empty() && name.schema != INVALID_SCHEMA) {
		result += name.schema;
		result += ".";
	}
	result += name.name;
	return result;
}

// CREATE TEXT SEARCH DICTIONARY name (k = v, ...) -> PRAGMA create_text_search_dictionary('name', if_not_exists, k :=
// v, ...)
unique_ptr<SQLStatement> PEGTransformerFactory::TransformCreateTSDictionaryStatement(PEGTransformer &transformer,
                                                                                     ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// Children:
	//   0: 'CREATE'
	//   1: 'TEXT'
	//   2: 'SEARCH'
	//   3: 'DICTIONARY'
	//   4: IfNotExists?
	//   5: QualifiedName
	//   6: TSDictionaryDefinition
	bool if_not_exists = list_pr.Child<OptionalParseResult>(4).HasResult();
	auto qname = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(5));
	auto full_name = QualifiedNameToDottedString(qname);

	auto result = make_uniq<PragmaStatement>();
	result->info->name = "create_text_search_dictionary";
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value(full_name)));
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value::BOOLEAN(if_not_exists)));

	// TSDictionaryDefinition <- Parens(List(TSDictionaryDefElement))
	auto &def_pr = list_pr.Child<ListParseResult>(6);
	// TSDictionaryDefinition body is a single Parens expression.
	auto &list_inside = ExtractResultFromParens(def_pr.Child<ListParseResult>(0));
	auto elements = ExtractParseResultsFromList(list_inside);
	for (auto &elem_ref : elements) {
		auto &elem_pr = elem_ref.get().Cast<ListParseResult>();
		// TSDictionaryDefElement <- ColId TSDictionaryDefArg?
		auto opt_name = transformer.Transform<string>(elem_pr.Child<ListParseResult>(0));
		auto &arg_opt = elem_pr.Child<OptionalParseResult>(1);
		unique_ptr<ParsedExpression> value_expr;
		if (arg_opt.HasResult()) {
			// TSDictionaryDefArg <- '=' DefArg — same structure as RelOptionArgumentOpt
			auto &arg_list = arg_opt.GetResult().Cast<ListParseResult>();
			auto &defarg_list = arg_list.Child<ListParseResult>(1);
			auto &def_arg_choice = defarg_list.Child<ChoiceParseResult>(0).GetResult();
			if (def_arg_choice.name == "ReservedKeyword") {
				auto &rw_list = def_arg_choice.Cast<ListParseResult>();
				auto keyword = rw_list.Child<ChoiceParseResult>(0).GetResult().Cast<KeywordParseResult>().keyword;
				value_expr = make_uniq<ConstantExpression>(Value(keyword));
			} else if (def_arg_choice.name == "StringLiteral") {
				value_expr = make_uniq<ConstantExpression>(Value(transformer.Transform<string>(def_arg_choice)));
			} else if (def_arg_choice.name == "NoneLiteral" || def_arg_choice.name == "NullLiteral") {
				value_expr = make_uniq<ConstantExpression>(Value());
			} else if (def_arg_choice.name == "NumberLiteral" || def_arg_choice.name == "Expression") {
				value_expr = transformer.Transform<unique_ptr<ParsedExpression>>(def_arg_choice);
			} else {
				throw ParserException("Unexpected rule in TransformCreateTSDictionaryStatement: %s",
				                      def_arg_choice.name);
			}
		} else {
			value_expr = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
		}
		auto [_, inserted] = result->info->named_parameters.emplace(opt_name, std::move(value_expr));
		if (!inserted) {
			throw InvalidInputException("conflicting or redundant options: \"%s\" specified more than once", opt_name);
		}
	}

	return std::move(result);
}

// DROP TEXT SEARCH DICTIONARY name -> PRAGMA drop_text_search_dictionary('name', if_exists)
unique_ptr<SQLStatement> PEGTransformerFactory::TransformDropTSDictionaryStatement(PEGTransformer &transformer,
                                                                                   ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// Children:
	//   0: 'DROP', 1: 'TEXT', 2: 'SEARCH', 3: 'DICTIONARY'
	//   4: IfExists?
	//   5: QualifiedName
	bool if_exists = list_pr.Child<OptionalParseResult>(4).HasResult();
	auto qname = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(5));
	auto full_name = QualifiedNameToDottedString(qname);

	auto result = make_uniq<PragmaStatement>();
	result->info->name = "drop_text_search_dictionary";
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value(full_name)));
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value::BOOLEAN(if_exists)));
	return std::move(result);
}

} // namespace duckdb
