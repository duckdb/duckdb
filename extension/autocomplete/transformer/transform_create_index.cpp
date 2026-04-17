#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateIndexStmt(PEGTransformer &transformer,
                                                                            ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<CreateStatement>();
	auto index_info = make_uniq<CreateIndexInfo>();
	bool unique = list_pr.Child<OptionalParseResult>(0).HasResult();
	index_info->constraint_type = unique ? IndexConstraintType::UNIQUE : IndexConstraintType::NONE;
	bool if_not_exists = list_pr.Child<OptionalParseResult>(2).HasResult();
	index_info->on_conflict =
	    if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	auto &index_name_opt = list_pr.Child<OptionalParseResult>(3);
	if (index_name_opt.HasResult()) {
		index_info->index_name = index_name_opt.GetResult().Cast<IdentifierParseResult>().identifier;
	} else {
		throw NotImplementedException("Please provide an index name, e.g., CREATE INDEX my_name ...");
	}
	auto table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(5));
	index_info->table = table->table_name;
	index_info->catalog = table->catalog_name;
	index_info->schema = table->schema_name;
	index_info->index_type = "ART";
	auto &column_list_opt = list_pr.Child<OptionalParseResult>(6);
	if (column_list_opt.HasResult()) {
		auto column_list = transformer.Transform<vector<string>>(column_list_opt.GetResult());
		for (auto &column : column_list) {
			index_info->expressions.push_back(make_uniq<ColumnRefExpression>(column, table->table_name));
			index_info->parsed_expressions.push_back(make_uniq<ColumnRefExpression>(column, table->table_name));
		}
	}
	transformer.TransformOptional<string>(list_pr, 7, index_info->index_type);
	auto &index_elements_opt = list_pr.Child<OptionalParseResult>(8);
	if (index_elements_opt.HasResult()) {
		auto &extract_parens = ExtractResultFromParens(index_elements_opt.GetResult());
		auto index_element_list = ExtractParseResultsFromList(extract_parens);
		for (auto index_element : index_element_list) {
			auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(index_element);
			if (expr->GetExpressionType() == ExpressionType::COLLATE) {
				throw NotImplementedException("Index with collation not supported yet!");
			}
			index_info->expressions.push_back(expr->Copy());
			index_info->parsed_expressions.push_back(expr->Copy());
		}
	}

	auto &with_list_opt = list_pr.Child<OptionalParseResult>(9);
	if (with_list_opt.HasResult()) {
		auto options_expr =
		    transformer.Transform<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(with_list_opt.GetResult());
		for (auto &option_entry : options_expr) {
			if (option_entry.second->GetExpressionClass() != ExpressionClass::CONSTANT) {
				throw InvalidInputException("Create index option must be a constant value");
			}
			index_info->options[option_entry.first] = option_entry.second->Cast<ConstantExpression>().value;
		}
	}
	auto &where_opt = list_pr.Child<OptionalParseResult>(10);
	if (where_opt.HasResult()) {
		throw NotImplementedException("Creating partial indexes is not supported currently");
	}
	result->info = std::move(index_info);
	return result;
}

string PEGTransformerFactory::TransformIndexType(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(1).identifier;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIndexElement(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// TODO(Dtenwolde): We currently ignore DescOrAsc? and NullsFirstOrLast?
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
}

case_insensitive_map_t<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformWithList(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(
	    list_pr.Child<ListParseResult>(1));
}

case_insensitive_map_t<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformRelOptionOrOids(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(
	    list_pr.Child<ChoiceParseResult>(0).GetResult());
}

case_insensitive_map_t<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformRelOptionList(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	case_insensitive_map_t<unique_ptr<ParsedExpression>> result;
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto rel_option_list = ExtractParseResultsFromList(extract_parens);
	for (auto rel_option : rel_option_list) {
		auto option = transformer.Transform<pair<string, unique_ptr<ParsedExpression>>>(rel_option);
		result.insert({option.first, std::move(option.second)});
	}
	return result;
}

case_insensitive_map_t<unique_ptr<ParsedExpression>> PEGTransformerFactory::TransformOids(PEGTransformer &transformer,
                                                                                          ParseResult &parse_result) {
	throw NotImplementedException("Oids for index are not yet implemented.");
}

string PEGTransformerFactory::TransformRelOptionName(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// RelOptionName <- DottedIdentifier / StringLiteral
	auto &choice = list_pr.Child<ChoiceParseResult>(0).GetResult();
	if (StringUtil::CIEquals(choice.name, "DottedIdentifier")) {
		auto dotted_identifier = transformer.Transform<vector<string>>(choice);
		return StringUtil::Join(dotted_identifier, ".");
	} else {
		return choice.Cast<StringLiteralParseResult>().GetRawString();
	}
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformRelOptionArgumentOpt(PEGTransformer &transformer,
                                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// RelOptionArgumentOpt <- '=' DefArg
	// child 0: '=' keyword, child 1: DefArg
	// DefArg <- ReservedKeyword / StringLiteral / NumberLiteral / NoneLiteral / Expression
	auto &defarg_list = list_pr.Child<ListParseResult>(1);
	auto &def_arg_choice = defarg_list.Child<ChoiceParseResult>(0).GetResult();
	if (def_arg_choice.name == "ReservedKeyword") {
		auto &rw_list = def_arg_choice.Cast<ListParseResult>();
		auto keyword = rw_list.Child<ChoiceParseResult>(0).GetResult().Cast<KeywordParseResult>().keyword;
		return make_uniq<ConstantExpression>(Value(keyword));
	} else if (def_arg_choice.name == "StringLiteral") {
		return make_uniq<ConstantExpression>(Value(transformer.Transform<string>(def_arg_choice)));
	} else if (def_arg_choice.name == "NoneLiteral" || def_arg_choice.name == "NullLiteral") {
		return make_uniq<ConstantExpression>(Value());
	} else if (def_arg_choice.name == "NumberLiteral" || def_arg_choice.name == "Expression") {
		return transformer.Transform<unique_ptr<ParsedExpression>>(def_arg_choice);
	} else {
		throw ParserException("Unexpected rule encountered in TransformRelOptionArgumentOpt: %s", def_arg_choice.name);
	}
}

pair<string, unique_ptr<ParsedExpression>> PEGTransformerFactory::TransformRelOption(PEGTransformer &transformer,
                                                                                     ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// RelOption <- RelOptionName RelOptionArgumentOpt?
	// child 0: RelOptionName, child 1: RelOptionArgumentOpt?
	auto option_name = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto &arg_opt = list_pr.Child<OptionalParseResult>(1);
	if (!arg_opt.HasResult()) {
		return {option_name, make_uniq<ConstantExpression>(Value())};
	}
	auto value = transformer.Transform<unique_ptr<ParsedExpression>>(arg_opt.GetResult());
	return {option_name, std::move(value)};
}

string PEGTransformerFactory::TransformIndexName(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

} // namespace duckdb
