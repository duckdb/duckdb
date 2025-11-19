#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateIndexStmt(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<CreateStatement>();
	auto index_info = make_uniq<CreateIndexInfo>();
	bool unique = list_pr.Child<OptionalParseResult>(0).HasResult();
	index_info->constraint_type = unique ? IndexConstraintType::UNIQUE : IndexConstraintType::NONE;
	bool if_not_exists = list_pr.Child<OptionalParseResult>(2).HasResult();
	index_info->on_conflict =
	    if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	auto index_name_opt = list_pr.Child<OptionalParseResult>(3);
	if (index_name_opt.HasResult()) {
		index_info->index_name = index_name_opt.optional_result->Cast<IdentifierParseResult>().identifier;
	} else {
		throw NotImplementedException("Please provide an index name, e.g., CREATE INDEX my_name ...");
	}
	auto table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(5));
	index_info->table = table->table_name;
	index_info->catalog = table->catalog_name;
	index_info->schema = table->schema_name;
	index_info->index_type = "ART";
	auto column_list_opt = list_pr.Child<OptionalParseResult>(6);
	if (column_list_opt.HasResult()) {
		auto column_list = transformer.Transform<vector<string>>(column_list_opt.optional_result);
		for (auto &column : column_list) {
			index_info->expressions.push_back(make_uniq<ColumnRefExpression>(column, table->table_name));
			index_info->parsed_expressions.push_back(make_uniq<ColumnRefExpression>(column, table->table_name));
		}
	}
	transformer.TransformOptional<string>(list_pr, 7, index_info->index_type);
	auto index_elements_opt = list_pr.Child<OptionalParseResult>(8);
	if (index_elements_opt.HasResult()) {
		auto extract_parens = ExtractResultFromParens(index_elements_opt.optional_result);
		auto index_element_list = ExtractParseResultsFromList(extract_parens);
		for (auto index_element : index_element_list) {
			auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(index_element);
			index_info->expressions.push_back(expr->Copy());
			index_info->parsed_expressions.push_back(expr->Copy());
		}
	}

	transformer.TransformOptional<case_insensitive_map_t<Value>>(list_pr, 9, index_info->options);
	auto where_opt = list_pr.Child<OptionalParseResult>(10);
	if (where_opt.HasResult()) {
		throw NotImplementedException("Creating partial indexes is not supported currently");
	}
	result->info = std::move(index_info);
	return result;
}

string PEGTransformerFactory::TransformIndexType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(1).identifier;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIndexElement(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	// TODO(Dtenwolde): We currently ignore DescOrAsc? and NullsFirstOrLast?
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
}

case_insensitive_map_t<Value> PEGTransformerFactory::TransformWithList(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<case_insensitive_map_t<Value>>(list_pr.Child<ListParseResult>(1));
}

case_insensitive_map_t<Value> PEGTransformerFactory::TransformRelOptionOrOids(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<case_insensitive_map_t<Value>>(list_pr.Child<ChoiceParseResult>(0).result);
}

case_insensitive_map_t<Value> PEGTransformerFactory::TransformRelOptionList(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	case_insensitive_map_t<Value> result;
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto rel_option_list = ExtractParseResultsFromList(extract_parens);
	for (auto rel_option : rel_option_list) {
		auto option = transformer.Transform<pair<string, Value>>(rel_option);
		result.insert({option.first, option.second});
	}
	return result;
}

case_insensitive_map_t<Value> PEGTransformerFactory::TransformOids(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	throw NotImplementedException("Oids for index are not yet implemented.");
}

pair<string, Value> PEGTransformerFactory::TransformRelOption(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto dotted_identifier = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(0));
	auto option_name = StringUtil::Join(dotted_identifier, ".");
	auto arg_opt = list_pr.Child<OptionalParseResult>(1);
	if (!arg_opt.HasResult()) {
		return {option_name, Value()};
	}
	auto inner_list = arg_opt.optional_result->Cast<ListParseResult>();
	if (inner_list.GetChild(0)->type == ParseResultType::KEYWORD) {
		auto keyword = inner_list.Child<KeywordParseResult>(0).keyword;
		return {option_name, Value(keyword)};
	}
	auto def_arg_choice = inner_list.Child<ChoiceParseResult>(0).result;
	Value option;
	if (def_arg_choice->name == "StringLiteral") {
		option = Value(transformer.Transform<string>(def_arg_choice));
	} else if (def_arg_choice->name == "NoneLiteral") {
		option = Value();
	} else if (def_arg_choice->name == "NumberLiteral") {
		auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(def_arg_choice);
		if (expr->GetExpressionClass() != ExpressionClass::CONSTANT) {
			throw InvalidInputException("Invalid constant expression.");
		}
		auto const_expr = expr->Cast<ConstantExpression>();
		option = const_expr.value.GetValue<int64_t>();
	} else {
		throw ParserException("Unexpected rule encountered in TransformRelOption");
	}
	return {option_name, option};
}

string PEGTransformerFactory::TransformIndexName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

} // namespace duckdb
