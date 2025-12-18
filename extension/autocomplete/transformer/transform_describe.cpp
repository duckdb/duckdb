#include "duckdb/parser/tableref/showref.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SelectStatement> PEGTransformerFactory::TransformDescribeStatement(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = transformer.Transform<unique_ptr<QueryNode>>(list_pr.Child<ChoiceParseResult>(0).result);
	return select_statement;
}

unique_ptr<QueryNode> PEGTransformerFactory::TransformShowSelect(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<ShowRef>();
	result->show_type = transformer.Transform<ShowType>(list_pr.Child<ListParseResult>(0));
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(1));
	result->query =	std::move(select_statement->node);
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(result);
	return select_node;
}

unique_ptr<QueryNode> PEGTransformerFactory::TransformShowAllTables(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<ShowRef>();
	// Legacy reasons, see bind_showref.cpp
	result->table_name = "__show_tables_expanded";
	result->show_type = ShowType::DESCRIBE;
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(result);
	return select_node;
}

unique_ptr<QueryNode> PEGTransformerFactory::TransformShowQualifiedName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<ShowRef>();
	result->show_type = transformer.Transform<ShowType>(list_pr.Child<ListParseResult>(0));
	auto opt_table_name_parens = list_pr.Child<OptionalParseResult>(1);
	if (opt_table_name_parens.HasResult()) {
		auto base_table_or_string = opt_table_name_parens.optional_result->Cast<ListParseResult>();
		auto choice_pr = base_table_or_string.Child<ChoiceParseResult>(0);
		if (choice_pr.result->type == ParseResultType::STRING) {
			result->table_name = choice_pr.result->Cast<StringLiteralParseResult>().result;
		} else {
			auto base_table = transformer.Transform<unique_ptr<BaseTableRef>>(choice_pr.result);
			if (IsInvalidSchema(base_table->schema_name)) {
				// Check for special table names
				auto table_name = StringUtil::Lower(base_table->table_name);
				if (table_name == "databases" || table_name == "tables" || table_name == "variables") {
					result->table_name = "\"" + table_name + "\"";
				} else {
					result->table_name = base_table->table_name;
				}
			} else {
				result->catalog_name = base_table->catalog_name;
				result->schema_name = base_table->schema_name;
				result->table_name = base_table->table_name;
			}
		}
	} else {
		if (result->show_type == ShowType::SUMMARY) {
			throw ParserException("Expected table name with SUMMARIZE");
		}
	}
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(result);
	return select_node;
}

ShowType PEGTransformerFactory::TransformShowOrDescribeOrSummarize(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<ShowType>(list_pr.Child<ChoiceParseResult>(0).result);
}

ShowType PEGTransformerFactory::TransformShowOrDescribe(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<ShowType>(list_pr.Child<ChoiceParseResult>(0).result);
}

ShowType PEGTransformerFactory::TransformSummarize(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<ShowType>(list_pr.Child<ListParseResult>(0));
}


}