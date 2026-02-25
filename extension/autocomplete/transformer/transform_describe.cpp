#include "duckdb/parser/tableref/showref.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SelectStatement> PEGTransformerFactory::TransformDescribeStatement(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = transformer.Transform<unique_ptr<QueryNode>>(list_pr.Child<ChoiceParseResult>(0).result);
	return select_statement;
}

unique_ptr<QueryNode> PEGTransformerFactory::TransformShowSelect(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<ShowRef>();
	result->show_type = transformer.Transform<ShowType>(list_pr.Child<ListParseResult>(0));
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(1));
	result->query = std::move(select_statement->node);
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(result);
	return std::move(select_node);
}

unique_ptr<QueryNode> PEGTransformerFactory::TransformShowTables(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto showref = make_uniq<ShowRef>();
	showref->show_type = ShowType::SHOW_FROM;
	auto qualified_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(3));
	if (!IsInvalidCatalog(qualified_name.catalog)) {
		throw ParserException("Expected \"SHOW TABLES FROM database\", \"SHOW TABLES FROM schema\", or "
		                      "\"SHOW TABLES FROM database.schema\"");
	}
	if (IsInvalidSchema(qualified_name.schema)) {
		showref->schema_name = qualified_name.name;
	} else {
		showref->catalog_name = qualified_name.schema;
		showref->schema_name = qualified_name.name;
	}
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(showref);
	return std::move(select_node);
}

unique_ptr<QueryNode> PEGTransformerFactory::TransformShowAllTables(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto result = make_uniq<ShowRef>();
	// Legacy reasons, see bind_showref.cpp
	result->table_name = "__show_tables_expanded";
	result->show_type = ShowType::DESCRIBE;
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(result);
	return std::move(select_node);
}

unique_ptr<QueryNode> PEGTransformerFactory::TransformShowQualifiedName(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto showref = make_uniq<ShowRef>();

	showref->show_type = transformer.Transform<ShowType>(list_pr.Child<ListParseResult>(0));

	auto opt_table_name_parens = list_pr.Child<OptionalParseResult>(1);
	if (opt_table_name_parens.HasResult()) {
		auto base_table_or_string = opt_table_name_parens.optional_result->Cast<ListParseResult>();
		auto choice_pr = base_table_or_string.Child<ChoiceParseResult>(0);

		if (choice_pr.result->type == ParseResultType::STRING) {
			// Case: SHOW 'something' or DESCRIBE 'something'
			showref->table_name = choice_pr.result->Cast<StringLiteralParseResult>().result;
		} else {
			// Case: A relation/table reference
			auto base_table = transformer.Transform<unique_ptr<BaseTableRef>>(choice_pr.result);

			if (showref->show_type == ShowType::SHOW_FROM) {
				// Logic for SHOW TABLES FROM [database].[schema]
				if (IsInvalidSchema(base_table->schema_name)) {
					showref->schema_name = base_table->table_name;
				} else {
					showref->catalog_name = base_table->schema_name;
					showref->schema_name = base_table->table_name;
				}
			} else if (IsInvalidSchema(base_table->schema_name)) {
				// Logic for unqualified relations (databases, tables, variables)
				auto table_name = StringUtil::Lower(base_table->table_name);
				if (table_name == "databases" || table_name == "tables" || table_name == "schemas" ||
				    table_name == "variables") {
					showref->table_name = "\"" + table_name + "\"";
					showref->show_type = ShowType::SHOW_UNQUALIFIED;
				}
			}
		}
		if (showref->table_name.empty() && showref->show_type != ShowType::SHOW_FROM) {
			auto show_select_node = make_uniq<SelectNode>();
			show_select_node->select_list.push_back(make_uniq<StarExpression>());
			if (choice_pr.result->type == ParseResultType::STRING) {
				// Case: SHOW 'something' or DESCRIBE 'something'
				auto table_ref = make_uniq<BaseTableRef>();
				table_ref->table_name = choice_pr.result->Cast<StringLiteralParseResult>().result;
				show_select_node->from_table = std::move(table_ref);
			} else {
				// Case: A relation/table reference
				show_select_node->from_table = transformer.Transform<unique_ptr<BaseTableRef>>(choice_pr.result);
			}
			showref->query = std::move(show_select_node);
		}
	} else {
		// Case: No relation specified (e.g., just "SHOW TABLES")
		if (showref->show_type == ShowType::SUMMARY) {
			throw ParserException("Expected table name with SUMMARIZE");
		}
		showref->table_name = "__show_tables_expanded";
		showref->show_type = ShowType::DESCRIBE;
	}

	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(showref);

	return std::move(select_node);
}
ShowType PEGTransformerFactory::TransformShowOrDescribeOrSummarize(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<ShowType>(list_pr.Child<ChoiceParseResult>(0).result);
}

ShowType PEGTransformerFactory::TransformShowOrDescribe(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<ShowType>(list_pr.Child<ChoiceParseResult>(0).result);
}

ShowType PEGTransformerFactory::TransformSummarize(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<ShowType>(list_pr.Child<ListParseResult>(0));
}

} // namespace duckdb
