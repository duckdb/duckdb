#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformFunctionArgument(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNamedParameter(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	result->alias = list_pr.Child<IdentifierParseResult>(0).identifier;
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformTableFunctionArguments(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	// TableFunctionArguments <- Parens(List(FunctionArgument)?)
	vector<unique_ptr<ParsedExpression>> result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto stripped_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0))->Cast<OptionalParseResult>();
	if (stripped_parens.HasResult()) {
		auto argument_list = ExtractParseResultsFromList(stripped_parens.optional_result);
		for (auto &argument : argument_list) {
			result.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(argument));
		}
	}
	return result;
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformBaseTableName(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		auto table_name = choice_pr.result->Cast<IdentifierParseResult>().identifier;
		const auto description = TableDescription(INVALID_CATALOG, INVALID_SCHEMA, table_name);
		return make_uniq<BaseTableRef>(description);
	}
	return transformer.Transform<unique_ptr<BaseTableRef>>(choice_pr.result);
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformSchemaReservedTable(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	// SchemaReservedTable <- SchemaQualification ReservedTableName
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto table_name = list_pr.Child<IdentifierParseResult>(1).identifier;

	const auto description = TableDescription(INVALID_CATALOG, schema, table_name);
	return make_uniq<BaseTableRef>(description);
}

unique_ptr<BaseTableRef>
PEGTransformerFactory::TransformCatalogReservedSchemaTable(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	// CatalogReservedSchemaTable <- CatalogQualification ReservedSchemaQualification ReservedTableName
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto catalog = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(1));
	auto table_name = list_pr.Child<IdentifierParseResult>(2).identifier;
	const auto description = TableDescription(catalog, catalog, table_name);
	return make_uniq<BaseTableRef>(description);
}

string PEGTransformerFactory::TransformSchemaQualification(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

string PEGTransformerFactory::TransformCatalogQualification(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

QualifiedName PEGTransformerFactory::TransformQualifiedName(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<QualifiedName>(list_pr.Child<ChoiceParseResult>(0).result);
}

QualifiedName PEGTransformerFactory::TransformCatalogReservedSchemaIdentifierOrStringLiteral(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	QualifiedName result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	result.catalog = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	result.schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(1));
	result.name = transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	return result;
}

QualifiedName PEGTransformerFactory::TransformSchemaReservedIdentifierOrStringLiteral(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	QualifiedName result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	result.catalog = INVALID_CATALOG;
	result.schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	result.name = transformer.Transform<string>(list_pr.Child<ListParseResult>(1));
	return result;
}

string PEGTransformerFactory::TransformReservedIdentifierOrStringLiteral(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		return choice_pr.result->Cast<IdentifierParseResult>().identifier;
	}
	return transformer.Transform<string>(choice_pr.result);
}

QualifiedName PEGTransformerFactory::TransformTableNameIdentifierOrStringLiteral(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	QualifiedName result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	result.catalog = INVALID_CATALOG;
	result.schema = INVALID_SCHEMA;
	result.name = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformWhereClause(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}


} // namespace duckdb
