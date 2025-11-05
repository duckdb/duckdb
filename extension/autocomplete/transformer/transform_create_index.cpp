#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateIndexStmt(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	throw NotImplementedException("TransformCreateIndexStmt");
	auto result = make_uniq<CreateStatement>();
	auto index_info = make_uniq<CreateIndexInfo>();
	bool unique = list_pr.Child<OptionalParseResult>(0).HasResult();
	index_info->constraint_type = unique ? IndexConstraintType::UNIQUE : IndexConstraintType::NONE;
	bool if_not_exists = list_pr.Child<OptionalParseResult>(2).HasResult();
	index_info->on_conflict =
	    if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	transformer.TransformOptional<string>(list_pr, 3, index_info->index_name);
	auto table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(5));
	index_info->table = table->table_name;
	index_info->catalog = table->catalog_name;
	index_info->schema = table->schema_name;
	index_info->index_type = "art";
	transformer.TransformOptional<string>(list_pr, 6, index_info->index_type);

	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(7));
	auto index_element_list = ExtractParseResultsFromList(extract_parens);
	for (auto index_element : index_element_list) {
		auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(index_element);
		index_info->expressions.push_back(expr->Copy());
		index_info->parsed_expressions.push_back(expr->Copy());
	}

	transformer.TransformOptional<case_insensitive_map_t<Value>>(list_pr, 8, index_info->options);

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
	throw NotImplementedException("Rule 'WithList' has not been implemented yet");
}

string PEGTransformerFactory::TransformIndexName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

} // namespace duckdb
