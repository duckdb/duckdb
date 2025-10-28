#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformDeleteStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto result = make_uniq<DeleteStatement>();
	auto with_opt = list_pr.Child<OptionalParseResult>(0);
	if (with_opt.HasResult()) {
		throw NotImplementedException("WITH for DeleteStatement not yet implemented");
	}
	result->table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(3));
	transformer.TransformOptional<vector<unique_ptr<TableRef>>>(list_pr, 4, result->using_clauses);
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 5, result->condition);
	transformer.TransformOptional<vector<unique_ptr<ParsedExpression>>>(list_pr, 6, result->returning_list);
	return std::move(result);
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformTargetOptAlias(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto table_ref = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<string>(list_pr, 2, table_ref->alias);
	return table_ref;
}

vector<unique_ptr<TableRef>> PEGTransformerFactory::TransformDeleteUsingClause(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto table_ref_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(1));
	vector<unique_ptr<TableRef>> result;
	for (auto table_ref : table_ref_list) {
		result.push_back(transformer.Transform<unique_ptr<TableRef>>(table_ref));
	}
	return result;
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformTruncateStatement(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<DeleteStatement>();
	result->table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(2));
	return std::move(result);
}

} // namespace duckdb
