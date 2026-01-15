#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateViewStmt(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	throw NotImplementedException("TransformCreateViewStmt");
	// TODO(Dtenwolde) handle recursive views
	auto if_not_exists = list_pr.Child<OptionalParseResult>(2).HasResult();
	auto qualified_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(3));
	auto insert_column_list_pr = list_pr.Child<OptionalParseResult>(4);
	vector<string> column_list;
	if (insert_column_list_pr.HasResult()) {
		column_list = transformer.Transform<vector<string>>(insert_column_list_pr.optional_result);
	}
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateViewInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->catalog = qualified_name.catalog;
	info->schema = qualified_name.schema;
	info->view_name = qualified_name.name;
	info->aliases = column_list;
	info->query = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(6));
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
