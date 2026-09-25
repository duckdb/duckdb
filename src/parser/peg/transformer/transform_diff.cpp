#include "duckdb/parser/tableref/diffref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

// "DIFF old, new [KEY (columns)] [CELLS]" - the query "SELECT * FROM (DIFF ...)"
unique_ptr<SelectStatement> PEGTransformerFactory::TransformDiffStatement(PEGTransformer &transformer,
                                                                          unique_ptr<TableRef> diff_side,
                                                                          unique_ptr<TableRef> diff_side_1,
                                                                          const optional<vector<string>> &diff_key,
                                                                          const optional<bool> &diff_cells) {
	auto diff_ref = make_uniq<DiffRef>();
	diff_ref->old_side = std::move(diff_side);
	diff_ref->new_side = std::move(diff_side_1);
	if (diff_key) {
		diff_ref->key = *diff_key;
	}
	diff_ref->cells = diff_cells && *diff_cells;

	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(diff_ref);
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	return select_statement;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformDiffSubquery(PEGTransformer &transformer,
                                                                  unique_ptr<SelectStatement> select_parens) {
	return make_uniq<SubqueryRef>(std::move(select_parens));
}

unique_ptr<TableRef> PEGTransformerFactory::TransformDiffTable(PEGTransformer &transformer,
                                                               unique_ptr<BaseTableRef> base_table_name) {
	return std::move(base_table_name);
}

bool PEGTransformerFactory::TransformDiffCells(PEGTransformer &transformer) {
	return true;
}

} // namespace duckdb
