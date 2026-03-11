#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"

namespace duckdb {
unique_ptr<QueryNode> PEGTransformerFactory::ToRecursiveCTE(unique_ptr<QueryNode> node, const string &name,
                                                            vector<string> &aliases,
                                                            vector<unique_ptr<ParsedExpression>> &key_targets) {
	if (node->type != QueryNodeType::SET_OPERATION_NODE) {
		return node;
	}

	auto &set_node = node->Cast<SetOperationNode>();

	if (set_node.setop_type != SetOperationType::UNION) {
		return node;
	}

	if (set_node.children.size() != 2) {
		throw ParserException("Expected exactly two children to set operation node in recursive CTE");
	}

	auto recursive_node = make_uniq<RecursiveCTENode>();
	recursive_node->cte_map = std::move(set_node.cte_map);
	recursive_node->ctename = name;
	recursive_node->aliases = aliases;

	auto owned_set_node = unique_ptr_cast<QueryNode, SetOperationNode>(std::move(node));
	recursive_node->left = std::move(owned_set_node->children[0]);
	recursive_node->right = std::move(owned_set_node->children[1]);
	recursive_node->union_all = owned_set_node->setop_all;
	for (auto &key : key_targets) {
		recursive_node->key_targets.emplace_back(key->Copy());
	}

	return std::move(recursive_node);
}

void PEGTransformerFactory::WrapRecursiveView(unique_ptr<CreateViewInfo> &info, unique_ptr<QueryNode> inner_node) {
	auto outer_select = make_uniq<SelectNode>();

	auto cte_info = make_uniq<CommonTableExpressionInfo>();
	cte_info->aliases = info->aliases;

	auto cte_select = make_uniq<SelectStatement>();
	cte_select->node = std::move(inner_node);
	cte_info->query = std::move(cte_select);

	outer_select->cte_map.map.insert(info->view_name, std::move(cte_info));

	for (const auto &column : info->aliases) {
		outer_select->select_list.push_back(make_uniq<ColumnRefExpression>(column));
	}

	auto table_description = TableDescription(info->catalog, info->schema, info->view_name);
	outer_select->from_table = make_uniq<BaseTableRef>(table_description);

	auto outer_select_statement = make_uniq<SelectStatement>();
	outer_select_statement->node = std::move(outer_select);
	info->query = std::move(outer_select_statement);
}

void PEGTransformerFactory::ConvertToRecursiveView(unique_ptr<CreateViewInfo> &info, unique_ptr<QueryNode> &node) {
	vector<unique_ptr<ParsedExpression>> empty_key_targets;
	auto result_node = ToRecursiveCTE(std::move(node), info->view_name, info->aliases, empty_key_targets);
	WrapRecursiveView(info, std::move(result_node));
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateViewStmt(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
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
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(6));
	if (list_pr.Child<OptionalParseResult>(0).HasResult()) {
		ConvertToRecursiveView(info, select_statement->node);
	} else {
		info->query = std::move(select_statement);
	}
	transformer.PivotEntryCheck("view");
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
