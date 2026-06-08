#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"

namespace duckdb {
unique_ptr<QueryNode> PEGTransformerFactory::ToRecursiveCTE(unique_ptr<QueryNode> node, const Identifier &name,
                                                            vector<Identifier> &aliases,
                                                            vector<unique_ptr<ParsedExpression>> &key_targets) {
	if (node->type != QueryNodeType::SET_OPERATION_NODE) {
		return node;
	}

	auto &set_node = node->Cast<SetOperationNode>();

	if (set_node.setop_type != SetOperationType::UNION) {
		return node;
	}

	if (set_node.children.size() < 2) {
		throw ParserException("Expected at least two children to set operation node in recursive CTE");
	}

	auto recursive_node = make_uniq<RecursiveCTENode>();
	recursive_node->cte_map = std::move(set_node.cte_map);
	recursive_node->ctename = name;
	recursive_node->aliases = aliases;

	auto owned_set_node = unique_ptr_cast<QueryNode, SetOperationNode>(std::move(node));
	recursive_node->union_all = owned_set_node->setop_all;

	for (auto &modifier : owned_set_node->modifiers) {
		if (modifier->type == ResultModifierType::LIMIT_MODIFIER) {
			throw ParserException("LIMIT or OFFSET in a recursive query is not allowed");
		}
		if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
			throw ParserException("ORDER BY in a recursive query is not allowed");
		}
	}
	if (owned_set_node->children.size() == 2) {
		recursive_node->left = std::move(owned_set_node->children[0]);
		recursive_node->right = std::move(owned_set_node->children[1]);
	} else {
		// N-ary flattened node: split into binary (left = all but last, right = last)
		// This matches the left-recursive binary tree structure from the grammar
		recursive_node->right = std::move(owned_set_node->children.back());
		owned_set_node->children.pop_back();
		if (owned_set_node->children.size() == 1) {
			recursive_node->left = std::move(owned_set_node->children[0]);
		} else {
			recursive_node->left = std::move(owned_set_node);
		}
	}
	for (auto &key : key_targets) {
		recursive_node->key_targets.emplace_back(key->Copy());
	}

	return std::move(recursive_node);
}

void PEGTransformerFactory::WrapRecursiveView(unique_ptr<CreateViewInfo> &info, unique_ptr<QueryNode> inner_node) {
	auto outer_select = make_uniq<SelectNode>();

	auto cte_info = make_uniq<CommonTableExpressionInfo>();
	cte_info->aliases = info->aliases;

	cte_info->query_node = std::move(inner_node);

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

unique_ptr<CreateStatement>
PEGTransformerFactory::TransformCreateViewStmt(PEGTransformer &transformer, const bool &create_recursive,
                                               const bool &if_not_exists, const QualifiedName &qualified_name,
                                               const vector<string> &insert_column_list,
                                               case_insensitive_map_t<unique_ptr<ParsedExpression>> with_list,
                                               unique_ptr<SelectStatement> select_statement_internal) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateViewInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->catalog = qualified_name.catalog;
	info->schema = qualified_name.schema;
	info->view_name = qualified_name.name;
	info->aliases = StringsToIdentifiers(insert_column_list);
	if (!with_list.empty()) {
		for (auto &option_entry : with_list) {
			if (!StringUtil::CIEquals(option_entry.first, "defer_binding")) {
				throw ParserException("Only DEFER_BINDING is currently supported as option for CREATE VIEW");
			}
			if (option_entry.second->GetExpressionClass() != ExpressionClass::CONSTANT) {
				throw InvalidInputException("Defer binding option must be a constant value");
			}
			auto &val = option_entry.second->Cast<ConstantExpression>().GetValue();
			if (val.IsNull()) {
				info->binding_mode = CreateViewBindingMode::SKIP_BINDING;
			} else if (val.type().id() != LogicalTypeId::BOOLEAN) {
				throw InvalidInputException("Defer binding option must be a boolean");
			} else if (BooleanValue::Get(val)) {
				info->binding_mode = CreateViewBindingMode::SKIP_BINDING;
			}
		}
	}
	if (create_recursive) {
		ConvertToRecursiveView(info, select_statement_internal->node);
	} else {
		info->query = std::move(select_statement_internal);
	}
	transformer.PivotEntryCheck("view");
	result->info = std::move(info);
	return result;
}

bool PEGTransformerFactory::TransformCreateRecursive(PEGTransformer &transformer) {
	return true;
}

} // namespace duckdb
