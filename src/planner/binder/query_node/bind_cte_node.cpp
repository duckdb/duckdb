#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/query_node/bound_cte_node.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

unique_ptr<BoundQueryNode> Binder::BindNode(CTENode &statement) {
	auto result = make_uniq<BoundCTENode>();

	// first recursively visit the materialized CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.query);
	D_ASSERT(statement.child);

	result->ctename = statement.ctename;
	result->setop_index = GenerateTableIndex();

	result->query_binder = Binder::CreateBinder(context, this);
	result->query = result->query_binder->BindNode(*statement.query);

	// the result types of the CTE are the types of the LHS
	result->types = result->query->types;
	// names are picked from the LHS, unless aliases are explicitly specified
	result->names = result->query->names;
	for (idx_t i = 0; i < statement.aliases.size() && i < result->names.size(); i++) {
		result->names[i] = statement.aliases[i];
	}

	// This allows the right side to reference the CTE
	bind_context.AddGenericBinding(result->setop_index, statement.ctename, result->names, result->types);

	result->child_binder = Binder::CreateBinder(context, this);

	// Move all modifiers to the child node.
	for (auto &modifier : statement.modifiers) {
		statement.child->modifiers.push_back(std::move(modifier));
	}

	statement.modifiers.clear();

	// Add bindings of left side to temporary CTE bindings context
	result->child_binder->bind_context.AddCTEBinding(result->setop_index, statement.ctename, result->names,
	                                                 result->types);
	result->child = result->child_binder->BindNode(*statement.child);

	// the result types of the CTE are the types of the LHS
	result->types = result->child->types;
	// names are picked from the LHS, unless aliases are explicitly specified
	result->names = result->child->names;
	for (idx_t i = 0; i < statement.aliases.size() && i < result->names.size(); i++) {
		result->names[i] = statement.aliases[i];
	}

	MoveCorrelatedExpressions(*result->query_binder);
	MoveCorrelatedExpressions(*result->child_binder);

	return std::move(result);
}

} // namespace duckdb
