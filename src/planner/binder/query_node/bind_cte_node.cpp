#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"

namespace duckdb {

BoundStatement Binder::BindNode(CTENode &statement) {
	// first recursively visit the materialized CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.query);

	return BindCTE(statement);
}

BoundStatement Binder::BindCTE(CTENode &statement) {
	BoundStatement result;

	// first recursively visit the materialized CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.query);

	auto ctename = statement.ctename;
	auto materialized = statement.materialized;
	auto setop_index = GenerateTableIndex();

	AddCTE(ctename);

	auto query_binder = Binder::CreateBinder(context, this);
	auto query = query_binder->BindNode(*statement.query);

	// the result types of the CTE are the types of the LHS
	result.types = query.types;
	// names are picked from the LHS, unless aliases are explicitly specified
	result.names = query.names;
	for (idx_t i = 0; i < statement.aliases.size() && i < result.names.size(); i++) {
		result.names[i] = statement.aliases[i];
	}

	// Rename columns if duplicate names are detected
	idx_t index = 1;
	vector<string> names;
	// Use a case-insensitive set to track names
	case_insensitive_set_t ci_names;
	for (auto &n : result.names) {
		string name = n;
		while (ci_names.find(name) != ci_names.end()) {
			name = n + "_" + std::to_string(index++);
		}
		names.push_back(name);
		ci_names.insert(name);
	}

	// This allows the right side to reference the CTE
	bind_context.AddGenericBinding(setop_index, statement.ctename, names, result.types);

	auto child_binder = Binder::CreateBinder(context, this);

	// Add bindings of left side to temporary CTE bindings context
	// If there is already a binding for the CTE, we need to remove it first
	// as we are binding a CTE currently, we take precendence over the existing binding.
	// This implements the CTE shadowing behavior.
	child_binder->bind_context.AddCTEBinding(setop_index, statement.ctename, names, result.types);

	BoundStatement child;
	if (statement.child) {
		child = child_binder->BindNode(*statement.child);
		for (auto &c : query_binder->correlated_columns) {
			child_binder->AddCorrelatedColumn(c);
		}

		// the result types of the CTE are the types of the LHS
		result.types = child.types;
		result.names = child.names;

		MoveCorrelatedExpressions(*child_binder);
	}

	MoveCorrelatedExpressions(*query_binder);

	auto cte_query = std::move(query.plan);
	auto cte_child = std::move(child.plan);

	auto root = make_uniq<LogicalMaterializedCTE>(ctename, setop_index, result.types.size(), std::move(cte_query),
	                                              std::move(cte_child), materialized);

	// check if there are any unplanned subqueries left in either child
	has_unplanned_dependent_joins = has_unplanned_dependent_joins || child_binder->has_unplanned_dependent_joins ||
	                                query_binder->has_unplanned_dependent_joins;
	result.plan = std::move(root);
	return result;
}

} // namespace duckdb
