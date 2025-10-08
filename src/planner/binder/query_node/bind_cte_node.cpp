#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/query_node/bound_cte_node.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

BoundStatement Binder::BindNode(CTENode &statement) {
	// first recursively visit the materialized CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.query);

	return BindCTE(statement);
}

BoundStatement Binder::BindCTE(CTENode &statement) {
	BoundCTENode result;

	// first recursively visit the materialized CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.query);

	result.ctename = statement.ctename;
	result.materialized = statement.materialized;
	result.setop_index = GenerateTableIndex();

	AddCTE(result.ctename);

	result.query_binder = Binder::CreateBinder(context, this);
	result.query = result.query_binder->BindNode(*statement.query);

	// the result types of the CTE are the types of the LHS
	result.types = result.query.types;
	// names are picked from the LHS, unless aliases are explicitly specified
	result.names = result.query.names;
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
	bind_context.AddGenericBinding(result.setop_index, statement.ctename, names, result.types);

	result.child_binder = Binder::CreateBinder(context, this);

	// Add bindings of left side to temporary CTE bindings context
	// If there is already a binding for the CTE, we need to remove it first
	// as we are binding a CTE currently, we take precendence over the existing binding.
	// This implements the CTE shadowing behavior.
	result.child_binder->bind_context.AddCTEBinding(result.setop_index, statement.ctename, names, result.types);

	if (statement.child) {
		// Move all modifiers to the child node.
		for (auto &modifier : statement.modifiers) {
			statement.child->modifiers.push_back(std::move(modifier));
		}

		statement.modifiers.clear();

		result.child = result.child_binder->BindNode(*statement.child);
		for (auto &c : result.query_binder->correlated_columns) {
			result.child_binder->AddCorrelatedColumn(c);
		}

		// the result types of the CTE are the types of the LHS
		result.types = result.child.types;
		result.names = result.child.names;

		MoveCorrelatedExpressions(*result.child_binder);
	}

	MoveCorrelatedExpressions(*result.query_binder);

	BoundStatement result_statement;
	result_statement.types = result.types;
	result_statement.names = result.names;
	result_statement.plan = CreatePlan(result);
	return result_statement;
}

} // namespace duckdb
