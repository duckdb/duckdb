#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/parser/query_node/list.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

struct BoundCTEData {
	string ctename;
	CTEMaterialize materialized;
	idx_t setop_index;
	shared_ptr<Binder> query_binder;
	shared_ptr<Binder> child_binder;
	BoundStatement query;
	vector<string> names;
	vector<LogicalType> types;
};

BoundStatement Binder::BindNode(QueryNode &node) {
	reference<Binder> current_binder(*this);
	vector<BoundCTEData> bound_ctes;
	for (auto &cte : node.cte_map.map) {
		bound_ctes.push_back(current_binder.get().PrepareCTE(cte.first, *cte.second));
		current_binder = *bound_ctes.back().child_binder;
	}
	BoundStatement result;
	// now we bind the node
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		result = current_binder.get().BindNode(node.Cast<SelectNode>());
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		result = current_binder.get().BindNode(node.Cast<RecursiveCTENode>());
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		result = current_binder.get().BindNode(node.Cast<SetOperationNode>());
		break;
	case QueryNodeType::STATEMENT_NODE:
		result = current_binder.get().BindNode(node.Cast<StatementNode>());
		break;
	default:
		throw InternalException("Unsupported query node type");
	}
	for (idx_t i = bound_ctes.size(); i > 0; i--) {
		auto &finish_binder = i == 1 ? *this : *bound_ctes[i - 2].child_binder;
		result = finish_binder.FinishCTE(bound_ctes[i - 1], std::move(result));
	}
	return result;
}

BoundCTEData Binder::PrepareCTE(const string &ctename, CommonTableExpressionInfo &statement) {
	BoundCTEData result;

	// first recursively visit the materialized CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.query);

	result.ctename = ctename;
	result.materialized = statement.materialized;
	result.setop_index = GenerateTableIndex();

	AddCTE(ctename);

	result.query_binder = Binder::CreateBinder(context, this);
	result.query = result.query_binder->BindNode(*statement.query->node);

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

	result.child_binder = Binder::CreateBinder(context, this);

	// Add bindings of left side to temporary CTE bindings context
	// as we are binding a CTE currently, we take precendence over the existing binding.
	// This implements the CTE shadowing behavior.
	result.child_binder->bind_context.AddCTEBinding(result.setop_index, ctename, names, result.types);
	return result;
}

BoundStatement Binder::FinishCTE(BoundCTEData &bound_cte, BoundStatement child) {
	for (auto &c : bound_cte.query_binder->correlated_columns) {
		bound_cte.child_binder->AddCorrelatedColumn(c);
	}

	BoundStatement result;
	// the result types of the CTE are the types of the LHS
	result.types = child.types;
	result.names = child.names;

	MoveCorrelatedExpressions(*bound_cte.child_binder);
	MoveCorrelatedExpressions(*bound_cte.query_binder);

	auto cte_query = std::move(bound_cte.query.plan);
	auto cte_child = std::move(child.plan);

	auto root = make_uniq<LogicalMaterializedCTE>(bound_cte.ctename, bound_cte.setop_index, result.types.size(),
	                                              std::move(cte_query), std::move(cte_child), bound_cte.materialized);

	// check if there are any unplanned subqueries left in either child
	has_unplanned_dependent_joins = has_unplanned_dependent_joins ||
	                                bound_cte.child_binder->has_unplanned_dependent_joins ||
	                                bound_cte.query_binder->has_unplanned_dependent_joins;
	result.plan = std::move(root);
	return result;
}

} // namespace duckdb
