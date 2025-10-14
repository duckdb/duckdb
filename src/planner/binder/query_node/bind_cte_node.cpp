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
	shared_ptr<Binder> child_binder;
	shared_ptr<CTEBindState> cte_bind_state;
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

CTEBindState::CTEBindState(Binder &parent_binder_p, QueryNode &cte_def_p, const vector<string> &aliases_p)
    : parent_binder(parent_binder_p), cte_def(cte_def_p), aliases(aliases_p),
      active_binder_count(parent_binder.GetActiveBinders().size()) {
}

CTEBindState::~CTEBindState() {
}

bool CTEBindState::IsBound() const {
	return query_binder.get() != nullptr;
}

void CTEBindState::Bind(CTEBinding &binding) {
	// we are lazily binding the CTE
	// we need to bind it as if we were binding it during PrepareCTE
	query_binder = Binder::CreateBinder(parent_binder.context, parent_binder);

	// we clear any expression binders that were added in the mean-time, to ensure we are not binding to any newly added
	// correlated columns
	auto &active_binders = parent_binder.GetActiveBinders();
	vector<reference<ExpressionBinder>> stored_binders;
	for (idx_t i = active_binder_count; i < active_binders.size(); i++) {
		stored_binders.push_back(active_binders[i]);
	}
	active_binders.erase(active_binders.begin() + UnsafeNumericCast<int64_t>(active_binder_count),
	                     active_binders.end());

	// add this CTE to the query binder on the RHS with "CANNOT_BE_REFERENCED" to detect recursive references to
	// ourselves
	query_binder->bind_context.AddCTEBinding(binding.GetIndex(), binding.GetBindingAlias(), vector<string>(),
	                                         vector<LogicalType>(), CTEType::CANNOT_BE_REFERENCED);

	// bind the actual CTE
	query = query_binder->Bind(cte_def);

	// after binding - we add the active binders we removed back so we can leave the binder in its original state
	for (auto &stored_binder : stored_binders) {
		active_binders.push_back(stored_binder);
	}

	// the result types of the CTE are the types of the LHS
	types = query.types;
	// names are picked from the LHS, unless aliases are explicitly specified
	names = query.names;
	for (idx_t i = 0; i < aliases.size() && i < names.size(); i++) {
		names[i] = aliases[i];
	}

	// Rename columns if duplicate names are detected
	idx_t index = 1;
	vector<string> new_names;
	// Use a case-insensitive set to track names
	case_insensitive_set_t ci_names;
	for (auto &n : names) {
		string name = n;
		while (ci_names.find(name) != ci_names.end()) {
			name = n + "_" + std::to_string(index++);
		}
		new_names.push_back(name);
		ci_names.insert(name);
	}
	names = std::move(new_names);
}

BoundCTEData Binder::PrepareCTE(const string &ctename, CommonTableExpressionInfo &statement) {
	BoundCTEData result;

	// first recursively visit the materialized CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.query);

	result.ctename = ctename;
	result.materialized = statement.materialized;
	result.setop_index = GenerateTableIndex();

	// instead of eagerly binding the CTE here we add the CTE bind state to the list of CTE bindings
	// the CTE is bound lazily - when referenced for the first time we perform the binding
	result.cte_bind_state = make_shared_ptr<CTEBindState>(*this, *statement.query->node, statement.aliases);

	result.child_binder = Binder::CreateBinder(context, this);

	// Add bindings of left side to temporary CTE bindings context
	// as we are binding a CTE currently, we take precendence over the existing binding.
	// This implements the CTE shadowing behavior.
	auto cte_binding = make_uniq<CTEBinding>(BindingAlias(ctename), result.cte_bind_state, result.setop_index);
	result.child_binder->bind_context.AddCTEBinding(std::move(cte_binding));
	return result;
}

BoundStatement Binder::FinishCTE(BoundCTEData &bound_cte, BoundStatement child) {
	if (!bound_cte.cte_bind_state->IsBound()) {
		// CTE was not bound - just ignore it
		return child;
	}
	auto &bind_state = *bound_cte.cte_bind_state;
	for (auto &c : bind_state.query_binder->correlated_columns) {
		bound_cte.child_binder->AddCorrelatedColumn(c);
	}

	BoundStatement result;
	// the result types of the CTE are the types of the LHS
	result.types = child.types;
	result.names = child.names;

	MoveCorrelatedExpressions(*bound_cte.child_binder);
	MoveCorrelatedExpressions(*bind_state.query_binder);

	auto cte_query = std::move(bind_state.query.plan);
	auto cte_child = std::move(child.plan);

	auto root = make_uniq<LogicalMaterializedCTE>(bound_cte.ctename, bound_cte.setop_index, result.types.size(),
	                                              std::move(cte_query), std::move(cte_child), bound_cte.materialized);

	// check if there are any unplanned subqueries left in either child
	has_unplanned_dependent_joins = has_unplanned_dependent_joins ||
	                                bound_cte.child_binder->has_unplanned_dependent_joins ||
	                                bind_state.query_binder->has_unplanned_dependent_joins;
	result.plan = std::move(root);
	return result;
}

} // namespace duckdb
