#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/materialized_cte_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/query_node/bound_cte_node.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

namespace duckdb {

BoundStatement Binder::Bind(MaterializedCTEStatement &statement) {
	auto result = make_uniq<BoundCTENode>();

	// first recursively visit the materialized CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.query);
	D_ASSERT(statement.child);

	// Add CTEs as bindable
	AddCTEMap(statement.cte_map);

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

	// Rename columns if duplicate names are detected
	idx_t index = 1;
	vector<string> names;
	for (auto &n : result->names) {
		string name = n;
		while (find(names.begin(), names.end(), name) != names.end()) {
			name = n + "_" + std::to_string(index++);
		}
		names.push_back(name);
	}

	// This allows the right side to reference the CTE
	bind_context.AddGenericBinding(result->setop_index, statement.ctename, names, result->types);

	result->child_binder = Binder::CreateBinder(context, this);

	// Add bindings of left side to temporary CTE bindings context
	result->child_binder->bind_context.AddCTEBinding(result->setop_index, statement.ctename, names, result->types);

	auto child_result = result->child_binder->Bind(*statement.child);

	for (auto &c : result->query_binder->correlated_columns) {
		result->child_binder->AddCorrelatedColumn(c);
	}

	// Create plan of sub-statement
	auto cte_query = CreatePlan(*result->query);

	D_ASSERT(child_result.plan->children.size() == 1);

	// extract operator below root operation
	auto plan = std::move(child_result.plan->children[0]);
	child_result.plan->children.clear();

	// add logical plan for materialized CTE in with children as right side operation
	auto root = make_uniq<LogicalMaterializedCTE>(result->ctename, result->setop_index, result->types.size(),
	                                              std::move(cte_query), std::move(plan));

	// re-construct statement plan with materialized CTE
	child_result.plan->children.push_back(std::move(root));

	return child_result;
}

} // namespace duckdb
