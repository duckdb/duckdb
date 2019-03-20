#include "planner/expression_binder.hpp"
#include "parser/expression/subquery_expression.hpp"
#include "planner/expression/bound_subquery_expression.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::Bind(SubqueryExpression &subquery, uint32_t depth) {
	throw BinderException("FIXME: subquery binding");
	// // first bind the children of the subquery, if any
	// if (expr.child) {
	// 	string result = Bind(&subquery.child, depth);
	// 	if (!result.empty()) {
	// 		return BindResult(result);
	// 	}
	// }
	// // bind the subquery in a new binder
	// auto subquery_binder = make_unique<Binder>(context, &binder);
	// // the subquery may refer to CTEs from the parent query
	// subquery_binder->CTE_bindings = binder.CTE_bindings;
	// auto bound_node = subquery_binder->Bind(*subquery.subquery);
	// // check the correlated columns of the subquery for correlated columns with depth > 1
	// for (size_t i = 0; i < subquery_binder->correlated_columns.size(); i++) {
	// 	CorrelatedColumnInfo corr = subquery_binder->correlated_columns[i];
	// 	if (corr.depth > 1) {
	// 		// depth > 1, the column references the query ABOVE the current one
	// 		// add to the set of correlated columns for THIS query
	// 		corr.depth -= 1;
	// 		binder.AddCorrelatedColumn(corr);
	// 	}
	// }
	// // get the type of the subquery
	// // FIXME: should we cast here? probably?
	// SQLType result_type;
	// if (subquery.subquery_type == SubqueryType::SCALAR) {
	// 	assert(bound_node->types.size() == 1);
	// 	// scalar query: return type of first argument
	// 	result_type = bound_node->types[0];
	// } else {
	// 	// ANY/ALL/EXISTS: boolean return type
	// 	result_type = SQLType(SQLTypeId::BOOLEAN);
	// }
	// assert(result_type.id != SQLTypeId::INVALID); // "Subquery has no type"
	// auto result_type = select_list[0]->return_type;
	// // create the bound subquery
	// auto result = make_unique<BoundSubqueryExpression>(GetInternalType(result_type), result_type);
	// result->binder = move(subquery_binder);
	// result->subquery = move(bound_node);
	// if (subquery.child) {
	// 	result->child = GetExpression(subquery.child);
	// }
	// return BindResult(move(result));
}
