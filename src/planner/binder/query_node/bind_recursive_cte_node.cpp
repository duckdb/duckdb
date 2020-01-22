#include <duckdb/planner/tableref/bound_cteref.hpp>
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/query_node/bound_recursive_cte_node.hpp"

using namespace duckdb;
using namespace std;


unique_ptr<BoundQueryNode> Binder::Bind(RecursiveCTENode &statement) {
    auto result = make_unique<BoundRecursiveCTENode>();

    // first recursively visit the set operations
    // both the left and right sides have an independent BindContext and Binder
    assert(statement.left);
    assert(statement.right);

    result->union_all = statement.union_all;
    result->setop_index = GenerateTableIndex();

    result->left_binder = make_unique<Binder>(context, this);
    result->left = result->left_binder->Bind(*statement.left);

    bind_context.AddGenericBinding(result->setop_index, statement.ctename, result->left->names, result->left->types);

    result->right_binder = make_unique<Binder>(context, this);
    result->right_binder->bind_context.AddGenericBinding(result->setop_index, statement.ctename, result->left->names, result->left->types);
    result->right = result->right_binder->Bind(*statement.right);

    result->names = result->left->names;

    // move the correlated expressions from the child binders to this binder
    MoveCorrelatedExpressions(*result->left_binder);
    MoveCorrelatedExpressions(*result->right_binder);

    // now both sides have been bound we can resolve types
    if (result->left->types.size() != result->right->types.size()) {
        throw Exception("Set operations can only apply to expressions with the "
                        "same number of result columns");
    }

    // figure out the types of the setop result by picking the max of both
    for (index_t i = 0; i < result->left->types.size(); i++) {
        auto result_type = MaxSQLType(result->left->types[i], result->right->types[i]);
        result->types.push_back(result_type);
    }

    return move(result);
}