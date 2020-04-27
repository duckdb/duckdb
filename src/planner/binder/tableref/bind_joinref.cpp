#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(JoinRef &ref) {
	auto result = make_unique<BoundJoinRef>();
	result->type = ref.type;
	if (ref.using_columns.size() > 0) {
		// USING columns
		assert(!result->condition);
		vector<string> left_join_bindings;
		vector<unordered_set<string>> matching_left_bindings;

		result->left = Bind(*ref.left);
		for (auto &using_column : ref.using_columns) {
			// for each using column, get the matching binding
			auto left_bindings = bind_context.GetMatchingBindings(using_column);
			if (left_bindings.size() == 0) {
				throw BinderException("Column \"%s\" does not exist on left side of join!", using_column.c_str());
			}
			// find the join binding
			string left_binding;
			for (auto &binding : left_bindings) {
				if (!bind_context.BindingIsHidden(binding, using_column)) {
					if (!left_binding.empty()) {
						string error = "Column name \"" + using_column +
						               "\" is ambiguous: it exists more than once on left side of join.\nCandidates:";
						for (auto &binding : left_bindings) {
							error += "\n\t" + binding + "." + using_column;
						}
						throw BinderException(error);
					} else {
						left_binding = binding;
					}
				}
			}
			left_join_bindings.push_back(left_binding);
			matching_left_bindings.push_back(move(left_bindings));
		}
		result->right = Bind(*ref.right);
		for (idx_t i = 0; i < ref.using_columns.size(); i++) {
			auto &using_column = ref.using_columns[i];
			auto &left_bindings = matching_left_bindings[i];
			auto left_binding = left_join_bindings[i];

			auto all_bindings = bind_context.GetMatchingBindings(using_column);
			string right_binding;
			for (auto &binding : all_bindings) {
				if (left_bindings.find(binding) == left_bindings.end()) {
					assert(right_binding.empty());
					right_binding = binding;
				}
			}
			if (right_binding.empty()) {
				throw BinderException("Column \"%s\" does not exist on right side of join!", using_column.c_str());
			}
			assert(!left_binding.empty());
			auto left_expr = make_unique<ColumnRefExpression>(using_column, left_binding);
			auto right_expr = make_unique<ColumnRefExpression>(using_column, right_binding);
			bind_context.hidden_columns.insert(right_expr->table_name + "." + right_expr->column_name);
			auto comp_expr =
			    make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(left_expr), move(right_expr));
			if (!ref.condition) {
				ref.condition = move(comp_expr);
			} else {
				ref.condition = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(ref.condition),
				                                                   move(comp_expr));
			}
		}
	} else {
		result->left = Bind(*ref.left);
		result->right = Bind(*ref.right);
	}
	if (ref.condition) {
		WhereBinder binder(*this, context);
		result->condition = binder.Bind(ref.condition);
	}
	return move(result);
}
