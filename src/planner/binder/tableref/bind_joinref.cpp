#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"

namespace duckdb {
using namespace std;

static void AddCondition(JoinRef &ref, string left_alias, string right_alias, string column_name) {
	auto left_expr = make_unique<ColumnRefExpression>(column_name, left_alias);
	auto right_expr = make_unique<ColumnRefExpression>(column_name, right_alias);
	auto comp_expr =
		make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(left_expr), move(right_expr));
	if (!ref.condition) {
		ref.condition = move(comp_expr);
	} else {
		ref.condition = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(ref.condition),
															move(comp_expr));
	}
}

unique_ptr<BoundTableRef> Binder::Bind(JoinRef &ref) {
	auto result = make_unique<BoundJoinRef>();
	result->left_binder = make_unique<Binder>(context, this);
	result->right_binder = make_unique<Binder>(context, this);
	auto &left_binder = *result->left_binder;
	auto &right_binder = *result->right_binder;

	result->type = ref.type;
	result->left = left_binder.Bind(*ref.left);
	result->right = right_binder.Bind(*ref.right);
	if (ref.is_natural) {
		// natural join, figure out which column names are present in both sides of the join
		// first bind the left hand side and get a list of all the tables and column names
		unordered_map<string, string> lhs_columns;
		auto &lhs_binding_list = left_binder.bind_context.GetBindingsList();
		for(auto &binding : lhs_binding_list) {
			for(auto &column_name : binding.second->names) {
				if (left_binder.bind_context.BindingIsHidden(binding.first, column_name)) {
					continue;
				}
				if (lhs_columns.find(column_name) == lhs_columns.end()) {
					// new column candidate: add it to the set
					lhs_columns[column_name] = binding.first;
				} else {
					// this column candidate appears multiple times on the left-hand side of the join
					// this is fine ONLY if the column name does not occur in the right hand side
					// replace the binding with an empty string
					lhs_columns[column_name] = string();
				}
			}
		}
		// now bind the rhs
		for (auto &column : lhs_columns) {
			auto &column_name = column.first;
			auto &left_binding = column.second;
			// loop over the set of lhs columns, and figure out if there is a table in the rhs with the same name
			auto right_bindings = right_binder.bind_context.GetMatchingBindings(column_name);
			string right_binding;

			if (right_bindings.size() == 0) {
				// no match found for this column on the rhs
				continue;
			}
			// found this column name in both the LHS and the RHS of this join
			// add it to the natural join!
			// first check if the binding is ambiguous
			bool left_ambiguous = left_binding.empty();
			bool right_ambiguous = right_bindings.size() > 1;
			if (left_ambiguous || right_ambiguous) {
				// binding is ambiguous on left or right side: throw an exception
				string error_msg = "Column name \"" + column_name + "\" is ambiguous: it exists more than once on the ";
				error_msg += left_ambiguous ? "left" : "right";
				error_msg += " side of the join.";
				throw BinderException(FormatError(ref, error_msg));
			}
			for (auto &binding : right_bindings) {
				right_binding = binding;
			}
			// there is a match! create the join condition
			AddCondition(ref, left_binding, right_binding, column_name);
			bind_context.HideBinding(right_binding, column_name);
		}
		if (!ref.condition) {
			// no matching bindings found in natural join: throw an exception
			string error_msg = "No columns found to join on in NATURAL JOIN.\n";
			error_msg += "Use CROSS JOIN if you intended for this to be a cross-product.";
			// gather all left/right candidates
			string left_candidates, right_candidates;
			auto &rhs_binding_list = right_binder.bind_context.GetBindingsList();
			for(auto &binding : lhs_binding_list) {
				for(auto &column_name : binding.second->names) {
					if (!left_candidates.empty()) {
						left_candidates += ", ";
					}
					left_candidates += binding.first + "." + column_name;
				}
			}
			for(auto &binding : rhs_binding_list) {
				for(auto &column_name : binding.second->names) {
					if (!right_candidates.empty()) {
						right_candidates += ", ";
					}
					right_candidates += binding.first + "." + column_name;
				}
			}
			error_msg += "\n   Left candidates: " + left_candidates;
			error_msg += "\n   Right candidates: " + right_candidates;
			throw BinderException(FormatError(ref, error_msg));
		}
	} else if (ref.using_columns.size() > 0) {
		// USING columns
		D_ASSERT(!result->condition);
		vector<string> left_join_bindings;

		for (auto &using_column : ref.using_columns) {
			// for each using column, get the matching binding
			auto left_bindings = left_binder.bind_context.GetMatchingBindings(using_column);
			if (left_bindings.size() == 0) {
				throw BinderException("Column \"%s\" does not exist on left side of join!", using_column);
			}
			// find the join binding
			string left_binding;
			for (auto &binding : left_bindings) {
				if (left_binder.bind_context.BindingIsHidden(binding, using_column)) {
					continue;
				}
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
			left_join_bindings.push_back(left_binding);
		}
		for (idx_t i = 0; i < ref.using_columns.size(); i++) {
			auto &using_column = ref.using_columns[i];
			auto left_binding = left_join_bindings[i];

			auto right_bindings = right_binder.bind_context.GetMatchingBindings(using_column);
			string right_binding;
			for (auto &binding : right_bindings) {
				if (right_binder.bind_context.BindingIsHidden(binding, using_column)) {
					continue;
				}
				if (!right_binding.empty()) {
					string error = "Column name \"" + using_column +
									"\" is ambiguous: it exists more than once on right side of join.\nCandidates:";
					for (auto &binding : right_bindings) {
						error += "\n\t" + binding + "." + using_column;
					}
					throw BinderException(error);
				} else {
					right_binding = binding;
				}
			}
			if (right_binding.empty()) {
				throw BinderException("Column \"%s\" does not exist on right side of join!", using_column);
			}
			D_ASSERT(!left_binding.empty());
			AddCondition(ref, left_binding, right_binding, using_column);
			right_binder.bind_context.HideBinding(right_binding, using_column);
		}
	}
	bind_context.AddContext(move(left_binder.bind_context));
	bind_context.AddContext(move(right_binder.bind_context));
	MoveCorrelatedExpressions(left_binder);
	MoveCorrelatedExpressions(right_binder);
	if (ref.condition) {
		WhereBinder binder(*this, context);
		result->condition = binder.Bind(ref.condition);
	}
	return move(result);
}

} // namespace duckdb
