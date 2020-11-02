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
	result->type = ref.type;
	if (ref.is_natural) {
		// natural join, figure out which column names are present in both sides of the join
		// first bind the left hand side and get a list of all the tables and column names
		result->left = Bind(*ref.left);
		unordered_set<string> lhs_tables;
		unordered_set<string> lhs_columns;
		auto &binding_list = bind_context.GetBindingsList();
		for(auto &binding : binding_list) {
			lhs_tables.insert(binding.first);
			for(auto &column_name : binding.second->names) {
				lhs_columns.insert(column_name);
			}
		}
		// now bind the rhs
		result->right = Bind(*ref.right);
		for (auto &column_name : lhs_columns) {
			// loop over the set of lhs columns, and figure out if there is a table in the rhs with the same name
			auto all_bindings = bind_context.GetMatchingBindings(column_name);
			string left_binding;
			string right_binding;
			for (auto &binding : all_bindings) {
				if (lhs_tables.find(binding) == lhs_tables.end()) {
					D_ASSERT(right_binding.empty());
					right_binding = binding;
				} else {
					left_binding = binding;
				}
			}
			if (right_binding.empty()) {
				continue;
			}
			D_ASSERT(!left_binding.empty());
			// there is a match! create the join condition
			AddCondition(ref, left_binding, right_binding, column_name);
			bind_context.hidden_columns.insert(right_binding + "." + column_name);
		}
		if (!ref.condition) {
			// no matching bindings found in natural join: throw an exception
			string error_msg = "No columns found to join on in NATURAL JOIN.\n";
			error_msg += "Use CROSS JOIN if you intended for this to be a cross-product.";
			// gather all left/right candidates
			string left_candidates, right_candidates;
			for(auto &binding : binding_list) {
				bool lhs_table = lhs_tables.find(binding.first) != lhs_tables.end();
				auto &candidates = lhs_table ? left_candidates : right_candidates;
				for(auto &column_name : binding.second->names) {
					if (!candidates.empty()) {
						candidates += ", ";
					}
					candidates += binding.first + "." + column_name;
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
		vector<unordered_set<string>> matching_left_bindings;

		result->left = Bind(*ref.left);
		for (auto &using_column : ref.using_columns) {
			// for each using column, get the matching binding
			auto left_bindings = bind_context.GetMatchingBindings(using_column);
			if (left_bindings.size() == 0) {
				throw BinderException("Column \"%s\" does not exist on left side of join!", using_column);
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
					D_ASSERT(right_binding.empty());
					right_binding = binding;
				}
			}
			if (right_binding.empty()) {
				throw BinderException("Column \"%s\" does not exist on right side of join!", using_column);
			}
			D_ASSERT(!left_binding.empty());
			AddCondition(ref, left_binding, right_binding, using_column);
			bind_context.hidden_columns.insert(right_binding + "." + using_column);
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

} // namespace duckdb
