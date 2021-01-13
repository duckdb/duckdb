#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

static unique_ptr<Expression> BindColumn(Binder &binder, ClientContext &context, const string &alias, const string &column_name) {
	auto expr = make_unique_base<ParsedExpression, ColumnRefExpression>(column_name, alias);
	ExpressionBinder expr_binder(binder, context);
	return expr_binder.Bind(expr);
}

static unique_ptr<Expression> AddCondition(ClientContext &context, Binder &left_binder, Binder &right_binder, const string &left_alias, const string &right_alias, const string &column_name) {
	auto left = BindColumn(left_binder, context, left_alias, column_name);
	auto right = BindColumn(right_binder, context, right_alias, column_name);
	return make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(left), move(right));
}

bool Binder::TryFindBinding(const string &using_column, const string &join_side, string &result) {
	// for each using column, get the matching binding
	auto bindings = bind_context.GetMatchingBindings(using_column);
	if (bindings.size() == 0) {
		return false;
	}
	// find the join binding
	for (auto &binding : bindings) {
		if (!result.empty()) {
			string error = "Column name \"" + using_column +
							"\" is ambiguous: it exists more than once on " + join_side + " side of join.\nCandidates:";
			for (auto &binding : bindings) {
				error += "\n\t" + binding + "." + using_column;
			}
			throw BinderException(error);
		} else {
			result = binding;
		}
	}
	return true;
}

string Binder::FindBinding(const string &using_column, const string &join_side) {
	string result;
	if (!TryFindBinding(using_column, join_side, result)) {
		throw BinderException("Column \"%s\" does not exist on %s side of join!", using_column, join_side);
	}
	return result;
}

void AddUsingCondition(BindContext &target, const string &column_name, const string &binding, BindContext &child, bool is_using_column) {
	if (is_using_column) {
		target.MergeUsingCondition(column_name, child);
	} else {
		target.AddUsingCondition(column_name, binding);
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

	vector<unique_ptr<Expression>> extra_conditions;
	if (ref.is_natural) {
		// natural join, figure out which column names are present in both sides of the join
		// first bind the left hand side and get a list of all the tables and column names
		unordered_map<string, string> lhs_columns;
		auto &lhs_binding_list = left_binder.bind_context.GetBindingsList();
		for (auto &binding : lhs_binding_list) {
			for (auto &column_name : binding.second->names) {
				bool is_using_column = left_binder.bind_context.IsUsingBinding(column_name);
				if (is_using_column) {
					lhs_columns[column_name] = string();
				} else if (lhs_columns.find(column_name) == lhs_columns.end()) {
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

			bool left_is_using = left_binder.bind_context.IsUsingBinding(column_name);
			bool right_is_using = right_binder.bind_context.IsUsingBinding(column_name);

			string right_binding;
			// loop over the set of lhs columns, and figure out if there is a table in the rhs with the same name
			if (!right_is_using) {
				if (!right_binder.TryFindBinding(column_name, "right", right_binding)) {
					// no match found for this column on the rhs: skip
					continue;
				}
			}
			// found this column name in both the LHS and the RHS of this join
			// add it to the natural join!
			// first check if the binding is ambiguous on the LHS
			if (!left_is_using && left_binding.empty()) {
				// binding is ambiguous on left or right side: throw an exception
				string error_msg = "Column name \"" + column_name + "\" is ambiguous: it exists more than once on the left side of the join.";
				throw BinderException(FormatError(ref, error_msg));
			}
			// there is a match! create the join condition
			extra_conditions.push_back(AddCondition(context, left_binder, right_binder, left_binding, right_binding, column_name));
			AddUsingCondition(bind_context, column_name, left_binding, left_binder.bind_context, left_is_using);
			AddUsingCondition(bind_context, column_name, right_binding, right_binder.bind_context, right_is_using);
		}
		if (extra_conditions.size() == 0) {
			// no matching bindings found in natural join: throw an exception
			string error_msg = "No columns found to join on in NATURAL JOIN.\n";
			error_msg += "Use CROSS JOIN if you intended for this to be a cross-product.";
			// gather all left/right candidates
			string left_candidates, right_candidates;
			auto &rhs_binding_list = right_binder.bind_context.GetBindingsList();
			for (auto &binding : lhs_binding_list) {
				for (auto &column_name : binding.second->names) {
					if (!left_candidates.empty()) {
						left_candidates += ", ";
					}
					left_candidates += binding.first + "." + column_name;
				}
			}
			for (auto &binding : rhs_binding_list) {
				for (auto &column_name : binding.second->names) {
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

		for (idx_t i = 0; i < ref.using_columns.size(); i++) {
			auto &using_column = ref.using_columns[i];
			string left_binding;
			string right_binding;
			bool left_is_using = left_binder.bind_context.IsUsingBinding(using_column);
			bool right_is_using = right_binder.bind_context.IsUsingBinding(using_column);
			if (!left_is_using) {
				left_binding = left_binder.FindBinding(using_column, "left");
			}
			if (!right_is_using) {
				right_binding = right_binder.FindBinding(using_column, "right");
			}
			extra_conditions.push_back(AddCondition(context, left_binder, right_binder, left_binding, right_binding, using_column));
			AddUsingCondition(bind_context, using_column, left_binding, left_binder.bind_context, left_is_using);
			AddUsingCondition(bind_context, using_column, right_binding, right_binder.bind_context, right_is_using);
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
	for(auto &condition : extra_conditions) {
		if (result->condition) {
			result->condition = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(result->condition), move(condition));
		} else {
			result->condition = move(condition);
		}
	}
	D_ASSERT(result->condition);
	return move(result);
}

} // namespace duckdb
