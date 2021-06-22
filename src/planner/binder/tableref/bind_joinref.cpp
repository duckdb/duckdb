#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"

namespace duckdb {

static unique_ptr<ParsedExpression> BindColumn(Binder &binder, ClientContext &context, const string &alias,
                                               const string &column_name) {
	auto expr = make_unique_base<ParsedExpression, ColumnRefExpression>(column_name, alias);
	ExpressionBinder expr_binder(binder, context);
	auto result = expr_binder.Bind(expr);
	return make_unique<BoundExpression>(move(result));
}

static unique_ptr<ParsedExpression> AddCondition(ClientContext &context, Binder &left_binder, Binder &right_binder,
                                                 const string &left_alias, const string &right_alias,
                                                 const string &column_name) {
	ExpressionBinder expr_binder(left_binder, context);
	auto left = BindColumn(left_binder, context, left_alias, column_name);
	auto right = BindColumn(right_binder, context, right_alias, column_name);
	return make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(left), move(right));
}

bool Binder::TryFindBinding(const string &using_column, const string &join_side, string &result) {
	// for each using column, get the matching binding
	auto bindings = bind_context.GetMatchingBindings(using_column);
	if (bindings.empty()) {
		return false;
	}
	// find the join binding
	for (auto &binding : bindings) {
		if (!result.empty()) {
			string error = "Column name \"";
			error += using_column;
			error += "\" is ambiguous: it exists more than once on ";
			error += join_side;
			error += " side of join.\nCandidates:";
			for (auto &binding : bindings) {
				error += "\n\t";
				error += binding;
				error += ".";
				error += using_column;
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

static void AddUsingBindings(UsingColumnSet &set, UsingColumnSet *input_set, const string &input_binding) {
	if (input_set) {
		for (auto &entry : input_set->bindings) {
			set.bindings.insert(entry);
		}
	} else {
		set.bindings.insert(input_binding);
	}
}

static void SetPrimaryBinding(UsingColumnSet &set, JoinType join_type, const string &left_binding,
                              const string &right_binding) {
	switch (join_type) {
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::SEMI:
	case JoinType::ANTI:
		set.primary_binding = left_binding;
		break;
	case JoinType::RIGHT:
		set.primary_binding = right_binding;
		break;
	default:
		break;
	}
}

unique_ptr<BoundTableRef> Binder::Bind(JoinRef &ref) {
	auto result = make_unique<BoundJoinRef>();
	result->left_binder = Binder::CreateBinder(context, this);
	result->right_binder = Binder::CreateBinder(context, this);
	auto &left_binder = *result->left_binder;
	auto &right_binder = *result->right_binder;

	result->type = ref.type;
	result->left = left_binder.Bind(*ref.left);
	result->right = right_binder.Bind(*ref.right);

	vector<unique_ptr<ParsedExpression>> extra_conditions;
	if (ref.is_natural) {
		// natural join, figure out which column names are present in both sides of the join
		// first bind the left hand side and get a list of all the tables and column names
		unordered_map<string, string> lhs_columns;
		auto &lhs_binding_list = left_binder.bind_context.GetBindingsList();
		for (auto &binding : lhs_binding_list) {
			for (auto &column_name : binding.second->names) {
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

			auto left_using_binding = left_binder.bind_context.GetUsingBinding(column_name, left_binding);
			auto right_using_binding = right_binder.bind_context.GetUsingBinding(column_name);

			string right_binding;
			// loop over the set of lhs columns, and figure out if there is a table in the rhs with the same name
			if (!right_using_binding) {
				if (!right_binder.TryFindBinding(column_name, "right", right_binding)) {
					// no match found for this column on the rhs: skip
					continue;
				}
			}
			// found this column name in both the LHS and the RHS of this join
			// add it to the natural join!
			// first check if the binding is ambiguous on the LHS
			if (!left_using_binding && left_binding.empty()) {
				// binding is ambiguous on left or right side: throw an exception
				string error_msg = "Column name \"" + column_name +
				                   "\" is ambiguous: it exists more than once on the left side of the join.";
				throw BinderException(FormatError(ref, error_msg));
			}
			// there is a match! create the join condition
			extra_conditions.push_back(
			    AddCondition(context, left_binder, right_binder, left_binding, right_binding, column_name));

			UsingColumnSet set;
			AddUsingBindings(set, left_using_binding, left_binding);
			AddUsingBindings(set, right_using_binding, right_binding);
			SetPrimaryBinding(set, ref.type, left_binding, right_binding);
			left_binder.bind_context.RemoveUsingBinding(column_name, left_using_binding);
			right_binder.bind_context.RemoveUsingBinding(column_name, right_using_binding);
			bind_context.AddUsingBinding(column_name, move(set));
		}
		if (extra_conditions.empty()) {
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
	} else if (!ref.using_columns.empty()) {
		// USING columns
		D_ASSERT(!result->condition);

		for (idx_t i = 0; i < ref.using_columns.size(); i++) {
			auto &using_column = ref.using_columns[i];
			string left_binding;
			string right_binding;
			auto left_using_binding = left_binder.bind_context.GetUsingBinding(using_column);
			auto right_using_binding = right_binder.bind_context.GetUsingBinding(using_column);
			if (!left_using_binding) {
				left_binding = left_binder.FindBinding(using_column, "left");
			} else {
				left_binding = left_using_binding->primary_binding;
			}
			if (!right_using_binding) {
				right_binding = right_binder.FindBinding(using_column, "right");
			} else {
				right_binding = right_using_binding->primary_binding;
			}
			extra_conditions.push_back(
			    AddCondition(context, left_binder, right_binder, left_binding, right_binding, using_column));

			UsingColumnSet set;
			AddUsingBindings(set, left_using_binding, left_binding);
			AddUsingBindings(set, right_using_binding, right_binding);
			SetPrimaryBinding(set, ref.type, left_binding, right_binding);
			left_binder.bind_context.RemoveUsingBinding(using_column, left_using_binding);
			right_binder.bind_context.RemoveUsingBinding(using_column, right_using_binding);
			bind_context.AddUsingBinding(using_column, move(set));
		}
	}
	bind_context.AddContext(move(left_binder.bind_context));
	bind_context.AddContext(move(right_binder.bind_context));
	MoveCorrelatedExpressions(left_binder);
	MoveCorrelatedExpressions(right_binder);
	for (auto &condition : extra_conditions) {
		if (ref.condition) {
			ref.condition = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(ref.condition),
			                                                   move(condition));
		} else {
			ref.condition = move(condition);
		}
	}
	if (ref.condition) {
		WhereBinder binder(*this, context);
		result->condition = binder.Bind(ref.condition);
	}
	D_ASSERT(result->condition);
	return move(result);
}

} // namespace duckdb
