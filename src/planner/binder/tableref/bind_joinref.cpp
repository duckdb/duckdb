#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

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
				error += bind_context.GetActualColumnName(binding, using_column);
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

string Binder::RetrieveUsingBinding(Binder &current_binder, UsingColumnSet *current_set, const string &using_column,
                                    const string &join_side, UsingColumnSet *new_set) {
	string binding;
	if (!current_set) {
		binding = current_binder.FindBinding(using_column, join_side);
	} else {
		binding = current_set->primary_binding;
	}
	return binding;
}

static vector<string> RemoveDuplicateUsingColumns(const vector<string> &using_columns) {
	vector<string> result;
	case_insensitive_set_t handled_columns;
	for (auto &using_column : using_columns) {
		if (handled_columns.find(using_column) == handled_columns.end()) {
			handled_columns.insert(using_column);
			result.push_back(using_column);
		}
	}
	return result;
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
	vector<string> extra_using_columns;
	if (ref.is_natural) {
		// natural join, figure out which column names are present in both sides of the join
		// first bind the left hand side and get a list of all the tables and column names
		case_insensitive_set_t lhs_columns;
		auto &lhs_binding_list = left_binder.bind_context.GetBindingsList();
		for (auto &binding : lhs_binding_list) {
			for (auto &column_name : binding.second->names) {
				lhs_columns.insert(column_name);
			}
		}
		// now bind the rhs
		for (auto &column_name : lhs_columns) {
			auto right_using_binding = right_binder.bind_context.GetUsingBinding(column_name);

			string right_binding;
			// loop over the set of lhs columns, and figure out if there is a table in the rhs with the same name
			if (!right_using_binding) {
				if (!right_binder.TryFindBinding(column_name, "right", right_binding)) {
					// no match found for this column on the rhs: skip
					continue;
				}
			}
			extra_using_columns.push_back(column_name);
		}
		if (extra_using_columns.empty()) {
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
		extra_using_columns = ref.using_columns;
	}
	extra_using_columns = RemoveDuplicateUsingColumns(extra_using_columns);

	if (!extra_using_columns.empty()) {
		vector<UsingColumnSet *> left_using_bindings;
		vector<UsingColumnSet *> right_using_bindings;
		for (idx_t i = 0; i < extra_using_columns.size(); i++) {
			auto &using_column = extra_using_columns[i];
			// we check if there is ALREADY a using column of the same name in the left and right set
			// this can happen if we chain USING clauses
			// e.g. x JOIN y USING (c) JOIN z USING (c)
			auto left_using_binding = left_binder.bind_context.GetUsingBinding(using_column);
			auto right_using_binding = right_binder.bind_context.GetUsingBinding(using_column);
			if (!left_using_binding) {
				left_binder.bind_context.GetMatchingBinding(using_column);
			}
			if (!right_using_binding) {
				right_binder.bind_context.GetMatchingBinding(using_column);
			}
			left_using_bindings.push_back(left_using_binding);
			right_using_bindings.push_back(right_using_binding);
		}

		for (idx_t i = 0; i < extra_using_columns.size(); i++) {
			auto &using_column = extra_using_columns[i];
			string left_binding;
			string right_binding;

			auto set = make_unique<UsingColumnSet>();
			auto left_using_binding = left_using_bindings[i];
			auto right_using_binding = right_using_bindings[i];
			left_binding = RetrieveUsingBinding(left_binder, left_using_binding, using_column, "left", set.get());
			right_binding = RetrieveUsingBinding(right_binder, right_using_binding, using_column, "right", set.get());

			extra_conditions.push_back(
			    AddCondition(context, left_binder, right_binder, left_binding, right_binding, using_column));

			AddUsingBindings(*set, left_using_binding, left_binding);
			AddUsingBindings(*set, right_using_binding, right_binding);
			SetPrimaryBinding(*set, ref.type, left_binding, right_binding);
			bind_context.TransferUsingBinding(left_binder.bind_context, left_using_binding, set.get(), left_binding,
			                                  using_column);
			bind_context.TransferUsingBinding(right_binder.bind_context, right_using_binding, set.get(), right_binding,
			                                  using_column);
			AddUsingBindingSet(move(set));
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
