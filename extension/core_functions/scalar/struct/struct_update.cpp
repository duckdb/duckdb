#include "core_functions/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/planner/expression_binder.hpp"

//class StructUpdateFunctionExpressionState : ExpressionState {};

namespace duckdb {

static void StructUpdateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &starting_vec = args.data[0];
	starting_vec.Verify(args.size());

	auto &starting_child_entries = StructVector::GetEntries(starting_vec);
	auto &result_child_entries = StructVector::GetEntries(result);

	auto &starting_types = StructType::GetChildTypes(starting_vec.GetType());

	auto &func_args = state.expr.Cast<BoundFunctionExpression>().children;
	auto new_entries = case_insensitive_tree_t<idx_t>();
	auto is_new_field = vector<bool>(args.ColumnCount(), true);

	for (idx_t j = 1; j < func_args.size(); j++) {
		auto &new_child = func_args[j];
		new_entries.emplace(new_child->alias, j);
	}

	// Assign the original child entries to the STRUCT.
	for (idx_t i = 0; i < starting_child_entries.size(); i++) {
		auto &starting_child = starting_child_entries[i];
		auto update = new_entries.find(starting_types[i].first.c_str());

		if(update == new_entries.end()) {
			// No update present, copy from source
			result_child_entries[i]->Reference(*starting_child);
		} else {
			// We found a replacement of the same name to update
			auto j = update->second;
			result_child_entries[i]->Reference(args.data[j]);
			is_new_field[j] = false;
		}

	}

	// Assign the new (not updated) children to the end of the result vector.
	for (idx_t j = 1, k = starting_child_entries.size(); j < args.ColumnCount(); j++) {
		if(is_new_field[j]) {
			result_child_entries[k++]->Reference(args.data[j]);
		}
	}

	result.Verify(args.size());
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> StructUpdateBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw InvalidInputException("Missing required arguments for struct_update function.");
	}
	if (LogicalTypeId::STRUCT != arguments[0]->return_type.id()) {
		throw InvalidInputException("The first argument to struct_update must be a STRUCT");
	}
	if (arguments.size() < 2) {
		throw InvalidInputException("Can't update nothing into a STRUCT");
	}

	child_list_t<LogicalType> new_children;
	auto &existing_children = StructType::GetChildTypes(arguments[0]->return_type);

	auto incomming_children = case_insensitive_tree_t<idx_t>();
	auto is_new_field = vector<bool>(arguments.size(), true);

	// Validate incomming arguments and record names
	for (idx_t j = 1; j < arguments.size(); j++) {
		auto &child = arguments[j];
		if (child->alias.empty()) {
			throw BinderException("Need named argument for struct insert, e.g., a := b");
		}
		incomming_children.emplace(child->alias, j);
	}

	for (idx_t i = 0; i < existing_children.size(); i++) {
		auto &existing_child = existing_children[i];
		auto update = incomming_children.find(existing_child.first);
		if (update == incomming_children.end()) {
			// No update provided for the named value
			new_children.push_back(make_pair(existing_child.first, existing_child.second));
		} else {
			// Update the struct with the new data of the same name
			auto j = update->second;
			auto &new_child = arguments[j];
			new_children.push_back(make_pair(new_child->alias, new_child->return_type));
			is_new_field[j] = false;
		}
	}

	// Loop through the additional arguments (name/value pairs)
	for (idx_t j = 1; j < arguments.size(); j++) {
		if(is_new_field[j]) {
			auto &child = arguments[j];
			new_children.push_back(make_pair(child->alias, child->return_type));
		}
	}

	bound_function.return_type = LogicalType::STRUCT(new_children);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

unique_ptr<BaseStatistics> StructUpdateStats(ClientContext &context, FunctionStatisticsInput &input) {
	printf("Stats\n");
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;

	auto incomming_children = case_insensitive_tree_t<idx_t>();
	auto is_new_field = vector<bool>(expr.children.size(), true);
	auto new_stats = StructStats::CreateUnknown(expr.return_type);

	for (idx_t j = 1; j < expr.children.size(); j++) {
		auto &new_child = expr.children[j];
		incomming_children.emplace(new_child->alias, j);
	}

	auto existing_count = StructType::GetChildCount(child_stats[0].GetType());
	auto existing_stats = StructStats::GetChildStats(child_stats[0]);
	for (idx_t i = 0; i < existing_count; i++) {
		auto &existing_child = existing_stats[i];
		auto update = incomming_children.find(existing_child.GetType().GetAlias());
		if(update == incomming_children.end()) {
			StructStats::SetChildStats(new_stats, i, existing_child);
		} else {
			auto j = update->second;
			StructStats::SetChildStats(new_stats, i, child_stats[j]);
			is_new_field[j] = false;
		}
	}

	for (idx_t j = 1, k = existing_count; j < expr.children.size(); j++) {
		if(is_new_field[j]) {
			StructStats::SetChildStats(new_stats, k++, child_stats[j]);
		}
	}

	return new_stats.ToUnique();
}

ScalarFunction StructUpdateFun::GetFunction() {
	ScalarFunction fun({}, LogicalTypeId::STRUCT, StructUpdateFunction, StructUpdateBind, nullptr, StructUpdateStats);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.varargs = LogicalType::ANY;
	fun.serialize = VariableReturnBindData::Serialize;
	fun.deserialize = VariableReturnBindData::Deserialize;
	return fun;
}

} // namespace duckdb
