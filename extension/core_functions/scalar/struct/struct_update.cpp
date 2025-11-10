#include "core_functions/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/planner/expression_binder.hpp"

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

	for (idx_t arg_idx = 1; arg_idx < func_args.size(); arg_idx++) {
		auto &new_child = func_args[arg_idx];
		new_entries.emplace(new_child->alias, arg_idx);
	}

	// Assign the original child entries to the STRUCT.
	for (idx_t field_idx = 0; field_idx < starting_child_entries.size(); field_idx++) {
		auto &starting_child = starting_child_entries[field_idx];
		auto update = new_entries.find(starting_types[field_idx].first.c_str());

		if (update == new_entries.end()) {
			// No update present, copy from source
			result_child_entries[field_idx]->Reference(*starting_child);
		} else {
			// We found a replacement of the same name to update
			auto arg_idx = update->second;
			result_child_entries[field_idx]->Reference(args.data[arg_idx]);
			is_new_field[arg_idx] = false;
		}
	}

	// Assign the new (not updated) children to the end of the result vector.
	for (idx_t arg_idx = 1, field_idx = starting_child_entries.size(); arg_idx < args.ColumnCount(); arg_idx++) {
		if (is_new_field[arg_idx]) {
			result_child_entries[field_idx++]->Reference(args.data[arg_idx]);
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
	for (idx_t arg_idx = 1; arg_idx < arguments.size(); arg_idx++) {
		auto &child = arguments[arg_idx];
		if (child->alias.empty()) {
			throw BinderException("Need named argument for struct update, e.g., a := b");
		} else if (incomming_children.find(child->alias) != incomming_children.end()) {
			throw InvalidInputException("Duplicate named argument provided for %s", child->alias.c_str());
		}
		incomming_children.emplace(child->alias, arg_idx);
	}

	for (idx_t field_idx = 0; field_idx < existing_children.size(); field_idx++) {
		auto &existing_child = existing_children[field_idx];
		auto update = incomming_children.find(existing_child.first);
		if (update == incomming_children.end()) {
			// No update provided for the named value
			new_children.push_back(make_pair(existing_child.first, existing_child.second));
		} else {
			// Update the struct with the new data of the same name
			auto arg_idx = update->second;
			auto &new_child = arguments[arg_idx];
			new_children.push_back(make_pair(new_child->alias, new_child->return_type));
			is_new_field[arg_idx] = false;
		}
	}

	// Loop through the additional arguments (name/value pairs)
	for (idx_t arg_idx = 1; arg_idx < arguments.size(); arg_idx++) {
		if (is_new_field[arg_idx]) {
			auto &child = arguments[arg_idx];
			new_children.push_back(make_pair(child->alias, child->return_type));
		}
	}

	bound_function.SetReturnType(LogicalType::STRUCT(new_children));
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

unique_ptr<BaseStatistics> StructUpdateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;

	auto incomming_children = case_insensitive_tree_t<idx_t>();
	auto is_new_field = vector<bool>(expr.children.size(), true);
	auto new_stats = StructStats::CreateUnknown(expr.return_type);

	for (idx_t arg_idx = 1; arg_idx < expr.children.size(); arg_idx++) {
		auto &new_child = expr.children[arg_idx];
		incomming_children.emplace(new_child->alias, arg_idx);
	}

	auto existing_type = child_stats[0].GetType();
	auto existing_count = StructType::GetChildCount(existing_type);
	auto existing_stats = StructStats::GetChildStats(child_stats[0]);
	for (idx_t field_idx = 0; field_idx < existing_count; field_idx++) {
		auto &existing_child = existing_stats[field_idx];
		auto update = incomming_children.find(StructType::GetChildName(existing_type, field_idx));
		if (update == incomming_children.end()) {
			StructStats::SetChildStats(new_stats, field_idx, existing_child);
		} else {
			auto arg_idx = update->second;
			StructStats::SetChildStats(new_stats, field_idx, child_stats[arg_idx]);
			is_new_field[arg_idx] = false;
		}
	}

	for (idx_t arg_idx = 1, field_idx = existing_count; arg_idx < expr.children.size(); arg_idx++) {
		if (is_new_field[arg_idx]) {
			StructStats::SetChildStats(new_stats, field_idx++, child_stats[arg_idx]);
		}
	}

	return new_stats.ToUnique();
}

ScalarFunction StructUpdateFun::GetFunction() {
	ScalarFunction fun({}, LogicalTypeId::STRUCT, StructUpdateFunction, StructUpdateBind, nullptr, StructUpdateStats);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.varargs = LogicalType::ANY;
	fun.serialize = VariableReturnBindData::Serialize;
	fun.deserialize = VariableReturnBindData::Deserialize;
	return fun;
}

} // namespace duckdb
