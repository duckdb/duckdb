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

		
	auto new_entries = case_insensitive_tree_t<idx_t>();
	auto updated = vector<bool>(args.ColumnCount(), false);

	for (idx_t j = 1; j < args.ColumnCount(); j++) {
		auto &new_child = args.data[j];
		new_entries.emplace(new_child.GetType().GetAlias(), j);
	}

	// Assign the original child entries to the STRUCT.
	for (idx_t i = 0; i < starting_child_entries.size(); i++) {
		auto &starting_child = starting_child_entries[i];
		auto update = new_entries.find(starting_child->GetType().GetAlias());

		if(update == new_entries.end()) {
			// No update present, copy from source
			result_child_entries[i]->Reference(*starting_child);
		} else {
			// We found a replacement of the same name to update
			auto j = update->second;
			result_child_entries[i]->Reference(args.data[j]);
			updated[j] = true;
		}

	}

	// Assign the new (not updated) children to the end of the result vector.
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		if(!updated[i]) {
			result_child_entries[starting_child_entries.size() + i - 1]->Reference(args.data[i]);
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

	auto incomming_children = case_insensitive_tree_t<idx_t>();
	auto updated = vector<bool>(arguments.size(), false);
	child_list_t<LogicalType> new_children;
	auto &existing_children = StructType::GetChildTypes(arguments[0]->return_type);

	// Validate incomming arguments and record names
	for (idx_t i = 1; i < arguments.size(); i++) {
		auto &child = arguments[i];
		if (child->alias.empty()) {
			throw BinderException("Need named argument for struct insert, e.g., a := b");
		}
		incomming_children.emplace(child->alias, i);
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
			updated[j] = true;
		}
	}

	// Loop through the additional arguments (name/value pairs)
	for (idx_t i = 1; i < arguments.size(); i++) {
		if(!updated[i]) {
			auto &child = arguments[i];
			new_children.push_back(make_pair(child->alias, arguments[i]->return_type));
		}
	}

	bound_function.return_type = LogicalType::STRUCT(new_children);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

unique_ptr<BaseStatistics> StructUpdateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto new_stats = StructStats::CreateUnknown(expr.return_type);

	auto existing_count = StructType::GetChildCount(child_stats[0].GetType());
	auto existing_stats = StructStats::GetChildStats(child_stats[0]);
	for (idx_t i = 0; i < existing_count; i++) {
		StructStats::SetChildStats(new_stats, i, existing_stats[i]);
	}

	auto new_count = StructType::GetChildCount(expr.return_type);
	auto offset = new_count - child_stats.size();
	for (idx_t i = 1; i < child_stats.size(); i++) {
		StructStats::SetChildStats(new_stats, offset + i, child_stats[i]);
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
