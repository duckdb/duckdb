#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/storage/statistics/struct_statistics.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static void StructInsertFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &starting_vec = args.data[0];

	starting_vec.Verify(args.size());

	auto &starting_child_entries = StructVector::GetEntries(starting_vec);

	auto &result_child_entries = StructVector::GetEntries(result);

	// Assign the starting vector entries to the result vector
	for (size_t i = 0; i < starting_child_entries.size(); i++) {
		auto &starting_child = starting_child_entries[i];
		result_child_entries[i]->Reference(*starting_child);
	}

	// Assign the new entries to the result vector
	for (size_t i = 1; i < args.ColumnCount(); i++) {
		result_child_entries[starting_child_entries.size() + i - 1]->Reference(args.data[i]);
	}

	result.Verify(args.size());
}

static unique_ptr<FunctionData> StructInsertBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	case_insensitive_set_t name_collision_set;

	if (arguments.empty()) {
		throw Exception("Missing required arguments for struct_insert function.");
	}

	if (LogicalTypeId::STRUCT != arguments[0]->return_type.id()) {
		throw Exception("The first argument to struct_insert must be a STRUCT");
	}

	if (arguments.size() < 2) {
		throw Exception("Can't insert nothing into a struct");
	}

	child_list_t<LogicalType> new_struct_children;

	auto &existing_struct_children = StructType::GetChildTypes(arguments[0]->return_type);

	for (size_t i = 0; i < existing_struct_children.size(); i++) {
		auto &child = existing_struct_children[i];
		name_collision_set.insert(child.first);
		new_struct_children.push_back(make_pair(child.first, child.second));
	}

	// Loop through the additional arguments (name/value pairs)
	for (idx_t i = 1; i < arguments.size(); i++) {
		auto &child = arguments[i];
		if (child->alias.empty() && bound_function.name == "struct_insert") {
			throw BinderException("Need named argument for struct insert, e.g. STRUCT_PACK(a := b)");
		}
		if (name_collision_set.find(child->alias) != name_collision_set.end()) {
			throw BinderException("Duplicate struct entry name \"%s\"", child->alias);
		}
		name_collision_set.insert(child->alias);
		new_struct_children.push_back(make_pair(child->alias, arguments[i]->return_type));
	}

	// this is more for completeness reasons
	bound_function.return_type = LogicalType::STRUCT(move(new_struct_children));
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

unique_ptr<BaseStatistics> StructInsertStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;

	auto &existing_struct_stats = (StructStatistics &)*child_stats[0];
	auto new_struct_stats = make_unique<StructStatistics>(expr.return_type);

	for (idx_t i = 0; i < existing_struct_stats.child_stats.size(); i++) {
		new_struct_stats->child_stats[i] =
		    existing_struct_stats.child_stats[i] ? existing_struct_stats.child_stats[i]->Copy() : nullptr;
	}

	auto offset = new_struct_stats->child_stats.size() - child_stats.size();
	for (idx_t i = 1; i < child_stats.size(); i++) {
		new_struct_stats->child_stats[offset + i] = child_stats[i] ? child_stats[i]->Copy() : nullptr;
	}
	return move(new_struct_stats);
}

void StructInsertFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("struct_insert", {}, LogicalTypeId::STRUCT, StructInsertFunction, StructInsertBind, nullptr,
	                   StructInsertStats);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
