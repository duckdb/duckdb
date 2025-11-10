#include "core_functions/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static void StructInsertFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &starting_vec = args.data[0];
	starting_vec.Verify(args.size());

	auto &starting_child_entries = StructVector::GetEntries(starting_vec);
	auto &result_child_entries = StructVector::GetEntries(result);

	// Assign the original child entries to the STRUCT.
	for (idx_t i = 0; i < starting_child_entries.size(); i++) {
		auto &starting_child = starting_child_entries[i];
		result_child_entries[i]->Reference(*starting_child);
	}

	// Assign the new children to the result vector.
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		result_child_entries[starting_child_entries.size() + i - 1]->Reference(args.data[i]);
	}

	result.Verify(args.size());
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> StructInsertBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw InvalidInputException("Missing required arguments for struct_insert function.");
	}
	if (LogicalTypeId::STRUCT != arguments[0]->return_type.id()) {
		throw InvalidInputException("The first argument to struct_insert must be a STRUCT");
	}
	if (arguments.size() < 2) {
		throw InvalidInputException("Can't insert nothing into a STRUCT");
	}

	case_insensitive_set_t name_collision_set;
	child_list_t<LogicalType> new_children;
	auto &existing_children = StructType::GetChildTypes(arguments[0]->return_type);

	for (idx_t i = 0; i < existing_children.size(); i++) {
		auto &child = existing_children[i];
		name_collision_set.insert(child.first);
		new_children.push_back(make_pair(child.first, child.second));
	}

	// Loop through the additional arguments (name/value pairs)
	for (idx_t i = 1; i < arguments.size(); i++) {
		auto &child = arguments[i];
		if (child->GetAlias().empty()) {
			throw BinderException("Need named argument for struct insert, e.g., a := b");
		}
		if (name_collision_set.find(child->GetAlias()) != name_collision_set.end()) {
			throw BinderException("Duplicate struct entry name \"%s\"", child->GetAlias());
		}
		name_collision_set.insert(child->GetAlias());
		new_children.push_back(make_pair(child->GetAlias(), arguments[i]->return_type));
	}

	bound_function.SetReturnType(LogicalType::STRUCT(new_children));
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

static unique_ptr<BaseStatistics> StructInsertStats(ClientContext &context, FunctionStatisticsInput &input) {
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

ScalarFunction StructInsertFun::GetFunction() {
	ScalarFunction fun({}, LogicalTypeId::STRUCT, StructInsertFunction, StructInsertBind, nullptr, StructInsertStats);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.varargs = LogicalType::ANY;
	fun.serialize = VariableReturnBindData::Serialize;
	fun.deserialize = VariableReturnBindData::Deserialize;
	return fun;
}

} // namespace duckdb
