#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "core_functions/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"

namespace duckdb {

static void StructInsertFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &starting_vec = args.data[0];
	starting_vec.Verify();

	auto &starting_child_entries = StructVector::GetEntries(starting_vec);
	auto &result_child_entries = StructVector::GetEntries(result);

	// Assign the original child entries to the STRUCT.
	for (idx_t i = 0; i < starting_child_entries.size(); i++) {
		auto &starting_child = starting_child_entries[i];
		result_child_entries[i].Reference(starting_child);
	}

	// Assign the new children to the result vector.
	auto &pack = ArgumentPack::GetInput(args.data[1]);
	for (idx_t i = 0; i < pack.size(); i++) {
		result_child_entries[starting_child_entries.size() + i].Reference(pack[i]);
	}
}

static unique_ptr<FunctionData> StructInsertBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();

	auto &pack = ArgumentPack::GetPackedChildren(*arguments[1]);

	if (pack.empty()) {
		throw InvalidInputException("Can't insert nothing into a STRUCT");
	}

	identifier_set_t name_collision_set;
	child_list_t<LogicalType> new_children;
	auto &existing_children = StructType::GetChildTypes(arguments[0]->GetReturnType());

	for (idx_t i = 0; i < existing_children.size(); i++) {
		auto &child = existing_children[i];
		name_collision_set.insert(child.first);
		new_children.push_back(make_pair(child.first, child.second));
	}

	// Loop through the additional arguments (name/value pairs)
	for (idx_t i = 0; i < pack.size(); i++) {
		auto &name = StructType::GetChildName(arguments[1]->GetReturnType(), i);

		if (name_collision_set.find(name) != name_collision_set.end()) {
			throw BinderException("Duplicate struct entry name \"%s\"", name);
		}

		name_collision_set.insert(name);
		new_children.emplace_back(make_pair(name, pack[i]->GetReturnType()));
	}

	bound_function.GetArguments()[0] = arguments[0]->GetReturnType();
	bound_function.SetReturnType(LogicalType::STRUCT(new_children));
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

static unique_ptr<BaseStatistics> StructInsertStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto new_stats = StructStats::CreateUnknown(expr.GetReturnType());

	auto existing_count = StructType::GetChildCount(child_stats[0].GetType());
	auto existing_stats = StructStats::GetChildStats(child_stats[0]);
	for (idx_t i = 0; i < existing_count; i++) {
		StructStats::SetChildStats(new_stats, i, existing_stats[i]);
	}

	auto pack_stats = StructStats::GetChildStats(child_stats[1]);
	auto pack_count = StructType::GetChildCount(child_stats[1].GetType());

	for (idx_t i = 0; i < pack_count; i++) {
		StructStats::SetChildStats(new_stats, existing_count + i, pack_stats[i]);
	}
	return new_stats.ToUnique();
}

ScalarFunction StructInsertFun::GetFunction() {
	auto sig = FunctionSignature()
	               .AddParameter("struct", LogicalTypeId::STRUCT)
	               .AddVarKeywordParameter("kwargs", LogicalTypeId::ANY)
	               .SetReturnType(LogicalTypeId::STRUCT);

	auto fun = ScalarFunction("struct_insert", std::move(sig))
	               .SetFunctionCallback(StructInsertFunction)
	               .SetBindCallback(StructInsertBind)
	               .SetStatisticsCallback(StructInsertStats)
	               .SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING)
	               .SetSerializeCallback(VariableReturnBindData::Serialize)
	               .SetDeserializeCallback(VariableReturnBindData::Deserialize);

	return fun;
}

} // namespace duckdb
