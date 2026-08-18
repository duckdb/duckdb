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

static void StructUpdateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &starting_vec = args.data[0];
	starting_vec.Verify();

	auto &starting_child_entries = StructVector::GetEntries(starting_vec);
	auto &result_child_entries = StructVector::GetEntries(result);

	auto &starting_types = StructType::GetChildTypes(starting_vec.GetType());

	auto &pack = ArgumentPack::GetInput(args.data[1]);
	auto &pack_types = ArgumentPack::GetTypes(args.data[1].GetType());

	auto new_entries = identifier_tree_t<idx_t>();
	auto is_new_field = vector<bool>(pack.size(), true);

	for (idx_t pack_idx = 0; pack_idx < pack.size(); pack_idx++) {
		new_entries.emplace(pack_types[pack_idx].first, pack_idx);
	}

	// Assign the original child entries to the STRUCT.
	for (idx_t field_idx = 0; field_idx < starting_child_entries.size(); field_idx++) {
		auto &starting_child = starting_child_entries[field_idx];
		auto update = new_entries.find(starting_types[field_idx].first);

		if (update == new_entries.end()) {
			// No update present, copy from source
			result_child_entries[field_idx].Reference(starting_child);
		} else {
			// We found a replacement of the same name to update
			auto pack_idx = update->second;
			result_child_entries[field_idx].Reference(pack[pack_idx]);
			is_new_field[pack_idx] = false;
		}
	}

	// Assign the new (not updated) children to the end of the result vector.
	for (idx_t pack_idx = 0, field_idx = starting_child_entries.size(); pack_idx < pack.size(); pack_idx++) {
		if (is_new_field[pack_idx]) {
			result_child_entries[field_idx++].Reference(pack[pack_idx]);
		}
	}
}

static unique_ptr<FunctionData> StructUpdateBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();

	auto &pack = ArgumentPack::GetPackedChildren(*arguments[1]);

	if (pack.empty()) {
		throw InvalidInputException("Can't update nothing into a STRUCT");
	}

	child_list_t<LogicalType> new_children;
	auto &existing_children = StructType::GetChildTypes(arguments[0]->GetReturnType());

	auto incoming_children = identifier_tree_t<idx_t>();
	auto is_new_field = vector<bool>(pack.size(), true);

	// Record the names of the incoming arguments (name/value pairs)
	for (idx_t pack_idx = 0; pack_idx < pack.size(); pack_idx++) {
		auto &name = StructType::GetChildName(arguments[1]->GetReturnType(), pack_idx);
		incoming_children.emplace(name, pack_idx);
	}

	for (idx_t field_idx = 0; field_idx < existing_children.size(); field_idx++) {
		auto &existing_child = existing_children[field_idx];
		auto update = incoming_children.find(existing_child.first);
		if (update == incoming_children.end()) {
			// No update provided for the named value
			new_children.push_back(make_pair(existing_child.first, existing_child.second));
		} else {
			// Update the struct with the new data of the same name
			auto pack_idx = update->second;
			auto &name = StructType::GetChildName(arguments[1]->GetReturnType(), pack_idx);
			new_children.emplace_back(make_pair(name, pack[pack_idx]->GetReturnType()));
			is_new_field[pack_idx] = false;
		}
	}

	// Append the arguments that did not update an existing entry
	for (idx_t pack_idx = 0; pack_idx < pack.size(); pack_idx++) {
		if (is_new_field[pack_idx]) {
			auto &name = StructType::GetChildName(arguments[1]->GetReturnType(), pack_idx);
			new_children.emplace_back(make_pair(name, pack[pack_idx]->GetReturnType()));
		}
	}

	bound_function.GetArguments()[0] = arguments[0]->GetReturnType();
	bound_function.SetReturnType(LogicalType::STRUCT(new_children));
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

static unique_ptr<BaseStatistics> StructUpdateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto new_stats = StructStats::CreateUnknown(expr.GetReturnType());

	auto pack_type = child_stats[1].GetType();
	auto pack_count = StructType::GetChildCount(pack_type);
	auto pack_stats = StructStats::GetChildStats(child_stats[1]);

	auto incoming_children = identifier_tree_t<idx_t>();
	auto is_new_field = vector<bool>(pack_count, true);

	for (idx_t pack_idx = 0; pack_idx < pack_count; pack_idx++) {
		incoming_children.emplace(StructType::GetChildName(pack_type, pack_idx), pack_idx);
	}

	auto existing_type = child_stats[0].GetType();
	auto existing_count = StructType::GetChildCount(existing_type);
	auto existing_stats = StructStats::GetChildStats(child_stats[0]);
	for (idx_t field_idx = 0; field_idx < existing_count; field_idx++) {
		auto &existing_child = existing_stats[field_idx];
		auto update = incoming_children.find(StructType::GetChildName(existing_type, field_idx));
		if (update == incoming_children.end()) {
			StructStats::SetChildStats(new_stats, field_idx, existing_child);
		} else {
			auto pack_idx = update->second;
			StructStats::SetChildStats(new_stats, field_idx, pack_stats[pack_idx]);
			is_new_field[pack_idx] = false;
		}
	}

	for (idx_t pack_idx = 0, field_idx = existing_count; pack_idx < pack_count; pack_idx++) {
		if (is_new_field[pack_idx]) {
			StructStats::SetChildStats(new_stats, field_idx++, pack_stats[pack_idx]);
		}
	}

	return new_stats.ToUnique();
}

ScalarFunction StructUpdateFun::GetFunction() {
	auto sig = FunctionSignature()
	               .AddParameter("struct", LogicalTypeId::STRUCT)
	               .AddVarKeywordParameter("kwargs", LogicalTypeId::ANY)
	               .SetReturnType(LogicalTypeId::STRUCT);

	auto fun = ScalarFunction("struct_update", std::move(sig))
	               .SetFunctionCallback(StructUpdateFunction)
	               .SetBindCallback(StructUpdateBind)
	               .SetStatisticsCallback(StructUpdateStats)
	               .SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING)
	               .SetSerializeCallback(VariableReturnBindData::Serialize)
	               .SetDeserializeCallback(VariableReturnBindData::Deserialize);

	return fun;
}

} // namespace duckdb
