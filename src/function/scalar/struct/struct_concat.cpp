#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"

namespace duckdb {

static void StructConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &result_cols = StructVector::GetEntries(result);

	const auto &head = args.data[0];
	const auto &tail = ArgumentPack::GetInput(args.data[1]);

	idx_t offset = 0;

	for (auto &child_col : StructVector::GetEntries(head)) {
		result_cols[offset++].Reference(child_col);
	}

	for (const auto &arg : tail) {
		for (auto &child_col : StructVector::GetEntries(arg)) {
			result_cols[offset++].Reference(child_col);
		}
	}

	D_ASSERT(offset == result_cols.size());
}

static unique_ptr<FunctionData> StructConcatBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();

	auto &raw_args = input.GetArguments();

	bound_function.GetArguments()[0] = raw_args[0]->GetReturnType();

	vector<reference<unique_ptr<Expression>>> arguments;
	arguments.emplace_back(raw_args[0]);
	for (auto &arg : ArgumentPack::GetPackedChildren(*raw_args[1])) {
		arguments.emplace_back(arg);
	}

	child_list_t<LogicalType> combined_children;
	identifier_set_t name_set;

	bool has_unnamed = false;

	for (idx_t arg_idx = 0; arg_idx < arguments.size(); arg_idx++) {
		const auto &arg = arguments[arg_idx];

		if (arg.get()->GetReturnType().id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}

		if (!StructType::IsStruct(arg.get()->GetReturnType())) {
			throw InvalidInputException("struct_concat: Argument at position \"%d\" is not a STRUCT", arg_idx + 1);
		}

		const auto &child_types = StructType::GetChildTypes(arg.get()->GetReturnType());
		for (const auto &child : child_types) {
			if (!child.first.empty()) {
				auto it = name_set.find(child.first);
				if (it != name_set.end()) {
					if (it->GetIdentifierName() == child.first.GetIdentifierName()) {
						throw InvalidInputException("struct_concat: Arguments contain duplicate STRUCT entry \"%s\"",
						                            child.first.GetIdentifierName());
					}
					throw InvalidInputException(
					    "struct_concat: Arguments contain case-insensitive duplicate STRUCT entry \"%s\" and \"%s\"",
					    child.first.GetIdentifierName(), it->GetIdentifierName());
				}
				name_set.insert(child.first);
			} else {
				has_unnamed = true;
			}
			combined_children.push_back(child);
		}
	}

	if (has_unnamed && !name_set.empty()) {
		throw InvalidInputException("struct_concat: Cannot mix named and unnamed STRUCTs");
	}

	// all-unnamed inputs produce an unnamed TUPLE, otherwise a named STRUCT
	if (has_unnamed) {
		bound_function.SetReturnType(LogicalType::TUPLE(combined_children));
	} else {
		bound_function.SetReturnType(LogicalType::STRUCT(combined_children));
	}
	return nullptr;
}

static unique_ptr<BaseStatistics> StructConcatStats(ClientContext &context, FunctionStatisticsInput &input) {
	const auto &expr = input.expr;

	const auto &head_stats = input.child_stats[0];
	const auto &tail_stats = input.child_stats[1];

	auto struct_stats = StructStats::CreateUnknown(expr.GetReturnType());

	idx_t offset = 0;

	for (idx_t child_idx = 0; child_idx < StructType::GetChildCount(head_stats.GetType()); child_idx++) {
		auto &child_stat = StructStats::GetChildStats(head_stats, child_idx);
		StructStats::SetChildStats(struct_stats, offset++, child_stat);
	}

	for (idx_t tail_idx = 0; tail_idx < StructType::GetChildCount(tail_stats.GetType()); tail_idx++) {
		auto &tail_stat = StructStats::GetChildStats(tail_stats, tail_idx);

		for (idx_t child_idx = 0; child_idx < StructType::GetChildCount(tail_stat.GetType()); child_idx++) {
			auto &child_stat = StructStats::GetChildStats(tail_stat, child_idx);
			StructStats::SetChildStats(struct_stats, offset++, child_stat);
		}
	}

	return struct_stats.ToUnique();
}

ScalarFunction StructConcatFun::GetFunction() {
	auto sig = FunctionSignature()
		.AddParameter("arg", LogicalTypeId::STRUCT)
		.AddVarPositionalParameter("args", LogicalType::ANY)
		.SetReturnType(LogicalTypeId::STRUCT);

	auto fun = ScalarFunction("struct_concat", std::move(sig))
		.SetFunctionCallback(StructConcatFunction)
		.SetBindCallback(StructConcatBind)
		.SetStatisticsCallback(StructConcatStats)
		.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	return fun;
}

} // namespace duckdb
