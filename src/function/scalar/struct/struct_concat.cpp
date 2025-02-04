#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"

namespace duckdb {

static void StructConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &result_cols = StructVector::GetEntries(result);
	idx_t offset = 0;

	if (!args.AllConstant()) {
		// Unless all arguments are constant, we flatten the input to make sure it's homogeneous
		args.Flatten();
	}

	for (auto &arg : args.data) {
		const auto &child_cols = StructVector::GetEntries(arg);
		for (auto &child_col : child_cols) {
			result_cols[offset++]->Reference(*child_col);
		}
	}
	D_ASSERT(offset == result_cols.size());

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(args.size());
}

static unique_ptr<FunctionData> StructConcatBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {

	// collect names and deconflict, construct return type
	if (arguments.empty()) {
		throw InvalidInputException("struct_concat: At least one argument is required");
	}

	child_list_t<LogicalType> combined_children;
	case_insensitive_set_t name_set;

	bool has_unnamed = false;

	for (idx_t arg_idx = 0; arg_idx < arguments.size(); arg_idx++) {
		const auto &arg = arguments[arg_idx];

		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}

		if (arg->return_type.id() != LogicalTypeId::STRUCT) {
			throw InvalidInputException("struct_concat: Argument at position \"%d\" is not a STRUCT", arg_idx + 1);
		}

		const auto &child_types = StructType::GetChildTypes(arg->return_type);
		for (const auto &child : child_types) {
			if (!child.first.empty()) {
				auto it = name_set.find(child.first);
				if (it != name_set.end()) {
					if (*it == child.first) {
						throw InvalidInputException("struct_concat: Arguments contain duplicate STRUCT entry \"%s\"",
						                            child.first);
					}
					throw InvalidInputException(
					    "struct_concat: Arguments contain case-insensitive duplicate STRUCT entry \"%s\" and \"%s\"",
					    child.first, *it);
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

	bound_function.return_type = LogicalType::STRUCT(combined_children);
	return nullptr;
}

unique_ptr<BaseStatistics> StructConcatStats(ClientContext &context, FunctionStatisticsInput &input) {
	const auto &expr = input.expr;

	auto &arg_stats = input.child_stats;
	auto &arg_exprs = input.expr.children;

	auto struct_stats = StructStats::CreateUnknown(expr.return_type);
	idx_t struct_index = 0;

	for (idx_t arg_idx = 0; arg_idx < arg_exprs.size(); arg_idx++) {
		auto &arg_stat = arg_stats[arg_idx];
		auto &arg_type = arg_exprs[arg_idx]->return_type;
		for (idx_t child_idx = 0; child_idx < StructType::GetChildCount(arg_type); child_idx++) {
			auto &child_stat = StructStats::GetChildStats(arg_stat, child_idx);
			StructStats::SetChildStats(struct_stats, struct_index++, child_stat);
		}
	}
	return struct_stats.ToUnique();
}

ScalarFunction StructConcatFun::GetFunction() {
	ScalarFunction fun("struct_concat", {}, LogicalTypeId::STRUCT, StructConcatFunction, StructConcatBind, nullptr,
	                   StructConcatStats);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
