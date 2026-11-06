#include "duckdb/optimizer/runtime_filter_cast.hpp"

#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

static LogicalType GetRuntimeFilterSourceType(const JoinFilterPushdownColumn &column) {
	auto type = column.storage_type;
	for (auto &child_idx : column.struct_extract_path) {
		type = StructType::GetChildType(type, child_idx);
	}
	return type;
}

bool RuntimeFilterCastUtil::RuntimeFilterCastCanFail(const LogicalType &source_type, const LogicalType &target_type) {
	if (source_type == target_type) {
		return false;
	}
	if (source_type.id() == LogicalTypeId::VARIANT || target_type.id() == LogicalTypeId::VARIANT ||
	    !source_type.IsIntegral() || !target_type.IsIntegral()) {
		return true;
	}

	const auto source_size = GetTypeIdSize(source_type.InternalType());
	const auto target_size = GetTypeIdSize(target_type.InternalType());
	if (source_size > target_size) {
		return true;
	}
	if (source_type.IsSigned() == target_type.IsSigned()) {
		return false;
	}
	if (source_type.IsSigned()) {
		return true;
	}
	return source_size >= target_size;
}

bool RuntimeFilterCastUtil::RuntimeFilterUsesTryCast(const JoinFilterPushdownColumn &column) {
	auto source_type = GetRuntimeFilterSourceType(column);
	for (auto &cast : column.runtime_filter_casts) {
		if (cast.mode == RuntimeFilterCastMode::TRY_CAST || RuntimeFilterCastCanFail(source_type, cast.target_type)) {
			return true;
		}
		source_type = cast.target_type;
	}
	return false;
}

LogicalType RuntimeFilterCastUtil::GetRuntimeFilterInputType(const JoinFilterPushdownColumn &column,
                                                             const LogicalType &runtime_type) {
	if (column.mode == JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION && !column.runtime_filter_casts.empty()) {
		return column.runtime_filter_casts.back().target_type;
	}
	if (!column.struct_extract_path.empty()) {
		return GetRuntimeFilterSourceType(column);
	}
	return runtime_type;
}

bool RuntimeFilterCastUtil::RequiresRuntimeFilterExpressionReconstruction(const JoinFilterPushdownColumn &column,
                                                                          const LogicalType &runtime_type) {
	if (column.mode != JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION) {
		return false;
	}
	if (!column.struct_extract_path.empty()) {
		return true;
	}
	auto source_type = column.storage_type;
	for (auto &cast : column.runtime_filter_casts) {
		if (RuntimeFilterCastCanFail(source_type, cast.target_type)) {
			return true;
		}
		source_type = cast.target_type;
	}
	D_ASSERT(source_type == GetRuntimeFilterInputType(column, runtime_type));
	return false;
}

unique_ptr<Expression> RuntimeFilterCastUtil::CreateRuntimeFilterInputExpression(ClientContext &context,
                                                                                 const JoinFilterPushdownColumn &column,
                                                                                 bool &preserves_cast_errors) {
	D_ASSERT(column.storage_type.IsValid());
	preserves_cast_errors = false;
	unique_ptr<Expression> input = make_uniq<BoundReferenceExpression>(column.storage_type, idx_t(0));
	for (auto &child_idx : column.struct_extract_path) {
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(input));
		children.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(child_idx + 1))));
		input = StructExtractAtFun::GetFunction().Bind(context, std::move(children));
	}
	auto source_type = input->GetReturnType();
	for (auto &cast : column.runtime_filter_casts) {
		const auto cast_can_fail = RuntimeFilterCastCanFail(source_type, cast.target_type);
		const auto is_try_cast = cast.mode == RuntimeFilterCastMode::TRY_CAST || cast_can_fail;
		if (source_type != cast.target_type) {
			input = BoundCastExpression::AddCastToType(context, std::move(input), cast.target_type, is_try_cast);
		}
		preserves_cast_errors |= cast.mode == RuntimeFilterCastMode::DEFAULT_CAST && cast_can_fail;
		source_type = cast.target_type;
	}
	return input;
}

} // namespace duckdb
