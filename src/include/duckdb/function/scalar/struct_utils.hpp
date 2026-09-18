//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/struct_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct StructExtractBindData : public FunctionData {
	explicit StructExtractBindData(idx_t index) : index(index) {
	}

	idx_t index;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StructExtractBindData>(index);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<StructExtractBindData>();
		return index == other.index;
	}
};

inline bool TryGetStructExtractChildIndex(const BoundFunctionExpression &func, idx_t &child_idx) {
	if (func.Function().GetName() == "struct_extract_at") {
		if (func.BindInfo()) {
			child_idx = func.BindInfo()->Cast<StructExtractBindData>().index;
			return true;
		}
		if (func.GetChildren().size() > 1 &&
		    func.GetChildren()[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &field_value = func.GetChildren()[1]->Cast<BoundConstantExpression>().GetValue();
			if (field_value.IsNull()) {
				return false;
			}
			auto index = field_value.GetValue<int64_t>();
			if (index <= 0) {
				return false;
			}
			child_idx = NumericCast<idx_t>(index - 1);
			return true;
		}
		return false;
	}
	if (func.Function().GetName() != "struct_extract" || func.GetChildren().size() <= 1 ||
	    func.GetChildren()[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
	    func.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &field_value = func.GetChildren()[1]->Cast<BoundConstantExpression>().GetValue();
	if (field_value.IsNull() || field_value.type().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	child_idx = StructType::GetChildIndexUnsafe(func.GetChildren()[0]->GetReturnType(), field_value.GetValue<string>());
	return true;
}

} // namespace duckdb
