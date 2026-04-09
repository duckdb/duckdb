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
	if (func.function.name == "struct_extract_at") {
		if (func.bind_info) {
			child_idx = func.bind_info->Cast<StructExtractBindData>().index;
			return true;
		}
		if (func.children.size() > 1 && func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &field_value = func.children[1]->Cast<BoundConstantExpression>().value;
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
	if (func.function.name != "struct_extract" || func.children.size() <= 1 ||
	    func.children[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
	    func.children[0]->return_type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &field_value = func.children[1]->Cast<BoundConstantExpression>().value;
	if (field_value.IsNull() || field_value.type().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	child_idx = StructType::GetChildIndexUnsafe(func.children[0]->return_type, field_value.GetValue<string>());
	return true;
}

} // namespace duckdb
