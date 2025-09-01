#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

static bool IsPrimitiveType(VariantLogicalType type) {
	return type != VariantLogicalType::OBJECT && type != VariantLogicalType::ARRAY;
}

static void VariantTypeofFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto count = input.size();

	D_ASSERT(input.ColumnCount() == 1);
	auto &variant_vec = input.data[0];
	D_ASSERT(variant_vec.GetType() == LogicalType::VARIANT());

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, count, source_format);

	UnifiedVariantVectorData variant(source_format);

	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto index = source_format.unified.sel->get_index(i);
		if (!source_format.unified.validity.RowIsValid(index)) {
			result_data[i] = StringVector::AddString(result, "VARIANT_NULL");
			continue;
		}

		auto type = variant.GetTypeId(i, 0);

		string type_str;
		if (IsPrimitiveType(type)) {
			if (type != VariantLogicalType::DECIMAL) {
				type_str = EnumUtil::ToString(type);
			} else {
				auto decimal_data = VariantUtils::DecodeDecimalData(variant, i, 0);
				type_str = StringUtil::Format("DECIMAL(%d, %d)", decimal_data.width, decimal_data.scale);
			}
			result_data[i] = StringVector::AddString(result, type_str.c_str());
			continue;
		}

		if (type == VariantLogicalType::OBJECT) {
			auto nested_data = VariantUtils::DecodeNestedData(variant, i, 0);
			vector<string> object_keys;
			for (idx_t child_idx = 0; child_idx < nested_data.child_count; child_idx++) {
				auto child_key_id = variant.GetKeysIndex(i, nested_data.children_idx + child_idx);
				object_keys.push_back(variant.GetKey(i, child_key_id).GetString());
			}
			type_str = StringUtil::Format("OBJECT(%s)", StringUtil::Join(object_keys, ", "));
		} else {
			D_ASSERT(type == VariantLogicalType::ARRAY);
			auto nested_data = VariantUtils::DecodeNestedData(variant, i, 0);
			type_str = StringUtil::Format("ARRAY(%d)", nested_data.child_count);
		}
		result_data[i] = StringVector::AddString(result, type_str.c_str());
	}

	if (input.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction VariantTypeofFun::GetFunction() {
	auto variant_type = LogicalType::VARIANT();
	auto res = ScalarFunction("variant_typeof", {variant_type}, LogicalType::VARCHAR, VariantTypeofFunction);
	res.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return res;
}

} // namespace duckdb
