#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {

static cast_function_t ValueStringCastSwitch(const LogicalType &source, const LogicalType &target) {
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(source.GetVectorType());
		} else {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
		for (idx_t i = 0; i < count; i++) {
			auto src_val = source.GetValue(i);
			if (src_val.IsNull()) {
				result.SetValue(i, Value(result.GetType()));
			} else {
				auto str_val = src_val.ToString();
				result.SetValue(i, Value(str_val));
			}
		}
		return true;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static cast_function_t ListCastSwitch(const LogicalType &source, const LogicalType &target) {
	switch (result.GetType().id()) {
	case LogicalTypeId::LIST: {
		// only handle constant and flat vectors here for now
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(source.GetVectorType());
			ConstantVector::SetNull(result, ConstantVector::IsNull(source));

			auto ldata = ConstantVector::GetData<list_entry_t>(source);
			auto tdata = ConstantVector::GetData<list_entry_t>(result);
			*tdata = *ldata;
		} else {
			source.Flatten(count);
			result.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::SetValidity(result, FlatVector::Validity(source));

			auto ldata = FlatVector::GetData<list_entry_t>(source);
			auto tdata = FlatVector::GetData<list_entry_t>(result);
			for (idx_t i = 0; i < count; i++) {
				tdata[i] = ldata[i];
			}
		}
		auto &source_cc = ListVector::GetEntry(source);
		auto source_size = ListVector::GetListSize(source);

		ListVector::Reserve(result, source_size);
		auto &append_vector = ListVector::GetEntry(result);

		VectorOperations::Cast(source_cc, append_vector, source_size);
		ListVector::SetListSize(result, source_size);
		D_ASSERT(ListVector::GetListSize(result) == source_size);
		return true;
	}
	default:
		return ValueStringCastSwitch(source, result, count, error_message);
	}
}

}
