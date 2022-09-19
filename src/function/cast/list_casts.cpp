#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace duckdb {

struct ListBoundCastData : public BoundCastData {
	ListBoundCastData(BoundCastInfo child_cast) : child_cast_info(move(child_cast)) {
	}

	BoundCastInfo child_cast_info;

public:
	unique_ptr<BoundCastData> Copy() const override {
		return make_unique<ListBoundCastData>(child_cast_info.Copy());
	}
};

unique_ptr<BoundCastData> BindListToListCast(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto &source_child_type = ListType::GetChildType(source);
	auto &result_child_type = ListType::GetChildType(target);
	auto child_cast = input.function_set.GetCastFunction(source_child_type, result_child_type);
	return make_unique<ListBoundCastData>(move(child_cast));
}

static bool ListToListCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = (ListBoundCastData &)*parameters.cast_data;

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

	CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data.get());
	if (!cast_data.child_cast_info.function(source_cc, append_vector, source_size, child_parameters)) {
		return false;
	}
	ListVector::SetListSize(result, source_size);
	D_ASSERT(ListVector::GetListSize(result) == source_size);
	return true;
}

static bool ListToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
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
}

BoundCastInfo DefaultCasts::ListCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::LIST:
		return BoundCastInfo(ListToListCast, BindListToListCast(input, source, target));
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		return ListToVarcharCast;
	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

} // namespace duckdb
