#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace duckdb {

template <class SRC_TYPE, class RES_TYPE>
bool EnumEnumCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	bool all_converted = true;
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &str_vec = EnumType::GetValuesInsertOrder(source.GetType());
	auto str_vec_ptr = FlatVector::GetData<string_t>(str_vec);

	auto res_enum_type = result.GetType();

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto source_data = UnifiedVectorFormat::GetData<SRC_TYPE>(vdata);
	auto source_sel = vdata.sel;
	auto source_mask = vdata.validity;

	auto result_data = FlatVector::GetData<RES_TYPE>(result);
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto src_idx = source_sel->get_index(i);
		if (!source_mask.RowIsValid(src_idx)) {
			result_mask.SetInvalid(i);
			continue;
		}
		auto key = EnumType::GetPos(res_enum_type, str_vec_ptr[source_data[src_idx]]);
		if (key == -1) {
			// key doesn't exist on result enum
			if (!parameters.error_message) {
				result_data[i] = HandleVectorCastError::Operation<RES_TYPE>(
				    CastExceptionText<SRC_TYPE, RES_TYPE>(source_data[src_idx]), result_mask, i,
				    parameters.error_message, all_converted);
			} else {
				result_mask.SetInvalid(i);
			}
			continue;
		}
		result_data[i] = key;
	}
	return all_converted;
}

template <class SRC_TYPE>
BoundCastInfo EnumEnumCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	switch (target.InternalType()) {
	case PhysicalType::UINT8:
		return EnumEnumCast<SRC_TYPE, uint8_t>;
	case PhysicalType::UINT16:
		return EnumEnumCast<SRC_TYPE, uint16_t>;
	case PhysicalType::UINT32:
		return EnumEnumCast<SRC_TYPE, uint32_t>;
	default:
		throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
	}
}

template <class SRC>
static bool EnumToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &enum_dictionary = EnumType::GetValuesInsertOrder(source.GetType());
	auto dictionary_data = FlatVector::GetData<string_t>(enum_dictionary);
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Validity(result);

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto source_data = UnifiedVectorFormat::GetData<SRC>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto source_idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(source_idx)) {
			result_mask.SetInvalid(i);
			continue;
		}
		auto enum_idx = source_data[source_idx];
		result_data[i] = dictionary_data[enum_idx];
	}
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);
	}
	return true;
}

struct EnumBoundCastData : public BoundCastData {
	EnumBoundCastData(BoundCastInfo to_varchar_cast, BoundCastInfo from_varchar_cast)
	    : to_varchar_cast(std::move(to_varchar_cast)), from_varchar_cast(std::move(from_varchar_cast)) {
	}

	BoundCastInfo to_varchar_cast;
	BoundCastInfo from_varchar_cast;

public:
	unique_ptr<BoundCastData> Copy() const override {
		return make_uniq<EnumBoundCastData>(to_varchar_cast.Copy(), from_varchar_cast.Copy());
	}
};

unique_ptr<BoundCastData> BindEnumCast(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	auto to_varchar_cast = input.GetCastFunction(source, LogicalType::VARCHAR);
	auto from_varchar_cast = input.GetCastFunction(LogicalType::VARCHAR, target);
	return make_uniq<EnumBoundCastData>(std::move(to_varchar_cast), std::move(from_varchar_cast));
}

struct EnumCastLocalState : public FunctionLocalState {
public:
	unique_ptr<FunctionLocalState> to_varchar_local;
	unique_ptr<FunctionLocalState> from_varchar_local;
};

static unique_ptr<FunctionLocalState> InitEnumCastLocalState(CastLocalStateParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<EnumBoundCastData>();
	auto result = make_uniq<EnumCastLocalState>();

	if (cast_data.from_varchar_cast.init_local_state) {
		CastLocalStateParameters from_varchar_params(parameters, cast_data.from_varchar_cast.cast_data);
		result->from_varchar_local = cast_data.from_varchar_cast.init_local_state(from_varchar_params);
	}
	if (cast_data.to_varchar_cast.init_local_state) {
		CastLocalStateParameters from_varchar_params(parameters, cast_data.to_varchar_cast.cast_data);
		result->from_varchar_local = cast_data.to_varchar_cast.init_local_state(from_varchar_params);
	}
	return std::move(result);
}

static bool EnumToAnyCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<EnumBoundCastData>();
	auto &lstate = parameters.local_state->Cast<EnumCastLocalState>();

	Vector varchar_cast(LogicalType::VARCHAR, count);

	// cast to varchar
	CastParameters to_varchar_params(parameters, cast_data.to_varchar_cast.cast_data, lstate.to_varchar_local);
	cast_data.to_varchar_cast.function(source, varchar_cast, count, to_varchar_params);

	// cast from varchar to the target
	CastParameters from_varchar_params(parameters, cast_data.from_varchar_cast.cast_data, lstate.from_varchar_local);
	cast_data.from_varchar_cast.function(varchar_cast, result, count, from_varchar_params);
	return true;
}

BoundCastInfo DefaultCasts::EnumCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	auto enum_physical_type = source.InternalType();
	switch (target.id()) {
	case LogicalTypeId::ENUM: {
		// This means they are both ENUMs, but of different types.
		switch (enum_physical_type) {
		case PhysicalType::UINT8:
			return EnumEnumCastSwitch<uint8_t>(input, source, target);
		case PhysicalType::UINT16:
			return EnumEnumCastSwitch<uint16_t>(input, source, target);
		case PhysicalType::UINT32:
			return EnumEnumCastSwitch<uint32_t>(input, source, target);
		default:
			throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
		}
	}
	case LogicalTypeId::VARCHAR:
		switch (enum_physical_type) {
		case PhysicalType::UINT8:
			return EnumToVarcharCast<uint8_t>;
		case PhysicalType::UINT16:
			return EnumToVarcharCast<uint16_t>;
		case PhysicalType::UINT32:
			return EnumToVarcharCast<uint32_t>;
		default:
			throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
		}
	default: {
		return BoundCastInfo(EnumToAnyCast, BindEnumCast(input, source, target), InitEnumCastLocalState);
	}
	}
}

} // namespace duckdb
