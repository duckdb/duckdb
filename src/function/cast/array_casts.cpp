#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"

namespace duckdb {

struct ArrayBoundCastData : public BoundCastData {
	explicit ArrayBoundCastData(BoundCastInfo child_cast) : child_cast_info(std::move(child_cast)) {
	}

	BoundCastInfo child_cast_info;

public:
	unique_ptr<BoundCastData> Copy() const override {
		return make_uniq<ListBoundCastData>(child_cast_info.Copy());
	}
};

static unique_ptr<BoundCastData> BindArrayToArrayCast(BindCastInput &input, const LogicalType &source,
                                                      const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto &source_child_type = ArrayType::GetChildType(source);
	auto &result_child_type = ArrayType::GetChildType(target);
	auto child_cast = input.GetCastFunction(source_child_type, result_child_type);
	return make_uniq<ArrayBoundCastData>(std::move(child_cast));
}

static unique_ptr<FunctionLocalState> InitArrayLocalState(CastLocalStateParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<ArrayBoundCastData>();
	if (!cast_data.child_cast_info.init_local_state) {
		return nullptr;
	}
	CastLocalStateParameters child_parameters(parameters, cast_data.child_cast_info.cast_data);
	return cast_data.child_cast_info.init_local_state(child_parameters);
}

//------------------------------------------------------------------------------
// ARRAY -> ARRAY
//------------------------------------------------------------------------------
static bool ArrayToArrayCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {

	auto &cast_data = parameters.cast_data->Cast<ArrayBoundCastData>();

	// TODO: Handle mismatched sizes
	if (ArrayType::GetSize(source.GetType()) != ArrayType::GetSize(result.GetType())) {
		return false;
	}

	// TODO: dont flatten
	source.Flatten(count);
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	auto fixed_size = ArrayType::GetSize(source.GetType());
	auto &source_cc = ArrayVector::GetEntry(source);
	auto &result_cc = ArrayVector::GetEntry(result);
	auto child_count = count * fixed_size;

	CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data, parameters.local_state);
	bool all_ok = cast_data.child_cast_info.function(source_cc, result_cc, child_count, child_parameters);
	return all_ok;
}

//------------------------------------------------------------------------------
// ARRAY -> VARCHAR
//------------------------------------------------------------------------------
static bool ArrayToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto is_constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;

	auto size = ArrayType::GetSize(source.GetType());
	Vector varchar_list(LogicalType::ARRAY(LogicalType::VARCHAR, size), count);
	ArrayToArrayCast(source, varchar_list, count, parameters);

	varchar_list.Flatten(count);
	auto &validity = FlatVector::Validity(varchar_list);
	auto &child = ArrayVector::GetEntry(varchar_list);

	child.Flatten(count);
	auto &child_validity = FlatVector::Validity(child);

	auto in_data = FlatVector::GetData<string_t>(child);
	auto out_data = FlatVector::GetData<string_t>(result);

	static constexpr const idx_t SEP_LENGTH = 2;
	static constexpr const idx_t NULL_LENGTH = 4;

	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// First pass, compute the length
		idx_t list_varchar_length = 2;
		for (idx_t j = 0; j < size; j++) {
			auto elem = in_data[(i * size) + j];
			if (j > 0) {
				list_varchar_length += SEP_LENGTH;
			}
			list_varchar_length += child_validity.RowIsValid(i) ? elem.GetSize() : NULL_LENGTH;
		}

		out_data[i] = StringVector::EmptyString(result, list_varchar_length);
		auto dataptr = out_data[i].GetDataWriteable();
		auto offset = 0;
		dataptr[offset++] = '[';

		// Second pass, write the actual data
		for (idx_t j = 0; j < size; j++) {
			auto elem = in_data[(i * size) + j];
			if (j > 0) {
				memcpy(dataptr + offset, ", ", SEP_LENGTH);
				offset += SEP_LENGTH;
			}
			if (child_validity.RowIsValid(i)) {
				auto len = elem.GetSize();
				memcpy(dataptr + offset, elem.GetData(), len);
				offset += len;
			} else {
				memcpy(dataptr + offset, "NULL", NULL_LENGTH);
				offset += NULL_LENGTH;
			}
		}
		dataptr[offset++] = ']';
		out_data[i].Finalize();
	}

	if (is_constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return true;
}

//------------------------------------------------------------------------------
// ARRAY -> LIST
//------------------------------------------------------------------------------
static bool ArrayToListCast(Vector &source, Vector &result, idx_t count, CastParameters &parameter) {
	throw NotImplementedException("Array -> List cast not implemented");
}

BoundCastInfo DefaultCasts::ArrayCastSwitch(BindCastInput &input, const LogicalType &source,
                                            const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::VARCHAR: {
		auto size = ArrayType::GetSize(source);
		return BoundCastInfo(ArrayToVarcharCast,
		                     BindArrayToArrayCast(input, source, LogicalType::ARRAY(LogicalType::VARCHAR, size)),
		                     InitArrayLocalState);
	}
	case LogicalTypeId::ARRAY:
		return BoundCastInfo(ArrayToArrayCast, BindArrayToArrayCast(input, source, target), InitArrayLocalState);
	case LogicalTypeId::LIST:
		return BoundCastInfo(ArrayToListCast, BindArrayToArrayCast(input, source, target), InitArrayLocalState);
	default:
		return DefaultCasts::TryVectorNullCast;
	};
}

} // namespace duckdb
