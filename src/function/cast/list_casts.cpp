#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"

namespace duckdb {

unique_ptr<BoundCastData> ListBoundCastData::BindListToListCast(BindCastInput &input, const LogicalType &source,
                                                                const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto &source_child_type = ListType::GetChildType(source);
	auto &result_child_type = ListType::GetChildType(target);
	auto child_cast = input.GetCastFunction(source_child_type, result_child_type);
	return make_uniq<ListBoundCastData>(std::move(child_cast));
}

unique_ptr<FunctionLocalState> ListBoundCastData::InitListLocalState(CastLocalStateParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<ListBoundCastData>();
	if (!cast_data.child_cast_info.init_local_state) {
		return nullptr;
	}
	CastLocalStateParameters child_parameters(parameters, cast_data.child_cast_info.cast_data);
	return cast_data.child_cast_info.init_local_state(child_parameters);
}

bool ListCast::ListToListCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<ListBoundCastData>();

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

	CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data, parameters.local_state);
	bool all_succeeded = cast_data.child_cast_info.function(source_cc, append_vector, source_size, child_parameters);
	ListVector::SetListSize(result, source_size);
	D_ASSERT(ListVector::GetListSize(result) == source_size);
	return all_succeeded;
}

static bool ListToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;
	// first cast the child vector to varchar
	Vector varchar_list(LogicalType::LIST(LogicalType::VARCHAR), count);
	ListCast::ListToListCast(source, varchar_list, count, parameters);

	// now construct the actual varchar vector
	varchar_list.Flatten(count);
	auto &child = ListVector::GetEntry(varchar_list);
	auto list_data = FlatVector::GetData<list_entry_t>(varchar_list);
	auto &validity = FlatVector::Validity(varchar_list);

	child.Flatten(count);
	auto child_data = FlatVector::GetData<string_t>(child);
	auto &child_validity = FlatVector::Validity(child);

	auto result_data = FlatVector::GetData<string_t>(result);
	static constexpr const idx_t SEP_LENGTH = 2;
	static constexpr const idx_t NULL_LENGTH = 4;
	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		auto list = list_data[i];
		// figure out how long the result needs to be
		idx_t list_length = 2; // "[" and "]"
		for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
			auto idx = list.offset + list_idx;
			if (list_idx > 0) {
				list_length += SEP_LENGTH; // ", "
			}
			// string length, or "NULL"
			list_length += child_validity.RowIsValid(idx) ? child_data[idx].GetSize() : NULL_LENGTH;
		}
		result_data[i] = StringVector::EmptyString(result, list_length);
		auto dataptr = result_data[i].GetDataWriteable();
		auto offset = 0;
		dataptr[offset++] = '[';
		for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
			auto idx = list.offset + list_idx;
			if (list_idx > 0) {
				memcpy(dataptr + offset, ", ", SEP_LENGTH);
				offset += SEP_LENGTH;
			}
			if (child_validity.RowIsValid(idx)) {
				auto len = child_data[idx].GetSize();
				memcpy(dataptr + offset, child_data[idx].GetData(), len);
				offset += len;
			} else {
				memcpy(dataptr + offset, "NULL", NULL_LENGTH);
				offset += NULL_LENGTH;
			}
		}
		dataptr[offset] = ']';
		result_data[i].Finalize();
	}

	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

BoundCastInfo DefaultCasts::ListCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::LIST:
		return BoundCastInfo(ListCast::ListToListCast, ListBoundCastData::BindListToListCast(input, source, target),
		                     ListBoundCastData::InitListLocalState);
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(
		    ListToVarcharCast,
		    ListBoundCastData::BindListToListCast(input, source, LogicalType::LIST(LogicalType::VARCHAR)),
		    ListBoundCastData::InitListLocalState);
	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

} // namespace duckdb
