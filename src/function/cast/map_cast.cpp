#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

unique_ptr<BoundCastData> MapBoundCastData::BindMapToMapCast(BindCastInput &input, const LogicalType &source,
                                                             const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto source_key = MapType::KeyType(source);
	auto target_key = MapType::KeyType(target);
	auto source_val = MapType::ValueType(source);
	auto target_val = MapType::ValueType(target);
	auto key_cast = input.GetCastFunction(source_key, target_key);
	auto value_cast = input.GetCastFunction(source_val, target_val);
	return make_uniq<MapBoundCastData>(std::move(key_cast), std::move(value_cast));
}

static bool MapToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto varchar_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	Vector varchar_map(varchar_type, count);

	// since map's physical type is a list, the ListCast can be utilized
	ListCast::ListToListCast(source, varchar_map, count, parameters);

	varchar_map.Flatten(count);
	auto &validity = FlatVector::ValidityMutable(varchar_map);
	auto &key_str = MapVector::GetKeys(varchar_map);
	auto &val_str = MapVector::GetValues(varchar_map);

	key_str.Flatten(ListVector::GetListSize(source));
	val_str.Flatten(ListVector::GetListSize(source));

	auto list_data = FlatVector::GetData<list_entry_t>(varchar_map);
	auto key_data = FlatVector::GetData<string_t>(key_str);
	auto val_data = FlatVector::GetData<string_t>(val_str);
	auto &key_validity = FlatVector::Validity(key_str);
	auto &val_validity = FlatVector::Validity(val_str);
	auto &struct_validity = FlatVector::Validity(ListVector::GetChild(varchar_map));

	//! {key=value[, ]}
	static constexpr const idx_t SEP_LENGTH = 2;
	static constexpr const idx_t KEY_VALUE_SEP_LENGTH = 1;
	static constexpr const idx_t NULL_LENGTH = 4;
	static constexpr const idx_t INVALID_LENGTH = 7;

	auto &key_vec = MapVector::GetKeys(source);
	auto &value_vec = MapVector::GetValues(source);

	auto key_is_nested = key_vec.GetType().IsNested();
	auto value_is_nested = value_vec.GetType().IsNested();

	auto key_strlen_func = key_is_nested ? VectorCastHelpers::CalculateStringLength
	                                     : VectorCastHelpers::CalculateEscapedStringLength<false>;
	auto key_write_func = key_is_nested ? VectorCastHelpers::WriteString : VectorCastHelpers::WriteEscapedString<false>;

	auto value_strlen_func = value_is_nested ? VectorCastHelpers::CalculateStringLength
	                                         : VectorCastHelpers::CalculateEscapedStringLength<false>;
	auto value_write_func =
	    value_is_nested ? VectorCastHelpers::WriteString : VectorCastHelpers::WriteEscapedString<false>;

	unsafe_unique_array<bool> key_needs_quotes;
	unsafe_unique_array<bool> value_needs_quotes;
	idx_t needs_quotes_length = DConstants::INVALID_INDEX;
	auto result_data = FlatVector::Writer<string_t>(result, count);
	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			result_data.PushInvalid();
			continue;
		}

		idx_t string_length = 2; // {}
		auto list = list_data[i];

		if (!key_needs_quotes || list.length > needs_quotes_length) {
			key_needs_quotes = make_unsafe_uniq_array_uninitialized<bool>(list.length);
			value_needs_quotes = make_unsafe_uniq_array_uninitialized<bool>(list.length);
			needs_quotes_length = list.length;
		}
		for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
			if (list_idx > 0) {
				string_length += SEP_LENGTH;
			}

			auto idx = list.offset + list_idx;

			if (!struct_validity.RowIsValid(idx)) {
				string_length += NULL_LENGTH;
				continue;
			}
			if (!key_validity.RowIsValid(idx)) {
				// throw InternalException("Error in map: key validity invalid?!");
				string_length += INVALID_LENGTH;
				continue;
			}
			string_length += key_strlen_func(key_data[idx], key_needs_quotes[list_idx]);
			string_length += KEY_VALUE_SEP_LENGTH;
			if (val_validity.RowIsValid(idx)) {
				string_length += value_strlen_func(val_data[idx], value_needs_quotes[list_idx]);
			} else {
				string_length += NULL_LENGTH;
			}
		}
		auto &result_str = result_data.PushEmptyString(string_length);
		auto dataptr = result_str.GetDataWriteable();
		idx_t offset = 0;

		dataptr[offset++] = '{';
		for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
			if (list_idx > 0) {
				memcpy(dataptr + offset, ", ", SEP_LENGTH);
				offset += SEP_LENGTH;
			}

			auto idx = list.offset + list_idx;
			if (!struct_validity.RowIsValid(idx)) {
				memcpy(dataptr + offset, "NULL", NULL_LENGTH);
				offset += NULL_LENGTH;
				continue;
			}
			if (!key_validity.RowIsValid(idx)) {
				// throw InternalException("Error in map: key validity invalid?!");
				memcpy(dataptr + offset, "invalid", INVALID_LENGTH);
				offset += INVALID_LENGTH;
				continue;
			}
			offset += key_write_func(dataptr + offset, key_data[idx], key_needs_quotes[list_idx]);
			dataptr[offset++] = '=';
			if (val_validity.RowIsValid(idx)) {
				offset += value_write_func(dataptr + offset, val_data[idx], value_needs_quotes[list_idx]);
			} else {
				memcpy(dataptr + offset, "NULL", NULL_LENGTH);
				offset += NULL_LENGTH;
			}
		}
		dataptr[offset++] = '}';
		result_str.Finalize();
	}

	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

static bool MapToMapCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	const bool succeeded = ListCast::ListToListCast(source, result, count, parameters);
	if (succeeded) {
		return true;
	}

	// We're not in TRY_CAST mode, so the error will be thrown.
	if (!parameters.error_message) {
		return false;
	}

	// We're in TRY_CAST mode: child cast failures may have produced NULL keys in the result maps.
	// NULL keys are not allowed, so NULL out those map entries.
	auto &keys = MapVector::GetKeys(result);
	auto maps_length = ListVector::GetListSize(result);
	auto key_validity = keys.Validity(maps_length);

	if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (!ConstantVector::IsNull(result)) {
			auto list_data = ConstantVector::GetData<list_entry_t>(result);
			for (idx_t j = 0; j < list_data->length; j++) {
				if (!key_validity.IsValid(list_data->offset + j)) {
					ConstantVector::SetNull(result);
					break;
				}
			}
		}
	} else {
		auto list_data = FlatVector::GetData<list_entry_t>(result);
		auto &result_validity = FlatVector::ValidityMutable(result);
		for (idx_t i = 0; i < count; i++) {
			if (!result_validity.RowIsValid(i)) {
				continue;
			}
			for (idx_t j = 0; j < list_data[i].length; j++) {
				if (!key_validity.IsValid(list_data[i].offset + j)) {
					result_validity.SetInvalid(i);
					break;
				}
			}
		}
	}
	return false;
}

BoundCastInfo DefaultCasts::MapCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::MAP:
		return BoundCastInfo(MapToMapCast, ListBoundCastData::BindListToListCast(input, source, target),
		                     ListBoundCastData::InitListLocalState);
	case LogicalTypeId::VARCHAR: {
		auto varchar_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
		return BoundCastInfo(MapToVarcharCast, ListBoundCastData::BindListToListCast(input, source, varchar_type),
		                     ListBoundCastData::InitListLocalState);
	}
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
