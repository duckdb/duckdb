#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"

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
	auto &validity = FlatVector::Validity(varchar_map);
	auto &key_str = MapVector::GetKeys(varchar_map);
	auto &val_str = MapVector::GetValues(varchar_map);

	key_str.Flatten(ListVector::GetListSize(source));
	val_str.Flatten(ListVector::GetListSize(source));

	auto list_data = ListVector::GetData(varchar_map);
	auto key_data = FlatVector::GetData<string_t>(key_str);
	auto val_data = FlatVector::GetData<string_t>(val_str);
	auto &key_validity = FlatVector::Validity(key_str);
	auto &val_validity = FlatVector::Validity(val_str);
	auto &struct_validity = FlatVector::Validity(ListVector::GetEntry(varchar_map));

	//! {key=value[, ]}
	static constexpr const idx_t SEP_LENGTH = 2;
	static constexpr const idx_t KEY_VALUE_SEP_LENGTH = 1;
	static constexpr const idx_t NULL_LENGTH = 4;
	static constexpr const idx_t INVALID_LENGTH = 7;
	static constexpr const char SPECIAL_CHARACTERS[] = "{}=\"',\\";

	auto &key_vec = MapVector::GetKeys(source);
	auto &value_vec = MapVector::GetValues(source);

	auto key_is_nested = key_vec.GetType().IsNested();
	auto value_is_nested = value_vec.GetType().IsNested();

	auto key_strlen_func =
	    key_is_nested ? VectorCastHelpers::CalculateStringLength : VectorCastHelpers::CalculateEscapedStringLength;
	auto key_write_func = key_is_nested ? VectorCastHelpers::WriteString : VectorCastHelpers::WriteEscapedString;

	auto value_strlen_func =
	    value_is_nested ? VectorCastHelpers::CalculateStringLength : VectorCastHelpers::CalculateEscapedStringLength;
	auto value_write_func = value_is_nested ? VectorCastHelpers::WriteString : VectorCastHelpers::WriteEscapedString;

	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		idx_t string_length = 2; // {}
		auto list = list_data[i];
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
			string_length += key_strlen_func(key_data[idx], SPECIAL_CHARACTERS, sizeof(SPECIAL_CHARACTERS));
			string_length += KEY_VALUE_SEP_LENGTH;
			if (val_validity.RowIsValid(idx)) {
				string_length += value_strlen_func(val_data[idx], SPECIAL_CHARACTERS, sizeof(SPECIAL_CHARACTERS));
			} else {
				string_length += NULL_LENGTH;
			}
		}
		result_data[i] = StringVector::EmptyString(result, string_length);
		auto dataptr = result_data[i].GetDataWriteable();
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
			offset += key_write_func(dataptr + offset, key_data[idx], SPECIAL_CHARACTERS, sizeof(SPECIAL_CHARACTERS));
			dataptr[offset++] = '=';
			if (val_validity.RowIsValid(idx)) {
				offset +=
				    value_write_func(dataptr + offset, val_data[idx], SPECIAL_CHARACTERS, sizeof(SPECIAL_CHARACTERS));
			} else {
				memcpy(dataptr + offset, "NULL", NULL_LENGTH);
				offset += NULL_LENGTH;
			}
		}
		dataptr[offset++] = '}';
		result_data[i].Finalize();
	}

	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

BoundCastInfo DefaultCasts::MapCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::MAP:
		return BoundCastInfo(ListCast::ListToListCast, ListBoundCastData::BindListToListCast(input, source, target),
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
