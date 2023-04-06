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

	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		auto list = list_data[i];
		string ret = "{";
		for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
			if (list_idx > 0) {
				ret += ", ";
			}
			auto idx = list.offset + list_idx;

			if (!struct_validity.RowIsValid(idx)) {
				ret += "NULL";
				continue;
			}
			if (!key_validity.RowIsValid(idx)) {
				// throw InternalException("Error in map: key validity invalid?!");
				ret += "invalid";
				continue;
			}
			ret += key_data[idx].GetString();
			ret += "=";
			ret += val_validity.RowIsValid(idx) ? val_data[idx].GetString() : "NULL";
		}
		ret += "}";
		result_data[i] = StringVector::AddString(result, ret);
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
