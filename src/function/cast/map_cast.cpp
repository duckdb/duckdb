#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace duckdb {

struct MapBoundCastData : public BoundCastData {
	MapBoundCastData(BoundCastInfo key_cast, BoundCastInfo value_cast)
	    : key_cast(move(key_cast)), value_cast(move(value_cast)) {
	}

	BoundCastInfo key_cast;
	BoundCastInfo value_cast;

public:
	unique_ptr<BoundCastData> Copy() const override {
		return make_unique<MapBoundCastData>(key_cast.Copy(), value_cast.Copy());
	}
};

unique_ptr<BoundCastData> BindMapToMapCast(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto source_key = MapType::KeyType(source);
	auto target_key = MapType::KeyType(target);
	auto source_val = MapType::ValueType(source);
	auto target_val = MapType::ValueType(target);
	auto key_cast = input.GetCastFunction(source_key, target_key);
	auto value_cast = input.GetCastFunction(source_val, target_val);
	return make_unique<MapBoundCastData>(move(key_cast), move(value_cast));
}

static bool MapToMapCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    auto src_size = ListVector::GetListSize(source); // size of child vector
    ListVector::Reserve(result, src_size);
    ListVector::SetListSize(result, src_size);

    if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
        ConstantVector::SetNull(result, ConstantVector::IsNull(source));
        auto ldata = ConstantVector::GetData<list_entry_t>(source);
        auto tdata = ConstantVector::GetData<list_entry_t>(result);
        *tdata = *ldata;
    } else {
        source.Flatten(count);
        FlatVector::Validity(result) = FlatVector::Validity(source);
        auto source_data = ListVector::GetData(source);
        auto result_data = ListVector::GetData(result);
        for (idx_t i = 0; i < count; i++) {
            result_data[i] = source_data[i];
        }
    }

	auto &cast_data = (MapBoundCastData &)*parameters.cast_data;
	CastParameters key_params(parameters, cast_data.key_cast.cast_data.get());
	if (!cast_data.key_cast.function(MapVector::GetKeys(source), MapVector::GetKeys(result), src_size, key_params)) {
		return false;
	}
	CastParameters val_params(parameters, cast_data.value_cast.cast_data.get());
	if (!cast_data.value_cast.function(MapVector::GetValues(source), MapVector::GetValues(result), src_size, val_params)) {
		return false;
	}

	return true;
}

static bool MapToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;
	// first cast the child elements to varchar
	auto varchar_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	Vector varchar_map(varchar_type, count);


//    auto struct_vec = ListVector::GetEntry(source);
//    for (idx_t i = 0; i < ListVector::GetListSize(source); i++ ) {
//        auto &children = StructVector::GetEntries(struct_vec);
//
//        printf("%d: Key: %s, Value: %s\n", i, children[0]->GetValue(i).ToString().c_str(), children[1]->GetValue(i).ToString().c_str());
//    }

	MapToMapCast(source, varchar_map, count, parameters);

	// now construct the actual varchar vector
	varchar_map.Flatten(count);

	auto &validity = FlatVector::Validity(varchar_map);
	auto &key_str = MapVector::GetKeys(varchar_map);
	auto &val_str = MapVector::GetValues(varchar_map);
//	auto &key_str = ListVector::GetEntry(key_lists);
//	auto &val_str = ListVector::GetEntry(val_lists);

//	key_str.Flatten(ListVector::GetListSize(key_lists));
//	val_str.Flatten(ListVector::GetListSize(val_lists));

	auto list_data = ListVector::GetData(varchar_map);

    auto key_data = FlatVector::GetData<string_t>(key_str);
	auto val_data = FlatVector::GetData<string_t>(val_str);
	auto &key_validity = FlatVector::Validity(key_str);
	auto &val_validity = FlatVector::Validity(val_str);

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
			if (!key_validity.RowIsValid(idx)) {
				//throw InternalException("Error in map: key validity invalid?!");
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
		return BoundCastInfo(MapToMapCast, BindMapToMapCast(input, source, target));
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR: {
		// bind a cast in which we convert the key/value to VARCHAR entries
		auto varchar_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
		return BoundCastInfo(MapToVarcharCast, BindMapToMapCast(input, source, varchar_type));
	}
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
