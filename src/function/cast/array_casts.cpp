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
// ARRAY -> VARCHAR
//------------------------------------------------------------------------------
static bool ArrayToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameter) {
    throw NotImplementedException("Array -> Varchar cast not implemented");
}

//------------------------------------------------------------------------------
// ARRAY -> ARRAY
//------------------------------------------------------------------------------
static bool ArrayToArrayCast(Vector &source, Vector &result, idx_t count, CastParameters &parameter) {
    throw NotImplementedException("Array -> Array cast not implemented");
}

//------------------------------------------------------------------------------
// ARRAY -> LIST
//------------------------------------------------------------------------------
static bool ArrayToListCast(Vector &source, Vector &result, idx_t count, CastParameters &parameter) {
    throw NotImplementedException("Array -> List cast not implemented");
}

BoundCastInfo DefaultCasts::ArrayCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
        return BoundCastInfo(ArrayToVarcharCast, BindArrayToArrayCast(input, source, target), InitArrayLocalState);
    case LogicalTypeId::ARRAY: 
        return BoundCastInfo(ArrayToArrayCast, BindArrayToArrayCast(input, source, target), InitArrayLocalState);
	case LogicalTypeId::LIST:
        return BoundCastInfo(ArrayToListCast, BindArrayToArrayCast(input, source, target), InitArrayLocalState);
	default:
		return DefaultCasts::TryVectorNullCast;
    };
}

}