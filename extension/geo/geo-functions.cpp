#include "geo-functions.hpp"
#include "postgis.hpp"
#include "geometry.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

namespace duckdb {

bool GeoFunctions::CastVarcharToGEO(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto input = (string_t *)vdata.data;
	auto result_data = FlatVector::GetData<string_t>(result);
	bool success = true;
	for (idx_t i = 0; i < (constant ? 1 : count); i++) {
		auto idx = vdata.sel->get_index(i);

		if (!vdata.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

        auto gser = Geometry::ToGserialized(input[idx]);
		if (!gser) {
			FlatVector::SetNull(result, i, true);
			success = false;
			continue;
		}
		idx_t rv_size = Geometry::GetGeometrySize(gser);
		string_t rv = StringVector::EmptyString(result, rv_size);
		Geometry::ToGeometry(gser, (data_ptr_t)rv.GetDataWriteable());
		Geometry::DestroyGeometry(gser);
		rv.Finalize();
		result_data[i] = rv;
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return success;
}

bool GeoFunctions::CastGeoToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	GenericExecutor::ExecuteUnary<PrimitiveType<string_t>, PrimitiveType<string_t>>(
	    source, result, count, [&](PrimitiveType<string_t> input) {
			auto text = Geometry::GetString(input.val);
			return StringVector::AddString(result, text);
	    });
	return true;
}

} // namespace duckdb
