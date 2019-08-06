#include "function/aggregate_function/covar.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include <cmath>

using namespace std;

namespace duckdb {

static Vector &CastVector(Vector &original, TypeId type, Vector &cast) {
	if (original.type != type) {
		cast.Initialize(type);
		VectorOperations::Cast(original, cast);
	} else {
		cast.Reference(original);
	}
	return cast;
}

SQLType covar_get_return_type(vector<SQLType> &arguments) {
	if (arguments.size() != 2)
		return SQLTypeId::INVALID;
	const auto &input_type = MaxSQLType(arguments[0], arguments[1]);
	switch (input_type.id) {
	case SQLTypeId::SQLNULL:
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return SQLType(SQLTypeId::DECIMAL);
	default:
		return SQLTypeId::INVALID;
	}
}

void covar_update(Vector **inputs, index_t input_count, Vector &state) {
	assert(input_count == 2);
	// Streaming approximate covariance

	// convert inputs to floating point if required
	Vector doublex;
	CastVector(*inputs[0], TypeId::DOUBLE, doublex);
	Vector doubley;
	CastVector(*inputs[1], TypeId::DOUBLE, doubley);

	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		if (doublex.nullmask[i] || doubley.nullmask[i]) {
			return;
		}
		// Layout of state for online covariance:
		//  uint64_t    count
		//  double      meanx
		//  double      meany
		//  double      co-moment

		auto base_ptr = ((data_ptr_t *)state.data)[i];
		auto count_ptr = (uint64_t *)base_ptr;
		auto meanx_ptr = (double *)(base_ptr + sizeof(uint64_t));
		auto meany_ptr = (double *)(base_ptr + sizeof(uint64_t) + sizeof(double));
		auto co_moment_ptr = (double *)(base_ptr + sizeof(uint64_t) + sizeof(double) + sizeof(double));

		// update running mean and d^2
		const uint64_t n = ++(*count_ptr);

		const double x = ((double *)doublex.data)[i];
		const double dx = (x - (*meanx_ptr));
		const double meanx = (*meanx_ptr) + dx / n;

		const double y = ((double *)doubley.data)[i];
		const double dy = (y - (*meany_ptr));
		const double meany = (*meany_ptr) + dy / n;

		const double C = (*co_moment_ptr) + dx * (y - meany);

		*meanx_ptr = meanx;
		*meany_ptr = meany;
		*co_moment_ptr = C;
		// see Finalize() methods below for final step
	});
}

void covarpop_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming population covariance
	VectorOperations::Exec(result, [&](uint64_t i, uint64_t k) {
		auto base_ptr = ((data_ptr_t *)state.data)[i];
		auto count_ptr = (uint64_t *)base_ptr;
		auto co_moment_ptr = (double *)(base_ptr + sizeof(uint64_t) + sizeof(double) + sizeof(double));

		if (*count_ptr == 0) {
			result.nullmask[i] = true;
			return;
		}
		double res = (*co_moment_ptr) / (*count_ptr);

		((double *)result.data)[i] = res;
	});
}

void covarsamp_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming sample covariance
	VectorOperations::Exec(result, [&](uint64_t i, uint64_t k) {
		auto base_ptr = ((data_ptr_t *)state.data)[i];
		auto count_ptr = (uint64_t *)base_ptr;
		auto co_moment_ptr = (double *)(base_ptr + sizeof(uint64_t) + sizeof(double) + sizeof(double));

		if (*count_ptr < 2) {
			result.nullmask[i] = true;
			return;
		}
		double res = (*co_moment_ptr) / ((*count_ptr) - 1);

		((double *)result.data)[i] = res;
	});
}

} // namespace duckdb
