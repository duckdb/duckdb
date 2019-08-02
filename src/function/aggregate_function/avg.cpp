#include "function/aggregate_function/avg.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

SQLType avg_get_return_type(vector<SQLType> &arguments) {
	if (arguments.size() != 1)
		throw BinderException("Unsupported argument count %d for AVG aggregate", (int) arguments.size());
	const auto& input_type = arguments[0];
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
		throw BinderException("Unsupported SQLType %s for AVG aggregate", SQLTypeToString(input_type).c_str());
	}
}

void avg_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1 );
	Vector payload_double;
	if (inputs[0].type != TypeId::DOUBLE) {
		payload_double.Initialize(TypeId::DOUBLE);
		VectorOperations::Cast(inputs[0], payload_double);
	} else {
		payload_double.Reference(inputs[0]);
	}

	VectorOperations::Exec(result, [&](index_t i, index_t k) {
		if (payload_double.nullmask[i]) {
			return;
		}
		// Layout of payload for AVG: count(uint64_t), sum(double)

		auto base_ptr = ((data_ptr_t *)result.data)[i];
		auto count_ptr = (uint64_t *)base_ptr;
		auto sum_ptr = (double *)(base_ptr + sizeof(uint64_t));

		// update count and running sum
		(*count_ptr)++;
		const auto new_value = ((double *)payload_double.data)[i];
		(*sum_ptr) += new_value;
		// see Finalize() method below for final step
	});
}

void avg_finalize(Vector& payloads, Vector &result) {
	// compute finalization of streaming avg
	VectorOperations::Exec(payloads, [&](uint64_t i, uint64_t k) {
		auto base_ptr = ((data_ptr_t *)payloads.data)[i];
		auto count_ptr = (uint64_t *)base_ptr;
		auto sum_ptr = (double *)(base_ptr + sizeof(uint64_t));

		if (*count_ptr == 0) {
			result.nullmask[i] = true;
		} else {
			((double *)result.data)[i] = *sum_ptr / *count_ptr;
		}
	});
}

} // namespace duckdb
