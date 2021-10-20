//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/abs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct AbsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return input < 0 ? -input : input;
	}
};

template <>
inline hugeint_t AbsOperator::Operation(const hugeint_t &input) {
	const hugeint_t zero(0);
	return (input < zero) ? (zero - input) : input;
}

template <>
inline dtime_t AbsOperator::Operation(const dtime_t &input) {
	return dtime_t(AbsOperator::Operation<int64_t, int64_t>(input.micros));
}

template <>
inline interval_t AbsOperator::Operation(const interval_t &input) {
	return {AbsOperator::Operation<int32_t, int32_t>(input.months),
	        AbsOperator::Operation<int32_t, int32_t>(input.days),
	        AbsOperator::Operation<int64_t, int64_t>(input.micros)};
}

} // namespace duckdb
