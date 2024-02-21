#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types.hpp"
#include "fast_float/fast_float.h"
#include "fmt/format.h"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/operator/integer_cast_operator.hpp"
#include "duckdb/common/operator/double_cast_operator.hpp"

#include <cctype>
#include <cmath>
#include <cstdlib>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Cast bool -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(bool input, bool &result, bool strict) {
	return NumericTryCast::Operation<bool, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<bool, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, float &result, bool strict) {
	return NumericTryCast::Operation<bool, float>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, double &result, bool strict) {
	return NumericTryCast::Operation<bool, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int8_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int8_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int8_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int8_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int16_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int16_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int16_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int16_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int32_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int32_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int32_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int32_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int64_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int64_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int64_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int64_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast hugeint_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(hugeint_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, float &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, double &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uhugeint_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uhugeint_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uhugeint_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uhugeint_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint8_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint16_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint32_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint64_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast float -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(float input, bool &result, bool strict) {
	return NumericTryCast::Operation<float, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<float, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<float, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<float, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<float, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<float, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<float, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, float &result, bool strict) {
	return NumericTryCast::Operation<float, float>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, double &result, bool strict) {
	return NumericTryCast::Operation<float, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast double -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(double input, bool &result, bool strict) {
	return NumericTryCast::Operation<double, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<double, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<double, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<double, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<double, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<double, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uhugeint_t &result, bool strict) {
	return NumericTryCast::Operation<double, uhugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, float &result, bool strict) {
	return NumericTryCast::Operation<double, float>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, double &result, bool strict) {
	return NumericTryCast::Operation<double, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast String -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(string_t input, bool &result, bool strict) {
	auto input_data = input.GetData();
	auto input_size = input.GetSize();

	switch (input_size) {
	case 1: {
		char c = std::tolower(*input_data);
		if (c == 't' || (!strict && c == '1')) {
			result = true;
			return true;
		} else if (c == 'f' || (!strict && c == '0')) {
			result = false;
			return true;
		}
		return false;
	}
	case 4: {
		char t = std::tolower(input_data[0]);
		char r = std::tolower(input_data[1]);
		char u = std::tolower(input_data[2]);
		char e = std::tolower(input_data[3]);
		if (t == 't' && r == 'r' && u == 'u' && e == 'e') {
			result = true;
			return true;
		}
		return false;
	}
	case 5: {
		char f = std::tolower(input_data[0]);
		char a = std::tolower(input_data[1]);
		char l = std::tolower(input_data[2]);
		char s = std::tolower(input_data[3]);
		char e = std::tolower(input_data[4]);
		if (f == 'f' && a == 'a' && l == 'l' && s == 's' && e == 'e') {
			result = false;
			return true;
		}
		return false;
	}
	default:
		return false;
	}
}
template <>
bool TryCast::Operation(string_t input, int8_t &result, bool strict) {
	return TrySimpleIntegerCast<int8_t>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int16_t &result, bool strict) {
	return TrySimpleIntegerCast<int16_t>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int32_t &result, bool strict) {
	return TrySimpleIntegerCast<int32_t>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int64_t &result, bool strict) {
	return TrySimpleIntegerCast<int64_t>(input.GetData(), input.GetSize(), result, strict);
}

template <>
bool TryCast::Operation(string_t input, uint8_t &result, bool strict) {
	return TrySimpleIntegerCast<uint8_t, false>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint16_t &result, bool strict) {
	return TrySimpleIntegerCast<uint16_t, false>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint32_t &result, bool strict) {
	return TrySimpleIntegerCast<uint32_t, false>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint64_t &result, bool strict) {
	return TrySimpleIntegerCast<uint64_t, false>(input.GetData(), input.GetSize(), result, strict);
}

template <>
bool TryCast::Operation(string_t input, float &result, bool strict) {
	return TryDoubleCast<float>(input.GetData(), input.GetSize(), result, strict);
}

template <>
bool TryCast::Operation(string_t input, double &result, bool strict) {
	return TryDoubleCast<double>(input.GetData(), input.GetSize(), result, strict);
}

template <>
bool TryCastErrorMessageCommaSeparated::Operation(string_t input, float &result, CastParameters &parameters) {
	if (!TryDoubleCast<float>(input.GetData(), input.GetSize(), result, parameters.strict, ',')) {
		HandleCastError::AssignError(StringUtil::Format("Could not cast string to float: \"%s\"", input.GetString()),
		                             parameters);
		return false;
	}
	return true;
}

template <>
bool TryCastErrorMessageCommaSeparated::Operation(string_t input, double &result, CastParameters &parameters) {
	if (!TryDoubleCast<double>(input.GetData(), input.GetSize(), result, parameters.strict, ',')) {
		HandleCastError::AssignError(StringUtil::Format("Could not cast string to double: \"%s\"", input.GetString()),
		                             parameters);
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Date
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(date_t input, date_t &result, bool strict) {
	result = input;
	return true;
}

template <>
bool TryCast::Operation(date_t input, timestamp_t &result, bool strict) {
	if (input == date_t::infinity()) {
		result = timestamp_t::infinity();
		return true;
	} else if (input == date_t::ninfinity()) {
		result = timestamp_t::ninfinity();
		return true;
	}
	return Timestamp::TryFromDatetime(input, Time::FromTime(0, 0, 0), result);
}

//===--------------------------------------------------------------------===//
// Cast From Time
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(dtime_t input, dtime_t &result, bool strict) {
	result = input;
	return true;
}

template <>
bool TryCast::Operation(dtime_t input, dtime_tz_t &result, bool strict) {
	result = dtime_tz_t(input, 0);
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Time With Time Zone (Offset)
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(dtime_tz_t input, dtime_tz_t &result, bool strict) {
	result = input;
	return true;
}

template <>
bool TryCast::Operation(dtime_tz_t input, dtime_t &result, bool strict) {
	result = input.time();
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Timestamps
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(timestamp_t input, date_t &result, bool strict) {
	result = Timestamp::GetDate(input);
	return true;
}

template <>
bool TryCast::Operation(timestamp_t input, dtime_t &result, bool strict) {
	if (!Timestamp::IsFinite(input)) {
		return false;
	}
	result = Timestamp::GetTime(input);
	return true;
}

template <>
bool TryCast::Operation(timestamp_t input, timestamp_t &result, bool strict) {
	result = input;
	return true;
}

template <>
bool TryCast::Operation(timestamp_t input, dtime_tz_t &result, bool strict) {
	if (!Timestamp::IsFinite(input)) {
		return false;
	}
	result = dtime_tz_t(Timestamp::GetTime(input), 0);
	return true;
}

//===--------------------------------------------------------------------===//
// Cast from Interval
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(interval_t input, interval_t &result, bool strict) {
	result = input;
	return true;
}

//===--------------------------------------------------------------------===//
// Non-Standard Timestamps
//===--------------------------------------------------------------------===//
template <>
duckdb::string_t CastFromTimestampNS::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(CastTimestampNsToUs::Operation<timestamp_t, timestamp_t>(input), result);
}
template <>
duckdb::string_t CastFromTimestampMS::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(CastTimestampMsToUs::Operation<timestamp_t, timestamp_t>(input), result);
}
template <>
duckdb::string_t CastFromTimestampSec::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(CastTimestampSecToUs::Operation<timestamp_t, timestamp_t>(input), result);
}

template <>
timestamp_t CastTimestampUsToMs::Operation(timestamp_t input) {
	if (!Timestamp::IsFinite(input)) {
		return input;
	}
	timestamp_t cast_timestamp(Timestamp::GetEpochMs(input));
	return cast_timestamp;
}

template <>
timestamp_t CastTimestampUsToNs::Operation(timestamp_t input) {
	if (!Timestamp::IsFinite(input)) {
		return input;
	}
	timestamp_t cast_timestamp(Timestamp::GetEpochNanoSeconds(input));
	return cast_timestamp;
}

template <>
timestamp_t CastTimestampUsToSec::Operation(timestamp_t input) {
	if (!Timestamp::IsFinite(input)) {
		return input;
	}
	timestamp_t cast_timestamp(Timestamp::GetEpochSeconds(input));
	return cast_timestamp;
}

template <>
timestamp_t CastTimestampMsToUs::Operation(timestamp_t input) {
	if (!Timestamp::IsFinite(input)) {
		return input;
	}
	return Timestamp::FromEpochMs(input.value);
}

template <>
date_t CastTimestampMsToDate::Operation(timestamp_t input) {
	return Timestamp::GetDate(Timestamp::FromEpochMs(input.value));
}

template <>
dtime_t CastTimestampMsToTime::Operation(timestamp_t input) {
	return Timestamp::GetTime(Timestamp::FromEpochMs(input.value));
}

template <>
timestamp_t CastTimestampMsToNs::Operation(timestamp_t input) {
	if (!Timestamp::IsFinite(input)) {
		return input;
	}
	auto us = CastTimestampMsToUs::Operation<timestamp_t, timestamp_t>(input);
	return CastTimestampUsToNs::Operation<timestamp_t, timestamp_t>(us);
}

template <>
timestamp_t CastTimestampNsToUs::Operation(timestamp_t input) {
	if (!Timestamp::IsFinite(input)) {
		return input;
	}
	return Timestamp::FromEpochNanoSeconds(input.value);
}

template <>
timestamp_t CastTimestampSecToUs::Operation(timestamp_t input) {
	if (!Timestamp::IsFinite(input)) {
		return input;
	}
	return Timestamp::FromEpochSeconds(input.value);
}

template <>
date_t CastTimestampNsToDate::Operation(timestamp_t input) {
	if (input == timestamp_t::infinity()) {
		return date_t::infinity();
	} else if (input == timestamp_t::ninfinity()) {
		return date_t::ninfinity();
	}
	const auto us = CastTimestampNsToUs::Operation<timestamp_t, timestamp_t>(input);
	return Timestamp::GetDate(us);
}

template <>
dtime_t CastTimestampNsToTime::Operation(timestamp_t input) {
	const auto us = CastTimestampNsToUs::Operation<timestamp_t, timestamp_t>(input);
	return Timestamp::GetTime(us);
}

template <>
timestamp_t CastTimestampSecToMs::Operation(timestamp_t input) {
	if (!Timestamp::IsFinite(input)) {
		return input;
	}
	auto us = CastTimestampSecToUs::Operation<timestamp_t, timestamp_t>(input);
	return CastTimestampUsToMs::Operation<timestamp_t, timestamp_t>(us);
}

template <>
timestamp_t CastTimestampSecToNs::Operation(timestamp_t input) {
	if (!Timestamp::IsFinite(input)) {
		return input;
	}
	auto us = CastTimestampSecToUs::Operation<timestamp_t, timestamp_t>(input);
	return CastTimestampUsToNs::Operation<timestamp_t, timestamp_t>(us);
}

template <>
date_t CastTimestampSecToDate::Operation(timestamp_t input) {
	const auto us = CastTimestampSecToUs::Operation<timestamp_t, timestamp_t>(input);
	return Timestamp::GetDate(us);
}

template <>
dtime_t CastTimestampSecToTime::Operation(timestamp_t input) {
	const auto us = CastTimestampSecToUs::Operation<timestamp_t, timestamp_t>(input);
	return Timestamp::GetTime(us);
}

//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <>
bool TryCastToTimestampNS::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	if (!Timestamp::IsFinite(result)) {
		return true;
	}

	int64_t nanoseconds;
	if (!Timestamp::TryGetEpochNanoSeconds(result, nanoseconds)) {
		throw ConversionException("Could not convert VARCHAR value '%s' to Timestamp(NS)", input.GetString());
	}
	result = nanoseconds;
	return true;
}

template <>
bool TryCastToTimestampMS::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	if (!Timestamp::IsFinite(result)) {
		return true;
	}
	result = Timestamp::GetEpochMs(result);
	return true;
}

template <>
bool TryCastToTimestampSec::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	if (!Timestamp::IsFinite(result)) {
		return true;
	}
	result = Timestamp::GetEpochSeconds(result);
	return true;
}

template <>
bool TryCastToTimestampNS::Operation(date_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<date_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	if (!Timestamp::IsFinite(result)) {
		return true;
	}
	if (!TryMultiplyOperator::Operation(result.value, Interval::NANOS_PER_MICRO, result.value)) {
		return false;
	}
	return true;
}

template <>
bool TryCastToTimestampMS::Operation(date_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<date_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	if (!Timestamp::IsFinite(result)) {
		return true;
	}
	result.value /= Interval::MICROS_PER_MSEC;
	return true;
}

template <>
bool TryCastToTimestampSec::Operation(date_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<date_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	if (!Timestamp::IsFinite(result)) {
		return true;
	}
	result.value /= Interval::MICROS_PER_MSEC * Interval::MSECS_PER_SEC;
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Blob
//===--------------------------------------------------------------------===//
template <>
string_t CastFromBlob::Operation(string_t input, Vector &vector) {
	idx_t result_size = Blob::GetStringSize(input);

	string_t result = StringVector::EmptyString(vector, result_size);
	Blob::ToString(input, result.GetDataWriteable());
	result.Finalize();

	return result;
}

template <>
string_t CastFromBlobToBit::Operation(string_t input, Vector &vector) {
	idx_t result_size = input.GetSize() + 1;
	if (result_size <= 1) {
		throw ConversionException("Cannot cast empty BLOB to BIT");
	}
	return StringVector::AddStringOrBlob(vector, Bit::BlobToBit(input));
}

//===--------------------------------------------------------------------===//
// Cast From Bit
//===--------------------------------------------------------------------===//
template <>
string_t CastFromBitToString::Operation(string_t input, Vector &vector) {

	idx_t result_size = Bit::BitLength(input);
	string_t result = StringVector::EmptyString(vector, result_size);
	Bit::ToString(input, result.GetDataWriteable());
	result.Finalize();

	return result;
}

//===--------------------------------------------------------------------===//
// Cast From Pointer
//===--------------------------------------------------------------------===//
template <>
string_t CastFromPointer::Operation(uintptr_t input, Vector &vector) {
	std::string s = duckdb_fmt::format("0x{:x}", input);
	return StringVector::AddString(vector, s);
}

//===--------------------------------------------------------------------===//
// Cast To Blob
//===--------------------------------------------------------------------===//
template <>
bool TryCastToBlob::Operation(string_t input, string_t &result, Vector &result_vector, CastParameters &parameters) {
	idx_t result_size;
	if (!Blob::TryGetBlobSize(input, result_size, parameters)) {
		return false;
	}

	result = StringVector::EmptyString(result_vector, result_size);
	Blob::ToBlob(input, data_ptr_cast(result.GetDataWriteable()));
	result.Finalize();
	return true;
}

//===--------------------------------------------------------------------===//
// Cast To Bit
//===--------------------------------------------------------------------===//
template <>
bool TryCastToBit::Operation(string_t input, string_t &result, Vector &result_vector, CastParameters &parameters) {
	idx_t result_size;
	if (!Bit::TryGetBitStringSize(input, result_size, parameters.error_message)) {
		return false;
	}

	result = StringVector::EmptyString(result_vector, result_size);
	Bit::ToBit(input, result);
	result.Finalize();
	return true;
}

template <>
bool CastFromBitToNumeric::Operation(string_t input, bool &result, CastParameters &parameters) {
	D_ASSERT(input.GetSize() > 1);

	uint8_t value;
	bool success = CastFromBitToNumeric::Operation(input, value, parameters);
	result = (value > 0);
	return (success);
}

template <>
bool CastFromBitToNumeric::Operation(string_t input, hugeint_t &result, CastParameters &parameters) {
	D_ASSERT(input.GetSize() > 1);

	if (input.GetSize() - 1 > sizeof(hugeint_t)) {
		throw ConversionException(parameters.query_location, "Bitstring doesn't fit inside of %s",
		                          GetTypeId<hugeint_t>());
	}
	Bit::BitToNumeric(input, result);
	return (true);
}

template <>
bool CastFromBitToNumeric::Operation(string_t input, uhugeint_t &result, CastParameters &parameters) {
	D_ASSERT(input.GetSize() > 1);

	if (input.GetSize() - 1 > sizeof(uhugeint_t)) {
		throw ConversionException(parameters.query_location, "Bitstring doesn't fit inside of %s",
		                          GetTypeId<uhugeint_t>());
	}
	Bit::BitToNumeric(input, result);
	return (true);
}

//===--------------------------------------------------------------------===//
// Cast From UUID
//===--------------------------------------------------------------------===//
template <>
string_t CastFromUUID::Operation(hugeint_t input, Vector &vector) {
	string_t result = StringVector::EmptyString(vector, 36);
	UUID::ToString(input, result.GetDataWriteable());
	result.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// Cast To UUID
//===--------------------------------------------------------------------===//
template <>
bool TryCastToUUID::Operation(string_t input, hugeint_t &result, Vector &result_vector, CastParameters &parameters) {
	return UUID::FromString(input.GetString(), result);
}

//===--------------------------------------------------------------------===//
// Cast To Date
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, date_t &result, CastParameters &parameters) {
	if (!TryCast::Operation<string_t, date_t>(input, result, parameters.strict)) {
		HandleCastError::AssignError(Date::ConversionError(input), parameters);
		return false;
	}
	return true;
}

template <>
bool TryCast::Operation(string_t input, date_t &result, bool strict) {
	idx_t pos;
	bool special = false;
	return Date::TryConvertDate(input.GetData(), input.GetSize(), pos, result, special, strict);
}

template <>
date_t Cast::Operation(string_t input) {
	return Date::FromCString(input.GetData(), input.GetSize());
}

//===--------------------------------------------------------------------===//
// Cast To Time
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, dtime_t &result, CastParameters &parameters) {
	if (!TryCast::Operation<string_t, dtime_t>(input, result, parameters.strict)) {
		HandleCastError::AssignError(Time::ConversionError(input), parameters);
		return false;
	}
	return true;
}

template <>
bool TryCast::Operation(string_t input, dtime_t &result, bool strict) {
	idx_t pos;
	return Time::TryConvertTime(input.GetData(), input.GetSize(), pos, result, strict);
}

template <>
dtime_t Cast::Operation(string_t input) {
	return Time::FromCString(input.GetData(), input.GetSize());
}

//===--------------------------------------------------------------------===//
// Cast To TimeTZ
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, dtime_tz_t &result, CastParameters &parameters) {
	if (!TryCast::Operation<string_t, dtime_tz_t>(input, result, parameters.strict)) {
		HandleCastError::AssignError(Time::ConversionError(input), parameters);
		return false;
	}
	return true;
}

template <>
bool TryCast::Operation(string_t input, dtime_tz_t &result, bool strict) {
	idx_t pos;
	return Time::TryConvertTimeTZ(input.GetData(), input.GetSize(), pos, result, strict);
}

template <>
dtime_tz_t Cast::Operation(string_t input) {
	dtime_tz_t result;
	if (!TryCast::Operation(input, result, false)) {
		throw ConversionException(Time::ConversionError(input));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, timestamp_t &result, CastParameters &parameters) {
	auto cast_result = Timestamp::TryConvertTimestamp(input.GetData(), input.GetSize(), result);
	if (cast_result == TimestampCastResult::SUCCESS) {
		return true;
	}
	if (cast_result == TimestampCastResult::ERROR_INCORRECT_FORMAT) {
		HandleCastError::AssignError(Timestamp::ConversionError(input), parameters);
	} else {
		HandleCastError::AssignError(Timestamp::UnsupportedTimezoneError(input), parameters);
	}
	return false;
}

template <>
bool TryCast::Operation(string_t input, timestamp_t &result, bool strict) {
	return Timestamp::TryConvertTimestamp(input.GetData(), input.GetSize(), result) == TimestampCastResult::SUCCESS;
}

template <>
timestamp_t Cast::Operation(string_t input) {
	return Timestamp::FromCString(input.GetData(), input.GetSize());
}

//===--------------------------------------------------------------------===//
// Cast From Interval
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, interval_t &result, CastParameters &parameters) {
	return Interval::FromCString(input.GetData(), input.GetSize(), result, parameters.error_message, parameters.strict);
}

//===--------------------------------------------------------------------===//
// Cast to hugeint / uhugeint
//===--------------------------------------------------------------------===//
// parsing hugeint from string is done a bit differently for performance reasons
// for other integer types we keep track of a single value
// and multiply that value by 10 for every digit we read
// however, for hugeints, multiplication is very expensive (>20X as expensive as for int64)
// for that reason, we parse numbers first into an int64 value
// when that value is full, we perform a HUGEINT multiplication to flush it into the hugeint
// this takes the number of HUGEINT multiplications down from [0-38] to [0-2]

template <typename T, typename OP>
struct HugeIntCastData {
	using ResultType = T;
	using Operation = OP;
	ResultType result;
	int64_t intermediate;
	uint8_t digits;

	ResultType decimal;
	uint16_t decimal_total_digits;
	int64_t decimal_intermediate;
	uint16_t decimal_intermediate_digits;

	bool Flush() {
		if (digits == 0 && intermediate == 0) {
			return true;
		}
		if (result.lower != 0 || result.upper != 0) {
			if (digits > 38) {
				return false;
			}
			if (!OP::TryMultiply(result, OP::POWERS_OF_TEN[digits], result)) {
				return false;
			}
		}
		if (!OP::TryAddInPlace(result, ResultType(intermediate))) {
			return false;
		}
		digits = 0;
		intermediate = 0;
		return true;
	}

	bool FlushDecimal() {
		if (decimal_intermediate_digits == 0 && decimal_intermediate == 0) {
			return true;
		}
		if (decimal.lower != 0 || decimal.upper != 0) {
			if (decimal_intermediate_digits > 38) {
				return false;
			}
			if (!OP::TryMultiply(decimal, OP::POWERS_OF_TEN[decimal_intermediate_digits], decimal)) {
				return false;
			}
		}
		if (!OP::TryAddInPlace(decimal, ResultType(decimal_intermediate))) {
			return false;
		}
		decimal_total_digits += decimal_intermediate_digits;
		decimal_intermediate_digits = 0;
		decimal_intermediate = 0;
		return true;
	}
};

struct HugeIntegerCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &state, uint8_t digit) {
		if (NEGATIVE) {
			if (DUCKDB_UNLIKELY(state.intermediate < (NumericLimits<int64_t>::Minimum() + digit) / 10)) {
				// intermediate is full: need to flush it
				if (!state.Flush()) {
					return false;
				}
			}
			state.intermediate = state.intermediate * 10 - digit;
		} else {
			if (DUCKDB_UNLIKELY(state.intermediate > (NumericLimits<int64_t>::Maximum() - digit) / 10)) {
				if (!state.Flush()) {
					return false;
				}
			}
			state.intermediate = state.intermediate * 10 + digit;
		}
		state.digits++;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleHexDigit(T &state, uint8_t digit) {
		return false;
	}

	template <class T, bool NEGATIVE>
	static bool HandleBinaryDigit(T &state, uint8_t digit) {
		return false;
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &state, int32_t exponent) {
		using result_t = typename T::ResultType;
		if (!state.Flush()) {
			return false;
		}

		int32_t e = exponent;
		if (e < -38) {
			state.result = 0;
			return true;
		}

		// Negative Exponent
		result_t remainder = 0;
		if (e < 0) {
			state.result = T::Operation::DivMod(state.result, T::Operation::POWERS_OF_TEN[-e], remainder);
			if (remainder < 0) {
				remainder *= -1;
			}
			state.decimal = remainder;
			state.decimal_total_digits = -e;
			state.decimal_intermediate = 0;
			state.decimal_intermediate_digits = 0;
			return Finalize<T, NEGATIVE>(state);
		}

		// Positive Exponent
		if (state.result != 0) {
			if (e > 38 || !TryMultiplyOperator::Operation(state.result, T::Operation::POWERS_OF_TEN[e], state.result)) {
				return false;
			}
		}
		if (!state.FlushDecimal()) {
			return false;
		}
		if (state.decimal == 0) {
			return Finalize<T, NEGATIVE>(state);
		}

		e = exponent - state.decimal_total_digits;
		if (e < 0) {
			state.decimal = T::Operation::DivMod(state.decimal, T::Operation::POWERS_OF_TEN[-e], remainder);
			state.decimal_total_digits -= (exponent);
		} else {
			if (e > 38 ||
			    !TryMultiplyOperator::Operation(state.decimal, T::Operation::POWERS_OF_TEN[e], state.decimal)) {
				return false;
			}
		}

		if (NEGATIVE) {
			if (!TrySubtractOperator::Operation(state.result, state.decimal, state.result)) {
				return false;
			}
		} else if (!TryAddOperator::Operation(state.result, state.decimal, state.result)) {
			return false;
		}
		state.decimal = remainder;
		return Finalize<T, NEGATIVE>(state);
	}

	template <class T, bool NEGATIVE, bool ALLOW_EXPONENT>
	static bool HandleDecimal(T &state, uint8_t digit) {
		if (!state.Flush()) {
			return false;
		}
		if (DUCKDB_UNLIKELY(state.decimal_intermediate > (NumericLimits<int64_t>::Maximum() - digit) / 10)) {
			if (!state.FlushDecimal()) {
				return false;
			}
		}
		state.decimal_intermediate = state.decimal_intermediate * 10 + digit;
		state.decimal_intermediate_digits++;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool Finalize(T &state) {
		using result_t = typename T::ResultType;
		if (!state.Flush() || !state.FlushDecimal()) {
			return false;
		}

		if (state.decimal == 0 || state.decimal_total_digits == 0) {
			return true;
		}

		// Get the first (left-most) digit of the decimals
		while (state.decimal_total_digits > 39) {
			state.decimal /= T::Operation::POWERS_OF_TEN[39];
			state.decimal_total_digits -= 39;
		}
		D_ASSERT((state.decimal_total_digits - 1) >= 0 && (state.decimal_total_digits - 1) <= 39);
		state.decimal /= T::Operation::POWERS_OF_TEN[state.decimal_total_digits - 1];

		if (state.decimal >= 5) {
			if (NEGATIVE) {
				return TrySubtractOperator::Operation(state.result, result_t(1), state.result);
			} else {
				return TryAddOperator::Operation(state.result, result_t(1), state.result);
			}
		}
		return true;
	}
};

template <>
bool TryCast::Operation(string_t input, hugeint_t &result, bool strict) {
	HugeIntCastData<hugeint_t, Hugeint> state {};
	if (!TryIntegerCast<HugeIntCastData<hugeint_t, Hugeint>, true, true, HugeIntegerCastOperation>(
	        input.GetData(), input.GetSize(), state, strict)) {
		return false;
	}
	result = state.result;
	return true;
}

template <>
bool TryCast::Operation(string_t input, uhugeint_t &result, bool strict) {
	HugeIntCastData<uhugeint_t, Uhugeint> state {};
	if (!TryIntegerCast<HugeIntCastData<uhugeint_t, Uhugeint>, false, true, HugeIntegerCastOperation>(
	        input.GetData(), input.GetSize(), state, strict)) {
		return false;
	}
	result = state.result;
	return true;
}

//===--------------------------------------------------------------------===//
// Decimal String Cast
//===--------------------------------------------------------------------===//

template <class T>
struct DecimalCastData {
	using StoreType = T;
	StoreType result;
	uint8_t width;
	uint8_t scale;
	uint8_t digit_count;
	uint8_t decimal_count;
	//! Whether we have determined if the result should be rounded
	bool round_set;
	//! If the result should be rounded
	bool should_round;
	//! Only set when ALLOW_EXPONENT is enabled
	enum class ExponentType : uint8_t { NONE, POSITIVE, NEGATIVE };
	uint8_t excessive_decimals;
	ExponentType exponent_type;
};

struct DecimalCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &state, uint8_t digit) {
		if (state.result == 0 && digit == 0) {
			// leading zero's don't count towards the digit count
			return true;
		}
		if (state.digit_count == state.width - state.scale) {
			// width of decimal type is exceeded!
			return false;
		}
		state.digit_count++;
		if (NEGATIVE) {
			if (state.result < (NumericLimits<typename T::StoreType>::Minimum() / 10)) {
				return false;
			}
			state.result = state.result * 10 - digit;
		} else {
			if (state.result > (NumericLimits<typename T::StoreType>::Maximum() / 10)) {
				return false;
			}
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleHexDigit(T &state, uint8_t digit) {
		return false;
	}

	template <class T, bool NEGATIVE>
	static bool HandleBinaryDigit(T &state, uint8_t digit) {
		return false;
	}

	template <class T, bool NEGATIVE>
	static void RoundUpResult(T &state) {
		if (NEGATIVE) {
			state.result -= 1;
		} else {
			state.result += 1;
		}
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &state, int32_t exponent) {
		auto decimal_excess = (state.decimal_count > state.scale) ? state.decimal_count - state.scale : 0;
		if (exponent > 0) {
			state.exponent_type = T::ExponentType::POSITIVE;
			// Positive exponents need up to 'exponent' amount of digits
			// Everything beyond that amount needs to be truncated
			if (decimal_excess > exponent) {
				// We've allowed too many decimals
				state.excessive_decimals = decimal_excess - exponent;
				exponent = 0;
			} else {
				exponent -= decimal_excess;
			}
			D_ASSERT(exponent >= 0);
		} else if (exponent < 0) {
			state.exponent_type = T::ExponentType::NEGATIVE;
		}
		if (!Finalize<T, NEGATIVE>(state)) {
			return false;
		}
		if (exponent < 0) {
			bool round_up = false;
			for (idx_t i = 0; i < idx_t(-int64_t(exponent)); i++) {
				auto mod = state.result % 10;
				round_up = NEGATIVE ? mod <= -5 : mod >= 5;
				state.result /= 10;
				if (state.result == 0) {
					break;
				}
			}
			if (round_up) {
				RoundUpResult<T, NEGATIVE>(state);
			}
			return true;
		} else {
			// positive exponent: append 0's
			for (idx_t i = 0; i < idx_t(exponent); i++) {
				if (!HandleDigit<T, NEGATIVE>(state, 0)) {
					return false;
				}
			}
			return true;
		}
	}

	template <class T, bool NEGATIVE, bool ALLOW_EXPONENT>
	static bool HandleDecimal(T &state, uint8_t digit) {
		if (state.decimal_count == state.scale && !state.round_set) {
			// Determine whether the last registered decimal should be rounded or not
			state.round_set = true;
			state.should_round = digit >= 5;
		}
		if (!ALLOW_EXPONENT && state.decimal_count == state.scale) {
			// we exceeded the amount of supported decimals
			// however, we don't throw an error here
			// we just truncate the decimal
			return true;
		}
		//! If we expect an exponent, we need to preserve the decimals
		//! But we don't want to overflow, so we prevent overflowing the result with this check
		if (state.digit_count + state.decimal_count >= DecimalWidth<decltype(state.result)>::max) {
			return true;
		}
		state.decimal_count++;
		if (NEGATIVE) {
			state.result = state.result * 10 - digit;
		} else {
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool TruncateExcessiveDecimals(T &state) {
		D_ASSERT(state.excessive_decimals);
		bool round_up = false;
		for (idx_t i = 0; i < state.excessive_decimals; i++) {
			auto mod = state.result % 10;
			round_up = NEGATIVE ? mod <= -5 : mod >= 5;
			state.result /= 10.0;
		}
		//! Only round up when exponents are involved
		if (state.exponent_type == T::ExponentType::POSITIVE && round_up) {
			RoundUpResult<T, NEGATIVE>(state);
		}
		D_ASSERT(state.decimal_count > state.scale);
		state.decimal_count = state.scale;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool Finalize(T &state) {
		if (state.exponent_type != T::ExponentType::POSITIVE && state.decimal_count > state.scale) {
			//! Did not encounter an exponent, but ALLOW_EXPONENT was on
			state.excessive_decimals = state.decimal_count - state.scale;
		}
		if (state.excessive_decimals && !TruncateExcessiveDecimals<T, NEGATIVE>(state)) {
			return false;
		}
		if (state.exponent_type == T::ExponentType::NONE && state.round_set && state.should_round) {
			RoundUpResult<T, NEGATIVE>(state);
		}
		//  if we have not gotten exactly "scale" decimals, we need to multiply the result
		//  e.g. if we have a string "1.0" that is cast to a DECIMAL(9,3), the value needs to be 1000
		//  but we have only gotten the value "10" so far, so we multiply by 1000
		for (uint8_t i = state.decimal_count; i < state.scale; i++) {
			state.result *= 10;
		}
		return true;
	}
};

template <class T, char decimal_separator = '.'>
bool TryDecimalStringCast(string_t input, T &result, CastParameters &parameters, uint8_t width, uint8_t scale) {
	DecimalCastData<T> state;
	state.result = 0;
	state.width = width;
	state.scale = scale;
	state.digit_count = 0;
	state.decimal_count = 0;
	state.excessive_decimals = 0;
	state.exponent_type = DecimalCastData<T>::ExponentType::NONE;
	state.round_set = false;
	state.should_round = false;
	if (!TryIntegerCast<DecimalCastData<T>, true, true, DecimalCastOperation, false, decimal_separator>(
	        input.GetData(), input.GetSize(), state, false)) {
		string error = StringUtil::Format("Could not convert string \"%s\" to DECIMAL(%d,%d)", input.GetString(),
		                                  (int)width, (int)scale);
		HandleCastError::AssignError(error, parameters);
		return false;
	}
	result = state.result;
	return true;
}

template <>
bool TryCastToDecimal::Operation(string_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return TryDecimalStringCast<int16_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return TryDecimalStringCast<int32_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return TryDecimalStringCast<int64_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return TryDecimalStringCast<hugeint_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimalCommaSeparated::Operation(string_t input, int16_t &result, CastParameters &parameters,
                                               uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int16_t, ','>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimalCommaSeparated::Operation(string_t input, int32_t &result, CastParameters &parameters,
                                               uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int32_t, ','>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimalCommaSeparated::Operation(string_t input, int64_t &result, CastParameters &parameters,
                                               uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int64_t, ','>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimalCommaSeparated::Operation(string_t input, hugeint_t &result, CastParameters &parameters,
                                               uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<hugeint_t, ','>(input, result, parameters, width, scale);
}

template <>
string_t StringCastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int16_t, uint16_t>(input, width, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int32_t, uint32_t>(input, width, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int64_t, uint64_t>(input, width, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale, Vector &result) {
	return HugeintToStringCast::FormatDecimal(input, width, scale, result);
}

//===--------------------------------------------------------------------===//
// Decimal Casts
//===--------------------------------------------------------------------===//
// Decimal <-> Bool
//===--------------------------------------------------------------------===//
template <class T, class OP = NumericHelper>
bool TryCastBoolToDecimal(bool input, T &result, CastParameters &parameters, uint8_t width, uint8_t scale) {
	if (width > scale) {
		result = input ? OP::POWERS_OF_TEN[scale] : 0;
		return true;
	} else {
		return TryCast::Operation<bool, T>(input, result);
	}
}

template <>
bool TryCastToDecimal::Operation(bool input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return TryCastBoolToDecimal<int16_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(bool input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return TryCastBoolToDecimal<int32_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(bool input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return TryCastBoolToDecimal<int64_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(bool input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return TryCastBoolToDecimal<hugeint_t, Hugeint>(input, result, parameters, width, scale);
}

template <>
bool TryCastFromDecimal::Operation(int16_t input, bool &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCast::Operation<int16_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, bool &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCast::Operation<int32_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, bool &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCast::Operation<int64_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, bool &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCast::Operation<hugeint_t, bool>(input, result);
}

//===--------------------------------------------------------------------===//
// Numeric -> Decimal Cast
//===--------------------------------------------------------------------===//
struct SignedToDecimalOperator {
	template <class SRC, class DST>
	static bool Operation(SRC input, DST max_width) {
		return int64_t(input) >= int64_t(max_width) || int64_t(input) <= int64_t(-max_width);
	}
};

struct UnsignedToDecimalOperator {
	template <class SRC, class DST>
	static bool Operation(SRC input, DST max_width) {
		return uint64_t(input) >= uint64_t(max_width);
	}
};

template <class SRC, class DST, class OP = SignedToDecimalOperator>
bool StandardNumericToDecimalCast(SRC input, DST &result, CastParameters &parameters, uint8_t width, uint8_t scale) {
	// check for overflow
	DST max_width = NumericHelper::POWERS_OF_TEN[width - scale];
	if (OP::template Operation<SRC, DST>(input, max_width)) {
		string error = StringUtil::Format("Could not cast value %d to DECIMAL(%d,%d)", input, width, scale);
		HandleCastError::AssignError(error, parameters);
		return false;
	}
	result = DST(input) * NumericHelper::POWERS_OF_TEN[scale];
	return true;
}

template <class SRC>
bool NumericToHugeDecimalCast(SRC input, hugeint_t &result, CastParameters &parameters, uint8_t width, uint8_t scale) {
	// check for overflow
	hugeint_t max_width = Hugeint::POWERS_OF_TEN[width - scale];
	hugeint_t hinput = Hugeint::Convert(input);
	if (hinput >= max_width || hinput <= -max_width) {
		string error = StringUtil::Format("Could not cast value %s to DECIMAL(%d,%d)", hinput.ToString(), width, scale);
		HandleCastError::AssignError(error, parameters);
		return false;
	}
	result = hinput * Hugeint::POWERS_OF_TEN[scale];
	return true;
}

//===--------------------------------------------------------------------===//
// Cast int8_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int8_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int16_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int8_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int32_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int8_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int64_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int8_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<int8_t>(input, result, parameters, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast int16_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int16_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int16_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int16_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int32_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int16_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int64_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int16_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<int16_t>(input, result, parameters, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast int32_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int32_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int16_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int32_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int32_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int32_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int64_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int32_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<int32_t>(input, result, parameters, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast int64_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int64_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int16_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int64_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int32_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int64_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int64_t>(input, result, parameters, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int64_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<int64_t>(input, result, parameters, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint8_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint8_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int16_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                 scale);
}
template <>
bool TryCastToDecimal::Operation(uint8_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int32_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                 scale);
}
template <>
bool TryCastToDecimal::Operation(uint8_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int64_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                 scale);
}
template <>
bool TryCastToDecimal::Operation(uint8_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint8_t>(input, result, parameters, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint16_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint16_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int16_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                  scale);
}
template <>
bool TryCastToDecimal::Operation(uint16_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int32_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                  scale);
}
template <>
bool TryCastToDecimal::Operation(uint16_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int64_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                  scale);
}
template <>
bool TryCastToDecimal::Operation(uint16_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint16_t>(input, result, parameters, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint32_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint32_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int16_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                  scale);
}
template <>
bool TryCastToDecimal::Operation(uint32_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int32_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                  scale);
}
template <>
bool TryCastToDecimal::Operation(uint32_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int64_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                  scale);
}
template <>
bool TryCastToDecimal::Operation(uint32_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint32_t>(input, result, parameters, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint64_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint64_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int16_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                  scale);
}
template <>
bool TryCastToDecimal::Operation(uint64_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int32_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                  scale);
}
template <>
bool TryCastToDecimal::Operation(uint64_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int64_t, UnsignedToDecimalOperator>(input, result, parameters, width,
	                                                                                  scale);
}
template <>
bool TryCastToDecimal::Operation(uint64_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint64_t>(input, result, parameters, width, scale);
}

//===--------------------------------------------------------------------===//
// Hugeint -> Decimal Cast
//===--------------------------------------------------------------------===//
template <class DST>
bool HugeintToDecimalCast(hugeint_t input, DST &result, CastParameters &parameters, uint8_t width, uint8_t scale) {
	// check for overflow
	hugeint_t max_width = Hugeint::POWERS_OF_TEN[width - scale];
	if (input >= max_width || input <= -max_width) {
		string error = StringUtil::Format("Could not cast value %s to DECIMAL(%d,%d)", input.ToString(), width, scale);
		HandleCastError::AssignError(error, parameters);
		return false;
	}
	result = Hugeint::Cast<DST>(input * Hugeint::POWERS_OF_TEN[scale]);
	return true;
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<int16_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<int32_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<int64_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<hugeint_t>(input, result, parameters, width, scale);
}

//===--------------------------------------------------------------------===//
// Uhugeint -> Decimal Cast
//===--------------------------------------------------------------------===//
template <class DST>
bool UhugeintToDecimalCast(uhugeint_t input, DST &result, CastParameters &parameters, uint8_t width, uint8_t scale) {
	// check for overflow
	uhugeint_t max_width = Uhugeint::POWERS_OF_TEN[width - scale];
	if (input >= max_width) {
		string error = StringUtil::Format("Could not cast value %s to DECIMAL(%d,%d)", input.ToString(), width, scale);
		HandleCastError::AssignError(error, parameters);
		return false;
	}
	result = Uhugeint::Cast<DST>(input * Uhugeint::POWERS_OF_TEN[scale]);
	return true;
}

template <>
bool TryCastToDecimal::Operation(uhugeint_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return UhugeintToDecimalCast<int16_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(uhugeint_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return UhugeintToDecimalCast<int32_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(uhugeint_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return UhugeintToDecimalCast<int64_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(uhugeint_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return UhugeintToDecimalCast<hugeint_t>(input, result, parameters, width, scale);
}

//===--------------------------------------------------------------------===//
// Float/Double -> Decimal Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool DoubleToDecimalCast(SRC input, DST &result, CastParameters &parameters, uint8_t width, uint8_t scale) {
	double value = input * NumericHelper::DOUBLE_POWERS_OF_TEN[scale];
	// Add the sign (-1, 0, 1) times a tiny value to fix floating point issues (issue 3091)
	double sign = (double(0) < value) - (value < double(0));
	value += 1e-9 * sign;
	if (value <= -NumericHelper::DOUBLE_POWERS_OF_TEN[width] || value >= NumericHelper::DOUBLE_POWERS_OF_TEN[width]) {
		string error = StringUtil::Format("Could not cast value %f to DECIMAL(%d,%d)", value, width, scale);
		HandleCastError::AssignError(error, parameters);
		return false;
	}
	result = Cast::Operation<SRC, DST>(value);
	return true;
}

template <>
bool TryCastToDecimal::Operation(float input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return DoubleToDecimalCast<float, int16_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return DoubleToDecimalCast<float, int32_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return DoubleToDecimalCast<float, int64_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return DoubleToDecimalCast<float, hugeint_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int16_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return DoubleToDecimalCast<double, int16_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int32_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return DoubleToDecimalCast<double, int32_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int64_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return DoubleToDecimalCast<double, int64_t>(input, result, parameters, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                 uint8_t scale) {
	return DoubleToDecimalCast<double, hugeint_t>(input, result, parameters, width, scale);
}

//===--------------------------------------------------------------------===//
// Decimal -> Numeric Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool TryCastDecimalToNumeric(SRC input, DST &result, CastParameters &parameters, uint8_t scale) {
	// Round away from 0.
	const auto power = NumericHelper::POWERS_OF_TEN[scale];
	// https://graphics.stanford.edu/~seander/bithacks.html#ConditionalNegate
	const auto fNegate = int64_t(input < 0);
	const auto rounding = ((power ^ -fNegate) + fNegate) / 2;
	const auto scaled_value = (input + rounding) / power;
	if (!TryCast::Operation<SRC, DST>(scaled_value, result)) {
		string error = StringUtil::Format("Failed to cast decimal value %d to type %s", scaled_value, GetTypeId<DST>());
		HandleCastError::AssignError(error, parameters);
		return false;
	}
	return true;
}

template <class DST>
bool TryCastHugeDecimalToNumeric(hugeint_t input, DST &result, CastParameters &parameters, uint8_t scale) {
	const auto power = Hugeint::POWERS_OF_TEN[scale];
	const auto rounding = ((input < 0) ? -power : power) / 2;
	auto scaled_value = (input + rounding) / power;
	if (!TryCast::Operation<hugeint_t, DST>(scaled_value, result)) {
		string error = StringUtil::Format("Failed to cast decimal value %s to type %s",
		                                  ConvertToString::Operation(scaled_value), GetTypeId<DST>());
		HandleCastError::AssignError(error, parameters);
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int8_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int8_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int8_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int8_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int8_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int8_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int8_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int8_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int8_t>(input, result, parameters, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int16_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int16_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int16_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int16_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int16_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int16_t>(input, result, parameters, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int32_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int32_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int32_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int32_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int32_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int32_t>(input, result, parameters, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int64_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int64_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int64_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int64_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int64_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int64_t>(input, result, parameters, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint8_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint8_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint8_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint8_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint8_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint8_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint8_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint8_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint8_t>(input, result, parameters, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint16_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint16_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint16_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint16_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint16_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint16_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint16_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint16_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint16_t>(input, result, parameters, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint32_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint32_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint32_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint32_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint32_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint32_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint32_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint32_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint32_t>(input, result, parameters, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint64_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint64_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint64_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint64_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint64_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint64_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint64_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint64_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint64_t>(input, result, parameters, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> hugeint_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, hugeint_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, hugeint_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, hugeint_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, hugeint_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<hugeint_t>(input, result, parameters, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uhugeint_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uhugeint_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uhugeint_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uhugeint_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uhugeint_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uhugeint_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uhugeint_t>(input, result, parameters, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uhugeint_t &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uhugeint_t>(input, result, parameters, scale);
}

//===--------------------------------------------------------------------===//
// Decimal -> Float/Double Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool TryCastDecimalToFloatingPoint(SRC input, DST &result, uint8_t scale) {
	result = Cast::Operation<SRC, DST>(input) / DST(NumericHelper::DOUBLE_POWERS_OF_TEN[scale]);
	return true;
}

// DECIMAL -> FLOAT
template <>
bool TryCastFromDecimal::Operation(int16_t input, float &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int16_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, float &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int32_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, float &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int64_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, float &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<hugeint_t, float>(input, result, scale);
}

// DECIMAL -> DOUBLE
template <>
bool TryCastFromDecimal::Operation(int16_t input, double &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int16_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, double &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int32_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, double &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int64_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, double &result, CastParameters &parameters, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<hugeint_t, double>(input, result, scale);
}

} // namespace duckdb
