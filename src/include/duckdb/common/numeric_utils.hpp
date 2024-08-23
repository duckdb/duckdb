//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/numeric_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <type_traits>

#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

template <class T>
struct MakeSigned {
	using type = typename std::make_signed<T>::type;
};

template <>
struct MakeSigned<hugeint_t> {
	using type = hugeint_t;
};

template <>
struct MakeSigned<uhugeint_t> {
	using type = hugeint_t;
};

template <class T>
struct MakeUnsigned {
	using type = typename std::make_unsigned<T>::type;
};

template <>
struct MakeUnsigned<hugeint_t> {
	using type = uhugeint_t;
};

template <>
struct MakeUnsigned<uhugeint_t> {
	using type = uhugeint_t;
};

template <class TO, class FROM>
static void ThrowNumericCastError(FROM in, TO minval, TO maxval) {
	throw InternalException("Information loss on integer cast: value %d outside of target range [%d, %d]", in, minval,
	                        maxval);
}

template <class TO, class FROM, bool are_same_type>
struct NumericCastImpl;

template <class TO, class FROM>
struct NumericCastImpl<TO, FROM, true> {
	static TO Convert(FROM val) {
		return static_cast<TO>(val);
	}
};

template <class TO, class FROM>
struct NumericCastImpl<TO, FROM, false> {
	static TO Convert(FROM val) {
		// some dance around signed-unsigned integer comparison below
		auto minval = NumericLimits<TO>::Minimum();
		auto maxval = NumericLimits<TO>::Maximum();
		auto unsigned_in = static_cast<typename MakeUnsigned<FROM>::type>(val);
		auto unsigned_min = static_cast<typename MakeUnsigned<TO>::type>(minval);
		auto unsigned_max = static_cast<typename MakeUnsigned<TO>::type>(maxval);
		auto signed_in = static_cast<typename MakeSigned<FROM>::type>(val);
		auto signed_min = static_cast<typename MakeSigned<TO>::type>(minval);
		auto signed_max = static_cast<typename MakeSigned<TO>::type>(maxval);

		if (!NumericLimits<FROM>::IsSigned() && !NumericLimits<TO>::IsSigned() &&
		    (unsigned_in < unsigned_min || unsigned_in > unsigned_max)) {
			ThrowNumericCastError(val, minval, maxval);
		}

		if (NumericLimits<FROM>::IsSigned() && NumericLimits<TO>::IsSigned() &&
		    (signed_in < signed_min || signed_in > signed_max)) {
			ThrowNumericCastError(val, minval, maxval);
		}

		if (NumericLimits<FROM>::IsSigned() != NumericLimits<TO>::IsSigned() &&
		    (signed_in < signed_min || unsigned_in > unsigned_max)) {
			ThrowNumericCastError(val, minval, maxval);
		}

		return static_cast<TO>(val);
	}
};

// NumericCast
// When: between same types, or when both types are integral
// Checks: perform checked casts on range
template <class TO, class FROM,
          class = typename std::enable_if<(NumericLimits<TO>::IsIntegral() && NumericLimits<FROM>::IsIntegral()) ||
                                          std::is_same<TO, FROM>::value>::type>
TO NumericCast(FROM val) {
	return NumericCastImpl<TO, FROM, std::is_same<TO, FROM>::value>::Convert(val);
}

// UnsafeNumericCast
// When: between same types, or when both types are integral
// Checks: perform checked casts on range (in DEBUG) otherwise no checks
template <class TO, class FROM,
          class = typename std::enable_if<(NumericLimits<TO>::IsIntegral() && NumericLimits<FROM>::IsIntegral()) ||
                                          std::is_same<TO, FROM>::value>::type>
TO UnsafeNumericCast(FROM in) {
#if defined(DEBUG) || defined(UNSAFE_NUMERIC_CAST)
	return NumericCast<TO, FROM>(in);
#endif
	return static_cast<TO>(in);
}

// LossyNumericCast
// When: between double/float to other convertible types
// Checks: no checks performed (at the moment, to be improved adding range checks)
template <class TO>
TO LossyNumericCast(double val) {
	return static_cast<TO>(val);
}

template <class TO>
TO LossyNumericCast(float val) {
	return static_cast<TO>(val);
}

// ExactNumericCast
// When: between double/float to other convertible types
// Checks: perform checks that casts are invertible (in DEBUG) otherwise no checks

template <class TO>
TO ExactNumericCast(double val) {
	auto res = LossyNumericCast<TO>(val);
#if defined(DEBUG) || defined(UNSAFE_NUMERIC_CAST)
	if (val != double(res)) {
		throw InternalException("Information loss on double cast: value %lf outside of target range [%lf, %lf]", val,
		                        double(res), double(res));
	}
#endif
	return res;
}

template <class TO>
TO ExactNumericCast(float val) {
	auto res = LossyNumericCast<TO>(val);
#if defined(DEBUG) || defined(UNSAFE_NUMERIC_CAST)
	if (val != float(res)) {
		throw InternalException("Information loss on float cast: value %f outside of target range [%f, %f]", val,
		                        float(res), float(res));
	}
#endif
	return res;
}

} // namespace duckdb
