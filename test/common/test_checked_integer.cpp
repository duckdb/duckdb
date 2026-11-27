#include "catch.hpp"
#include "duckdb/common/checked_integer.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/type_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "test_exception_matcher.hpp"

using namespace duckdb; // NOLINT

namespace {
template <typename T>
using ci = CheckedInteger<T, OutOfRangeException>;

using ci8 = ci<int8_t>;
using cu8 = ci<uint8_t>;
using ci16 = ci<int16_t>;
using cu16 = ci<uint16_t>;
using ci32 = ci<int32_t>;
using cu32 = ci<uint32_t>;
using ci64 = ci<int64_t>;
using cu64 = ci<uint64_t>;
} // namespace

TEST_CASE("CheckedInteger default initialization is zero", "[checked_integer]") {
	SECTION("non-atomic CheckedInteger default ctor zero-initializes") {
		ci64 a;
		REQUIRE(a == 0);
	}

	SECTION("std::atomic<CheckedInteger> default ctor zero-initializes") {
		std::atomic<ci64> a;
		REQUIRE(a == 0);
	}
}

TEST_CASE("Checked integer increment/decrement overflow", "[checked_integer]") {
	SECTION("overflow at maximum (pre and post forms)") {
		ci8 s(NumericLimits<int8_t>::Maximum());
		REQUIRE_THROWS_WITH(++s, Catch::Contains("Overflow in increment of CheckedInteger"));
		REQUIRE_THROWS_WITH(s++, Catch::Contains("Overflow in increment of CheckedInteger"));

		cu8 u(NumericLimits<uint8_t>::Maximum());
		REQUIRE_THROWS_WITH(++u, Catch::Contains("Overflow in increment of CheckedInteger"));
		REQUIRE_THROWS_WITH(u++, Catch::Contains("Overflow in increment of CheckedInteger"));
	}

	SECTION("underflow at minimum (pre and post forms)") {
		ci8 s(NumericLimits<int8_t>::Minimum());
		REQUIRE_THROWS_WITH(--s, Catch::Contains("Underflow in decrement of CheckedInteger"));
		REQUIRE_THROWS_WITH(s--, Catch::Contains("Underflow in decrement of CheckedInteger"));

		cu8 u(0);
		REQUIRE_THROWS_WITH(--u, Catch::Contains("Underflow in decrement of CheckedInteger"));
		REQUIRE_THROWS_WITH(u--, Catch::Contains("Underflow in decrement of CheckedInteger"));
	}

	SECTION("happy path") {
		ci32 val(100);
		++val;
		REQUIRE(val == 101);
		val--;
		REQUIRE(val == 100);
	}
}

TEST_CASE("Checked integer comparisons", "[checked_integer]") {
	SECTION("strict ordering between two CheckedIntegers") {
		ci64 a(100);
		ci64 b(200);

		REQUIRE(a < b);
		REQUIRE_FALSE(b < a);
		REQUIRE_FALSE(a < a);

		REQUIRE(b > a);
		REQUIRE_FALSE(a > b);
		REQUIRE_FALSE(a > a);

		REQUIRE(a <= b);
		REQUIRE(a <= a);
		REQUIRE_FALSE(b <= a);

		REQUIRE(b >= a);
		REQUIRE(a >= a);
		REQUIRE_FALSE(a >= b);
	}

	SECTION("equality / inequality") {
		ci64 a(100);
		ci64 b(200);
		ci64 c(100);

		REQUIRE(a == c);
		REQUIRE_FALSE(a == b);
		REQUIRE(a != b);
		REQUIRE_FALSE(a != c);
	}
}

TEST_CASE("CheckedInteger narrow constructor overflow", "[checked_integer]") {
	SECTION("same-sign widening always passes") {
		REQUIRE(ci64(int8_t(-128)).GetValue() == -128);
		REQUIRE(ci64(int8_t(127)).GetValue() == 127);
		REQUIRE(cu64(uint8_t(255)).GetValue() == 255u);
	}

	SECTION("narrowing same-sign throws on overflow / underflow") {
		// signed -> smaller signed
		REQUIRE_THROWS_WITH(ci8(1000), Catch::Contains("out of range for CheckedInteger"));
		REQUIRE_THROWS_WITH(ci8(-1000), Catch::Contains("out of range for CheckedInteger"));
		REQUIRE_THROWS_WITH(ci32(int64_t(NumericLimits<int32_t>::Maximum()) + 1),
		                    Catch::Contains("out of range for CheckedInteger"));

		// unsigned -> smaller unsigned
		REQUIRE_THROWS_WITH(cu8(256), Catch::Contains("out of range for CheckedInteger"));
		REQUIRE_THROWS_WITH(cu32(uint64_t(1) << 32), Catch::Contains("out of range for CheckedInteger"));
	}

	SECTION("signed -> unsigned rejects negatives and too-large positives") {
		REQUIRE_THROWS_WITH(cu8(int8_t(-1)), Catch::Contains("out of range for CheckedInteger"));
		REQUIRE_THROWS_WITH(cu32(int32_t(-1)), Catch::Contains("out of range for CheckedInteger"));
		REQUIRE_THROWS_WITH(cu64(int64_t(-1)), Catch::Contains("out of range for CheckedInteger"));
		REQUIRE_THROWS_WITH(cu8(int16_t(300)), Catch::Contains("out of range for CheckedInteger"));
	}

	SECTION("bool maps to 0/1") {
		REQUIRE(ci8(true).GetValue() == 1);
		REQUIRE(cu32(false).GetValue() == 0u);
	}
}

TEST_CASE("CheckedInteger cross-type arithmetic", "[checked_integer]") {
	const uint64_t U64_MAX = NumericLimits<uint64_t>::Maximum();
	const int64_t I64_MAX = NumericLimits<int64_t>::Maximum();
	const int64_t I64_MIN = NumericLimits<int64_t>::Minimum();

	SECTION("happy path matches native") {
		// binary: one case per op, hitting different sign/width pairings
		cu16 a(100);
		auto r1 = a + int8_t(-30); // unsigned + negative-signed
		REQUIRE(r1.GetValue() == 70u);

		ci64 b(10);
		auto r2 = b - uint32_t(30); // signed - unsigned
		REQUIRE(r2.GetValue() == -20);

		cu8 c(10);
		auto r3 = c * int32_t(20); // narrow-unsigned * wider-signed
		REQUIRE(r3.GetValue() == 200u);

		cu64 d(1000);
		auto r4 = d / int32_t(3); // unsigned / signed
		REQUIRE(r4.GetValue() == 333u);

		// compound: one case per op
		cu32 e(100);
		e += int8_t(-40);
		REQUIRE(e.GetValue() == 60u);

		ci64 f(100);
		f -= uint32_t(250);
		REQUIRE(f.GetValue() == -150);

		ci16 g(-7);
		g *= uint8_t(6);
		REQUIRE(g.GetValue() == -42);

		ci32 h(-100);
		h /= uint8_t(10);
		REQUIRE(h.GetValue() == -10);
	}

	SECTION("overflow / underflow detection") {
		// addition overflow
		cu8 a(250);
		REQUIRE_THROWS_WITH(a + int32_t(10), Catch::Contains("Overflow in addition for CheckedInteger"));
		REQUIRE_THROWS_WITH(a += int32_t(100), Catch::Contains("Overflow in addition for CheckedInteger"));

		// subtraction underflow
		cu16 b(5);
		REQUIRE_THROWS_WITH(b + int32_t(-10), Catch::Contains("Overflow in addition for CheckedInteger"));

		// signed -= unsigned underflow
		ci16 c(-30000);
		REQUIRE_THROWS_WITH(c -= uint16_t(5000), Catch::Contains("Underflow in subtraction for CheckedInteger"));

		// multiplication overflow
		ci16 d(200);
		REQUIRE_THROWS_WITH(d * uint16_t(200), Catch::Contains("Overflow in multiplication for CheckedInteger"));
	}

	SECTION("division by zero and T_MIN / -1 overflow") {
		// cross-type division by zero
		cu32 a(100);
		REQUIRE_THROWS_WITH(a / int16_t(0), Catch::Contains("Division by zero in CheckedInteger"));

		// T_MIN / -1 = |T_MIN| does not fit back in T
		ci8 b(NumericLimits<int8_t>::Minimum());
		REQUIRE_THROWS_WITH(b /= int64_t(-1), Catch::Contains("Overflow in division for CheckedInteger"));

		// At T_MIN, dividing by 2 still works and preserves sign
		ci8 d(NumericLimits<int8_t>::Minimum());
		d /= int64_t(2);
		REQUIRE(d == NumericLimits<int8_t>::Minimum() / 2);
	}

	SECTION("cross-type multiplication at the hugeint_t boundary") {
		REQUIRE_THROWS_WITH(ci64(I64_MAX) * U64_MAX, Catch::Contains("Overflow in multiplication for CheckedInteger"));
		REQUIRE_THROWS_WITH(ci64(I64_MIN) * U64_MAX, Catch::Contains("Overflow in multiplication for CheckedInteger"));
		REQUIRE_THROWS_WITH(ci64(-1) * U64_MAX, Catch::Contains("Overflow in multiplication for CheckedInteger"));
		ci64 a(I64_MIN);
		REQUIRE_THROWS_WITH(a *= uint64_t(U64_MAX), Catch::Contains("Overflow in multiplication for CheckedInteger"));
	}

	SECTION("int64 / uint64 wide-path correctness") {
		// i64 += uint64: overflow + just-fits boundary
		ci64 a(I64_MAX);
		REQUIRE_THROWS_WITH(a += uint64_t(1), Catch::Contains("Overflow in addition for CheckedInteger"));
		REQUIRE_THROWS_WITH(ci64(0) + U64_MAX, Catch::Contains("Overflow in addition for CheckedInteger"));
		ci64 b(0);
		b += uint64_t(I64_MAX);
		REQUIRE(b == I64_MAX);

		// i64 -= uint64: underflow + just-fits boundary
		ci64 c(I64_MIN);
		REQUIRE_THROWS_WITH(c -= uint64_t(1), Catch::Contains("Underflow in subtraction for CheckedInteger"));
		REQUIRE_THROWS_WITH(ci64(-1) - U64_MAX, Catch::Contains("Underflow in subtraction for CheckedInteger"));
		ci64 d(0);
		d -= uint64_t(I64_MAX);
		REQUIRE(d == -I64_MAX);

		// i64 *= uint64: overflow on both signs
		ci64 e(2);
		REQUIRE_THROWS_WITH(e *= U64_MAX, Catch::Contains("Overflow in multiplication for CheckedInteger"));

		// i64 /= uint64: sign preserved
		ci64 g(-100);
		g /= uint64_t(2);
		REQUIRE(g == -50);

		ci64 h(-100);
		h /= U64_MAX;
		REQUIRE(h == 0);
	}
}

TEST_CASE("CheckedInteger atomic operations", "[checked_integer]") {
	SECTION("is_lock_free / is_always_lock_free forward to underlying type") {
		std::atomic<ci64> a(0);
		REQUIRE(a.is_lock_free() == std::atomic<int64_t>().is_lock_free());
		REQUIRE(std::atomic<ci64>::is_always_lock_free == std::atomic<int64_t>::is_always_lock_free);
	}

	SECTION("store and load via member functions") {
		std::atomic<ci64> a = 50;
		REQUIRE(a == 50);
		a = 42;
		REQUIRE(a == 42);
	}

	SECTION("construct and assign with invalid value") {
		std::atomic<cu8> c;
		REQUIRE_THROWS_WITH(c = 1000, Catch::Contains("out of range for CheckedInteger"));
	}

	SECTION("fetch_add / fetch_sub happy path returns previous value") {
		std::atomic<ci32> a(10);
		auto prev = a.fetch_add(ci32(5));
		REQUIRE(prev == 10);
		REQUIRE(a.load() == 15);

		auto prev2 = a.fetch_sub(ci32(3));
		REQUIRE(prev2 == 15);
		REQUIRE(a.load() == 12);
	}

	SECTION("fetch_add overflow / fetch_sub underflow throw") {
		std::atomic<ci32> a(NumericLimits<int32_t>::Maximum());
		REQUIRE_THROWS_WITH(a.fetch_add(ci32(1)), Catch::Contains("Overflow in addition for CheckedInteger"));

		std::atomic<ci32> b(NumericLimits<int32_t>::Minimum());
		REQUIRE_THROWS_WITH(b.fetch_sub(ci32(1)), Catch::Contains("Underflow in subtraction for CheckedInteger"));

		std::atomic<cu8> c(0);
		REQUIRE_THROWS_WITH(c.fetch_sub(cu8(1)), Catch::Contains("Underflow in subtraction for CheckedInteger"));
	}

	SECTION("operator+=/-= on atomic propagates overflow") {
		std::atomic<ci64> a(NumericLimits<int64_t>::Maximum() - 5);
		REQUIRE_THROWS_WITH(a += int32_t(100), Catch::Contains("Overflow in addition for CheckedInteger"));

		std::atomic<cu32> b(5);
		REQUIRE_THROWS_WITH(b -= uint32_t(100), Catch::Contains("Underflow in subtraction for CheckedInteger"));

		// happy path returning new value
		std::atomic<ci64> c(100);
		auto sum = c += int8_t(7);
		REQUIRE(sum == 107);
		REQUIRE(c.load() == 107);
	}

	SECTION("custom error category propagates through atomic") {
		using i32_invalid_t = CheckedInteger<int32_t, InvalidInputException>;
		std::atomic<i32_invalid_t> a(NumericLimits<int32_t>::Maximum());
		REQUIRE_THROWS_MATCHES(
		    a.fetch_add(i32_invalid_t(1)), std::exception,
		    ExceptionMatches(ExceptionType::INVALID_INPUT, Catch::Contains("Overflow in addition for CheckedInteger")));
	}

	SECTION("compare_exchange_strong success and failure") {
		std::atomic<ci32> a(10);
		ci32 expected(10);
		REQUIRE(a.compare_exchange_strong(expected, ci32(20)));
		REQUIRE(a.load() == 20);

		expected = ci32(99);
		REQUIRE_FALSE(a.compare_exchange_strong(expected, ci32(7)));
		REQUIRE(a.load() == 20);
	}

	SECTION("operator++ / operator-- ") {
		std::atomic<ci32> a(10);
		auto pre = ++a;
		REQUIRE(pre == 11);
		REQUIRE(a.load() == 11);

		auto post = a++;
		REQUIRE(post == 11);
		REQUIRE(a.load() == 12);

		auto pre_dec = --a;
		REQUIRE(pre_dec == 11);
		REQUIRE(a.load() == 11);

		auto post_dec = a--;
		REQUIRE(post_dec == 11);
		REQUIRE(a.load() == 10);
	}

	SECTION("operator++ / operator-- throw at boundaries") {
		std::atomic<ci8> hi(NumericLimits<int8_t>::Maximum());
		REQUIRE_THROWS_WITH(++hi, Catch::Contains("Overflow in addition for CheckedInteger"));
		REQUIRE_THROWS_WITH(hi++, Catch::Contains("Overflow in addition for CheckedInteger"));

		std::atomic<ci8> lo(NumericLimits<int8_t>::Minimum());
		REQUIRE_THROWS_WITH(--lo, Catch::Contains("Underflow in subtraction for CheckedInteger"));
		REQUIRE_THROWS_WITH(lo--, Catch::Contains("Underflow in subtraction for CheckedInteger"));

		std::atomic<cu8> u_lo(0);
		REQUIRE_THROWS_WITH(--u_lo, Catch::Contains("Underflow in subtraction for CheckedInteger"));
		REQUIRE_THROWS_WITH(u_lo--, Catch::Contains("Underflow in subtraction for CheckedInteger"));
	}
}

TEST_CASE("std::numeric_limits<CheckedInteger> specialization", "[checked_integer]") {
	SECTION("min / max / lowest match underlying type") {
		REQUIRE(std::numeric_limits<ci8>::min() == NumericLimits<int8_t>::Minimum());
		REQUIRE(std::numeric_limits<ci8>::max() == NumericLimits<int8_t>::Maximum());
		REQUIRE(std::numeric_limits<ci8>::lowest() == std::numeric_limits<int8_t>::lowest());

		REQUIRE(std::numeric_limits<cu8>::min() == 0u);
		REQUIRE(std::numeric_limits<cu8>::max() == NumericLimits<uint8_t>::Maximum());

		REQUIRE(std::numeric_limits<ci64>::min() == NumericLimits<int64_t>::Minimum());
		REQUIRE(std::numeric_limits<ci64>::max() == NumericLimits<int64_t>::Maximum());
		REQUIRE(std::numeric_limits<cu64>::max() == NumericLimits<uint64_t>::Maximum());
	}

	SECTION("inherited integer traits forward to underlying type") {
		REQUIRE(std::numeric_limits<ci8>::is_specialized);
		REQUIRE(std::numeric_limits<ci8>::is_signed == std::numeric_limits<int8_t>::is_signed);
		REQUIRE(std::numeric_limits<ci8>::is_integer);
		REQUIRE(std::numeric_limits<ci8>::digits == std::numeric_limits<int8_t>::digits);
		REQUIRE(std::numeric_limits<ci8>::radix == std::numeric_limits<int8_t>::radix);

		REQUIRE_FALSE(std::numeric_limits<cu32>::is_signed);
		REQUIRE(std::numeric_limits<cu32>::is_integer);
		REQUIRE(std::numeric_limits<cu32>::digits == std::numeric_limits<uint32_t>::digits);
	}

	SECTION("min / max are constexpr-usable") {
		constexpr auto lo = std::numeric_limits<ci32>::min();
		constexpr auto hi = std::numeric_limits<ci32>::max();
		REQUIRE(lo == NumericLimits<int32_t>::Minimum());
		REQUIRE(hi == NumericLimits<int32_t>::Maximum());
	}
}

TEST_CASE("std::hash<CheckedInteger> specialization", "[checked_integer]") {
	SECTION("usable as a key in unordered_set") {
		unordered_set<ci32> s;
		s.insert(ci32(1));
		s.insert(ci32(2));
		s.insert(ci32(1));
		REQUIRE(s.size() == 2);
		REQUIRE(s.count(ci32(1)) == 1);
		REQUIRE(s.count(ci32(2)) == 1);
		REQUIRE(s.count(ci32(3)) == 0);
	}
}

TEST_CASE("GetTypeId recurses through CheckedInteger", "[checked_integer]") {
	REQUIRE(GetTypeId<tinyint_t>() == PhysicalType::INT8);
	REQUIRE(GetTypeId<smallint_t>() == PhysicalType::INT16);
	REQUIRE(GetTypeId<integer_t>() == PhysicalType::INT32);
	REQUIRE(GetTypeId<bigint_t>() == PhysicalType::INT64);

	REQUIRE(GetTypeId<utinyint_t>() == PhysicalType::UINT8);
	REQUIRE(GetTypeId<usmallint_t>() == PhysicalType::UINT16);
	REQUIRE(GetTypeId<uinteger_t>() == PhysicalType::UINT32);
	REQUIRE(GetTypeId<ubigint_t>() == PhysicalType::UINT64);

	REQUIRE(GetTypeId<CheckedInteger<int32_t, InternalException>>() == PhysicalType::INT32);
	REQUIRE(GetTypeId<CheckedInteger<int32_t, InvalidInputException>>() == PhysicalType::INT32);
	REQUIRE(GetTypeId<const ubigint_t>() == PhysicalType::UINT64);
}
