#include "catch.hpp"
#include "duckdb/common/checked_integer.hpp"

using duckdb::CheckedInteger;
using duckdb::NumericLimits;
using duckdb::OutOfRangeException;

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

TEST_CASE("Checked integer increment/decrement overflow", "[checked_integer]") {
	SECTION("overflow at maximum (pre and post forms)") {
		ci8 s(NumericLimits<int8_t>::Maximum());
		REQUIRE_THROWS_AS(++s, OutOfRangeException);
		REQUIRE_THROWS_AS(s++, OutOfRangeException);

		cu8 u(NumericLimits<uint8_t>::Maximum());
		REQUIRE_THROWS_AS(++u, OutOfRangeException);
		REQUIRE_THROWS_AS(u++, OutOfRangeException);
	}

	SECTION("underflow at minimum (pre and post forms)") {
		ci8 s(NumericLimits<int8_t>::Minimum());
		REQUIRE_THROWS_AS(--s, OutOfRangeException);
		REQUIRE_THROWS_AS(s--, OutOfRangeException);

		cu8 u(0);
		REQUIRE_THROWS_AS(--u, OutOfRangeException);
		REQUIRE_THROWS_AS(u--, OutOfRangeException);
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
		REQUIRE_THROWS_AS(ci8(1000), OutOfRangeException);
		REQUIRE_THROWS_AS(ci8(-1000), OutOfRangeException);
		REQUIRE_THROWS_AS(ci32(int64_t(NumericLimits<int32_t>::Maximum()) + 1), OutOfRangeException);

		// unsigned -> smaller unsigned
		REQUIRE_THROWS_AS(cu8(256), OutOfRangeException);
		REQUIRE_THROWS_AS(cu32(uint64_t(1) << 32), OutOfRangeException);
	}

	SECTION("signed -> unsigned rejects negatives and too-large positives") {
		REQUIRE_THROWS_AS(cu8(int8_t(-1)), OutOfRangeException);
		REQUIRE_THROWS_AS(cu32(int32_t(-1)), OutOfRangeException);
		REQUIRE_THROWS_AS(cu64(int64_t(-1)), OutOfRangeException);
		REQUIRE_THROWS_AS(cu8(int16_t(300)), OutOfRangeException);
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
		REQUIRE_THROWS_AS(a + int32_t(10), OutOfRangeException);
		REQUIRE_THROWS_AS(a += int32_t(100), OutOfRangeException);

		// subtraction underflow
		cu16 b(5);
		REQUIRE_THROWS_AS(b + int32_t(-10), OutOfRangeException);

		// signed -= unsigned underflow
		ci16 c(-30000);
		REQUIRE_THROWS_AS(c -= uint16_t(5000), OutOfRangeException);

		// multiplication overflow
		ci16 d(200);
		REQUIRE_THROWS_AS(d * uint16_t(200), OutOfRangeException);
	}

	SECTION("division by zero and T_MIN / -1 overflow") {
		// cross-type division by zero
		cu32 a(100);
		REQUIRE_THROWS_AS(a / int16_t(0), OutOfRangeException);

		// T_MIN / -1 = |T_MIN| does not fit back in T
		ci8 b(NumericLimits<int8_t>::Minimum());
		REQUIRE_THROWS_AS(b /= int64_t(-1), OutOfRangeException);

		// At T_MIN, dividing by 2 still works and preserves sign
		ci8 d(NumericLimits<int8_t>::Minimum());
		d /= int64_t(2);
		REQUIRE(d == NumericLimits<int8_t>::Minimum() / 2);
	}

	SECTION("int64 / uint64 wide-path correctness") {
		// i64 += uint64: overflow + just-fits boundary
		ci64 a(I64_MAX);
		REQUIRE_THROWS_AS(a += uint64_t(1), OutOfRangeException);
		REQUIRE_THROWS_AS(ci64(0) + U64_MAX, OutOfRangeException);
		ci64 b(0);
		b += uint64_t(I64_MAX);
		REQUIRE(b == I64_MAX);

		// i64 -= uint64: underflow + just-fits boundary
		ci64 c(I64_MIN);
		REQUIRE_THROWS_AS(c -= uint64_t(1), OutOfRangeException);
		REQUIRE_THROWS_AS(ci64(-1) - U64_MAX, OutOfRangeException);
		ci64 d(0);
		d -= uint64_t(I64_MAX);
		REQUIRE(d == -I64_MAX);

		// i64 *= uint64: overflow on both signs
		ci64 e(2);
		REQUIRE_THROWS_AS(e *= U64_MAX, OutOfRangeException);

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
	SECTION("store and load via member functions") {
		std::atomic<ci64> a = 50;
		REQUIRE(a == 50);
		a = 42;
		REQUIRE(a == 42);
	}

	SECTION("construct and assign with invalid value") {
		std::atomic<cu8> c;
		REQUIRE_THROWS_AS(c = 1000, OutOfRangeException);
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
		REQUIRE_THROWS_AS(a.fetch_add(ci32(1)), OutOfRangeException);

		std::atomic<ci32> b(NumericLimits<int32_t>::Minimum());
		REQUIRE_THROWS_AS(b.fetch_sub(ci32(1)), OutOfRangeException);

		std::atomic<cu8> c(0);
		REQUIRE_THROWS_AS(c.fetch_sub(cu8(1)), OutOfRangeException);
	}

	SECTION("operator+=/-= on atomic propagates overflow") {
		std::atomic<ci64> a(NumericLimits<int64_t>::Maximum() - 5);
		REQUIRE_THROWS_AS(a += int32_t(100), OutOfRangeException);

		std::atomic<cu32> b(5);
		REQUIRE_THROWS_AS(b -= uint32_t(100), OutOfRangeException);

		// happy path returning new value
		std::atomic<ci64> c(100);
		auto sum = c += int8_t(7);
		REQUIRE(sum == 107);
		REQUIRE(c.load() == 107);
	}

	SECTION("custom exception type propagates through atomic") {
		// Verify that selecting a non-default ExceptionT actually changes the thrown type.
		using i32_invalid_t = CheckedInteger<int32_t, duckdb::InvalidInputException>;
		std::atomic<i32_invalid_t> a(NumericLimits<int32_t>::Maximum());
		REQUIRE_THROWS_AS(a.fetch_add(i32_invalid_t(1)), duckdb::InvalidInputException);
	}
}
