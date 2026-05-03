#include "catch.hpp"
#include "duckdb/common/checked_integer.hpp"

using namespace duckdb;

TEST_CASE("Checked integer increment/decrement overflow", "[checked_integer]") {
	SECTION("signed widths overflow at maximum") {
		i8_t i8_max(NumericLimits<int8_t>::Maximum());
		REQUIRE_THROWS_AS(++i8_max, InternalException);
		i8_t i8_max2(NumericLimits<int8_t>::Maximum());
		REQUIRE_THROWS_AS(i8_max2++, InternalException);

		i16_t i16_max(NumericLimits<int16_t>::Maximum());
		REQUIRE_THROWS_AS(++i16_max, InternalException);
		i16_t i16_max2(NumericLimits<int16_t>::Maximum());
		REQUIRE_THROWS_AS(i16_max2++, InternalException);

		i32_t i32_max(NumericLimits<int32_t>::Maximum());
		REQUIRE_THROWS_AS(++i32_max, InternalException);
		i32_t i32_max2(NumericLimits<int32_t>::Maximum());
		REQUIRE_THROWS_AS(i32_max2++, InternalException);

		i64_t i64_max(NumericLimits<int64_t>::Maximum());
		REQUIRE_THROWS_AS(++i64_max, InternalException);
		i64_t i64_max2(NumericLimits<int64_t>::Maximum());
		REQUIRE_THROWS_AS(i64_max2++, InternalException);
	}

	SECTION("signed widths underflow at minimum") {
		i8_t i8_min(NumericLimits<int8_t>::Minimum());
		REQUIRE_THROWS_AS(--i8_min, InternalException);
		i8_t i8_min2(NumericLimits<int8_t>::Minimum());
		REQUIRE_THROWS_AS(i8_min2--, InternalException);

		i16_t i16_min(NumericLimits<int16_t>::Minimum());
		REQUIRE_THROWS_AS(--i16_min, InternalException);
		i16_t i16_min2(NumericLimits<int16_t>::Minimum());
		REQUIRE_THROWS_AS(i16_min2--, InternalException);

		i32_t i32_min(NumericLimits<int32_t>::Minimum());
		REQUIRE_THROWS_AS(--i32_min, InternalException);
		i32_t i32_min2(NumericLimits<int32_t>::Minimum());
		REQUIRE_THROWS_AS(i32_min2--, InternalException);

		i64_t i64_min(NumericLimits<int64_t>::Minimum());
		REQUIRE_THROWS_AS(--i64_min, InternalException);
		i64_t i64_min2(NumericLimits<int64_t>::Minimum());
		REQUIRE_THROWS_AS(i64_min2--, InternalException);
	}

	SECTION("unsigned widths overflow at maximum") {
		u8_t u8_max(NumericLimits<uint8_t>::Maximum());
		REQUIRE_THROWS_AS(++u8_max, InternalException);
		u8_t u8_max2(NumericLimits<uint8_t>::Maximum());
		REQUIRE_THROWS_AS(u8_max2++, InternalException);

		u16_t u16_max(NumericLimits<uint16_t>::Maximum());
		REQUIRE_THROWS_AS(++u16_max, InternalException);
		u16_t u16_max2(NumericLimits<uint16_t>::Maximum());
		REQUIRE_THROWS_AS(u16_max2++, InternalException);

		u32_t u32_max(NumericLimits<uint32_t>::Maximum());
		REQUIRE_THROWS_AS(++u32_max, InternalException);
		u32_t u32_max2(NumericLimits<uint32_t>::Maximum());
		REQUIRE_THROWS_AS(u32_max2++, InternalException);

		u64_t u64_max(NumericLimits<uint64_t>::Maximum());
		REQUIRE_THROWS_AS(++u64_max, InternalException);
		u64_t u64_max2(NumericLimits<uint64_t>::Maximum());
		REQUIRE_THROWS_AS(u64_max2++, InternalException);
	}

	SECTION("unsigned widths underflow at zero") {
		u8_t u8_min(0);
		REQUIRE_THROWS_AS(--u8_min, InternalException);
		u8_t u8_min2(0);
		REQUIRE_THROWS_AS(u8_min2--, InternalException);

		u16_t u16_min(0);
		REQUIRE_THROWS_AS(--u16_min, InternalException);
		u16_t u16_min2(0);
		REQUIRE_THROWS_AS(u16_min2--, InternalException);

		u32_t u32_min(0);
		REQUIRE_THROWS_AS(--u32_min, InternalException);
		u32_t u32_min2(0);
		REQUIRE_THROWS_AS(u32_min2--, InternalException);

		u64_t u64_min(0);
		REQUIRE_THROWS_AS(--u64_min, InternalException);
		u64_t u64_min2(0);
		REQUIRE_THROWS_AS(u64_min2--, InternalException);
	}

	SECTION("post-increment returns the previous value before throwing on next call") {
		i32_t v(NumericLimits<int32_t>::Maximum() - 1);
		auto prev = v++;
		REQUIRE(prev == NumericLimits<int32_t>::Maximum() - 1);
		REQUIRE(v == NumericLimits<int32_t>::Maximum());
		REQUIRE_THROWS_AS(v++, InternalException);
	}

	SECTION("valid increment/decrement should work") {
		i32_t val(100);
		++val;
		REQUIRE(val == 101);
		val--;
		REQUIRE(val == 100);

		u64_t big(NumericLimits<uint64_t>::Maximum() - 1);
		++big;
		REQUIRE(big == NumericLimits<uint64_t>::Maximum());
		--big;
		REQUIRE(big == NumericLimits<uint64_t>::Maximum() - 1);
	}
}

TEST_CASE("Checked integer compound assignment overflow", "[checked_integer]") {
	// += overflow
	i64_t a(NumericLimits<int64_t>::Maximum() - 10);
	REQUIRE_THROWS_AS(a += 20, InternalException);

	// -= underflow
	i64_t b(NumericLimits<int64_t>::Minimum() + 10);
	REQUIRE_THROWS_AS(b -= 20, InternalException);

	// *= overflow
	i32_t c(100000);
	REQUIRE_THROWS_AS(c *= 100000, InternalException);

	// /= division by zero
	i64_t d(100);
	REQUIRE_THROWS_AS(d /= 0, InternalException);

	// /= INT_MIN / -1 overflow
	i64_t e(NumericLimits<int64_t>::Minimum());
	REQUIRE_THROWS_AS(e /= -1, InternalException);

	// valid operations
	u32_t u(100);
	u += 50;
	REQUIRE(u == 150u);
	u -= 25;
	REQUIRE(u == 125u);
	u *= 2;
	REQUIRE(u == 250u);
	u /= 5;
	REQUIRE(u == 50u);
}

TEST_CASE("Checked integer binary arithmetic overflow", "[checked_integer]") {
	// + overflow
	i16_t x(NumericLimits<int16_t>::Maximum() - 5);
	REQUIRE_THROWS_AS(x + 10, InternalException);

	// - underflow
	i16_t y(NumericLimits<int16_t>::Minimum() + 5);
	REQUIRE_THROWS_AS(y - 10, InternalException);

	// * overflow
	i8_t z(100);
	REQUIRE_THROWS_AS(z * 2, InternalException);

	// / division by zero
	i32_t w(42);
	REQUIRE_THROWS_AS(w / 0, InternalException);

	// valid operations return correct results
	u64_t u1(1000);
	auto u2 = u1 + 500;
	REQUIRE(u2 == 1500u);

	i64_t s1(-100);
	auto s2 = s1 - 50;
	REQUIRE(s2 == -150);
}

TEST_CASE("Checked integer comparisons", "[checked_integer]") {
	i64_t a(100);
	i64_t b(200);

	REQUIRE(a < b);
	REQUIRE(a <= b);
	REQUIRE(b > a);
	REQUIRE(b >= a);
	REQUIRE(a != b);
	REQUIRE_FALSE(a == b);

	REQUIRE(a < 150);
	REQUIRE(a == 100);
}

TEST_CASE("CheckedInteger mixed integer-type arithmetic", "[checked_integer]") {
	// uint32_t / int
	u32_t g(100);
	auto h = g / 3;
	REQUIRE(h.GetValue() == 33u);

	// int16_t * int8_t (uses promoted-type branch)
	i16_t a(7);
	auto b = a * int8_t(20);
	REQUIRE(b.GetValue() == 140);

	// Compound: int8_t += int (same-type Promoted branch with range check via TryAdd)
	i32_t c(100);
	c += int16_t(23);
	REQUIRE(c.GetValue() == 123);
}

TEST_CASE("CheckedInteger unsigned cannot be negative", "[checked_integer]") {
	// Cannot construct unsigned from negative
	REQUIRE_THROWS_AS(u32_t(-1), InternalException);
	REQUIRE_THROWS_AS(u64_t(-100), InternalException);
	REQUIRE_THROWS_AS(u8_t(-1), InternalException);

	// Can construct from positive
	REQUIRE_NOTHROW(u32_t(100));
	REQUIRE(u32_t(100).GetValue() == 100u);

	// Cross-type: unsigned += negative is valid when result is non-negative
	u16_t x(50);
	x += -10;
	REQUIRE(x.GetValue() == 40u);
	REQUIRE_THROWS_AS(x -= 100, InternalException); // underflow caught by checked sub

	// Conforms to normal C++ arithmetic: uint16_t(9) -= -10 → 19
	u16_t z(9);
	z -= -10;
	REQUIRE(z.GetValue() == 19u);
}

#define CHECK_MATCHES_NATIVE(checked_val, native_expr)                                                                 \
	REQUIRE(static_cast<decltype(native_expr)>((checked_val).GetValue()) == (native_expr))

TEST_CASE("Cross-type binary arithmetic matches native behavior", "[checked_integer]") {
	SECTION("unsigned + signed") {
		u16_t a(100);
		auto r = a + int8_t(-30);
		CHECK_MATCHES_NATIVE(r, static_cast<uint16_t>(uint16_t(100) + int8_t(-30)));
		REQUIRE(r.GetValue() == 70u);
	}

	SECTION("unsigned - negative signed") {
		u32_t a(50);
		auto r = a - int16_t(-25);
		CHECK_MATCHES_NATIVE(r, static_cast<uint32_t>(uint32_t(50) - int16_t(-25)));
		REQUIRE(r.GetValue() == 75u);
	}

	SECTION("signed + unsigned") {
		i32_t a(-200);
		auto r = a + uint16_t(300);
		CHECK_MATCHES_NATIVE(r, static_cast<int32_t>(int32_t(-200) + uint16_t(300)));
		REQUIRE(r.GetValue() == 100);
	}

	SECTION("signed - unsigned") {
		i64_t a(10);
		auto r = a - uint32_t(30);
		CHECK_MATCHES_NATIVE(r, static_cast<int64_t>(int64_t(10) - uint32_t(30)));
		REQUIRE(r.GetValue() == -20);
	}

	SECTION("small unsigned * large signed") {
		u8_t a(10);
		auto r = a * int32_t(20);
		CHECK_MATCHES_NATIVE(r, static_cast<uint8_t>(uint8_t(10) * int32_t(20)));
		REQUIRE(r.GetValue() == 200u);
	}

	SECTION("signed / unsigned") {
		i32_t a(100);
		auto r = a / uint16_t(7);
		CHECK_MATCHES_NATIVE(r, static_cast<int32_t>(int32_t(100) / uint16_t(7)));
		REQUIRE(r.GetValue() == 14);
	}

	SECTION("unsigned / signed") {
		u64_t a(1000);
		auto r = a / int32_t(3);
		CHECK_MATCHES_NATIVE(r, static_cast<uint64_t>(uint64_t(1000) / int32_t(3)));
		REQUIRE(r.GetValue() == 333u);
	}

	SECTION("narrower signed + wider unsigned") {
		i16_t a(500);
		auto r = a + uint32_t(100);
		CHECK_MATCHES_NATIVE(r, static_cast<int16_t>(static_cast<uint32_t>(int16_t(500)) + uint32_t(100)));
		REQUIRE(r.GetValue() == 600);
	}
}

TEST_CASE("Cross-type compound assignment matches native behavior", "[checked_integer]") {
	SECTION("unsigned += negative signed") {
		u32_t a(100);
		a += int8_t(-40);
		REQUIRE(a.GetValue() == 60u);
	}

	SECTION("unsigned -= negative signed") {
		u16_t a(50);
		a -= int16_t(-50);
		REQUIRE(a.GetValue() == 100u);
	}

	SECTION("signed += unsigned") {
		i32_t a(-50);
		a += uint16_t(200);
		REQUIRE(a.GetValue() == 150);
	}

	SECTION("signed -= unsigned") {
		i64_t a(100);
		a -= uint32_t(250);
		REQUIRE(a.GetValue() == -150);
	}

	SECTION("signed *= unsigned") {
		i16_t a(-7);
		a *= uint8_t(6);
		REQUIRE(a.GetValue() == -42);
	}

	SECTION("unsigned *= signed positive") {
		u32_t a(25);
		a *= int16_t(4);
		REQUIRE(a.GetValue() == 100u);
	}

	SECTION("signed /= unsigned") {
		i32_t a(-100);
		a /= uint8_t(10);
		REQUIRE(a.GetValue() == -10);
	}

	SECTION("unsigned /= signed positive") {
		u64_t a(999);
		a /= int32_t(10);
		REQUIRE(a.GetValue() == 99u);
	}
}

TEST_CASE("CheckedInteger narrow constructor overflow", "[checked_integer]") {
	SECTION("same-sign widening: always passes (round-trip is exact)") {
		// signed -> wider signed
		REQUIRE(i64_t(int8_t(-128)).GetValue() == -128);
		REQUIRE(i64_t(int8_t(127)).GetValue() == 127);
		REQUIRE(i32_t(int16_t(-32768)).GetValue() == -32768);
		REQUIRE(i32_t(int16_t(32767)).GetValue() == 32767);

		// unsigned -> wider unsigned
		REQUIRE(u64_t(uint8_t(255)).GetValue() == 255u);
		REQUIRE(u64_t(uint16_t(65535)).GetValue() == 65535u);
		REQUIRE(u32_t(uint8_t(0)).GetValue() == 0u);
	}

	SECTION("signed -> smaller signed: round-trip detects narrow overflow / underflow") {
		REQUIRE_THROWS_AS(i8_t(1000), InternalException);
		REQUIRE_THROWS_AS(i8_t(-1000), InternalException);
		REQUIRE_THROWS_AS(i16_t(40000), InternalException);
		REQUIRE_THROWS_AS(i16_t(-40000), InternalException);
		REQUIRE_THROWS_AS(i32_t(int64_t(NumericLimits<int32_t>::Maximum()) + 1), InternalException);
		REQUIRE_THROWS_AS(i32_t(int64_t(NumericLimits<int32_t>::Minimum()) - 1), InternalException);

		// boundary values pass
		REQUIRE(i8_t(127).GetValue() == 127);
		REQUIRE(i8_t(-128).GetValue() == -128);
		REQUIRE(i16_t(32767).GetValue() == 32767);
		REQUIRE(i16_t(-32768).GetValue() == -32768);
	}

	SECTION("unsigned -> smaller unsigned: round-trip detects narrow overflow") {
		REQUIRE_THROWS_AS(u8_t(256), InternalException);
		REQUIRE_THROWS_AS(u8_t(1000), InternalException);
		REQUIRE_THROWS_AS(u16_t(70000), InternalException);
		REQUIRE_THROWS_AS(u8_t(uint16_t(256)), InternalException);
		REQUIRE_THROWS_AS(u32_t(uint64_t(1) << 32), InternalException);

		// boundary values pass
		REQUIRE(u8_t(0).GetValue() == 0u);
		REQUIRE(u8_t(255).GetValue() == 255u);
		REQUIRE(u32_t(uint64_t(NumericLimits<uint32_t>::Maximum())).GetValue() == NumericLimits<uint32_t>::Maximum());
	}

	SECTION("signed -> unsigned: branch 1 rejects negatives, round-trip rejects too-large") {
		// Branch 1: negative source rejected for unsigned target
		REQUIRE_THROWS_AS(u8_t(int8_t(-1)), InternalException);
		REQUIRE_THROWS_AS(u8_t(int16_t(-128)), InternalException);
		REQUIRE_THROWS_AS(u32_t(int32_t(-1)), InternalException);
		REQUIRE_THROWS_AS(u64_t(int64_t(-1)), InternalException);

		// Round-trip: positive that doesn't fit
		REQUIRE_THROWS_AS(u8_t(int16_t(300)), InternalException);
		REQUIRE_THROWS_AS(u8_t(int32_t(256)), InternalException);

		// Boundary: 0 and target max pass
		REQUIRE(u8_t(int8_t(127)).GetValue() == 127u);
		REQUIRE(u32_t(int32_t(0)).GetValue() == 0u);
		REQUIRE(u32_t(int32_t(NumericLimits<int32_t>::Maximum())).GetValue() ==
		        static_cast<uint32_t>(NumericLimits<int32_t>::Maximum()));
		REQUIRE(u32_t(int64_t(1) << 31).GetValue() == (uint32_t(1) << 31));
	}

	SECTION("unsigned -> signed: branch 2 rejects sign-flips, round-trip catches narrowing") {
		// Branch 2: source value > T::max wraps result into negative
		REQUIRE_THROWS_AS(i8_t(uint8_t(128)), InternalException);
		REQUIRE_THROWS_AS(i8_t(uint16_t(200)), InternalException);
		REQUIRE_THROWS_AS(i16_t(uint16_t(NumericLimits<int16_t>::Maximum()) + 1), InternalException);
		REQUIRE_THROWS_AS(i32_t(uint64_t(NumericLimits<int32_t>::Maximum()) + 1), InternalException);

		// Boundary: T::max passes
		REQUIRE(i8_t(uint8_t(127)).GetValue() == 127);
		REQUIRE(i16_t(uint16_t(NumericLimits<int16_t>::Maximum())).GetValue() == NumericLimits<int16_t>::Maximum());
		REQUIRE(i32_t(uint8_t(255)).GetValue() == 255);
	}

	SECTION("max-width edges: int64 <-> uint64") {
		// uint64 -> int64
		REQUIRE(i64_t(uint64_t(NumericLimits<int64_t>::Maximum())).GetValue() == NumericLimits<int64_t>::Maximum());
		REQUIRE_THROWS_AS(i64_t(uint64_t(NumericLimits<int64_t>::Maximum()) + 1), InternalException);
		REQUIRE_THROWS_AS(i64_t(NumericLimits<uint64_t>::Maximum()), InternalException);

		// int64 -> uint64
		REQUIRE(u64_t(int64_t(NumericLimits<int64_t>::Maximum())).GetValue() ==
		        static_cast<uint64_t>(NumericLimits<int64_t>::Maximum()));
		REQUIRE_THROWS_AS(u64_t(int64_t(-1)), InternalException);
		REQUIRE_THROWS_AS(u64_t(NumericLimits<int64_t>::Minimum()), InternalException);
	}

	SECTION("bool is integral and maps to 0/1") {
		REQUIRE(i8_t(true).GetValue() == 1);
		REQUIRE(i8_t(false).GetValue() == 0);
		REQUIRE(u32_t(true).GetValue() == 1u);
		REQUIRE(u64_t(false).GetValue() == 0u);
	}
}

TEST_CASE("CheckedInteger custom exception type", "[checked_integer]") {
	using i32_range_t = CheckedInteger<int32_t, OutOfRangeException>;

	// Constructor narrow overflow throws the customized exception
	REQUIRE_THROWS_AS(i32_range_t(int64_t(1) << 40), OutOfRangeException);

	// Arithmetic overflow also uses the customized exception
	i32_range_t a(NumericLimits<int32_t>::Maximum());
	REQUIRE_THROWS_AS(++a, OutOfRangeException);
	REQUIRE_THROWS_AS(a + 1, OutOfRangeException);

	// Division by zero too
	i32_range_t b(100);
	REQUIRE_THROWS_AS(b / 0, OutOfRangeException);

	// Default alias still throws InternalException
	i32_t c(NumericLimits<int32_t>::Maximum());
	REQUIRE_THROWS_AS(++c, InternalException);
}

TEST_CASE("Cross-type arithmetic overflow detection", "[checked_integer]") {
	SECTION("unsigned + signed overflows T") {
		u8_t a(250);
		REQUIRE_THROWS_AS(a + int32_t(10), InternalException);
	}

	SECTION("unsigned - signed underflows T") {
		u16_t a(5);
		REQUIRE_THROWS_AS(a + int32_t(-10), InternalException);
	}

	SECTION("signed * unsigned overflows T") {
		i16_t a(200);
		REQUIRE_THROWS_AS(a * uint16_t(200), InternalException);
	}

	SECTION("compound: unsigned += signed overflows") {
		u8_t a(200);
		REQUIRE_THROWS_AS(a += int32_t(100), InternalException);
	}

	SECTION("compound: signed -= unsigned underflows") {
		i16_t a(-30000);
		REQUIRE_THROWS_AS(a -= uint16_t(5000), InternalException);
	}

	SECTION("compound: unsigned *= signed overflows") {
		u16_t a(1000);
		REQUIRE_THROWS_AS(a *= int32_t(100), InternalException);
	}

	SECTION("cross-type division by zero") {
		u32_t a(100);
		REQUIRE_THROWS_AS(a / int16_t(0), InternalException);
		REQUIRE_THROWS_AS(a /= int8_t(0), InternalException);
	}
}

// These cases exercise the wide-arithmetic (branch B) path for the trickiest pairing:
// signed 64-bit with unsigned 64-bit. The natural common_type is uint64_t, so the previous
// implementation that did the math in `Promoted` would silently wrap modulo 2^64 and the
// round-trip cast could be fooled. The hugeint_t-based path must reject every overflow below.
TEST_CASE("CheckedInteger int64 / uint64 mixed arithmetic overflow", "[checked_integer]") {
	const uint64_t U64_MAX = NumericLimits<uint64_t>::Maximum();
	const int64_t I64_MAX = NumericLimits<int64_t>::Maximum();
	const int64_t I64_MIN = NumericLimits<int64_t>::Minimum();

	SECTION("i64 += large uint64 overflows positively") {
		i64_t a(0);
		REQUIRE_THROWS_AS(a += U64_MAX, InternalException);
		i64_t b(I64_MAX);
		REQUIRE_THROWS_AS(b += uint64_t(1), InternalException);
		// just-fits case must succeed
		i64_t c(0);
		c += uint64_t(I64_MAX);
		REQUIRE(c == I64_MAX);
	}

	SECTION("i64 + large uint64 overflows positively") {
		i64_t a(0);
		REQUIRE_THROWS_AS(a + U64_MAX, InternalException);
		i64_t b(I64_MAX - 5);
		REQUIRE_THROWS_AS(b + uint64_t(10), InternalException);
		// just-fits case must succeed
		i64_t c(1);
		auto r = c + uint64_t(I64_MAX - 1);
		REQUIRE(r == I64_MAX);
	}

	SECTION("i64 -= uint64 underflows below INT64_MIN") {
		// any negative value minus a uint64 large enough underflows
		i64_t a(-1);
		REQUIRE_THROWS_AS(a -= U64_MAX, InternalException);
		i64_t b(I64_MIN + 5);
		REQUIRE_THROWS_AS(b -= uint64_t(10), InternalException);
		// just-fits case must succeed: 0 - INT64_MAX = -INT64_MAX
		i64_t c(0);
		c -= uint64_t(I64_MAX);
		REQUIRE(c == -I64_MAX);
	}

	SECTION("i64 - uint64 underflows below INT64_MIN") {
		i64_t a(0);
		REQUIRE_THROWS_AS(a - uint64_t(uint64_t(I64_MAX) + 2), InternalException);
		i64_t b(I64_MIN);
		REQUIRE_THROWS_AS(b - uint64_t(1), InternalException);
	}

	SECTION("i64 *= large uint64 overflows") {
		i64_t a(2);
		REQUIRE_THROWS_AS(a *= U64_MAX, InternalException);
		// negative * large uint64 magnitude overflows below INT64_MIN
		i64_t b(-2);
		REQUIRE_THROWS_AS(b *= uint64_t(I64_MAX), InternalException);
		// 1 * uint64 is still bounded by uint64; if it doesn't fit in int64, throw
		i64_t c(1);
		REQUIRE_THROWS_AS(c *= uint64_t(uint64_t(I64_MAX) + 1), InternalException);
	}

	SECTION("i64 * large uint64 overflows") {
		i64_t a(3);
		REQUIRE_THROWS_AS(a * U64_MAX, InternalException);
		i64_t b(-1);
		REQUIRE_THROWS_AS(b * U64_MAX, InternalException);
	}

	SECTION("i64 /= uint64 keeps sign and detects overflow") {
		// negative-dividend signed division: -100 / 2 must remain -50 (not silently turned positive)
		i64_t a(-100);
		a /= uint64_t(2);
		REQUIRE(a == -50);

		// dividing by a uint64 too large to fit in int64 yields 0 (no overflow)
		i64_t b(-100);
		b /= U64_MAX;
		REQUIRE(b == 0);

		// division-by-zero through the cross-type path
		i64_t c(42);
		REQUIRE_THROWS_AS(c /= uint64_t(0), InternalException);
	}

	SECTION("i64 / uint64 keeps sign and detects overflow") {
		i64_t a(-200);
		auto r = a / uint64_t(4);
		REQUIRE(r == -50);

		i64_t b(42);
		REQUIRE_THROWS_AS(b / uint64_t(0), InternalException);
	}

	SECTION("u64 op signed handles negative rhs via wide arithmetic") {
		// u64 += negative: wide add gives signed result, narrowed back to u64
		u64_t a(100);
		a += int64_t(-30);
		REQUIRE(a == 70u);
		REQUIRE_THROWS_AS(a += int64_t(-1000), InternalException);

		// u64 -= negative: wide sub yields a larger u64
		u64_t b(100);
		b -= int64_t(-30);
		REQUIRE(b == 130u);
		REQUIRE_THROWS_AS(b -= int64_t(-int64_t(U64_MAX - 50)), InternalException);

		// u64(>0) * negative still throws via the bounds check (negative result not representable)
		u64_t c(10);
		REQUIRE_THROWS_AS(c *= int64_t(-1), InternalException);

		// u64(>0) / negative still throws via the bounds check
		u64_t d(10);
		REQUIRE_THROWS_AS(d /= int64_t(-1), InternalException);
	}

	// The unified wide-arithmetic path no longer eagerly rejects "unsigned op negative"; it lets
	// the bounds check decide. So mathematically valid edge cases that produce a representable
	// non-negative result (i.e. 0) succeed, where the previous strict path would have thrown.
	SECTION("unsigned * 0-equivalent negative succeeds with 0 (behavior change)") {
		u64_t a(0);
		a *= int64_t(-1);
		REQUIRE(a == 0u);

		u32_t b(0);
		b *= int8_t(-100);
		REQUIRE(b == 0u);

		auto c = u16_t(0) * int32_t(-7);
		REQUIRE(c == 0u);
	}

	SECTION("unsigned 0 / negative succeeds with 0 (behavior change)") {
		u64_t a(0);
		a /= int64_t(-5);
		REQUIRE(a == 0u);

		auto b = u32_t(0) / int16_t(-3);
		REQUIRE(b == 0u);
	}
}

// std::atomic<CheckedInteger<T>> exposes load/store/fetch_add/fetch_sub/operator+=/-=/operator=.
// We need to verify both the happy path and that overflow/underflow from the atomic methods
// propagates the checked exception.
TEST_CASE("CheckedInteger atomic operations", "[checked_integer]") {
	SECTION("default-construct, store, load") {
		std::atomic<i64_t> a;
		a.store(i64_t(42));
		REQUIRE(a.load() == 42);
	}

	SECTION("templated operator= from raw integral literal disambiguates") {
		std::atomic<u64_t> a;
		// This used to produce an ambiguous-overload error; the templated operator=(U) resolves it.
		a = 0;
		REQUIRE(a.load() == 0u);
		a = uint32_t(123);
		REQUIRE(a.load() == 123u);

		// Out-of-range literal still fails through the constructor's ValidateAndCast
		std::atomic<u8_t> b;
		REQUIRE_THROWS_AS(b = 1000, InternalException);
	}

	SECTION("fetch_add / fetch_sub happy path returns previous value") {
		std::atomic<i32_t> a(10);
		auto prev = a.fetch_add(i32_t(5));
		REQUIRE(prev == 10);
		REQUIRE(a.load() == 15);

		auto prev2 = a.fetch_sub(i32_t(3));
		REQUIRE(prev2 == 15);
		REQUIRE(a.load() == 12);
	}

	SECTION("fetch_add overflow throws") {
		std::atomic<i32_t> a(NumericLimits<int32_t>::Maximum());
		REQUIRE_THROWS_AS(a.fetch_add(i32_t(1)), InternalException);

		std::atomic<u8_t> b(250);
		REQUIRE_THROWS_AS(b.fetch_add(u8_t(10)), InternalException);
	}

	SECTION("fetch_sub underflow throws") {
		std::atomic<i32_t> a(NumericLimits<int32_t>::Minimum());
		REQUIRE_THROWS_AS(a.fetch_sub(i32_t(1)), InternalException);

		std::atomic<u8_t> b(0);
		REQUIRE_THROWS_AS(b.fetch_sub(u8_t(1)), InternalException);
	}

	SECTION("operator+=/-= on atomic propagates overflow") {
		std::atomic<i64_t> a(NumericLimits<int64_t>::Maximum() - 5);
		REQUIRE_THROWS_AS(a += int32_t(100), InternalException);

		std::atomic<u32_t> b(5);
		REQUIRE_THROWS_AS(b -= uint32_t(100), InternalException);

		// happy path returning new value
		std::atomic<i64_t> c(100);
		auto sum = c += int8_t(7);
		REQUIRE(sum == 107);
		REQUIRE(c.load() == 107);
	}

	SECTION("custom exception type propagates through atomic") {
		using i32_range_t = CheckedInteger<int32_t, OutOfRangeException>;
		std::atomic<i32_range_t> a(NumericLimits<int32_t>::Maximum());
		REQUIRE_THROWS_AS(a.fetch_add(i32_range_t(1)), OutOfRangeException);
	}
}

// Branch B division of a narrow signed type by a wider signed type triggers the only true
// integer-division overflow case: T_MIN / -1 == -T_MIN, which doesn't fit back in T.
TEST_CASE("CheckedInteger cross-type division overflow", "[checked_integer]") {
	SECTION("i8 / int64(-1) at INT8_MIN throws") {
		i8_t a(NumericLimits<int8_t>::Minimum());
		REQUIRE_THROWS_AS(a / int64_t(-1), InternalException);
		i8_t b(NumericLimits<int8_t>::Minimum());
		REQUIRE_THROWS_AS(b /= int64_t(-1), InternalException);
	}

	SECTION("i32 / int64(-1) at INT32_MIN throws") {
		i32_t a(NumericLimits<int32_t>::Minimum());
		REQUIRE_THROWS_AS(a / int64_t(-1), InternalException);
		i32_t b(NumericLimits<int32_t>::Minimum());
		REQUIRE_THROWS_AS(b /= int64_t(-1), InternalException);
	}

	SECTION("i16 / int32(-1) at INT16_MIN throws") {
		i16_t a(NumericLimits<int16_t>::Minimum());
		REQUIRE_THROWS_AS(a / int32_t(-1), InternalException);
	}

	SECTION("i8 / int64(2) at INT8_MIN succeeds with sign preserved") {
		i8_t a(NumericLimits<int8_t>::Minimum());
		a /= int64_t(2);
		REQUIRE(a == NumericLimits<int8_t>::Minimum() / 2);
	}
}
