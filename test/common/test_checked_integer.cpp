#include "catch.hpp"
#include "duckdb/common/checked_integer.hpp"

using namespace duckdb;

TEST_CASE("Checked integer increment/decrement overflow", "[checked_integer]") {
	// signed i64_t overflow on ++
	i64_t max_val(NumericLimits<int64_t>::Maximum());
	REQUIRE_THROWS_AS(++max_val, InternalException);
	REQUIRE_THROWS_AS(max_val++, InternalException);

	// signed i64_t underflow on --
	i64_t min_val(NumericLimits<int64_t>::Minimum());
	REQUIRE_THROWS_AS(--min_val, InternalException);
	REQUIRE_THROWS_AS(min_val--, InternalException);

	// unsigned u8_t overflow on ++
	u8_t u8_max(NumericLimits<uint8_t>::Maximum());
	REQUIRE_THROWS_AS(++u8_max, InternalException);
	REQUIRE_THROWS_AS(u8_max++, InternalException);

	// unsigned u8_t underflow on --
	u8_t u8_min(NumericLimits<uint8_t>::Minimum());
	REQUIRE_THROWS_AS(--u8_min, InternalException);
	REQUIRE_THROWS_AS(u8_min--, InternalException);

	// valid increment/decrement should work
	i32_t val(100);
	++val;
	REQUIRE(val == 101);
	val--;
	REQUIRE(val == 100);
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

TEST_CASE("CheckedInteger mixed type arithmetic", "[checked_integer]") {
	// int8_t * double -> int8_t (as per user request)
	i8_t a(10);
	auto b = a * 2.5;  // Should cast 2.5 to int8_t (2) and multiply
	REQUIRE(b.GetValue() == 20);

	// int8_t + double
	auto c = a + 1.7;  // Casts to 1
	REQUIRE(c.GetValue() == 11);

	// Compound assignment with different type
	i16_t d(100);
	d += 50.9;  // Casts to 50
	REQUIRE(d.GetValue() == 150);

	// int64_t - float
	i64_t e(1000);
	auto f = e - 123.9f;  // Casts to 123
	REQUIRE(f.GetValue() == 877);

	// uint32_t / int
	u32_t g(100);
	auto h = g / 3;  // Casts int(3) to uint32_t
	REQUIRE(h.GetValue() == 33u);

	// Compound assignment: int8_t *= double
	i8_t i(5);
	i *= 3.9;  // Casts to 3
	REQUIRE(i.GetValue() == 15);
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
	REQUIRE_THROWS_AS(x -= 100, InternalException);  // underflow caught by checked sub

	// Mixed: uint32_t * negative double truncates -2.5 to uint32_t, wraps → overflow
	u32_t y(10);
	REQUIRE_THROWS_AS(y *= -2.5, InternalException);

	// Conforms to normal C++ arithmetic: uint16_t(9) -= -10 → 19
	u16_t z(9);
	z -= -10;
	REQUIRE(z.GetValue() == 19u);
}

#define CHECK_MATCHES_NATIVE(checked_val, native_expr) \
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
