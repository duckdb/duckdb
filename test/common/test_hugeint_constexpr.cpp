#include "catch.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"

using namespace duckdb;

namespace hugeint_constexpr_test {

constexpr hugeint_t minimum(INT64_MIN, 0);
constexpr hugeint_t maximum(INT64_MAX, UINT64_MAX);
constexpr hugeint_t limb(1, 0);

static_assert(minimum * 1 == minimum, "minimum times one is representable");
static_assert(hugeint_t(1) * minimum == minimum, "one times minimum is representable");
static_assert(minimum * 0 == 0, "minimum times zero");
static_assert(minimum * -1 == minimum, "unchecked multiplication wraps");
static_assert(-minimum == minimum, "unchecked negation wraps");
static_assert(maximum + 1 == minimum, "unchecked addition wraps");
static_assert(minimum - 1 == maximum, "unchecked subtraction wraps");
static_assert(hugeint_t(0, UINT64_MAX) + 1 == limb, "addition carries between limbs");
static_assert(limb - 1 == hugeint_t(0, UINT64_MAX), "subtraction borrows between limbs");
static_assert(limb * limb == 0, "unchecked multiplication discards high bits");
static_assert(hugeint_t(0, UINT64_MAX) * hugeint_t(0, UINT64_MAX) == hugeint_t(-2, 1),
              "multiplication carries between limbs");
static_assert(minimum + maximum == -1, "addition with opposite signs");
static_assert(minimum < -1 && hugeint_t(-1) < 0 && hugeint_t(0) < maximum, "signed ordering");
static_assert(minimum / 10 == hugeint_t(-922337203685477581LL, 3689348814741910324ULL),
              "negative decimal parsing limit");
static_assert(maximum / 10 == hugeint_t(922337203685477580LL, 14757395258967641292ULL),
              "positive decimal parsing limit");

struct DivisionCase {
	hugeint_t lhs;
	hugeint_t rhs;
	hugeint_t quotient;
	hugeint_t remainder;
};

constexpr DivisionCase divisions[] = {
    {7, 3, 2, 1},
    {-7, 3, -2, -1},
    {7, -3, -2, 1},
    {-7, -3, 2, -1},
    {minimum, 1, minimum, 0},
    {minimum, -1, minimum, 0},
    {minimum, 2, hugeint_t(INT64_MIN / 2, 0), 0},
    {minimum, 3, hugeint_t(-3074457345618258603LL, 6148914691236517206ULL), -2},
    {minimum, maximum, -1, -1},
    {minimum, minimum, 1, 0},
    {maximum, minimum, 0, maximum},
    {maximum, maximum, 1, 0},
    {minimum, 0, 0, minimum},
    {limb, 3, hugeint_t(0, 6148914691236517205ULL), 1},
    {maximum, hugeint_t(3, 5), hugeint_t(0, 3074457345618258602ULL), hugeint_t(1, 3074457345618258605ULL)},
    {minimum, hugeint_t(3, 5), hugeint_t(-1, 15372286728091293014ULL), hugeint_t(-2, 15372286728091293010ULL)},
    {maximum, -hugeint_t(3, 5), hugeint_t(-1, 15372286728091293014ULL), hugeint_t(1, 3074457345618258605ULL)},
    {minimum, -hugeint_t(3, 5), hugeint_t(0, 3074457345618258602ULL), hugeint_t(-2, 15372286728091293010ULL)},
};

constexpr bool CheckDivisions() {
	for (const auto &test : divisions) {
		const auto result = HugeintDivMod(test.lhs, test.rhs);
		if (result.quotient != test.quotient || result.remainder != test.remainder ||
		    test.lhs / test.rhs != test.quotient) {
			return false;
		}
	}
	return true;
}

static_assert(CheckDivisions(), "constant division agrees with signed truncation and unchecked overflow rules");

struct UnsignedDivisionCase {
	uint64_t lhs_upper;
	uint64_t lhs_lower;
	uint64_t rhs_upper;
	uint64_t rhs_lower;
	Uint128DivModResult expected;
};

constexpr UnsignedDivisionCase unsigned_divisions[] = {
    {5421010862427522170ULL,
     687399551400673279ULL,
     0,
     100000000,
     {54210108624ULL, 5076944270305263615ULL, 0, 99999999}},
    {UINT64_MAX, UINT64_MAX, 1, 1, {0, UINT64_MAX, 0, 0}},
    {9223372036854775808ULL, 12345, 4294967296ULL, 7, {0, 2147483647, 4294967295ULL, 18446744058677178432ULL}},
    {542101, 1590897978359414787ULL, 0, 10000000000000000000ULL, {0, 1000000, 0, 3}},
};

constexpr bool SameResult(const Uint128DivModResult &a, const Uint128DivModResult &b) {
	return a.quotient_upper == b.quotient_upper && a.quotient_lower == b.quotient_lower &&
	       a.remainder_upper == b.remainder_upper && a.remainder_lower == b.remainder_lower;
}

constexpr bool CheckUnsignedDivisions() {
	for (const auto &test : unsigned_divisions) {
		if (!SameResult(Uint128DivModPortable(test.lhs_upper, test.lhs_lower, test.rhs_upper, test.rhs_lower),
		                test.expected) ||
		    !SameResult(Uint128DivMod(test.lhs_upper, test.lhs_lower, test.rhs_upper, test.rhs_lower), test.expected)) {
			return false;
		}
	}
	return true;
}

static_assert(CheckUnsignedDivisions(), "portable and native unsigned division agree with known results");

//! Operands around the limb, half-limb and sign boundaries, which exercise the estimate corrections of the portable
//! division
constexpr uint64_t operand_limbs[] = {0,
                                      1,
                                      2,
                                      3,
                                      7,
                                      10,
                                      0xFFFFFFFFULL,
                                      0x100000000ULL,
                                      0x100000001ULL,
                                      10000000000000000000ULL,
                                      0x7FFFFFFFFFFFFFFFULL,
                                      0x8000000000000000ULL,
                                      0x8000000000000001ULL,
                                      0xFFFFFFFF00000000ULL,
                                      0xFFFFFFFFFFFFFFFEULL,
                                      UINT64_MAX};

hugeint_t RuntimeValue(hugeint_t value) {
	volatile int64_t upper = value.upper;
	volatile uint64_t lower = value.lower;
	return hugeint_t(upper, lower);
}

} // namespace hugeint_constexpr_test

TEST_CASE("HUGEINT constexpr and runtime arithmetic agree at boundaries", "[hugeint]") {
	using namespace hugeint_constexpr_test;
	for (const auto &test : divisions) {
		const auto lhs = RuntimeValue(test.lhs);
		const auto rhs = RuntimeValue(test.rhs);
		hugeint_t remainder;
		REQUIRE(lhs / rhs == test.quotient);
		REQUIRE(Hugeint::DivMod(lhs, rhs, remainder) == test.quotient);
		REQUIRE(remainder == test.remainder);
		REQUIRE(lhs % rhs == test.remainder);
	}
	const auto min = RuntimeValue(minimum);
	const auto max = RuntimeValue(maximum);
	REQUIRE(min * 1 == min);
	REQUIRE(min * -1 == min);
	REQUIRE(-min == min);
	REQUIRE(max + 1 == min);
	REQUIRE(min - 1 == max);

	const hugeint_t operands[] = {minimum, minimum + 1, -limb, -1, 0, 1, limb, maximum};
	for (auto lhs : operands) {
		for (auto rhs : operands) {
			lhs = RuntimeValue(lhs);
			rhs = RuntimeValue(rhs);
			auto product = lhs;
			product *= rhs;
			REQUIRE(product == lhs * rhs);
		}
	}
}

TEST_CASE("Unsigned 128-bit division agrees with long division", "[hugeint]") {
	using namespace hugeint_constexpr_test;
	for (uint64_t lhs_upper : operand_limbs) {
		for (uint64_t lhs_lower : operand_limbs) {
			for (uint64_t rhs_upper : operand_limbs) {
				for (uint64_t rhs_lower : operand_limbs) {
					if (rhs_upper == 0 && rhs_lower == 0) {
						continue;
					}
					volatile uint64_t runtime_lhs_lower = lhs_lower;
					const auto result = Uint128DivModPortable(lhs_upper, runtime_lhs_lower, rhs_upper, rhs_lower);
					// the quotient and remainder fully determine the dividend when the remainder is below the divisor
					const uhugeint_t lhs(lhs_upper, lhs_lower);
					const uhugeint_t rhs(rhs_upper, rhs_lower);
					const uhugeint_t quotient(result.quotient_upper, result.quotient_lower);
					const uhugeint_t remainder(result.remainder_upper, result.remainder_lower);
					REQUIRE(remainder < rhs);
					uhugeint_t product;
					REQUIRE(Uhugeint::TryMultiply(quotient, rhs, product));
					REQUIRE(product + remainder == lhs);

					uhugeint_t runtime_remainder;
					REQUIRE(Uhugeint::DivMod(lhs, rhs, runtime_remainder) == quotient);
					REQUIRE(runtime_remainder == remainder);

					if (rhs_upper == 0) {
						const uint64_t high = lhs_upper % rhs_lower;
						const auto expected = Uint128DivMod(high, lhs_lower, 0, rhs_lower);
						uint64_t limb_remainder;
						REQUIRE(Uint128DivRem64Portable(high, lhs_lower, rhs_lower, limb_remainder) ==
						        expected.quotient_lower);
						REQUIRE(limb_remainder == expected.remainder_lower);
					}
				}
			}
		}
	}
}
