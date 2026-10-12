#include "catch.hpp"
#include "duckdb/common/bignum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/string_heap.hpp"

using namespace duckdb; // NOLINT

namespace {

//! Holds the blob of a bignum so that a BignumIntermediate can point into it
struct TestBignum {
	explicit TestBignum(const string &decimal_value)
	    : blob(Bignum::VarcharToBignum(string_t(decimal_value))), value(string_t(blob)), intermediate(value) {
	}

	string blob;
	bignum_t value;
	BignumIntermediate intermediate;
};

string ToDecimal(const string_t &blob) {
	const bignum_t result(blob);
	return Bignum::BignumToVarchar(result);
}

string Multiply(const string &lhs, const string &rhs) {
	StringHeap heap;
	TestBignum l(lhs);
	TestBignum r(rhs);
	return ToDecimal(BignumIntermediate::Multiply(heap, l.intermediate, r.intermediate));
}

string Divide(const string &lhs, const string &rhs) {
	StringHeap heap;
	TestBignum l(lhs);
	TestBignum r(rhs);
	return ToDecimal(BignumIntermediate::Divide(heap, l.intermediate, r.intermediate));
}

string Modulo(const string &lhs, const string &rhs) {
	StringHeap heap;
	TestBignum l(lhs);
	TestBignum r(rhs);
	return ToDecimal(BignumIntermediate::Modulo(heap, l.intermediate, r.intermediate));
}

} // namespace

TEST_CASE("BIGNUM multiplication, division and modulo", "[bignum]") {
	REQUIRE(Multiply("6", "7") == "42");
	REQUIRE(Multiply("-6", "7") == "-42");
	REQUIRE(Multiply("0", "-7") == "0");
	REQUIRE(Multiply("123456789012345678901234567890", "987654321098765432109876543210") ==
	        "121932631137021795226185032733622923332237463801111263526900");

	REQUIRE(Divide("121932631137021795226185032733622923332237463801111263526900", "987654321098765432109876543210") ==
	        "123456789012345678901234567890");
	REQUIRE(Divide("-7", "2") == "-3");
	REQUIRE(Divide("7", "-2") == "-3");
	REQUIRE(Divide("3", "7") == "0");
	REQUIRE(Divide("-3", "7") == "0");

	REQUIRE(Modulo("-7", "2") == "-1");
	REQUIRE(Modulo("7", "-2") == "1");
	REQUIRE(Modulo("-14", "7") == "0");
	REQUIRE(Modulo("987654321098765432109876543210", "123456789012345678901234567890") == "9000000000900000000090");

	REQUIRE(TestBignum("0").intermediate.IsZero());
	REQUIRE(!TestBignum("1").intermediate.IsZero());
	REQUIRE(!TestBignum("-1").intermediate.IsZero());
}

TEST_CASE("BIGNUM division by zero is rejected", "[bignum]") {
	StringHeap heap;
	TestBignum one("1");
	TestBignum zero("0");
	REQUIRE_THROWS_AS(BignumIntermediate::Divide(heap, one.intermediate, zero.intermediate), InvalidInputException);
	REQUIRE_THROWS_AS(BignumIntermediate::Modulo(heap, one.intermediate, zero.intermediate), InvalidInputException);
}

TEST_CASE("BIGNUM multiplication overflow is detected before multiplying", "[bignum]") {
	StringHeap heap;
	TestBignum one("1");

	// two operands of (MAX_DATA_SIZE / 2 + 1) bytes that start with 0xFF have a product of MAX_DATA_SIZE + 1 bytes
	// (the schoolbook multiplication of two 4 MiB operands would take forever, so this must be caught up front)
	const idx_t data_bytes = Bignum::MAX_DATA_SIZE / 2 + 1;
	string blob(Bignum::BIGNUM_HEADER_SIZE + data_bytes, '\xFF');
	Bignum::SetHeader(&blob[0], data_bytes, false);
	const string_t blob_string(blob);
	const bignum_t value(blob_string);
	const BignumIntermediate operand(value);
	REQUIRE_THROWS_AS(BignumIntermediate::Multiply(heap, operand, operand), OutOfRangeException);

	// multiplying the same operand by one is fine and returns the operand unchanged
	auto result = BignumIntermediate::Multiply(heap, operand, one.intermediate);
	REQUIRE(result.GetSize() == blob.size());
	REQUIRE(memcmp(result.GetData(), blob.data(), blob.size()) == 0);

	// two operands of (MAX_DATA_SIZE / 2 + 2) bytes overflow regardless of their top bytes
	const idx_t small_top_bytes = Bignum::MAX_DATA_SIZE / 2 + 2;
	string small_top_blob(Bignum::BIGNUM_HEADER_SIZE + small_top_bytes, '\x01');
	Bignum::SetHeader(&small_top_blob[0], small_top_bytes, false);
	const string_t small_top_string(small_top_blob);
	const bignum_t small_top_value(small_top_string);
	const BignumIntermediate small_top(small_top_value);
	REQUIRE_THROWS_AS(BignumIntermediate::Multiply(heap, small_top, small_top), OutOfRangeException);
	REQUIRE_THROWS_AS(BignumIntermediate::Multiply(heap, small_top, operand), OutOfRangeException);
}
