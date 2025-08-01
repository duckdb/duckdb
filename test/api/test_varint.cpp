#include "test_helpers.hpp"
#include "duckdb/common/types/varint.hpp"
#include "catch.hpp"

using namespace duckdb;
using namespace std;

void PrintBits(const char value) {
	for (int i = 7; i >= 0; --i) {
		std::cout << ((value >> i) & 1);
	}
}

TEST_CASE("Test Varint Overflow and Underflow", "[api]") {
	// Lets generate the max possible varint
	duckdb::vector<uint8_t> data;
	idx_t total_data_size = Varint::VARINT_HEADER_SIZE + Varint::MAX_DATA_SIZE;
	data.resize(total_data_size);
	// Let's set our header
	Varint::SetHeader(reinterpret_cast<char *>(data.data()), Varint::MAX_DATA_SIZE, false);
	// Let's verify our header is properly set
	for (idx_t i = 0; i < Varint::VARINT_HEADER_SIZE; i++) {
		// All bits must be set in our header
		REQUIRE(data[i] == 0xFF);
	}

	// Set all our other bits
	for (idx_t i = Varint::VARINT_HEADER_SIZE; i < total_data_size; i++) {
		data[i] = 0xFF;
	}

	duckdb::vector<uint8_t> value_one;
	idx_t value_one_data_size = Varint::VARINT_HEADER_SIZE + 1;
	value_one.resize(value_one_data_size);
	// Let's set our header
	Varint::SetHeader(reinterpret_cast<char *>(value_one.data()), 1, false);
	// Set all our other bits
	value_one[Varint::VARINT_HEADER_SIZE] = 1;

	Vector result(LogicalType::VARINT);

	{
		VarintIntermediate lhs(data.data(), total_data_size);
		VarintIntermediate rhs(value_one.data(), value_one_data_size);
		REQUIRE_THROWS(VarintIntermediate::Add(result, lhs, rhs));
	}

	// Now let's try the underflow
	Varint::SetHeader(reinterpret_cast<char *>(data.data()), Varint::MAX_DATA_SIZE, true);
	// Let's verify our header is properly set
	for (idx_t i = 0; i < Varint::VARINT_HEADER_SIZE; i++) {
		// All bits must be set in our header
		REQUIRE(data[i] == 0x00);
	}
	// Set all our other bits
	for (idx_t i = Varint::VARINT_HEADER_SIZE; i < total_data_size; i++) {
		data[i] = 0x00;
	}

	duckdb::vector<uint8_t> value_minus_one;
	value_minus_one.resize(value_one_data_size);
	// Let's set our header
	Varint::SetHeader(reinterpret_cast<char *>(value_minus_one.data()), 1, true);
	// Set our one bit
	value_minus_one[Varint::VARINT_HEADER_SIZE] = 1;
	value_minus_one[Varint::VARINT_HEADER_SIZE] = ~value_one[Varint::VARINT_HEADER_SIZE];
	{
		VarintIntermediate lhs(data.data(), total_data_size);
		VarintIntermediate rhs(value_minus_one.data(), value_one_data_size);
		REQUIRE_THROWS(VarintIntermediate::Add(result, lhs, rhs));
	}
}

TEST_CASE("Test Varint::FromByteArray", "[api]") {
	{ // 0
		uint8_t data[] {0};
		idx_t size = 1;
		bool is_negative = false;
		auto str = Varint::FromByteArray(data, size, is_negative);
		REQUIRE(str.size() == size + 3);
		REQUIRE(uint8_t(str[0]) == 0x80);
		REQUIRE(uint8_t(str[1]) == 0x00);
		REQUIRE(uint8_t(str[2]) == uint8_t(size));
		for (idx_t i = 0; i < size; i++) {
			REQUIRE(uint8_t(str[3 + i]) == data[i]);
		}
	}
	{ // -1
		uint8_t data[] {1};
		idx_t size = 1;
		bool is_negative = true;
		auto str = Varint::FromByteArray(data, size, is_negative);
		REQUIRE(str.size() == size + 3);
		REQUIRE(uint8_t(str[0]) == 0x7f);
		REQUIRE(uint8_t(str[1]) == 0xff);
		REQUIRE(uint8_t(str[2]) == uint8_t(~size));
		for (idx_t i = 0; i < size; i++) {
			REQUIRE(uint8_t(str[3 + i]) == uint8_t(~data[i]));
		}
	}
	{ // max varint == max double == 2^1023 * (1 + (1 − 2^−52)) == 2^1024 - 2^971 ==
		// 179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368
		uint8_t data[] {
		    // little endian
		    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		};
		idx_t size = 128;
		bool is_negative = false;
		auto str = Varint::FromByteArray(data, size, is_negative);
		REQUIRE(str.size() == size + 3);
		REQUIRE(uint8_t(str[0]) == 0x80);
		REQUIRE(uint8_t(str[1]) == 0x00);
		REQUIRE(uint8_t(str[2]) == uint8_t(size));
		for (idx_t i = 0; i < size; i++) {
			REQUIRE(uint8_t(str[3 + i]) == data[i]);
		}
	}
	{ // min varint == min double == -(2^1023 * (1 + (1 − 2^−52))) == -(2^1024 - 2^971) ==
		// -179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368
		uint8_t data[] {
		    // little endian (absolute value)
		    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		};
		idx_t size = 128;
		bool is_negative = true;
		auto str = Varint::FromByteArray(data, size, is_negative);
		REQUIRE(str.size() == size + 3);
		REQUIRE(uint8_t(str[0]) == 0x7f);
		REQUIRE(uint8_t(str[1]) == 0xff);
		REQUIRE(uint8_t(str[2]) == uint8_t(~size));
		for (idx_t i = 0; i < size; i++) {
			REQUIRE(uint8_t(str[3 + i]) == uint8_t(~data[i]));
		}
	}
}
