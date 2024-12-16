#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test MAP getters", "[capi]") {
	auto uint_val = duckdb_create_uint64(42);
	REQUIRE(uint_val);

	auto size = duckdb_get_map_size(nullptr);
	REQUIRE(size == 0);
	size = duckdb_get_map_size(uint_val);
	REQUIRE(size == 0);

	auto key = duckdb_get_map_key(nullptr, 0);
	REQUIRE(!key);
	key = duckdb_get_map_key(uint_val, 0);
	REQUIRE(!key);

	auto value = duckdb_get_map_value(nullptr, 0);
	REQUIRE(!value);
	value = duckdb_get_map_value(uint_val, 0);
	REQUIRE(!value);

	duckdb_destroy_value(&uint_val);
}

TEST_CASE("Test LIST getters", "[capi]") {
	duckdb_value list_vals[2];
	list_vals[0] = duckdb_create_uint64(42);
	list_vals[1] = duckdb_create_uint64(43);
	duckdb_logical_type uint64_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
	duckdb_value list_value = duckdb_create_list_value(uint64_type, list_vals, 2);
	duckdb_destroy_value(&list_vals[0]);
	duckdb_destroy_value(&list_vals[1]);
	duckdb_destroy_logical_type(&uint64_type);

	auto size = duckdb_get_list_size(nullptr);
	REQUIRE(size == 0);

	size = duckdb_get_list_size(list_value);
	REQUIRE(size == 2);

	auto val = duckdb_get_list_child(nullptr, 0);
	REQUIRE(!val);
	duckdb_destroy_value(&val);

	val = duckdb_get_list_child(list_value, 0);
	REQUIRE(val);
	REQUIRE(duckdb_get_uint64(val) == 42);
	duckdb_destroy_value(&val);

	val = duckdb_get_list_child(list_value, 1);
	REQUIRE(val);
	REQUIRE(duckdb_get_uint64(val) == 43);
	duckdb_destroy_value(&val);

	val = duckdb_get_list_child(list_value, 2);
	REQUIRE(!val);
	duckdb_destroy_value(&val);

	duckdb_destroy_value(&list_value);
}

TEST_CASE("Test ENUM getters", "[capi]") {
	const char *mnames[5] = {"apple", "banana", "cherry", "orange", "elderberry"};
	duckdb_logical_type enum_type = duckdb_create_enum_type(mnames, 5);

	duckdb_value enum_val = duckdb_create_enum_value(enum_type, 2);
	REQUIRE(enum_val);

	auto val = duckdb_get_enum_value(nullptr);
	REQUIRE(val == 0);

	val = duckdb_get_enum_value(enum_val);
	REQUIRE(val == 2);

	duckdb_destroy_value(&enum_val);

	enum_val = duckdb_create_enum_value(enum_type, 4);
	REQUIRE(enum_val);

	val = duckdb_get_enum_value(enum_val);
	REQUIRE(val == 4);

	duckdb_destroy_value(&enum_val);

	enum_val = duckdb_create_enum_value(enum_type, 5);
	REQUIRE(!enum_val);

	enum_val = duckdb_create_enum_value(enum_type, 6);
	REQUIRE(!enum_val);

	duckdb_destroy_value(&enum_val);

	duckdb_destroy_logical_type(&enum_type);
}

TEST_CASE("Test STRUCT getters", "[capi]") {
	duckdb_logical_type mtypes[2] = {duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT),
	                                 duckdb_create_logical_type(DUCKDB_TYPE_BIGINT)};
	const char *mnames[2] = {"a", "b"};
	duckdb_logical_type struct_type = duckdb_create_struct_type(mtypes, mnames, 2);
	duckdb_destroy_logical_type(&mtypes[0]);
	duckdb_destroy_logical_type(&mtypes[1]);

	duckdb_value svals[2] = {duckdb_create_uint64(42), duckdb_create_int64(-42)};
	duckdb_value struct_val = duckdb_create_struct_value(struct_type, svals);
	duckdb_destroy_logical_type(&struct_type);
	duckdb_destroy_value(&svals[0]);
	duckdb_destroy_value(&svals[1]);

	auto val = duckdb_get_struct_child(nullptr, 0);
	REQUIRE(!val);

	val = duckdb_get_struct_child(struct_val, 0);
	REQUIRE(val);
	REQUIRE(duckdb_get_uint64(val) == 42);
	duckdb_destroy_value(&val);

	val = duckdb_get_struct_child(struct_val, 1);
	REQUIRE(val);
	REQUIRE(duckdb_get_int64(val) == -42);
	duckdb_destroy_value(&val);

	val = duckdb_get_struct_child(struct_val, 2);
	REQUIRE(!val);

	duckdb_destroy_value(&struct_val);
}

TEST_CASE("Test NULL value", "[capi]") {
	auto null_value = duckdb_create_null_value();
	REQUIRE(null_value);

	REQUIRE(!duckdb_is_null_value(nullptr));
	auto uint_val = duckdb_create_uint64(42);
	REQUIRE(!duckdb_is_null_value(uint_val));
	REQUIRE(duckdb_is_null_value(null_value));

	duckdb_destroy_value(&uint_val);
	duckdb_destroy_value(&null_value);
}

TEST_CASE("Test VARINT value", "[capi]") {
	{
		uint8_t data[] {0};
		duckdb_varint input {data, 1, false};
		auto value = duckdb_create_varint(input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(value)) == DUCKDB_TYPE_VARINT);
		auto output = duckdb_get_varint(value);
		REQUIRE(output.is_negative == input.is_negative);
		REQUIRE(output.size == input.size);
		REQUIRE_FALSE(memcmp(output.data, input.data, input.size));
		duckdb_free(output.data);
		duckdb_destroy_value(&value);
	}
	{
		uint8_t data[] {1};
		duckdb_varint input {data, 1, true};
		auto value = duckdb_create_varint(input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(value)) == DUCKDB_TYPE_VARINT);
		auto output = duckdb_get_varint(value);
		REQUIRE(output.is_negative == input.is_negative);
		REQUIRE(output.size == input.size);
		REQUIRE_FALSE(memcmp(output.data, input.data, input.size));
		duckdb_free(output.data);
		duckdb_destroy_value(&value);
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
		duckdb_varint input {data, 128, false};
		auto value = duckdb_create_varint(input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(value)) == DUCKDB_TYPE_VARINT);
		auto output = duckdb_get_varint(value);
		REQUIRE(output.is_negative == input.is_negative);
		REQUIRE(output.size == input.size);
		REQUIRE_FALSE(memcmp(output.data, input.data, input.size));
		duckdb_free(output.data);
		duckdb_destroy_value(&value);
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
		duckdb_varint input {data, 128, true};
		auto value = duckdb_create_varint(input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(value)) == DUCKDB_TYPE_VARINT);
		auto output = duckdb_get_varint(value);
		REQUIRE(output.is_negative == input.is_negative);
		REQUIRE(output.size == input.size);
		REQUIRE_FALSE(memcmp(output.data, input.data, input.size));
		duckdb_free(output.data);
		duckdb_destroy_value(&value);
	}
}

TEST_CASE("Test DECIMAL value", "[capi]") {
	{
		auto hugeint = Hugeint::POWERS_OF_TEN[4] - hugeint_t(1);
		duckdb_decimal input {4, 1, {hugeint.lower, hugeint.upper}};
		auto value = duckdb_create_decimal(input);
		auto type = duckdb_get_value_type(value);
		REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_DECIMAL);
		REQUIRE(duckdb_decimal_width(type) == input.width);
		REQUIRE(duckdb_decimal_scale(type) == input.scale);
		REQUIRE(duckdb_decimal_internal_type(type) == DUCKDB_TYPE_SMALLINT);
		auto output = duckdb_get_decimal(value);
		REQUIRE(output.width == input.width);
		REQUIRE(output.scale == input.scale);
		REQUIRE(output.value.lower == input.value.lower);
		REQUIRE(output.value.upper == input.value.upper);
		duckdb_destroy_value(&value);
	}
	{
		auto hugeint = -(Hugeint::POWERS_OF_TEN[4] - hugeint_t(1));
		duckdb_decimal input {4, 1, {hugeint.lower, hugeint.upper}};
		auto value = duckdb_create_decimal(input);
		auto type = duckdb_get_value_type(value);
		REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_DECIMAL);
		REQUIRE(duckdb_decimal_width(type) == input.width);
		REQUIRE(duckdb_decimal_scale(type) == input.scale);
		REQUIRE(duckdb_decimal_internal_type(type) == DUCKDB_TYPE_SMALLINT);
		auto output = duckdb_get_decimal(value);
		REQUIRE(output.width == input.width);
		REQUIRE(output.scale == input.scale);
		REQUIRE(output.value.lower == input.value.lower);
		REQUIRE(output.value.upper == input.value.upper);
		duckdb_destroy_value(&value);
	}
	{
		auto hugeint = Hugeint::POWERS_OF_TEN[9] - hugeint_t(1);
		duckdb_decimal input {9, 4, {hugeint.lower, hugeint.upper}};
		auto value = duckdb_create_decimal(input);
		auto type = duckdb_get_value_type(value);
		REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_DECIMAL);
		REQUIRE(duckdb_decimal_width(type) == input.width);
		REQUIRE(duckdb_decimal_scale(type) == input.scale);
		REQUIRE(duckdb_decimal_internal_type(type) == DUCKDB_TYPE_INTEGER);
		auto output = duckdb_get_decimal(value);
		REQUIRE(output.width == input.width);
		REQUIRE(output.scale == input.scale);
		REQUIRE(output.value.lower == input.value.lower);
		REQUIRE(output.value.upper == input.value.upper);
		duckdb_destroy_value(&value);
	}
	{
		auto hugeint = -(Hugeint::POWERS_OF_TEN[9] - hugeint_t(1));
		duckdb_decimal input {9, 4, {hugeint.lower, hugeint.upper}};
		auto value = duckdb_create_decimal(input);
		auto type = duckdb_get_value_type(value);
		REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_DECIMAL);
		REQUIRE(duckdb_decimal_width(type) == input.width);
		REQUIRE(duckdb_decimal_scale(type) == input.scale);
		REQUIRE(duckdb_decimal_internal_type(type) == DUCKDB_TYPE_INTEGER);
		auto output = duckdb_get_decimal(value);
		REQUIRE(output.width == input.width);
		REQUIRE(output.scale == input.scale);
		REQUIRE(output.value.lower == input.value.lower);
		REQUIRE(output.value.upper == input.value.upper);
		duckdb_destroy_value(&value);
	}
	{
		auto hugeint = Hugeint::POWERS_OF_TEN[18] - hugeint_t(1);
		duckdb_decimal input {18, 6, {hugeint.lower, hugeint.upper}};
		auto value = duckdb_create_decimal(input);
		auto type = duckdb_get_value_type(value);
		REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_DECIMAL);
		REQUIRE(duckdb_decimal_width(type) == input.width);
		REQUIRE(duckdb_decimal_scale(type) == input.scale);
		REQUIRE(duckdb_decimal_internal_type(type) == DUCKDB_TYPE_BIGINT);
		auto output = duckdb_get_decimal(value);
		REQUIRE(output.width == input.width);
		REQUIRE(output.scale == input.scale);
		REQUIRE(output.value.lower == input.value.lower);
		REQUIRE(output.value.upper == input.value.upper);
		duckdb_destroy_value(&value);
	}
	{
		auto hugeint = -(Hugeint::POWERS_OF_TEN[18] - hugeint_t(1));
		duckdb_decimal input {18, 8, {hugeint.lower, hugeint.upper}};
		auto value = duckdb_create_decimal(input);
		auto type = duckdb_get_value_type(value);
		REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_DECIMAL);
		REQUIRE(duckdb_decimal_width(type) == input.width);
		REQUIRE(duckdb_decimal_scale(type) == input.scale);
		REQUIRE(duckdb_decimal_internal_type(type) == DUCKDB_TYPE_BIGINT);
		auto output = duckdb_get_decimal(value);
		REQUIRE(output.width == input.width);
		REQUIRE(output.scale == input.scale);
		REQUIRE(output.value.lower == input.value.lower);
		REQUIRE(output.value.upper == input.value.upper);
		duckdb_destroy_value(&value);
	}
	{
		auto hugeint = Hugeint::POWERS_OF_TEN[38] - hugeint_t(1);
		duckdb_decimal input {38, 10, {hugeint.lower, hugeint.upper}};
		auto value = duckdb_create_decimal(input);
		auto type = duckdb_get_value_type(value);
		REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_DECIMAL);
		REQUIRE(duckdb_decimal_width(type) == input.width);
		REQUIRE(duckdb_decimal_scale(type) == input.scale);
		REQUIRE(duckdb_decimal_internal_type(type) == DUCKDB_TYPE_HUGEINT);
		auto output = duckdb_get_decimal(value);
		REQUIRE(output.width == input.width);
		REQUIRE(output.scale == input.scale);
		REQUIRE(output.value.lower == input.value.lower);
		REQUIRE(output.value.upper == input.value.upper);
		duckdb_destroy_value(&value);
	}
	{
		auto hugeint = -(Hugeint::POWERS_OF_TEN[38] - hugeint_t(1));
		duckdb_decimal input {38, 10, {hugeint.lower, hugeint.upper}};
		auto value = duckdb_create_decimal(input);
		auto type = duckdb_get_value_type(value);
		REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_DECIMAL);
		REQUIRE(duckdb_decimal_width(type) == input.width);
		REQUIRE(duckdb_decimal_scale(type) == input.scale);
		REQUIRE(duckdb_decimal_internal_type(type) == DUCKDB_TYPE_HUGEINT);
		auto output = duckdb_get_decimal(value);
		REQUIRE(output.width == input.width);
		REQUIRE(output.scale == input.scale);
		REQUIRE(output.value.lower == input.value.lower);
		REQUIRE(output.value.upper == input.value.upper);
		duckdb_destroy_value(&value);
	}
}

TEST_CASE("Test BIT value", "[capi]") {
	{
		uint8_t data[] {5, 0xf9, 0x56}; // 0b11111001 0b01010110
		duckdb_bit input {data, 3};
		auto value = duckdb_create_bit(input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(value)) == DUCKDB_TYPE_BIT);
		auto output = duckdb_get_bit(value);
		REQUIRE(output.size == input.size);
		REQUIRE_FALSE(memcmp(output.data, input.data, input.size));
		duckdb_free(output.data);
		duckdb_destroy_value(&value);
	}
	{
		uint8_t data[] {0, 0x00};
		duckdb_bit input {data, 2};
		auto value = duckdb_create_bit(input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(value)) == DUCKDB_TYPE_BIT);
		auto output = duckdb_get_bit(value);
		REQUIRE(output.size == input.size);
		REQUIRE_FALSE(memcmp(output.data, input.data, input.size));
		duckdb_free(output.data);
		duckdb_destroy_value(&value);
	}
}

TEST_CASE("Test UUID value", "[capi]") {
	{
		duckdb_uhugeint uhugeint_input {0x0000000000000000, 0x0000000000000000};
		auto uuid_value = duckdb_create_uuid(uhugeint_input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(uuid_value)) == DUCKDB_TYPE_UUID);
		auto uhugeint_output = duckdb_get_uuid(uuid_value);
		REQUIRE(uhugeint_output.lower == uhugeint_input.lower);
		REQUIRE(uhugeint_output.upper == uhugeint_input.upper);
		duckdb_destroy_value(&uuid_value);
	}
	{
		duckdb_uhugeint uhugeint_input {0x0000000000000001, 0x0000000000000000};
		auto uuid_value = duckdb_create_uuid(uhugeint_input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(uuid_value)) == DUCKDB_TYPE_UUID);
		auto uhugeint_output = duckdb_get_uuid(uuid_value);
		REQUIRE(uhugeint_output.lower == uhugeint_input.lower);
		REQUIRE(uhugeint_output.upper == uhugeint_input.upper);
		duckdb_destroy_value(&uuid_value);
	}
	{
		duckdb_uhugeint uhugeint_input {0xffffffffffffffff, 0xffffffffffffffff};
		auto uuid_value = duckdb_create_uuid(uhugeint_input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(uuid_value)) == DUCKDB_TYPE_UUID);
		auto uhugeint_output = duckdb_get_uuid(uuid_value);
		REQUIRE(uhugeint_output.lower == uhugeint_input.lower);
		REQUIRE(uhugeint_output.upper == uhugeint_input.upper);
		duckdb_destroy_value(&uuid_value);
	}
	{
		duckdb_uhugeint uhugeint_input {0xfffffffffffffffe, 0xffffffffffffffff};
		auto uuid_value = duckdb_create_uuid(uhugeint_input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(uuid_value)) == DUCKDB_TYPE_UUID);
		auto uhugeint_output = duckdb_get_uuid(uuid_value);
		REQUIRE(uhugeint_output.lower == uhugeint_input.lower);
		REQUIRE(uhugeint_output.upper == uhugeint_input.upper);
		duckdb_destroy_value(&uuid_value);
	}
	{
		duckdb_uhugeint uhugeint_input {0xffffffffffffffff, 0x8fffffffffffffff};
		auto uuid_value = duckdb_create_uuid(uhugeint_input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(uuid_value)) == DUCKDB_TYPE_UUID);
		auto uhugeint_output = duckdb_get_uuid(uuid_value);
		REQUIRE(uhugeint_output.lower == uhugeint_input.lower);
		REQUIRE(uhugeint_output.upper == uhugeint_input.upper);
		duckdb_destroy_value(&uuid_value);
	}
	{
		duckdb_uhugeint uhugeint_input {0x0000000000000000, 0x7000000000000000};
		auto uuid_value = duckdb_create_uuid(uhugeint_input);
		REQUIRE(duckdb_get_type_id(duckdb_get_value_type(uuid_value)) == DUCKDB_TYPE_UUID);
		auto uhugeint_output = duckdb_get_uuid(uuid_value);
		REQUIRE(uhugeint_output.lower == uhugeint_input.lower);
		REQUIRE(uhugeint_output.upper == uhugeint_input.upper);
		duckdb_destroy_value(&uuid_value);
	}
}
