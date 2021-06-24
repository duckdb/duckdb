//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/value.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {

class Deserializer;
class Serializer;

//! The Value object holds a single arbitrary value of any type that can be
//! stored in the database.
class Value {
	friend class Vector;

public:
	//! Create an empty NULL value of the specified type
	explicit Value(LogicalType type = LogicalType::SQLNULL);
	//! Create an INTEGER value
	Value(int32_t val); // NOLINT: Allow implicit conversion from `int32_t`
	//! Create a BIGINT value
	Value(int64_t val); // NOLINT: Allow implicit conversion from `int64_t`
	//! Create a FLOAT value
	Value(float val); // NOLINT: Allow implicit conversion from `float`
	//! Create a DOUBLE value
	Value(double val); // NOLINT: Allow implicit conversion from `double`
	//! Create a VARCHAR value
	Value(const char *val); // NOLINT: Allow implicit conversion from `const char *`
	//! Create a NULL value
	Value(std::nullptr_t val); // NOLINT: Allow implicit conversion from `nullptr_t`
	//! Create a VARCHAR value
	Value(string_t val); // NOLINT: Allow implicit conversion from `string_t`
	//! Create a VARCHAR value
	Value(string val); // NOLINT: Allow implicit conversion from `string`

	const LogicalType &type() const {
		return type_;
	}

	//! Create the lowest possible value of a given type (numeric only)
	static Value MinimumValue(const LogicalType &type);
	//! Create the highest possible value of a given type (numeric only)
	static Value MaximumValue(const LogicalType &type);
	//! Create a Numeric value of the specified type with the specified value
	static Value Numeric(const LogicalType &type, int64_t value);
	static Value Numeric(const LogicalType &type, hugeint_t value);

	//! Create a tinyint Value from a specified value
	static Value BOOLEAN(int8_t value);
	//! Create a tinyint Value from a specified value
	static Value TINYINT(int8_t value);
	//! Create a smallint Value from a specified value
	static Value SMALLINT(int16_t value);
	//! Create an integer Value from a specified value
	static Value INTEGER(int32_t value);
	//! Create a bigint Value from a specified value
	static Value BIGINT(int64_t value);
	//! Create an unsigned tinyint Value from a specified value
	static Value UTINYINT(uint8_t value);
	//! Create an unsigned smallint Value from a specified value
	static Value USMALLINT(uint16_t value);
	//! Create an unsigned integer Value from a specified value
	static Value UINTEGER(uint32_t value);
	//! Create an unsigned bigint Value from a specified value
	static Value UBIGINT(uint64_t value);
	//! Create a hugeint Value from a specified value
	static Value HUGEINT(hugeint_t value);
	//! Create a hash Value from a specified value
	static Value HASH(hash_t value);
	//! Create a pointer Value from a specified value
	static Value POINTER(uintptr_t value);
	//! Create a date Value from a specified date
	static Value DATE(date_t date);
	//! Create a date Value from a specified date
	static Value DATE(int32_t year, int32_t month, int32_t day);
	//! Create a time Value from a specified time
	static Value TIME(dtime_t time);
	//! Create a time Value from a specified time
	static Value TIME(int32_t hour, int32_t min, int32_t sec, int32_t micros);
	//! Create a timestamp Value from a specified date/time combination
	static Value TIMESTAMP(date_t date, dtime_t time);
	//! Create a timestamp Value from a specified timestamp
	static Value TIMESTAMP(timestamp_t timestamp);
	static Value TimestampNs(timestamp_t timestamp);
	static Value TimestampMs(timestamp_t timestamp);
	static Value TimestampSec(timestamp_t timestamp);
	//! Create a timestamp Value from a specified timestamp in separate values
	static Value TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec,
	                       int32_t micros);
	static Value INTERVAL(int32_t months, int32_t days, int64_t micros);
	static Value INTERVAL(interval_t interval);

	// Decimal values
	static Value DECIMAL(int16_t value, uint8_t width, uint8_t scale);
	static Value DECIMAL(int32_t value, uint8_t width, uint8_t scale);
	static Value DECIMAL(int64_t value, uint8_t width, uint8_t scale);
	static Value DECIMAL(hugeint_t value, uint8_t width, uint8_t scale);
	//! Create a float Value from a specified value
	static Value FLOAT(float value);
	//! Create a double Value from a specified value
	static Value DOUBLE(double value);
	//! Create a struct value with given list of entries
	static Value STRUCT(child_list_t<Value> values);
	//! Create a list value with the given entries
	static Value LIST(vector<Value> values);
	//! Creat a map value from a (key, value) pair
	static Value MAP(Value key, Value value);

	//! Create a blob Value from a data pointer and a length: no bytes are interpreted
	static Value BLOB(const_data_ptr_t data, idx_t len);
	static Value BLOB_RAW(const string &data) {
		return Value::BLOB((const_data_ptr_t) data.c_str(), data.size());
	}
	//! Creates a blob by casting a specified string to a blob (i.e. interpreting \x characters)
	static Value BLOB(const string &data);

	template <class T>
	T GetValue() const {
		throw NotImplementedException("Unimplemented template type for Value::GetValue");
	}
	template <class T>
	static Value CreateValue(T value) {
		throw NotImplementedException("Unimplemented template type for Value::CreateValue");
	}
	// Returns the internal value. Unlike GetValue(), this method does not perform casting, and assumes T matches the
	// type of the value. Only use this if you know what you are doing.
	template <class T>
	T &GetValueUnsafe() {
		throw NotImplementedException("Unimplemented template type for Value::GetValueUnsafe");
	}

	//! Return a copy of this value
	Value Copy() const {
		return Value(*this);
	}

	//! Convert this value to a string
	DUCKDB_API string ToString() const;

	DUCKDB_API uintptr_t GetPointer() const;

	//! Cast this value to another type
	DUCKDB_API Value CastAs(const LogicalType &target_type, bool strict = false) const;
	//! Tries to cast value to another type, throws exception if its not possible
	DUCKDB_API bool TryCastAs(const LogicalType &target_type, bool strict = false);

	//! Serializes a Value to a stand-alone binary blob
	DUCKDB_API void Serialize(Serializer &serializer);
	//! Deserializes a Value from a blob
	DUCKDB_API static Value Deserialize(Deserializer &source);

	//===--------------------------------------------------------------------===//
	// Numeric Operators
	//===--------------------------------------------------------------------===//
	Value operator+(const Value &rhs) const;
	Value operator-(const Value &rhs) const;
	Value operator*(const Value &rhs) const;
	Value operator/(const Value &rhs) const;
	Value operator%(const Value &rhs) const;

	//===--------------------------------------------------------------------===//
	// Comparison Operators
	//===--------------------------------------------------------------------===//
	bool operator==(const Value &rhs) const;
	bool operator!=(const Value &rhs) const;
	bool operator<(const Value &rhs) const;
	bool operator>(const Value &rhs) const;
	bool operator<=(const Value &rhs) const;
	bool operator>=(const Value &rhs) const;

	bool operator==(const int64_t &rhs) const;
	bool operator!=(const int64_t &rhs) const;
	bool operator<(const int64_t &rhs) const;
	bool operator>(const int64_t &rhs) const;
	bool operator<=(const int64_t &rhs) const;
	bool operator>=(const int64_t &rhs) const;

	static bool FloatIsValid(float value);
	static bool DoubleIsValid(double value);

	template <class T>
	static bool IsValid(T value) {
		return true;
	}

	//! Returns true if the values are (approximately) equivalent. Note this is NOT the SQL equivalence. For this
	//! function, NULL values are equivalent and floating point values that are close are equivalent.
	static bool ValuesAreEqual(const Value &result_value, const Value &value);

	friend std::ostream &operator<<(std::ostream &out, const Value &val) {
		out << val.ToString();
		return out;
	}
	void Print();

private:
	//! The logical of the value
	LogicalType type_;

public:
	//! Whether or not the value is NULL
	bool is_null;

	//! The value of the object, if it is of a constant size Type
	union Val {
		int8_t boolean;
		int8_t tinyint;
		int16_t smallint;
		int32_t integer;
		int64_t bigint;
		uint8_t utinyint;
		uint16_t usmallint;
		uint32_t uinteger;
		uint64_t ubigint;
		hugeint_t hugeint;
		float float_;
		double double_;
		uintptr_t pointer;
		uint64_t hash;
		date_t date;
		dtime_t time;
		timestamp_t timestamp;
		interval_t interval;
	} value_;

	//! The value of the object, if it is of a variable size type
	string str_value;

	vector<Value> struct_value;
	vector<Value> list_value;

private:
	template <class T>
	T GetValueInternal() const;
	//! Templated helper function for casting
	template <class DST, class OP>
	static DST _cast(const Value &v);

	//! Templated helper function for binary operations
	template <class OP>
	static void _templated_binary_operation(const Value &left, const Value &right, Value &result, bool ignore_null);

	//! Templated helper function for boolean operations
	template <class OP>
	static bool _templated_boolean_operation(const Value &left, const Value &right);
};

template <>
Value DUCKDB_API Value::CreateValue(bool value);
template <>
Value DUCKDB_API Value::CreateValue(uint8_t value);
template <>
Value DUCKDB_API Value::CreateValue(uint16_t value);
template <>
Value DUCKDB_API Value::CreateValue(uint32_t value);
template <>
Value DUCKDB_API Value::CreateValue(uint64_t value);
template <>
Value DUCKDB_API Value::CreateValue(int8_t value);
template <>
Value DUCKDB_API Value::CreateValue(int16_t value);
template <>
Value DUCKDB_API Value::CreateValue(int32_t value);
template <>
Value DUCKDB_API Value::CreateValue(int64_t value);
template <>
Value DUCKDB_API Value::CreateValue(hugeint_t value);
template <>
Value DUCKDB_API Value::CreateValue(date_t value);
template <>
Value DUCKDB_API Value::CreateValue(dtime_t value);
template <>
Value DUCKDB_API Value::CreateValue(timestamp_t value);
template <>
Value DUCKDB_API Value::CreateValue(const char *value);
template <>
Value DUCKDB_API Value::CreateValue(string value);
template <>
Value DUCKDB_API Value::CreateValue(string_t value);
template <>
Value DUCKDB_API Value::CreateValue(float value);
template <>
Value DUCKDB_API Value::CreateValue(double value);
template <>
Value DUCKDB_API Value::CreateValue(interval_t value);
template <>
Value DUCKDB_API Value::CreateValue(Value value);

template <>
DUCKDB_API bool Value::GetValue() const;
template <>
DUCKDB_API int8_t Value::GetValue() const;
template <>
DUCKDB_API int16_t Value::GetValue() const;
template <>
DUCKDB_API int32_t Value::GetValue() const;
template <>
DUCKDB_API int64_t Value::GetValue() const;
template <>
DUCKDB_API uint8_t Value::GetValue() const;
template <>
DUCKDB_API uint16_t Value::GetValue() const;
template <>
DUCKDB_API uint32_t Value::GetValue() const;
template <>
DUCKDB_API uint64_t Value::GetValue() const;
template <>
DUCKDB_API hugeint_t Value::GetValue() const;
template <>
DUCKDB_API string Value::GetValue() const;
template <>
DUCKDB_API float Value::GetValue() const;
template <>
DUCKDB_API double Value::GetValue() const;
template <>
DUCKDB_API date_t Value::GetValue() const;
template <>
DUCKDB_API dtime_t Value::GetValue() const;
template <>
DUCKDB_API timestamp_t Value::GetValue() const;

template <>
DUCKDB_API int8_t &Value::GetValueUnsafe();
template <>
DUCKDB_API int16_t &Value::GetValueUnsafe();
template <>
DUCKDB_API int32_t &Value::GetValueUnsafe();
template <>
DUCKDB_API int64_t &Value::GetValueUnsafe();
template <>
DUCKDB_API hugeint_t &Value::GetValueUnsafe();
template <>
DUCKDB_API uint8_t &Value::GetValueUnsafe();
template <>
DUCKDB_API uint16_t &Value::GetValueUnsafe();
template <>
DUCKDB_API uint32_t &Value::GetValueUnsafe();
template <>
DUCKDB_API uint64_t &Value::GetValueUnsafe();
template <>
DUCKDB_API string &Value::GetValueUnsafe();
template <>
DUCKDB_API float &Value::GetValueUnsafe();
template <>
DUCKDB_API double &Value::GetValueUnsafe();
template <>
DUCKDB_API date_t &Value::GetValueUnsafe();
template <>
DUCKDB_API dtime_t &Value::GetValueUnsafe();
template <>
DUCKDB_API timestamp_t &Value::GetValueUnsafe();
template <>
DUCKDB_API interval_t &Value::GetValueUnsafe();

template <>
DUCKDB_API bool Value::IsValid(float value);
template <>
DUCKDB_API bool Value::IsValid(double value);

} // namespace duckdb
