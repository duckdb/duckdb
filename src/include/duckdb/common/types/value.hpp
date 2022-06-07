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
	friend struct StringValue;
	friend struct StructValue;
	friend struct ListValue;

public:
	//! Create an empty NULL value of the specified type
	DUCKDB_API explicit Value(LogicalType type = LogicalType::SQLNULL);
	//! Create an INTEGER value
	DUCKDB_API Value(int32_t val); // NOLINT: Allow implicit conversion from `int32_t`
	//! Create a BIGINT value
	DUCKDB_API Value(int64_t val); // NOLINT: Allow implicit conversion from `int64_t`
	//! Create a FLOAT value
	DUCKDB_API Value(float val); // NOLINT: Allow implicit conversion from `float`
	//! Create a DOUBLE value
	DUCKDB_API Value(double val); // NOLINT: Allow implicit conversion from `double`
	//! Create a VARCHAR value
	DUCKDB_API Value(const char *val); // NOLINT: Allow implicit conversion from `const char *`
	//! Create a NULL value
	DUCKDB_API Value(std::nullptr_t val); // NOLINT: Allow implicit conversion from `nullptr_t`
	//! Create a VARCHAR value
	DUCKDB_API Value(string_t val); // NOLINT: Allow implicit conversion from `string_t`
	//! Create a VARCHAR value
	DUCKDB_API Value(string val); // NOLINT: Allow implicit conversion from `string`
	//! Copy constructor
	DUCKDB_API Value(const Value &other);
	//! Move constructor
	DUCKDB_API Value(Value &&other) noexcept;
	//! Destructor
	DUCKDB_API ~Value();

	// copy assignment
	DUCKDB_API Value &operator=(const Value &other);
	// move assignment
	DUCKDB_API Value &operator=(Value &&other) noexcept;

	inline const LogicalType &type() const {
		return type_;
	}
	inline bool IsNull() const {
		return is_null;
	}

	//! Create the lowest possible value of a given type (numeric only)
	DUCKDB_API static Value MinimumValue(const LogicalType &type);
	//! Create the highest possible value of a given type (numeric only)
	DUCKDB_API static Value MaximumValue(const LogicalType &type);
	//! Create a Numeric value of the specified type with the specified value
	DUCKDB_API static Value Numeric(const LogicalType &type, int64_t value);
	DUCKDB_API static Value Numeric(const LogicalType &type, hugeint_t value);

	//! Create a tinyint Value from a specified value
	DUCKDB_API static Value BOOLEAN(int8_t value);
	//! Create a tinyint Value from a specified value
	DUCKDB_API static Value TINYINT(int8_t value);
	//! Create a smallint Value from a specified value
	DUCKDB_API static Value SMALLINT(int16_t value);
	//! Create an integer Value from a specified value
	DUCKDB_API static Value INTEGER(int32_t value);
	//! Create a bigint Value from a specified value
	DUCKDB_API static Value BIGINT(int64_t value);
	//! Create an unsigned tinyint Value from a specified value
	DUCKDB_API static Value UTINYINT(uint8_t value);
	//! Create an unsigned smallint Value from a specified value
	DUCKDB_API static Value USMALLINT(uint16_t value);
	//! Create an unsigned integer Value from a specified value
	DUCKDB_API static Value UINTEGER(uint32_t value);
	//! Create an unsigned bigint Value from a specified value
	DUCKDB_API static Value UBIGINT(uint64_t value);
	//! Create a hugeint Value from a specified value
	DUCKDB_API static Value HUGEINT(hugeint_t value);
	//! Create a uuid Value from a specified value
	DUCKDB_API static Value UUID(const string &value);
	//! Create a uuid Value from a specified value
	DUCKDB_API static Value UUID(hugeint_t value);
	//! Create a hash Value from a specified value
	DUCKDB_API static Value HASH(hash_t value);
	//! Create a pointer Value from a specified value
	DUCKDB_API static Value POINTER(uintptr_t value);
	//! Create a date Value from a specified date
	DUCKDB_API static Value DATE(date_t date);
	//! Create a date Value from a specified date
	DUCKDB_API static Value DATE(int32_t year, int32_t month, int32_t day);
	//! Create a time Value from a specified time
	DUCKDB_API static Value TIME(dtime_t time);
	DUCKDB_API static Value TIMETZ(dtime_t time);
	//! Create a time Value from a specified time
	DUCKDB_API static Value TIME(int32_t hour, int32_t min, int32_t sec, int32_t micros);
	//! Create a timestamp Value from a specified date/time combination
	DUCKDB_API static Value TIMESTAMP(date_t date, dtime_t time);
	//! Create a timestamp Value from a specified timestamp
	DUCKDB_API static Value TIMESTAMP(timestamp_t timestamp);
	DUCKDB_API static Value TIMESTAMPNS(timestamp_t timestamp);
	DUCKDB_API static Value TIMESTAMPMS(timestamp_t timestamp);
	DUCKDB_API static Value TIMESTAMPSEC(timestamp_t timestamp);
	DUCKDB_API static Value TIMESTAMPTZ(timestamp_t timestamp);
	//! Create a timestamp Value from a specified timestamp in separate values
	DUCKDB_API static Value TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec,
	                                  int32_t micros);
	DUCKDB_API static Value INTERVAL(int32_t months, int32_t days, int64_t micros);
	DUCKDB_API static Value INTERVAL(interval_t interval);
	//! Creates a JSON Value
	DUCKDB_API static Value JSON(const char *val);
	DUCKDB_API static Value JSON(string_t val);
	DUCKDB_API static Value JSON(string val);

	// Create a enum Value from a specified uint value
	DUCKDB_API static Value ENUM(uint64_t value, const LogicalType &original_type);

	// Decimal values
	DUCKDB_API static Value DECIMAL(int16_t value, uint8_t width, uint8_t scale);
	DUCKDB_API static Value DECIMAL(int32_t value, uint8_t width, uint8_t scale);
	DUCKDB_API static Value DECIMAL(int64_t value, uint8_t width, uint8_t scale);
	DUCKDB_API static Value DECIMAL(hugeint_t value, uint8_t width, uint8_t scale);
	//! Create a float Value from a specified value
	DUCKDB_API static Value FLOAT(float value);
	//! Create a double Value from a specified value
	DUCKDB_API static Value DOUBLE(double value);
	//! Create a struct value with given list of entries
	DUCKDB_API static Value STRUCT(child_list_t<Value> values);
	//! Create a list value with the given entries, list type is inferred from children
	//! Cannot be called with an empty list, use either EMPTYLIST or LIST with a type instead
	DUCKDB_API static Value LIST(vector<Value> values);
	//! Create a list value with the given entries
	DUCKDB_API static Value LIST(LogicalType child_type, vector<Value> values);
	//! Create an empty list with the specified child-type
	DUCKDB_API static Value EMPTYLIST(LogicalType child_type);
	//! Create a map value from a (key, value) pair
	DUCKDB_API static Value MAP(Value key, Value value);

	//! Create a blob Value from a data pointer and a length: no bytes are interpreted
	DUCKDB_API static Value BLOB(const_data_ptr_t data, idx_t len);
	DUCKDB_API static Value BLOB_RAW(const string &data) {
		return Value::BLOB((const_data_ptr_t)data.c_str(), data.size());
	}
	//! Creates a blob by casting a specified string to a blob (i.e. interpreting \x characters)
	DUCKDB_API static Value BLOB(const string &data);

	template <class T>
	T GetValue() const {
		throw InternalException("Unimplemented template type for Value::GetValue");
	}
	template <class T>
	static Value CreateValue(T value) {
		throw InternalException("Unimplemented template type for Value::CreateValue");
	}
	// Returns the internal value. Unlike GetValue(), this method does not perform casting, and assumes T matches the
	// type of the value. Only use this if you know what you are doing.
	template <class T>
	T GetValueUnsafe() const {
		throw InternalException("Unimplemented template type for Value::GetValueUnsafe");
	}
	//! Returns a reference to the internal value. This can only be used for primitive types.
	template <class T>
	T &GetReferenceUnsafe() {
		throw InternalException("Unimplemented template type for Value::GetReferenceUnsafe");
	}

	//! Return a copy of this value
	Value Copy() const {
		return Value(*this);
	}

	//! Hashes the Value
	DUCKDB_API hash_t Hash() const;
	//! Convert this value to a string
	DUCKDB_API string ToString() const;
	//! Convert this value to a SQL-parseable string
	DUCKDB_API string ToSQLString() const;

	DUCKDB_API uintptr_t GetPointer() const;

	//! Cast this value to another type, throws exception if its not possible
	DUCKDB_API Value CastAs(const LogicalType &target_type, bool strict = false) const;
	//! Tries to cast this value to another type, and stores the result in "new_value"
	DUCKDB_API bool TryCastAs(const LogicalType &target_type, Value &new_value, string *error_message,
	                          bool strict = false) const;
	//! Tries to cast this value to another type, and stores the result in THIS value again
	DUCKDB_API bool TryCastAs(const LogicalType &target_type, bool strict = false);

	//! Serializes a Value to a stand-alone binary blob
	DUCKDB_API void Serialize(Serializer &serializer) const;
	//! Deserializes a Value from a blob
	DUCKDB_API static Value Deserialize(Deserializer &source);

	//===--------------------------------------------------------------------===//
	// Comparison Operators
	//===--------------------------------------------------------------------===//
	DUCKDB_API bool operator==(const Value &rhs) const;
	DUCKDB_API bool operator!=(const Value &rhs) const;
	DUCKDB_API bool operator<(const Value &rhs) const;
	DUCKDB_API bool operator>(const Value &rhs) const;
	DUCKDB_API bool operator<=(const Value &rhs) const;
	DUCKDB_API bool operator>=(const Value &rhs) const;

	DUCKDB_API bool operator==(const int64_t &rhs) const;
	DUCKDB_API bool operator!=(const int64_t &rhs) const;
	DUCKDB_API bool operator<(const int64_t &rhs) const;
	DUCKDB_API bool operator>(const int64_t &rhs) const;
	DUCKDB_API bool operator<=(const int64_t &rhs) const;
	DUCKDB_API bool operator>=(const int64_t &rhs) const;

	DUCKDB_API static bool FloatIsFinite(float value);
	DUCKDB_API static bool DoubleIsFinite(double value);
	template <class T>
	static bool IsNan(T value) {
		throw InternalException("Unimplemented template type for Value::IsNan");
	}
	template <class T>
	static bool IsFinite(T value) {
		return true;
	}
	DUCKDB_API static bool StringIsValid(const char *str, idx_t length);
	static bool StringIsValid(const string &str) {
		return StringIsValid(str.c_str(), str.size());
	}

	//! Returns true if the values are (approximately) equivalent. Note this is NOT the SQL equivalence. For this
	//! function, NULL values are equivalent and floating point values that are close are equivalent.
	DUCKDB_API static bool ValuesAreEqual(const Value &result_value, const Value &value);
	//! Returns true if the values are not distinct from each other, following SQL semantics for NOT DISTINCT FROM.
	DUCKDB_API static bool NotDistinctFrom(const Value &lvalue, const Value &rvalue);

	friend std::ostream &operator<<(std::ostream &out, const Value &val) {
		out << val.ToString();
		return out;
	}
	DUCKDB_API void Print() const;

private:
	//! The logical of the value
	LogicalType type_;

#if DUCKDB_API_VERSION < DUCKDB_API_0_3_2
public:
#endif
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
};

//===--------------------------------------------------------------------===//
// Type-specific getters
//===--------------------------------------------------------------------===//
// Note that these are equivalent to calling GetValueUnsafe<X>, meaning no cast will be performed
// instead, an assertion will be triggered if the value is not of the correct type
struct BooleanValue {
	DUCKDB_API static bool Get(const Value &value);
};

struct TinyIntValue {
	DUCKDB_API static int8_t Get(const Value &value);
};

struct SmallIntValue {
	DUCKDB_API static int16_t Get(const Value &value);
};

struct IntegerValue {
	DUCKDB_API static int32_t Get(const Value &value);
};

struct BigIntValue {
	DUCKDB_API static int64_t Get(const Value &value);
};

struct HugeIntValue {
	DUCKDB_API static hugeint_t Get(const Value &value);
};

struct UTinyIntValue {
	DUCKDB_API static uint8_t Get(const Value &value);
};

struct USmallIntValue {
	DUCKDB_API static uint16_t Get(const Value &value);
};

struct UIntegerValue {
	DUCKDB_API static uint32_t Get(const Value &value);
};

struct UBigIntValue {
	DUCKDB_API static uint64_t Get(const Value &value);
};

struct FloatValue {
	DUCKDB_API static float Get(const Value &value);
};

struct DoubleValue {
	DUCKDB_API static double Get(const Value &value);
};

struct StringValue {
	DUCKDB_API static const string &Get(const Value &value);
};

struct DateValue {
	DUCKDB_API static date_t Get(const Value &value);
};

struct TimeValue {
	DUCKDB_API static dtime_t Get(const Value &value);
};

struct TimestampValue {
	DUCKDB_API static timestamp_t Get(const Value &value);
};

struct IntervalValue {
	DUCKDB_API static interval_t Get(const Value &value);
};

struct StructValue {
	DUCKDB_API static const vector<Value> &GetChildren(const Value &value);
};

struct ListValue {
	DUCKDB_API static const vector<Value> &GetChildren(const Value &value);
};

//! Return the internal integral value for any type that is stored as an integral value internally
//! This can be used on values of type integer, uinteger, but also date, timestamp, decimal, etc
struct IntegralValue {
	static hugeint_t Get(const Value &value);
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
DUCKDB_API interval_t Value::GetValue() const;
template <>
DUCKDB_API Value Value::GetValue() const;

template <>
DUCKDB_API bool Value::GetValueUnsafe() const;
template <>
DUCKDB_API int8_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API int16_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API int32_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API int64_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API hugeint_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API uint8_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API uint16_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API uint32_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API uint64_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API string Value::GetValueUnsafe() const;
template <>
DUCKDB_API string_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API float Value::GetValueUnsafe() const;
template <>
DUCKDB_API double Value::GetValueUnsafe() const;
template <>
DUCKDB_API date_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API dtime_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API timestamp_t Value::GetValueUnsafe() const;
template <>
DUCKDB_API interval_t Value::GetValueUnsafe() const;

template <>
DUCKDB_API int8_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API int16_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API int32_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API int64_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API hugeint_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API uint8_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API uint16_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API uint32_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API uint64_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API float &Value::GetReferenceUnsafe();
template <>
DUCKDB_API double &Value::GetReferenceUnsafe();
template <>
DUCKDB_API date_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API dtime_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API timestamp_t &Value::GetReferenceUnsafe();
template <>
DUCKDB_API interval_t &Value::GetReferenceUnsafe();

template <>
DUCKDB_API bool Value::IsNan(float input);
template <>
DUCKDB_API bool Value::IsNan(double input);

template <>
DUCKDB_API bool Value::IsFinite(float input);
template <>
DUCKDB_API bool Value::IsFinite(double input);
template <>
DUCKDB_API bool Value::IsFinite(date_t input);
template <>
DUCKDB_API bool Value::IsFinite(timestamp_t input);

} // namespace duckdb
