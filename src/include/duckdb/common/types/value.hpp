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

#include <iosfwd>
#include <memory.h>

namespace duckdb {

class Deserializer;
class Serializer;

//! The Value object holds a single arbitrary value of any type that can be
//! stored in the database.
class Value {
	friend class Vector;

public:
	//! Create an empty NULL value of the specified type
	Value(TypeId type = TypeId::INT32) : type(type), is_null(true) {
	}
	//! Create a BIGINT value
	Value(int32_t val) : type(TypeId::INT32), is_null(false) {
		value_.integer = val;
	}
	//! Create a BIGINT value
	Value(int64_t val) : type(TypeId::INT64), is_null(false) {
		value_.bigint = val;
	}
	//! Create a FLOAT value
	Value(float val) : type(TypeId::FLOAT), is_null(false) {
		value_.float_ = val;
	}
	//! Create a DOUBLE value
	Value(double val) : type(TypeId::DOUBLE), is_null(false) {
		value_.double_ = val;
	}
	//! Create a VARCHAR value
	Value(const char *val) : Value(val ? string(val) : string()) {
	}
	Value(string_t val);
	//! Create a VARCHAR value
	Value(string val);

	//! Create the lowest possible value of a given type (numeric only)
	static Value MinimumValue(TypeId type);
	//! Create the highest possible value of a given type (numeric only)
	static Value MaximumValue(TypeId type);
	//! Create a Numeric value of the specified type with the specified value
	static Value Numeric(TypeId id, int64_t value);

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
	//! Create a hash Value from a specified value
	static Value HASH(hash_t value);
	//! Create a pointer Value from a specified value
	static Value POINTER(uintptr_t value);
	//! Create a date Value from a specified date
	static Value DATE(date_t date);
	//! Create a date Value from a specified date
	static Value DATE(int32_t year, int32_t month, int32_t day);
	//! Create a time Value from a specified date
	static Value TIME(int32_t hour, int32_t min, int32_t sec, int32_t msec);
	//! Create a timestamp Value from a specified date/time combination
	static Value TIMESTAMP(date_t date, dtime_t time);
	//! Create a timestamp Value from a specified timestamp
	static Value TIMESTAMP(timestamp_t timestamp);
	//! Create a timestamp Value from a specified timestamp in separate values
	static Value TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec,
	                       int32_t msec);

	//! Create a blob value
	static Value BLOB(string value);

	//! Create a float Value from a specified value
	static Value FLOAT(float value);
	//! Create a double Value from a specified value
	static Value DOUBLE(double value);
	//! Create a struct value with given list of entries
	static Value STRUCT(child_list_t<Value> values);
	//! Create a list value with the given entries
	static Value LIST(std::vector<Value> values);

	template <class T> T GetValue() {
		throw NotImplementedException("Unimplemented template type for Value::GetValue");
	}
	template <class T> static Value CreateValue(T value) {
		throw NotImplementedException("Unimplemented template type for Value::CreateValue");
	}

	//! Return a copy of this value
	Value Copy() const {
		return Value(*this);
	}

	//! Convert this value to a string
	string ToString() const;
	//! Convert this value to a string, with the given display format
	string ToString(SQLType type) const;

	//! Cast this value to another type
	Value CastAs(TypeId target_type, bool strict = false) const;
	//! Cast this value to another type
	Value CastAs(SQLType source_type, SQLType target_type, bool strict = false);
	//! Tries to cast value to another type, throws exception if its not possible
	bool TryCastAs(SQLType source_type, SQLType target_type, bool strict = false);

	//! The type of the value
	TypeId type;
	//! Whether or not the value is NULL
	bool is_null;

	SQLType GetSQLType() {
		return sql_type.id == SQLTypeId::INVALID ? SQLTypeFromInternalType(type) : sql_type;
	}

	//! The value of the object, if it is of a constant size Type
	union Val {
		int8_t boolean;
		int8_t tinyint;
		int16_t smallint;
		int32_t integer;
		int64_t bigint;
		float float_;
		double double_;
		uintptr_t pointer;
		uint64_t hash;
	} value_;

	//! The value of the object, if it is of a variable size type
	string str_value;

	child_list_t<Value> struct_value;
	std::vector<Value> list_value;

	//! Serializes a Value to a stand-alone binary blob
	void Serialize(Serializer &serializer);
	//! Deserializes a Value from a blob
	static Value Deserialize(Deserializer &source);

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
	//! Returns true if the values are (approximately) equivalent. Note this is NOT the SQL equivalence. For this
	//! function, NULL values are equivalent and floating point values that are close are equivalent.
	static bool ValuesAreEqual(Value result_value, Value value);

	friend std::ostream &operator<<(std::ostream &out, const Value &val) {
		out << val.ToString();
		return out;
	}
	void Print();

private:
	SQLType sql_type = SQLType(SQLTypeId::INVALID);

private:
	template <class T> T GetValueInternal();
	//! Templated helper function for casting
	template <class DST, class OP> static DST _cast(const Value &v);

	//! Templated helper function for binary operations
	template <class OP>
	static void _templated_binary_operation(const Value &left, const Value &right, Value &result, bool ignore_null);

	//! Templated helper function for boolean operations
	template <class OP> static bool _templated_boolean_operation(const Value &left, const Value &right);
};

template <> Value Value::CreateValue(bool value);
template <> Value Value::CreateValue(int8_t value);
template <> Value Value::CreateValue(int16_t value);
template <> Value Value::CreateValue(int32_t value);
template <> Value Value::CreateValue(int64_t value);
template <> Value Value::CreateValue(const char *value);
template <> Value Value::CreateValue(string value);
template <> Value Value::CreateValue(string_t value);
template <> Value Value::CreateValue(float value);
template <> Value Value::CreateValue(double value);
template <> Value Value::CreateValue(Value value);

template <> bool Value::GetValue();
template <> int8_t Value::GetValue();
template <> int16_t Value::GetValue();
template <> int32_t Value::GetValue();
template <> int64_t Value::GetValue();
template <> string Value::GetValue();
template <> float Value::GetValue();
template <> double Value::GetValue();

} // namespace duckdb
