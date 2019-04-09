//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types/value.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/exception.hpp"

#include <iostream>
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
	Value(TypeId type = TypeId::INTEGER) : type(type), is_null(true) {
	}
	//! Create a BIGINT value
	Value(int32_t val) : type(TypeId::INTEGER), is_null(false) {
		value_.integer = val;
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
	//! Create a VARCHAR value
	Value(string val) : type(TypeId::VARCHAR), is_null(false) {
		if (IsUTF8String(val.c_str())) {
			str_value = val;
		} else {
			throw Exception("String value is not valid UTF8");
		}
	}
	Value(const Value &other);

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
	//! Create a pointer Value from a specified value
	static Value POINTER(uint64_t value);
	//! Create a date Value from a specified date
	static Value DATE(int32_t year, int32_t month, int32_t day);

	int64_t GetNumericValue();

	//! Return a copy of this value
	Value Copy() const {
		return Value(*this);
	}

	//! Convert this value to a string
	string ToString() const;
	//! Convert this value to a string, with the given display format
	string ToString(SQLType type) const;

	//! Cast this value to another type
	Value CastAs(TypeId target_type) const;
	//! Cast this value to another type
	Value CastAs(SQLType source_type, SQLType target_type);

	//! The type of the value
	TypeId type;
	//! Whether or not the value is NULL
	bool is_null;

	//! The value of the object, if it is of a constant size Type
	union Val {
		int8_t boolean;
		int8_t tinyint;
		int16_t smallint;
		int32_t integer;
		int64_t bigint;
		float float_;
		double double_;
		uint64_t pointer;
	} value_;

	//! The value of the object, if it is of a variable size type
	string str_value;

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

	static bool IsUTF8String(const char *s);

	friend std::ostream &operator<<(std::ostream &out, const Value &val) {
		out << val.ToString();
		return out;
	}
	void Print();

private:
	//! Templated helper function for casting
	template <class DST, class OP> static DST _cast(const Value &v);

	//! Templated helper function for binary operations
	template <class OP>
	static void _templated_binary_operation(const Value &left, const Value &right, Value &result, bool ignore_null);

	//! Templated helper function for boolean operations
	template <class OP> static bool _templated_boolean_operation(const Value &left, const Value &right);
};
} // namespace duckdb
