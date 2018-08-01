//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/types/value.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory.h>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "common/types/date.hpp"

namespace duckdb {

//! The Value object holds a single arbitrary value of any type that can be
//! stored in the database.
class Value : public Printable {
	friend class Vector;

  public:
	//! Create an empty NULL value of the specified type
	Value(TypeId type = TypeId::INTEGER) : type(type), is_null(true) {}
	//! Create a boolean value
	Value(bool val) : type(TypeId::BOOLEAN), is_null(false) {
		value_.boolean = val;
	}
	//! Create a TINYINT value
	Value(int8_t val) : type(TypeId::TINYINT), is_null(false) {
		value_.tinyint = val;
	}
	//! Create a SMALLINT value
	Value(int16_t val) : type(TypeId::SMALLINT), is_null(false) {
		value_.smallint = val;
	}
	//! Create an INTEGER value
	Value(int32_t val) : type(TypeId::INTEGER), is_null(false) {
		value_.integer = val;
	}
	//! Create a BIGINT value
	Value(int64_t val) : type(TypeId::BIGINT), is_null(false) {
		value_.bigint = val;
	}
	//! Create an OID value
	Value(uint64_t val) : type(TypeId::POINTER), is_null(false) {
		value_.pointer = val;
	}
	//! Create a DOUBLE value
	Value(double val) : type(TypeId::DECIMAL), is_null(false) {
		value_.decimal = val;
	}
	//! Create a VARCHAR value
	Value(std::string val)
	    : type(TypeId::VARCHAR), is_null(false), str_value(val) {}
	Value(const Value &other);

	//! Create a Numeric value of the specified type with the specified value
	static Value NumericValue(TypeId id, int64_t value);

	//! Return a copy of this value
	Value Copy() { return Value(*this); }

	//! Convert this value to a string
	virtual std::string ToString() const;

	//! Cast this value to another type
	Value CastAs(TypeId new_type);

	//===--------------------------------------------------------------------===//
	// Numeric Operations
	//===--------------------------------------------------------------------===//
	// A + B
	static void Add(Value &left, Value &right, Value &result);
	// A - B
	static void Subtract(Value &left, Value &right, Value &result);
	// A * B
	static void Multiply(Value &left, Value &right, Value &result);
	// A / B
	static void Divide(Value &left, Value &right, Value &result);
	// A % B
	static void Modulo(Value &left, Value &right, Value &result);
	// MIN(A, B)
	static void Min(Value &left, Value &right, Value &result);
	// MAX(A, B)
	static void Max(Value &left, Value &right, Value &result);
	//===--------------------------------------------------------------------===//
	// Comparison Operations
	//===--------------------------------------------------------------------===//
	// A == B
	static bool Equals(Value &left, Value &right);
	// A != B
	static bool NotEquals(Value &left, Value &right);
	// A > B
	static bool GreaterThan(Value &left, Value &right);
	// A >= B
	static bool GreaterThanEquals(Value &left, Value &right);
	// A < B
	static bool LessThan(Value &left, Value &right);
	// A <= B
	static bool LessThanEquals(Value &left, Value &right);

	//! The type of the value
	TypeId type;
	//! Whether or not the value is NULL
	bool is_null;

  private:
	//! The value of the object, if it is of a constant size Type
	union Val {
		int8_t boolean;
		int8_t tinyint;
		int16_t smallint;
		int32_t integer;
		int64_t bigint;
		double decimal;
		uint64_t pointer;
		date_t date;
	} value_;

	//! The value of the object, if it is of a variable size Type
	std::string str_value;

	//! Templated helper function for casting
	template <class DST, class OP> static DST _cast(Value &v);

	//! Templated helper function for binary operations
	template <class OP>
	static void _templated_binary_operation(Value &left, Value &right,
	                                        Value &result);

	//! Templated helper function for boolean operations
	template <class OP>
	static bool _templated_boolean_operation(Value &left, Value &right);
};
} // namespace duckdb
