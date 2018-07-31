
#pragma once

#include <memory.h>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

namespace duckdb {
class Value : public Printable {
  public:
	Value() : type(TypeId::INTEGER), is_null(true) {}
	Value(bool val) : type(TypeId::BOOLEAN), is_null(false) {
		value_.boolean = val;
	}
	Value(int8_t val) : type(TypeId::TINYINT), is_null(false) {
		value_.tinyint = val;
	}
	Value(int16_t val) : type(TypeId::SMALLINT), is_null(false) {
		value_.smallint = val;
	}
	Value(int32_t val) : type(TypeId::INTEGER), is_null(false) {
		value_.integer = val;
	}
	Value(int64_t val) : type(TypeId::BIGINT), is_null(false) {
		value_.bigint = val;
	}
	Value(uint64_t val) : type(TypeId::POINTER), is_null(false) {
		value_.pointer = val;
	}
	Value(double val) : type(TypeId::INTEGER), is_null(false) {
		value_.decimal = val;
	}
	Value(std::string val) : type(TypeId::VARCHAR), is_null(false) {
		value_.data = new char[val.size() + 1];
		memcpy(value_.data, val.c_str(), val.size());
		value_.data[val.size()] = '\0';
		len = val.size();
	}
	Value(const Value &other);
	~Value() {
		if ((type == TypeId::VARCHAR || type == TypeId::VARBINARY ||
		     type == TypeId::ARRAY) &&
		    value_.data) {
			delete[] value_.data;
		}
	}

	static Value NumericValue(TypeId id, int64_t value);

	Value Copy() { return Value(*this); }

	virtual std::string ToString() const;

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

	//  private:
	TypeId type;
	bool is_null;

	union Val {
		int8_t boolean;
		int8_t tinyint;
		int16_t smallint;
		int32_t integer;
		int64_t bigint;
		double decimal;
		uint64_t pointer;
		int32_t date;
		char *data;
	} value_;

	uint32_t len;
};
} // namespace duckdb
