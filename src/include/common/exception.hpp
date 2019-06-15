//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.hpp"

#include <stdarg.h>
#include <stdexcept>

namespace duckdb {

inline void ASSERT_RESTRICT(void *left_start, void *left_end, void *right_start, void *right_end) {
	// assert that the two pointers do not overlap
	assert(left_end < right_start || right_end < left_start);
}

//===--------------------------------------------------------------------===//
// Exception Types
//===--------------------------------------------------------------------===//

enum class ExceptionType {
	INVALID = 0,          // invalid type
	OUT_OF_RANGE = 1,     // value out of range error
	CONVERSION = 2,       // conversion/casting error
	UNKNOWN_TYPE = 3,     // unknown type
	DECIMAL = 4,          // decimal related
	MISMATCH_TYPE = 5,    // type mismatch
	DIVIDE_BY_ZERO = 6,   // divide by 0
	OBJECT_SIZE = 7,      // object size exceeded
	INVALID_TYPE = 8,     // incompatible for operation
	SERIALIZATION = 9,    // serialization
	TRANSACTION = 10,     // transaction management
	NOT_IMPLEMENTED = 11, // method not implemented
	EXPRESSION = 12,      // expression parsing
	CATALOG = 13,         // catalog related
	PARSER = 14,          // parser related
	PLANNER = 15,         // planner related
	SCHEDULER = 16,       // scheduler related
	EXECUTOR = 17,        // executor related
	CONSTRAINT = 18,      // constraint related
	INDEX = 19,           // index related
	STAT = 20,            // stat related
	CONNECTION = 21,      // connection related
	SYNTAX = 22,          // syntax related
	SETTINGS = 23,        // settings related
	BINDER = 24,          // binder related
	NETWORK = 25,         // network related
	OPTIMIZER = 26,       // optimizer related
	NULL_POINTER = 27,    // nullptr exception
	IO = 28,              // IO exception
	INTERRUPT = 29        // interrupt
};

class Exception : public std::exception {
public:
	Exception(string message);
	Exception(ExceptionType exception_type, string message);

	ExceptionType type;

public:
	const char *what() const noexcept override;

	string ExceptionTypeToString(ExceptionType type);

protected:
	void Format(va_list ap);

private:
	string exception_message_;
};

//===--------------------------------------------------------------------===//
// Exception derived classes
//===--------------------------------------------------------------------===//

class CastException : public Exception {
public:
	CastException(const TypeId origType, const TypeId newType);
};

class ValueOutOfRangeException : public Exception {
public:
	ValueOutOfRangeException(const int64_t value, const TypeId origType, const TypeId newType);
	ValueOutOfRangeException(const double value, const TypeId origType, const TypeId newType);
	ValueOutOfRangeException(const TypeId varType, const index_t length);
};

class ConversionException : public Exception {
public:
	ConversionException(string msg, ...);
};

class InvalidTypeException : public Exception {
public:
	InvalidTypeException(TypeId type, string msg);
};

class TypeMismatchException : public Exception {
public:
	TypeMismatchException(const TypeId type_1, const TypeId type_2, string msg);
};

class TransactionException : public Exception {
public:
	TransactionException(string msg, ...);
};

class NotImplementedException : public Exception {
public:
	NotImplementedException(string msg, ...);
};

class OutOfRangeException : public Exception {
public:
	OutOfRangeException(string msg, ...);
};

class CatalogException : public Exception {
public:
	CatalogException(string msg, ...);
};

class ParserException : public Exception {
public:
	ParserException(string msg, ...);
};

class SyntaxException : public Exception {
public:
	SyntaxException(string msg, ...);
};

class ConstraintException : public Exception {
public:
	ConstraintException(string msg, ...);
};

class BinderException : public Exception {
public:
	BinderException(string msg, ...);
};

class IOException : public Exception {
public:
	IOException(string msg, ...);
};

class SerializationException : public Exception {
public:
	SerializationException(string msg, ...);
};

class SequenceException : public Exception {
public:
	SequenceException(string msg, ...);
};

class InterruptException : public Exception {
public:
	InterruptException();
};

} // namespace duckdb
