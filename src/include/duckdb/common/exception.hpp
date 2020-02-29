//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

#include <stdarg.h>
#include <stdexcept>

namespace duckdb {

inline void assert_restrict_function(void *left_start, void *left_end, void *right_start, void *right_end,
                                     const char *fname, int linenr) {
	// assert that the two pointers do not overlap
#ifdef DEBUG
	if (!(left_end <= right_start || right_end <= left_start)) {
		printf("ASSERT RESTRICT FAILED: %s:%d\n", fname, linenr);
		assert(0);
	}
#endif
}

#define ASSERT_RESTRICT(left_start, left_end, right_start, right_end)                                                  \
	assert_restrict_function(left_start, left_end, right_start, right_end, __FILE__, __LINE__)

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
	INTERRUPT = 29,       // interrupt
	FATAL = 30, // Fatal exception: fatal exceptions are non-recoverable, and render the entire DB in an unusable state
	INTERNAL =
	    31 // Internal exception: exception that indicates something went wrong internally (i.e. bug in the code base)
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

//! Exceptions that are StandardExceptions do NOT invalidate the current transaction when thrown
class StandardException : public Exception {
public:
	StandardException(ExceptionType exception_type, string message) : Exception(exception_type, message) {
	}
};

class CatalogException : public StandardException {
public:
	CatalogException(string msg, ...);
};

class ParserException : public StandardException {
public:
	ParserException(string msg, ...);
};

class BinderException : public StandardException {
public:
	BinderException(string msg, ...);
};

class CastException : public Exception {
public:
	CastException(const TypeId origType, const TypeId newType);
};

class ValueOutOfRangeException : public Exception {
public:
	ValueOutOfRangeException(const int64_t value, const TypeId origType, const TypeId newType);
	ValueOutOfRangeException(const double value, const TypeId origType, const TypeId newType);
	ValueOutOfRangeException(const TypeId varType, const idx_t length);
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

class SyntaxException : public Exception {
public:
	SyntaxException(string msg, ...);
};

class ConstraintException : public Exception {
public:
	ConstraintException(string msg, ...);
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

class FatalException : public Exception {
public:
	FatalException(string msg, ...);
};

class InternalException : public Exception {
public:
	InternalException(string msg, ...);
};

} // namespace duckdb
