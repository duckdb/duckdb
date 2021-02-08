//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/exception_format_value.hpp"

#include <stdexcept>

namespace duckdb {
enum class PhysicalType : uint8_t;
struct LogicalType;
struct hugeint_t;

inline void assert_restrict_function(void *left_start, void *left_end, void *right_start, void *right_end,
                                     const char *fname, int linenr) {
	// assert that the two pointers do not overlap
#ifdef DEBUG
	if (!(left_end <= right_start || right_end <= left_start)) {
		printf("ASSERT RESTRICT FAILED: %s:%d\n", fname, linenr);
		D_ASSERT(0);
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
	    31, // Internal exception: exception that indicates something went wrong internally (i.e. bug in the code base)
	INVALID_INPUT = 32 // Input or arguments error
};

class Exception : public std::exception {
public:
	explicit Exception(string message);
	Exception(ExceptionType exception_type, const string &message);

	ExceptionType type;

public:
	const char *what() const noexcept override;

	string ExceptionTypeToString(ExceptionType type);

	template <typename... Args>
	static string ConstructMessage(string msg, Args... params) {
		vector<ExceptionFormatValue> values;
		return ConstructMessageRecursive(msg, values, params...);
	}

	static string ConstructMessageRecursive(const string &msg, vector<ExceptionFormatValue> &values);

	template <class T, typename... Args>
	static string ConstructMessageRecursive(const string &msg, vector<ExceptionFormatValue> &values, T param, Args... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		return ConstructMessageRecursive(msg, values, params...);
	}

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
	explicit CatalogException(string msg);

	template <typename... Args>
	explicit CatalogException(string msg, Args... params) : CatalogException(ConstructMessage(msg, params...)) {
	}
};

class ParserException : public StandardException {
public:
	explicit ParserException(string msg);

	template <typename... Args>
	explicit ParserException(string msg, Args... params) : ParserException(ConstructMessage(msg, params...)) {
	}
};

class BinderException : public StandardException {
public:
	explicit BinderException(string msg);

	template <typename... Args>
	explicit BinderException(string msg, Args... params) : BinderException(ConstructMessage(msg, params...)) {
	}
};

class ConversionException : public Exception {
public:
	explicit ConversionException(string msg);

	template <typename... Args>
	explicit ConversionException(string msg, Args... params) : ConversionException(ConstructMessage(msg, params...)) {
	}
};

class TransactionException : public Exception {
public:
	explicit TransactionException(string msg);

	template <typename... Args>
	explicit TransactionException(string msg, Args... params) : TransactionException(ConstructMessage(msg, params...)) {
	}
};

class NotImplementedException : public Exception {
public:
	explicit NotImplementedException(string msg);

	template <typename... Args>
	explicit NotImplementedException(string msg, Args... params) : NotImplementedException(ConstructMessage(msg, params...)) {
	}
};

class OutOfRangeException : public Exception {
public:
	explicit OutOfRangeException(string msg);

	template <typename... Args>
	explicit OutOfRangeException(string msg, Args... params) : OutOfRangeException(ConstructMessage(msg, params...)) {
	}
};

class SyntaxException : public Exception {
public:
	explicit SyntaxException(string msg);

	template <typename... Args>
	explicit SyntaxException(string msg, Args... params) : SyntaxException(ConstructMessage(msg, params...)) {
	}
};

class ConstraintException : public Exception {
public:
	explicit ConstraintException(string msg);

	template <typename... Args>
	explicit ConstraintException(string msg, Args... params) : ConstraintException(ConstructMessage(msg, params...)) {
	}
};

class IOException : public Exception {
public:
	explicit IOException(string msg);

	template <typename... Args>
	explicit IOException(string msg, Args... params) : IOException(ConstructMessage(msg, params...)) {
	}
};

class SerializationException : public Exception {
public:
	explicit SerializationException(string msg);

	template <typename... Args>
	explicit SerializationException(string msg, Args... params) : SerializationException(ConstructMessage(msg, params...)) {
	}
};

class SequenceException : public Exception {
public:
	explicit SequenceException(string msg);

	template <typename... Args>
	explicit SequenceException(string msg, Args... params) : SequenceException(ConstructMessage(msg, params...)) {
	}
};

class InterruptException : public Exception {
public:
	InterruptException();
};

class FatalException : public Exception {
public:
	explicit FatalException(string msg);

	template <typename... Args>
	explicit FatalException(string msg, Args... params) : FatalException(ConstructMessage(msg, params...)) {
	}
};

class InternalException : public Exception {
public:
	explicit InternalException(string msg);

	template <typename... Args>
	explicit InternalException(string msg, Args... params) : InternalException(ConstructMessage(msg, params...)) {
	}
};

class InvalidInputException : public Exception {
public:
	explicit InvalidInputException(string msg);

	template <typename... Args>
	explicit InvalidInputException(string msg, Args... params) : InvalidInputException(ConstructMessage(msg, params...)) {
	}
};

class CastException : public Exception {
public:
	CastException(const PhysicalType origType, const PhysicalType newType);
	CastException(const LogicalType &origType, const LogicalType &newType);
};

class InvalidTypeException : public Exception {
public:
	InvalidTypeException(PhysicalType type, const string &msg);
	InvalidTypeException(const LogicalType &type, const string &msg);
};

class TypeMismatchException : public Exception {
public:
	TypeMismatchException(const PhysicalType type_1, const PhysicalType type_2, const string &msg);
	TypeMismatchException(const LogicalType &type_1, const LogicalType &type_2, const string &msg);
};

class ValueOutOfRangeException : public Exception {
public:
	ValueOutOfRangeException(const int64_t value, const PhysicalType origType, const PhysicalType newType);
	ValueOutOfRangeException(const hugeint_t value, const PhysicalType origType, const PhysicalType newType);
	ValueOutOfRangeException(const double value, const PhysicalType origType, const PhysicalType newType);
	ValueOutOfRangeException(const PhysicalType varType, const idx_t length);
};

} // namespace duckdb
