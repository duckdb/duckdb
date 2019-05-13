//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/string_util.hpp"
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

#define FORMAT_CONSTRUCTOR(msg)                                                                                        \
	va_list ap;                                                                                                        \
	va_start(ap, msg);                                                                                                 \
	Format(ap);                                                                                                        \
	va_end(ap);

class Exception : public std::runtime_error {
public:
	Exception(string message) : std::runtime_error(message), type(ExceptionType::INVALID) {
		exception_message_ = message;
	}

	Exception(ExceptionType exception_type, string message) : std::runtime_error(message), type(exception_type) {
		exception_message_ = ExceptionTypeToString(exception_type) + ": " + message;
	}

protected:
	void Format(va_list ap) {
		exception_message_ = StringUtil::VFormat(exception_message_, ap);
	}

public:
	string GetMessage() {
		return exception_message_;
	}

	string ExceptionTypeToString(ExceptionType type) {
		switch (type) {
		case ExceptionType::INVALID:
			return "Invalid";
		case ExceptionType::OUT_OF_RANGE:
			return "Out of Range";
		case ExceptionType::CONVERSION:
			return "Conversion";
		case ExceptionType::UNKNOWN_TYPE:
			return "Unknown Type";
		case ExceptionType::DECIMAL:
			return "Decimal";
		case ExceptionType::MISMATCH_TYPE:
			return "Mismatch Type";
		case ExceptionType::DIVIDE_BY_ZERO:
			return "Divide by Zero";
		case ExceptionType::OBJECT_SIZE:
			return "Object Size";
		case ExceptionType::INVALID_TYPE:
			return "Invalid type";
		case ExceptionType::SERIALIZATION:
			return "Serialization";
		case ExceptionType::TRANSACTION:
			return "TransactionContext";
		case ExceptionType::NOT_IMPLEMENTED:
			return "Not implemented";
		case ExceptionType::EXPRESSION:
			return "Expression";
		case ExceptionType::CATALOG:
			return "Catalog";
		case ExceptionType::PARSER:
			return "Parser";
		case ExceptionType::PLANNER:
			return "Planner";
		case ExceptionType::SCHEDULER:
			return "Scheduler";
		case ExceptionType::EXECUTOR:
			return "Executor";
		case ExceptionType::CONSTRAINT:
			return "Constraint";
		case ExceptionType::INDEX:
			return "Index";
		case ExceptionType::STAT:
			return "Stat";
		case ExceptionType::CONNECTION:
			return "Connection";
		case ExceptionType::SYNTAX:
			return "Syntax";
		case ExceptionType::SETTINGS:
			return "Settings";
		case ExceptionType::OPTIMIZER:
			return "Optimizer";
		case ExceptionType::NULL_POINTER:
			return "NullPointer";
		case ExceptionType::IO:
			return "IO";
		case ExceptionType::INTERRUPT:
			return "INTERRUPT";
		default:
			return "Unknown";
		}
	}

	friend std::ostream &operator<<(std::ostream &os, const Exception &e);

private:
	// type
	ExceptionType type;
	string exception_message_;
};

//===--------------------------------------------------------------------===//
// Exception derived classes
//===--------------------------------------------------------------------===//

class CastException : public Exception {
	CastException() = delete;

public:
	CastException(const TypeId origType, const TypeId newType)
	    : Exception(ExceptionType::CONVERSION,
	                "Type " + TypeIdToString(origType) + " can't be cast as " + TypeIdToString(newType)) {
	}
};

class ValueOutOfRangeException : public Exception {
	ValueOutOfRangeException() = delete;

public:
	ValueOutOfRangeException(const int64_t value, const TypeId origType, const TypeId newType)
	    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(origType) + " with value " +
	                                               std::to_string((intmax_t)value) +
	                                               " can't be cast because the value is out of range "
	                                               "for the destination type " +
	                                               TypeIdToString(newType)) {
	}

	ValueOutOfRangeException(const double value, const TypeId origType, const TypeId newType)
	    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(origType) + " with value " +
	                                               std::to_string(value) +
	                                               " can't be cast because the value is out of range "
	                                               "for the destination type " +
	                                               TypeIdToString(newType)) {
	}
	ValueOutOfRangeException(const TypeId varType, const count_t length)
	    : Exception(ExceptionType::OUT_OF_RANGE, "The value is too long to fit into type " + TypeIdToString(varType) +
	                                                 "(" + std::to_string(length) + ")"){};
};

class ConversionException : public Exception {
	ConversionException() = delete;

public:
	ConversionException(string msg, ...) : Exception(ExceptionType::CONVERSION, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class InvalidTypeException : public Exception {
	InvalidTypeException() = delete;

public:
	InvalidTypeException(TypeId type, string msg)
	    : Exception(ExceptionType::INVALID_TYPE, "Invalid Type [" + TypeIdToString(type) + "]: " + msg) {
	}
};

class TypeMismatchException : public Exception {
	TypeMismatchException() = delete;

public:
	TypeMismatchException(const TypeId type_1, const TypeId type_2, string msg)
	    : Exception(ExceptionType::MISMATCH_TYPE,
	                "Type " + TypeIdToString(type_1) + " does not match with " + TypeIdToString(type_2) + ". " + msg) {
	}
};

class TransactionException : public Exception {
	TransactionException() = delete;

public:
	TransactionException(string msg, ...) : Exception(ExceptionType::TRANSACTION, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class NotImplementedException : public Exception {
	NotImplementedException() = delete;

public:
	NotImplementedException(string msg, ...) : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class OutOfRangeException : public Exception {
	OutOfRangeException() = delete;

public:
	OutOfRangeException(string msg, ...) : Exception(ExceptionType::OUT_OF_RANGE, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class CatalogException : public Exception {
	CatalogException() = delete;

public:
	CatalogException(string msg, ...) : Exception(ExceptionType::CATALOG, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class ParserException : public Exception {
	ParserException() = delete;

public:
	ParserException(string msg, ...) : Exception(ExceptionType::PARSER, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class SyntaxException : public Exception {
	SyntaxException() = delete;

public:
	SyntaxException(string msg, ...) : Exception(ExceptionType::SYNTAX, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class ConstraintException : public Exception {
	ConstraintException() = delete;

public:
	ConstraintException(string msg, ...) : Exception(ExceptionType::CONSTRAINT, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class BinderException : public Exception {
	BinderException() = delete;

public:
	BinderException(string msg, ...) : Exception(ExceptionType::BINDER, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class IOException : public Exception {
	IOException() = delete;

public:
	IOException(string msg, ...) : Exception(ExceptionType::IO, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class SerializationException : public Exception {
	SerializationException() = delete;

public:
	SerializationException(string msg, ...) : Exception(ExceptionType::SERIALIZATION, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class SequenceException : public Exception {
	SequenceException() = delete;

public:
	SequenceException(string msg, ...) : Exception(ExceptionType::SERIALIZATION, msg) {
		FORMAT_CONSTRUCTOR(msg);
	}
};

class InterruptException : public Exception {
public:
	InterruptException() : Exception(ExceptionType::INTERRUPT, "Interrupted!") {
	}
};

} // namespace duckdb
