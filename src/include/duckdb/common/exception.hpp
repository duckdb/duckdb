//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception_format_value.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/typedefs.hpp"

#include <vector>
#include <stdexcept>

namespace duckdb {
enum class PhysicalType : uint8_t;
struct LogicalType;
class Expression;
class ParsedExpression;
class QueryErrorContext;
class TableRef;
struct hugeint_t;
class optional_idx;

inline void assert_restrict_function(const void *left_start, const void *left_end, const void *right_start,
                                     const void *right_end, const char *fname, int linenr) {
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
	FATAL = 30,           // Fatal exceptions are non-recoverable, and render the entire DB in an unusable state
	INTERNAL = 31,        // Internal exceptions indicate something went wrong internally (i.e. bug in the code base)
	INVALID_INPUT = 32,   // Input or arguments error
	OUT_OF_MEMORY = 33,   // out of memory
	PERMISSION = 34,      // insufficient permissions
	PARAMETER_NOT_RESOLVED = 35, // parameter types could not be resolved
	PARAMETER_NOT_ALLOWED = 36,  // parameter types not allowed
	DEPENDENCY = 37,             // dependency
	HTTP = 38,
	MISSING_EXTENSION = 39, // Thrown when an extension is used but not loaded
	AUTOLOAD = 40,          // Thrown when an extension is used but not loaded
	SEQUENCE = 41
};

class Exception : public std::runtime_error {
public:
	DUCKDB_API Exception(ExceptionType exception_type, const string &message);
	DUCKDB_API Exception(ExceptionType exception_type, const string &message,
	                     const unordered_map<string, string> &extra_info);

public:
	DUCKDB_API static string ExceptionTypeToString(ExceptionType type);
	DUCKDB_API static ExceptionType StringToExceptionType(const string &type);

	template <typename... Args>
	static string ConstructMessage(const string &msg, Args... params) {
		const std::size_t num_args = sizeof...(Args);
		if (num_args == 0)
			return msg;
		std::vector<ExceptionFormatValue> values;
		return ConstructMessageRecursive(msg, values, params...);
	}

	DUCKDB_API static unordered_map<string, string> InitializeExtraInfo(const Expression &expr);
	DUCKDB_API static unordered_map<string, string> InitializeExtraInfo(const ParsedExpression &expr);
	DUCKDB_API static unordered_map<string, string> InitializeExtraInfo(const QueryErrorContext &error_context);
	DUCKDB_API static unordered_map<string, string> InitializeExtraInfo(const TableRef &ref);
	DUCKDB_API static unordered_map<string, string> InitializeExtraInfo(optional_idx error_location);
	DUCKDB_API static unordered_map<string, string> InitializeExtraInfo(const string &subtype,
	                                                                    optional_idx error_location);

	DUCKDB_API static string ToJSON(ExceptionType type, const string &message);
	DUCKDB_API static string ToJSON(ExceptionType type, const string &message,
	                                const unordered_map<string, string> &extra_info);

	DUCKDB_API static bool InvalidatesTransaction(ExceptionType exception_type);
	DUCKDB_API static bool InvalidatesDatabase(ExceptionType exception_type);

	DUCKDB_API static string ConstructMessageRecursive(const string &msg, std::vector<ExceptionFormatValue> &values);

	template <class T, typename... Args>
	static string ConstructMessageRecursive(const string &msg, std::vector<ExceptionFormatValue> &values, T param,
	                                        Args... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		return ConstructMessageRecursive(msg, values, params...);
	}

	DUCKDB_API static bool UncaughtException();

	DUCKDB_API static string GetStackTrace(int max_depth = 120);
	static string FormatStackTrace(string message = "") {
		return (message + "\n" + GetStackTrace());
	}

	DUCKDB_API static void SetQueryLocation(optional_idx error_location, unordered_map<string, string> &extra_info);
};

//===--------------------------------------------------------------------===//
// Exception derived classes
//===--------------------------------------------------------------------===//
class ConnectionException : public Exception {
public:
	DUCKDB_API explicit ConnectionException(const string &msg);

	template <typename... Args>
	explicit ConnectionException(const string &msg, Args... params)
	    : ConnectionException(ConstructMessage(msg, params...)) {
	}
};

class PermissionException : public Exception {
public:
	DUCKDB_API explicit PermissionException(const string &msg);

	template <typename... Args>
	explicit PermissionException(const string &msg, Args... params)
	    : PermissionException(ConstructMessage(msg, params...)) {
	}
};

class OutOfRangeException : public Exception {
public:
	DUCKDB_API explicit OutOfRangeException(const string &msg);

	template <typename... Args>
	explicit OutOfRangeException(const string &msg, Args... params)
	    : OutOfRangeException(ConstructMessage(msg, params...)) {
	}
	DUCKDB_API OutOfRangeException(const int64_t value, const PhysicalType origType, const PhysicalType newType);
	DUCKDB_API OutOfRangeException(const hugeint_t value, const PhysicalType origType, const PhysicalType newType);
	DUCKDB_API OutOfRangeException(const double value, const PhysicalType origType, const PhysicalType newType);
	DUCKDB_API OutOfRangeException(const PhysicalType varType, const idx_t length);
};

class OutOfMemoryException : public Exception {
public:
	DUCKDB_API explicit OutOfMemoryException(const string &msg);

	template <typename... Args>
	explicit OutOfMemoryException(const string &msg, Args... params)
	    : OutOfMemoryException(ConstructMessage(msg, params...)) {
	}
};

class SyntaxException : public Exception {
public:
	DUCKDB_API explicit SyntaxException(const string &msg);

	template <typename... Args>
	explicit SyntaxException(const string &msg, Args... params) : SyntaxException(ConstructMessage(msg, params...)) {
	}
};

class ConstraintException : public Exception {
public:
	DUCKDB_API explicit ConstraintException(const string &msg);

	template <typename... Args>
	explicit ConstraintException(const string &msg, Args... params)
	    : ConstraintException(ConstructMessage(msg, params...)) {
	}
};

class DependencyException : public Exception {
public:
	DUCKDB_API explicit DependencyException(const string &msg);

	template <typename... Args>
	explicit DependencyException(const string &msg, Args... params)
	    : DependencyException(ConstructMessage(msg, params...)) {
	}
};

class IOException : public Exception {
public:
	DUCKDB_API explicit IOException(const string &msg);
	explicit IOException(ExceptionType exception_type, const string &msg) : Exception(exception_type, msg) {
	}

	template <typename... Args>
	explicit IOException(const string &msg, Args... params) : IOException(ConstructMessage(msg, params...)) {
	}
};

class MissingExtensionException : public Exception {
public:
	DUCKDB_API explicit MissingExtensionException(const string &msg);

	template <typename... Args>
	explicit MissingExtensionException(const string &msg, Args... params)
	    : MissingExtensionException(ConstructMessage(msg, params...)) {
	}
};

class NotImplementedException : public Exception {
public:
	DUCKDB_API explicit NotImplementedException(const string &msg);

	template <typename... Args>
	explicit NotImplementedException(const string &msg, Args... params)
	    : NotImplementedException(ConstructMessage(msg, params...)) {
	}
};

class AutoloadException : public Exception {
public:
	DUCKDB_API explicit AutoloadException(const string &extension_name, const string &message);
};

class SerializationException : public Exception {
public:
	DUCKDB_API explicit SerializationException(const string &msg);

	template <typename... Args>
	explicit SerializationException(const string &msg, Args... params)
	    : SerializationException(ConstructMessage(msg, params...)) {
	}
};

class SequenceException : public Exception {
public:
	DUCKDB_API explicit SequenceException(const string &msg);

	template <typename... Args>
	explicit SequenceException(const string &msg, Args... params)
	    : SequenceException(ConstructMessage(msg, params...)) {
	}
};

class InterruptException : public Exception {
public:
	DUCKDB_API InterruptException();
};

class FatalException : public Exception {
public:
	explicit FatalException(const string &msg) : FatalException(ExceptionType::FATAL, msg) {
	}
	template <typename... Args>
	explicit FatalException(const string &msg, Args... params) : FatalException(ConstructMessage(msg, params...)) {
	}

protected:
	DUCKDB_API explicit FatalException(ExceptionType type, const string &msg);
	template <typename... Args>
	explicit FatalException(ExceptionType type, const string &msg, Args... params)
	    : FatalException(type, ConstructMessage(msg, params...)) {
	}
};

class InternalException : public Exception {
public:
	DUCKDB_API explicit InternalException(const string &msg);

	template <typename... Args>
	explicit InternalException(const string &msg, Args... params)
	    : InternalException(ConstructMessage(msg, params...)) {
	}
};

class InvalidInputException : public Exception {
public:
	DUCKDB_API explicit InvalidInputException(const string &msg);
	DUCKDB_API explicit InvalidInputException(const string &msg, const unordered_map<string, string> &extra_info);

	template <typename... Args>
	explicit InvalidInputException(const string &msg, Args... params)
	    : InvalidInputException(ConstructMessage(msg, params...)) {
	}
	template <typename... Args>
	explicit InvalidInputException(Expression &expr, const string &msg, Args... params)
	    : InvalidInputException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(expr)) {
	}
};

class InvalidTypeException : public Exception {
public:
	DUCKDB_API InvalidTypeException(PhysicalType type, const string &msg);
	DUCKDB_API InvalidTypeException(const LogicalType &type, const string &msg);
	DUCKDB_API
	InvalidTypeException(const string &msg); //! Needed to be able to recreate the exception after it's been serialized
};

class TypeMismatchException : public Exception {
public:
	DUCKDB_API TypeMismatchException(const PhysicalType type_1, const PhysicalType type_2, const string &msg);
	DUCKDB_API TypeMismatchException(const LogicalType &type_1, const LogicalType &type_2, const string &msg);
	DUCKDB_API
	TypeMismatchException(const string &msg); //! Needed to be able to recreate the exception after it's been serialized
};

class ParameterNotAllowedException : public Exception {
public:
	DUCKDB_API explicit ParameterNotAllowedException(const string &msg);

	template <typename... Args>
	explicit ParameterNotAllowedException(const string &msg, Args... params)
	    : ParameterNotAllowedException(ConstructMessage(msg, params...)) {
	}
};

//! Special exception that should be thrown in the binder if parameter types could not be resolved
//! This will cause prepared statements to be forcibly rebound with the actual parameter values
//! This exception is fatal if thrown outside of the binder (i.e. it should never be thrown outside of the binder)
class ParameterNotResolvedException : public Exception {
public:
	DUCKDB_API explicit ParameterNotResolvedException();
};

} // namespace duckdb
