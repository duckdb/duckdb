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
#include "duckdb/common/map.hpp"
#include "duckdb/common/typedefs.hpp"

#include <vector>
#include <stdexcept>

namespace duckdb {
enum class PhysicalType : uint8_t;
struct LogicalType;
struct hugeint_t;

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
	AUTOLOAD = 40           // Thrown when an extension is used but not loaded
};
class HTTPException;

class Exception : public std::exception {
public:
	DUCKDB_API explicit Exception(const string &msg);
	DUCKDB_API Exception(ExceptionType exception_type, const string &message);

	ExceptionType type;

public:
	DUCKDB_API const char *what() const noexcept override;
	DUCKDB_API const string &RawMessage() const;

	DUCKDB_API static string ExceptionTypeToString(ExceptionType type);
	[[noreturn]] DUCKDB_API static void ThrowAsTypeWithMessage(ExceptionType type, const string &message,
	                                                           const std::shared_ptr<Exception> &original);
	virtual std::shared_ptr<Exception> Copy() const {
		return make_shared<Exception>(type, raw_message_);
	}
	DUCKDB_API const HTTPException &AsHTTPException() const;

	template <typename... Args>
	static string ConstructMessage(const string &msg, Args... params) {
		const std::size_t num_args = sizeof...(Args);
		if (num_args == 0)
			return msg;
		std::vector<ExceptionFormatValue> values;
		return ConstructMessageRecursive(msg, values, params...);
	}

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

private:
	string exception_message_;
	string raw_message_;
};

//===--------------------------------------------------------------------===//
// Exception derived classes
//===--------------------------------------------------------------------===//

//! Exceptions that are StandardExceptions do NOT invalidate the current transaction when thrown
class StandardException : public Exception {
public:
	DUCKDB_API StandardException(ExceptionType exception_type, const string &message);
};

class CatalogException : public StandardException {
public:
	DUCKDB_API explicit CatalogException(const string &msg);

	template <typename... Args>
	explicit CatalogException(const string &msg, Args... params) : CatalogException(ConstructMessage(msg, params...)) {
	}
};

class ConnectionException : public StandardException {
public:
	DUCKDB_API explicit ConnectionException(const string &msg);

	template <typename... Args>
	explicit ConnectionException(const string &msg, Args... params)
	    : ConnectionException(ConstructMessage(msg, params...)) {
	}
};

class ParserException : public StandardException {
public:
	DUCKDB_API explicit ParserException(const string &msg);

	template <typename... Args>
	explicit ParserException(const string &msg, Args... params) : ParserException(ConstructMessage(msg, params...)) {
	}
};

class PermissionException : public StandardException {
public:
	DUCKDB_API explicit PermissionException(const string &msg);

	template <typename... Args>
	explicit PermissionException(const string &msg, Args... params)
	    : PermissionException(ConstructMessage(msg, params...)) {
	}
};

class BinderException : public StandardException {
public:
	DUCKDB_API explicit BinderException(const string &msg);

	template <typename... Args>
	explicit BinderException(const string &msg, Args... params) : BinderException(ConstructMessage(msg, params...)) {
	}
};

class ConversionException : public Exception {
public:
	DUCKDB_API explicit ConversionException(const string &msg);

	template <typename... Args>
	explicit ConversionException(const string &msg, Args... params)
	    : ConversionException(ConstructMessage(msg, params...)) {
	}
};

class TransactionException : public Exception {
public:
	DUCKDB_API explicit TransactionException(const string &msg);

	template <typename... Args>
	explicit TransactionException(const string &msg, Args... params)
	    : TransactionException(ConstructMessage(msg, params...)) {
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

class OutOfRangeException : public Exception {
public:
	DUCKDB_API explicit OutOfRangeException(const string &msg);

	template <typename... Args>
	explicit OutOfRangeException(const string &msg, Args... params)
	    : OutOfRangeException(ConstructMessage(msg, params...)) {
	}
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

class AutoloadException : public Exception {
public:
	DUCKDB_API explicit AutoloadException(const string &extension_name, Exception &e);

	template <typename... Args>
	explicit AutoloadException(const string &extension_name, Exception &e, Args... params)
	    : AutoloadException(ConstructMessage(extension_name, e, params...)) {
	}

protected:
	Exception &wrapped_exception;
};

class HTTPException : public IOException {
public:
	template <typename>
	struct ResponseShape {
		typedef int status;
	};

	template <class RESPONSE, typename ResponseShape<decltype(RESPONSE::status)>::status = 0, typename... ARGS>
	explicit HTTPException(RESPONSE &response, const string &msg, ARGS... params)
	    : HTTPException(response.status, response.body, response.headers, response.reason, msg, params...) {
	}

	template <typename>
	struct ResponseWrapperShape {
		typedef int code;
	};
	template <class RESPONSE, typename ResponseWrapperShape<decltype(RESPONSE::code)>::code = 0, typename... ARGS>
	explicit HTTPException(RESPONSE &response, const string &msg, ARGS... params)
	    : HTTPException(response.code, response.body, response.headers, response.error, msg, params...) {
	}

	template <typename HEADERS, typename... ARGS>
	explicit HTTPException(int status_code, string response_body, HEADERS headers, const string &reason,
	                       const string &msg, ARGS... params)
	    : IOException(ExceptionType::HTTP, ConstructMessage(msg, params...)), status_code(status_code), reason(reason),
	      response_body(std::move(response_body)) {
		this->headers.insert(headers.begin(), headers.end());
		D_ASSERT(this->headers.size() > 0);
	}

	std::shared_ptr<Exception> Copy() const {
		return make_shared<HTTPException>(status_code, response_body, headers, reason, RawMessage());
	}

	const std::multimap<std::string, std::string> GetHeaders() const {
		return headers;
	}
	int GetStatusCode() const {
		return status_code;
	}
	const string &GetResponseBody() const {
		return response_body;
	}
	const string &GetReason() const {
		return reason;
	}
	[[noreturn]] void Throw() const {
		throw HTTPException(status_code, response_body, headers, reason, RawMessage());
	}

private:
	int status_code;
	string reason;
	string response_body;
	std::multimap<string, string> headers;
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

class InternalException : public FatalException {
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

	template <typename... Args>
	explicit InvalidInputException(const string &msg, Args... params)
	    : InvalidInputException(ConstructMessage(msg, params...)) {
	}
};

class CastException : public Exception {
public:
	DUCKDB_API CastException(const PhysicalType origType, const PhysicalType newType);
	DUCKDB_API CastException(const LogicalType &origType, const LogicalType &newType);
	DUCKDB_API
	CastException(const string &msg); //! Needed to be able to recreate the exception after it's been serialized
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

class ValueOutOfRangeException : public Exception {
public:
	DUCKDB_API ValueOutOfRangeException(const int64_t value, const PhysicalType origType, const PhysicalType newType);
	DUCKDB_API ValueOutOfRangeException(const hugeint_t value, const PhysicalType origType, const PhysicalType newType);
	DUCKDB_API ValueOutOfRangeException(const double value, const PhysicalType origType, const PhysicalType newType);
	DUCKDB_API ValueOutOfRangeException(const PhysicalType varType, const idx_t length);
	DUCKDB_API ValueOutOfRangeException(
	    const string &msg); //! Needed to be able to recreate the exception after it's been serialized
};

class ParameterNotAllowedException : public StandardException {
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
