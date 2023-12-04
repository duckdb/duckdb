#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types.hpp"

#ifdef DUCKDB_CRASH_ON_ASSERT
#include "duckdb/common/printer.hpp"
#include <stdio.h>
#include <stdlib.h>
#endif
#ifdef DUCKDB_DEBUG_STACKTRACE
#include <execinfo.h>
#endif

namespace duckdb {

Exception::Exception(const string &msg) : std::exception(), type(ExceptionType::INVALID), raw_message_(msg) {
	exception_message_ = msg;
}

Exception::Exception(ExceptionType exception_type, const string &message)
    : std::exception(), type(exception_type), raw_message_(message) {
	exception_message_ = ExceptionTypeToString(exception_type) + " Error: " + message;
}

const char *Exception::what() const noexcept {
	return exception_message_.c_str();
}

const string &Exception::RawMessage() const {
	return raw_message_;
}

bool Exception::UncaughtException() {
#if __cplusplus >= 201703L
	return std::uncaught_exceptions() > 0;
#else
	return std::uncaught_exception();
#endif
}

string Exception::GetStackTrace(int max_depth) {
#ifdef DUCKDB_DEBUG_STACKTRACE
	string result;
	auto callstack = unique_ptr<void *[]>(new void *[max_depth]);
	int frames = backtrace(callstack.get(), max_depth);
	char **strs = backtrace_symbols(callstack.get(), frames);
	for (int i = 0; i < frames; i++) {
		result += strs[i];
		result += "\n";
	}
	free(strs);
	return "\n" + result;
#else
	// Stack trace not available. Toggle DUCKDB_DEBUG_STACKTRACE in exception.cpp to enable stack traces.
	return "";
#endif
}

string Exception::ConstructMessageRecursive(const string &msg, std::vector<ExceptionFormatValue> &values) {
#ifdef DEBUG
	// Verify that we have the required amount of values for the message
	idx_t parameter_count = 0;
	for (idx_t i = 0; i + 1 < msg.size(); i++) {
		if (msg[i] != '%') {
			continue;
		}
		if (msg[i + 1] == '%') {
			i++;
			continue;
		}
		parameter_count++;
	}
	if (parameter_count != values.size()) {
		throw InternalException("Primary exception: %s\nSecondary exception in ConstructMessageRecursive: Expected %d "
		                        "parameters, received %d",
		                        msg.c_str(), parameter_count, values.size());
	}

#endif
	return ExceptionFormatValue::Format(msg, values);
}

struct ExceptionEntry {
	ExceptionType type;
	char text[48];
};

static constexpr ExceptionEntry EXCEPTION_MAP[] = {{ExceptionType::INVALID, "Invalid"},
                                                   {ExceptionType::OUT_OF_RANGE, "Out of Range"},
                                                   {ExceptionType::CONVERSION, "Conversion"},
                                                   {ExceptionType::UNKNOWN_TYPE, "Unknown Type"},
                                                   {ExceptionType::DECIMAL, "Decimal"},
                                                   {ExceptionType::MISMATCH_TYPE, "Mismatch Type"},
                                                   {ExceptionType::DIVIDE_BY_ZERO, "Divide by Zero"},
                                                   {ExceptionType::OBJECT_SIZE, "Object Size"},
                                                   {ExceptionType::INVALID_TYPE, "Invalid type"},
                                                   {ExceptionType::SERIALIZATION, "Serialization"},
                                                   {ExceptionType::TRANSACTION, "TransactionContext"},
                                                   {ExceptionType::NOT_IMPLEMENTED, "Not implemented"},
                                                   {ExceptionType::EXPRESSION, "Expression"},
                                                   {ExceptionType::CATALOG, "Catalog"},
                                                   {ExceptionType::PARSER, "Parser"},
                                                   {ExceptionType::BINDER, "Binder"},
                                                   {ExceptionType::PLANNER, "Planner"},
                                                   {ExceptionType::SCHEDULER, "Scheduler"},
                                                   {ExceptionType::EXECUTOR, "Executor"},
                                                   {ExceptionType::CONSTRAINT, "Constraint"},
                                                   {ExceptionType::INDEX, "Index"},
                                                   {ExceptionType::STAT, "Stat"},
                                                   {ExceptionType::CONNECTION, "Connection"},
                                                   {ExceptionType::SYNTAX, "Syntax"},
                                                   {ExceptionType::SETTINGS, "Settings"},
                                                   {ExceptionType::OPTIMIZER, "Optimizer"},
                                                   {ExceptionType::NULL_POINTER, "NullPointer"},
                                                   {ExceptionType::IO, "IO"},
                                                   {ExceptionType::INTERRUPT, "INTERRUPT"},
                                                   {ExceptionType::FATAL, "FATAL"},
                                                   {ExceptionType::INTERNAL, "INTERNAL"},
                                                   {ExceptionType::INVALID_INPUT, "Invalid Input"},
                                                   {ExceptionType::OUT_OF_MEMORY, "Out of Memory"},
                                                   {ExceptionType::PERMISSION, "Permission"},
                                                   {ExceptionType::PARAMETER_NOT_RESOLVED, "Parameter Not Resolved"},
                                                   {ExceptionType::PARAMETER_NOT_ALLOWED, "Parameter Not Allowed"},
                                                   {ExceptionType::DEPENDENCY, "Dependency"},
                                                   {ExceptionType::MISSING_EXTENSION, "Missing Extension"},
                                                   {ExceptionType::HTTP, "HTTP"},
                                                   {ExceptionType::AUTOLOAD, "Extension Autoloading"}};

string Exception::ExceptionTypeToString(ExceptionType type) {
	for (auto &e : EXCEPTION_MAP) {
		if (e.type == type) {
			return e.text;
		}
	}
	return "Unknown";
}

ExceptionType Exception::StringToExceptionType(const string &type) {
	for (auto &e : EXCEPTION_MAP) {
		if (e.text == type) {
			return e.type;
		}
	}
	return ExceptionType::INVALID;
}

const HTTPException &Exception::AsHTTPException() const {
	D_ASSERT(type == ExceptionType::HTTP);
	const auto &e = static_cast<const HTTPException *>(this);
	D_ASSERT(e->GetStatusCode() != 0);
	D_ASSERT(e->GetHeaders().size() > 0);
	return *e;
}

void Exception::ThrowAsTypeWithMessage(ExceptionType type, const string &message,
                                       const std::shared_ptr<Exception> &original) {
	switch (type) {
	case ExceptionType::OUT_OF_RANGE:
		throw OutOfRangeException(message);
	case ExceptionType::CONVERSION:
		throw ConversionException(message); // FIXME: make a separation between Conversion/Cast exception?
	case ExceptionType::INVALID_TYPE:
		throw InvalidTypeException(message);
	case ExceptionType::MISMATCH_TYPE:
		throw TypeMismatchException(message);
	case ExceptionType::TRANSACTION:
		throw TransactionException(message);
	case ExceptionType::NOT_IMPLEMENTED:
		throw NotImplementedException(message);
	case ExceptionType::CATALOG:
		throw CatalogException(message);
	case ExceptionType::CONNECTION:
		throw ConnectionException(message);
	case ExceptionType::PARSER:
		throw ParserException(message);
	case ExceptionType::PERMISSION:
		throw PermissionException(message);
	case ExceptionType::SYNTAX:
		throw SyntaxException(message);
	case ExceptionType::CONSTRAINT:
		throw ConstraintException(message);
	case ExceptionType::BINDER:
		throw BinderException(message);
	case ExceptionType::IO:
		throw IOException(message);
	case ExceptionType::SERIALIZATION:
		throw SerializationException(message);
	case ExceptionType::INTERRUPT:
		throw InterruptException();
	case ExceptionType::INTERNAL:
		throw InternalException(message);
	case ExceptionType::INVALID_INPUT:
		throw InvalidInputException(message);
	case ExceptionType::OUT_OF_MEMORY:
		throw OutOfMemoryException(message);
	case ExceptionType::PARAMETER_NOT_ALLOWED:
		throw ParameterNotAllowedException(message);
	case ExceptionType::PARAMETER_NOT_RESOLVED:
		throw ParameterNotResolvedException();
	case ExceptionType::FATAL:
		throw FatalException(message);
	case ExceptionType::DEPENDENCY:
		throw DependencyException(message);
	case ExceptionType::HTTP: {
		original->AsHTTPException().Throw();
	}
	case ExceptionType::MISSING_EXTENSION:
		throw MissingExtensionException(message);
	default:
		throw Exception(type, message);
	}
}

StandardException::StandardException(ExceptionType exception_type, const string &message)
    : Exception(exception_type, message) {
}

CastException::CastException(const PhysicalType orig_type, const PhysicalType new_type)
    : Exception(ExceptionType::CONVERSION,
                "Type " + TypeIdToString(orig_type) + " can't be cast as " + TypeIdToString(new_type)) {
}

CastException::CastException(const LogicalType &orig_type, const LogicalType &new_type)
    : Exception(ExceptionType::CONVERSION,
                "Type " + orig_type.ToString() + " can't be cast as " + new_type.ToString()) {
}

CastException::CastException(const string &msg) : Exception(ExceptionType::CONVERSION, msg) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const int64_t value, const PhysicalType orig_type,
                                                   const PhysicalType new_type)
    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(orig_type) + " with value " +
                                               to_string((intmax_t)value) +
                                               " can't be cast because the value is out of range "
                                               "for the destination type " +
                                               TypeIdToString(new_type)) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const double value, const PhysicalType orig_type,
                                                   const PhysicalType new_type)
    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(orig_type) + " with value " + to_string(value) +
                                               " can't be cast because the value is out of range "
                                               "for the destination type " +
                                               TypeIdToString(new_type)) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const hugeint_t value, const PhysicalType orig_type,
                                                   const PhysicalType new_type)
    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(orig_type) + " with value " + value.ToString() +
                                               " can't be cast because the value is out of range "
                                               "for the destination type " +
                                               TypeIdToString(new_type)) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const PhysicalType var_type, const idx_t length)
    : Exception(ExceptionType::OUT_OF_RANGE,
                "The value is too long to fit into type " + TypeIdToString(var_type) + "(" + to_string(length) + ")") {
}

ValueOutOfRangeException::ValueOutOfRangeException(const string &msg) : Exception(ExceptionType::OUT_OF_RANGE, msg) {
}

ConversionException::ConversionException(const string &msg) : Exception(ExceptionType::CONVERSION, msg) {
}

InvalidTypeException::InvalidTypeException(PhysicalType type, const string &msg)
    : Exception(ExceptionType::INVALID_TYPE, "Invalid Type [" + TypeIdToString(type) + "]: " + msg) {
}

InvalidTypeException::InvalidTypeException(const LogicalType &type, const string &msg)
    : Exception(ExceptionType::INVALID_TYPE, "Invalid Type [" + type.ToString() + "]: " + msg) {
}

InvalidTypeException::InvalidTypeException(const string &msg) : Exception(ExceptionType::INVALID_TYPE, msg) {
}

TypeMismatchException::TypeMismatchException(const PhysicalType type_1, const PhysicalType type_2, const string &msg)
    : Exception(ExceptionType::MISMATCH_TYPE,
                "Type " + TypeIdToString(type_1) + " does not match with " + TypeIdToString(type_2) + ". " + msg) {
}

TypeMismatchException::TypeMismatchException(const LogicalType &type_1, const LogicalType &type_2, const string &msg)
    : Exception(ExceptionType::MISMATCH_TYPE,
                "Type " + type_1.ToString() + " does not match with " + type_2.ToString() + ". " + msg) {
}

TypeMismatchException::TypeMismatchException(const string &msg) : Exception(ExceptionType::MISMATCH_TYPE, msg) {
}

TransactionException::TransactionException(const string &msg) : Exception(ExceptionType::TRANSACTION, msg) {
}

NotImplementedException::NotImplementedException(const string &msg) : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {
}

OutOfRangeException::OutOfRangeException(const string &msg) : Exception(ExceptionType::OUT_OF_RANGE, msg) {
}

CatalogException::CatalogException(const string &msg) : StandardException(ExceptionType::CATALOG, msg) {
}

ConnectionException::ConnectionException(const string &msg) : StandardException(ExceptionType::CONNECTION, msg) {
}

ParserException::ParserException(const string &msg) : StandardException(ExceptionType::PARSER, msg) {
}

PermissionException::PermissionException(const string &msg) : StandardException(ExceptionType::PERMISSION, msg) {
}

SyntaxException::SyntaxException(const string &msg) : Exception(ExceptionType::SYNTAX, msg) {
}

ConstraintException::ConstraintException(const string &msg) : Exception(ExceptionType::CONSTRAINT, msg) {
}

DependencyException::DependencyException(const string &msg) : Exception(ExceptionType::DEPENDENCY, msg) {
}

BinderException::BinderException(const string &msg) : StandardException(ExceptionType::BINDER, msg) {
}

IOException::IOException(const string &msg) : Exception(ExceptionType::IO, msg) {
}

MissingExtensionException::MissingExtensionException(const string &msg)
    : Exception(ExceptionType::MISSING_EXTENSION, msg) {
}

AutoloadException::AutoloadException(const string &extension_name, Exception &e)
    : Exception(ExceptionType::AUTOLOAD,
                "An error occurred while trying to automatically install the required extension '" + extension_name +
                    "':\n" + e.RawMessage()),
      wrapped_exception(e) {
}

SerializationException::SerializationException(const string &msg) : Exception(ExceptionType::SERIALIZATION, msg) {
}

SequenceException::SequenceException(const string &msg) : Exception(ExceptionType::SERIALIZATION, msg) {
}

InterruptException::InterruptException() : Exception(ExceptionType::INTERRUPT, "Interrupted!") {
}

FatalException::FatalException(ExceptionType type, const string &msg) : Exception(type, msg) {
}

InternalException::InternalException(const string &msg) : FatalException(ExceptionType::INTERNAL, msg) {
#ifdef DUCKDB_CRASH_ON_ASSERT
	Printer::Print("ABORT THROWN BY INTERNAL EXCEPTION: " + msg);
	abort();
#endif
}

InvalidInputException::InvalidInputException(const string &msg) : Exception(ExceptionType::INVALID_INPUT, msg) {
}

OutOfMemoryException::OutOfMemoryException(const string &msg) : Exception(ExceptionType::OUT_OF_MEMORY, msg) {
}

ParameterNotAllowedException::ParameterNotAllowedException(const string &msg)
    : StandardException(ExceptionType::PARAMETER_NOT_ALLOWED, msg) {
}

ParameterNotResolvedException::ParameterNotResolvedException()
    : Exception(ExceptionType::PARAMETER_NOT_RESOLVED, "Parameter types could not be resolved") {
}

} // namespace duckdb
