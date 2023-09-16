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

string Exception::ExceptionTypeToString(ExceptionType type) {
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
	case ExceptionType::BINDER:
		return "Binder";
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
	case ExceptionType::FATAL:
		return "FATAL";
	case ExceptionType::INTERNAL:
		return "INTERNAL";
	case ExceptionType::INVALID_INPUT:
		return "Invalid Input";
	case ExceptionType::OUT_OF_MEMORY:
		return "Out of Memory";
	case ExceptionType::PERMISSION:
		return "Permission";
	case ExceptionType::PARAMETER_NOT_RESOLVED:
		return "Parameter Not Resolved";
	case ExceptionType::PARAMETER_NOT_ALLOWED:
		return "Parameter Not Allowed";
	case ExceptionType::DEPENDENCY:
		return "Dependency";
	case ExceptionType::MISSING_EXTENSION:
		return "Missing Extension";
	case ExceptionType::HTTP:
		return "HTTP";
	case ExceptionType::AUTOLOAD:
		return "Extension Autoloading";
	default:
		return "Unknown";
	}
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
