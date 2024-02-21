#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception/list.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/planner/expression.hpp"

#ifdef DUCKDB_CRASH_ON_ASSERT
#include "duckdb/common/printer.hpp"
#include <stdio.h>
#include <stdlib.h>
#endif
#ifdef DUCKDB_DEBUG_STACKTRACE
#include <execinfo.h>
#endif

namespace duckdb {

Exception::Exception(ExceptionType exception_type, const string &message)
    : std::runtime_error(ToJSON(exception_type, message)) {
}

Exception::Exception(ExceptionType exception_type, const string &message,
                     const unordered_map<string, string> &extra_info)
    : std::runtime_error(ToJSON(exception_type, message, extra_info)) {
}

string Exception::ToJSON(ExceptionType type, const string &message) {
	unordered_map<string, string> extra_info;
	return ToJSON(type, message, extra_info);
}

string Exception::ToJSON(ExceptionType type, const string &message, const unordered_map<string, string> &extra_info) {
#ifdef DUCKDB_DEBUG_STACKTRACE
	auto extended_extra_info = extra_info;
	extended_extra_info["stack_trace"] = Exception::GetStackTrace();
	return StringUtil::ToJSONMap(type, message, extended_extra_info);
#else
	return StringUtil::ToJSONMap(type, message, extra_info);
#endif
}

bool Exception::UncaughtException() {
#if __cplusplus >= 201703L
	return std::uncaught_exceptions() > 0;
#else
	return std::uncaught_exception();
#endif
}

bool Exception::InvalidatesTransaction(ExceptionType exception_type) {
	switch (exception_type) {
	case ExceptionType::BINDER:
	case ExceptionType::CATALOG:
	case ExceptionType::CONNECTION:
	case ExceptionType::PARAMETER_NOT_ALLOWED:
	case ExceptionType::PARSER:
	case ExceptionType::PERMISSION:
		return false;
	default:
		return true;
	}
}

bool Exception::InvalidatesDatabase(ExceptionType exception_type) {
	switch (exception_type) {
	case ExceptionType::INTERNAL:
	case ExceptionType::FATAL:
		return true;
	default:
		return false;
	}
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
                                                   {ExceptionType::AUTOLOAD, "Extension Autoloading"},
                                                   {ExceptionType::SEQUENCE, "Sequence"}};

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

unordered_map<string, string> Exception::InitializeExtraInfo(const Expression &expr) {
	return InitializeExtraInfo(expr.query_location);
}

unordered_map<string, string> Exception::InitializeExtraInfo(const ParsedExpression &expr) {
	return InitializeExtraInfo(expr.query_location);
}

unordered_map<string, string> Exception::InitializeExtraInfo(const QueryErrorContext &error_context) {
	return InitializeExtraInfo(error_context.query_location);
}

unordered_map<string, string> Exception::InitializeExtraInfo(const TableRef &ref) {
	return InitializeExtraInfo(ref.query_location);
}

unordered_map<string, string> Exception::InitializeExtraInfo(optional_idx error_location) {
	unordered_map<string, string> result;
	SetQueryLocation(error_location, result);
	return result;
}

unordered_map<string, string> Exception::InitializeExtraInfo(const string &subtype, optional_idx error_location) {
	unordered_map<string, string> result;
	result["error_subtype"] = subtype;
	SetQueryLocation(error_location, result);
	return result;
}

void Exception::SetQueryLocation(optional_idx error_location, unordered_map<string, string> &extra_info) {
	if (error_location.IsValid()) {
		extra_info["position"] = to_string(error_location.GetIndex());
	}
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
    : TypeMismatchException(optional_idx(), type_1, type_2, msg) {
}

TypeMismatchException::TypeMismatchException(optional_idx error_location, const LogicalType &type_1,
                                             const LogicalType &type_2, const string &msg)
    : Exception(ExceptionType::MISMATCH_TYPE,
                "Type " + type_1.ToString() + " does not match with " + type_2.ToString() + ". " + msg,
                Exception::InitializeExtraInfo(error_location)) {
}

TypeMismatchException::TypeMismatchException(const string &msg) : Exception(ExceptionType::MISMATCH_TYPE, msg) {
}

TransactionException::TransactionException(const string &msg) : Exception(ExceptionType::TRANSACTION, msg) {
}

NotImplementedException::NotImplementedException(const string &msg) : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {
}

OutOfRangeException::OutOfRangeException(const string &msg) : Exception(ExceptionType::OUT_OF_RANGE, msg) {
}

OutOfRangeException::OutOfRangeException(const int64_t value, const PhysicalType orig_type, const PhysicalType new_type)
    : Exception(ExceptionType::OUT_OF_RANGE, "Type " + TypeIdToString(orig_type) + " with value " +
                                                 to_string((intmax_t)value) +
                                                 " can't be cast because the value is out of range "
                                                 "for the destination type " +
                                                 TypeIdToString(new_type)) {
}

OutOfRangeException::OutOfRangeException(const double value, const PhysicalType orig_type, const PhysicalType new_type)
    : Exception(ExceptionType::OUT_OF_RANGE, "Type " + TypeIdToString(orig_type) + " with value " + to_string(value) +
                                                 " can't be cast because the value is out of range "
                                                 "for the destination type " +
                                                 TypeIdToString(new_type)) {
}

OutOfRangeException::OutOfRangeException(const hugeint_t value, const PhysicalType orig_type,
                                         const PhysicalType new_type)
    : Exception(ExceptionType::OUT_OF_RANGE, "Type " + TypeIdToString(orig_type) + " with value " + value.ToString() +
                                                 " can't be cast because the value is out of range "
                                                 "for the destination type " +
                                                 TypeIdToString(new_type)) {
}

OutOfRangeException::OutOfRangeException(const PhysicalType var_type, const idx_t length)
    : Exception(ExceptionType::OUT_OF_RANGE,
                "The value is too long to fit into type " + TypeIdToString(var_type) + "(" + to_string(length) + ")") {
}

ConnectionException::ConnectionException(const string &msg) : Exception(ExceptionType::CONNECTION, msg) {
}

PermissionException::PermissionException(const string &msg) : Exception(ExceptionType::PERMISSION, msg) {
}

SyntaxException::SyntaxException(const string &msg) : Exception(ExceptionType::SYNTAX, msg) {
}

ConstraintException::ConstraintException(const string &msg) : Exception(ExceptionType::CONSTRAINT, msg) {
}

DependencyException::DependencyException(const string &msg) : Exception(ExceptionType::DEPENDENCY, msg) {
}

IOException::IOException(const string &msg) : Exception(ExceptionType::IO, msg) {
}

IOException::IOException(const string &msg, const unordered_map<string, string> &extra_info)
    : Exception(ExceptionType::IO, msg, extra_info) {
}

MissingExtensionException::MissingExtensionException(const string &msg)
    : Exception(ExceptionType::MISSING_EXTENSION, msg) {
}

AutoloadException::AutoloadException(const string &extension_name, const string &message)
    : Exception(ExceptionType::AUTOLOAD,
                "An error occurred while trying to automatically install the required extension '" + extension_name +
                    "':\n" + message) {
}

SerializationException::SerializationException(const string &msg) : Exception(ExceptionType::SERIALIZATION, msg) {
}

SequenceException::SequenceException(const string &msg) : Exception(ExceptionType::SEQUENCE, msg) {
}

InterruptException::InterruptException() : Exception(ExceptionType::INTERRUPT, "Interrupted!") {
}

FatalException::FatalException(ExceptionType type, const string &msg) : Exception(type, msg) {
}

InternalException::InternalException(const string &msg) : Exception(ExceptionType::INTERNAL, msg) {
#ifdef DUCKDB_CRASH_ON_ASSERT
	Printer::Print("ABORT THROWN BY INTERNAL EXCEPTION: " + msg);
	abort();
#endif
}

InvalidInputException::InvalidInputException(const string &msg) : Exception(ExceptionType::INVALID_INPUT, msg) {
}

InvalidInputException::InvalidInputException(const string &msg, const unordered_map<string, string> &extra_info)
    : Exception(ExceptionType::INVALID_INPUT, msg, extra_info) {
}

OutOfMemoryException::OutOfMemoryException(const string &msg) : Exception(ExceptionType::OUT_OF_MEMORY, msg) {
}

ParameterNotAllowedException::ParameterNotAllowedException(const string &msg)
    : Exception(ExceptionType::PARAMETER_NOT_ALLOWED, msg) {
}

ParameterNotResolvedException::ParameterNotResolvedException()
    : Exception(ExceptionType::PARAMETER_NOT_RESOLVED, "Parameter types could not be resolved") {
}

} // namespace duckdb
