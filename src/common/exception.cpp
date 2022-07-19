#include "duckdb/common/exception.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

Exception::Exception(const string &msg) : std::exception(), type(ExceptionType::INVALID) {
	exception_message_ = msg;
}

Exception::Exception(ExceptionType exception_type, const string &message) : std::exception(), type(exception_type) {
	exception_message_ = ExceptionTypeToString(exception_type) + " Error: " + message;
}

const char *Exception::what() const noexcept {
	return exception_message_.c_str();
}

bool Exception::UncaughtException() {
#if __cplusplus >= 201703L
	return std::uncaught_exceptions() > 0;
#else
	return std::uncaught_exception();
#endif
}

string Exception::ConstructMessageRecursive(const string &msg, vector<ExceptionFormatValue> &values) {
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
	default:
		return "Unknown";
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

ConversionException::ConversionException(const string &msg) : Exception(ExceptionType::CONVERSION, msg) {
}

InvalidTypeException::InvalidTypeException(PhysicalType type, const string &msg)
    : Exception(ExceptionType::INVALID_TYPE, "Invalid Type [" + TypeIdToString(type) + "]: " + msg) {
}

InvalidTypeException::InvalidTypeException(const LogicalType &type, const string &msg)
    : Exception(ExceptionType::INVALID_TYPE, "Invalid Type [" + type.ToString() + "]: " + msg) {
}

TypeMismatchException::TypeMismatchException(const PhysicalType type_1, const PhysicalType type_2, const string &msg)
    : Exception(ExceptionType::MISMATCH_TYPE,
                "Type " + TypeIdToString(type_1) + " does not match with " + TypeIdToString(type_2) + ". " + msg) {
}

TypeMismatchException::TypeMismatchException(const LogicalType &type_1, const LogicalType &type_2, const string &msg)
    : Exception(ExceptionType::MISMATCH_TYPE,
                "Type " + type_1.ToString() + " does not match with " + type_2.ToString() + ". " + msg) {
}

TransactionException::TransactionException(const string &msg) : Exception(ExceptionType::TRANSACTION, msg) {
}

NotImplementedException::NotImplementedException(const string &msg) : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {
}

OutOfRangeException::OutOfRangeException(const string &msg) : Exception(ExceptionType::OUT_OF_RANGE, msg) {
}

CatalogException::CatalogException(const string &msg) : StandardException(ExceptionType::CATALOG, msg) {
}

ParserException::ParserException(const string &msg) : StandardException(ExceptionType::PARSER, msg) {
}

PermissionException::PermissionException(const string &msg) : StandardException(ExceptionType::PERMISSION, msg) {
}

SyntaxException::SyntaxException(const string &msg) : Exception(ExceptionType::SYNTAX, msg) {
}

ConstraintException::ConstraintException(const string &msg) : Exception(ExceptionType::CONSTRAINT, msg) {
}

BinderException::BinderException(const string &msg) : StandardException(ExceptionType::BINDER, msg) {
}

IOException::IOException(const string &msg) : Exception(ExceptionType::IO, msg) {
}

SerializationException::SerializationException(const string &msg) : Exception(ExceptionType::SERIALIZATION, msg) {
}

SequenceException::SequenceException(const string &msg) : Exception(ExceptionType::SERIALIZATION, msg) {
}

InterruptException::InterruptException() : Exception(ExceptionType::INTERRUPT, "Interrupted!") {
}

FatalException::FatalException(const string &msg) : Exception(ExceptionType::FATAL, msg) {
}

InternalException::InternalException(const string &msg) : Exception(ExceptionType::INTERNAL, msg) {
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
