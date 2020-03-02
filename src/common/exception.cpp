#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

#define FORMAT_CONSTRUCTOR(msg)                                                                                        \
	va_list ap;                                                                                                        \
	va_start(ap, msg);                                                                                                 \
	Format(ap);                                                                                                        \
	va_end(ap);

Exception::Exception(string message) : std::exception(), type(ExceptionType::INVALID) {
	exception_message_ = message;
}

Exception::Exception(ExceptionType exception_type, string message) : std::exception(), type(exception_type) {
	exception_message_ = ExceptionTypeToString(exception_type) + ": " + message;
}

const char *Exception::what() const noexcept {
	return exception_message_.c_str();
}

void Exception::Format(va_list ap) {
	exception_message_ = StringUtil::VFormat(exception_message_, ap);
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
	default:
		return "Unknown";
	}
}

CastException::CastException(const TypeId origType, const TypeId newType)
    : Exception(ExceptionType::CONVERSION,
                "Type " + TypeIdToString(origType) + " can't be cast as " + TypeIdToString(newType)) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const int64_t value, const TypeId origType, const TypeId newType)
    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(origType) + " with value " +
                                               std::to_string((intmax_t)value) +
                                               " can't be cast because the value is out of range "
                                               "for the destination type " +
                                               TypeIdToString(newType)) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const double value, const TypeId origType, const TypeId newType)
    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(origType) + " with value " + std::to_string(value) +
                                               " can't be cast because the value is out of range "
                                               "for the destination type " +
                                               TypeIdToString(newType)) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const TypeId varType, const idx_t length)
    : Exception(ExceptionType::OUT_OF_RANGE, "The value is too long to fit into type " + TypeIdToString(varType) + "(" +
                                                 std::to_string(length) + ")"){};

ConversionException::ConversionException(string msg, ...) : Exception(ExceptionType::CONVERSION, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

InvalidTypeException::InvalidTypeException(TypeId type, string msg)
    : Exception(ExceptionType::INVALID_TYPE, "Invalid Type [" + TypeIdToString(type) + "]: " + msg) {
}

TypeMismatchException::TypeMismatchException(const TypeId type_1, const TypeId type_2, string msg)
    : Exception(ExceptionType::MISMATCH_TYPE,
                "Type " + TypeIdToString(type_1) + " does not match with " + TypeIdToString(type_2) + ". " + msg) {
}

TransactionException::TransactionException(string msg, ...) : Exception(ExceptionType::TRANSACTION, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

NotImplementedException::NotImplementedException(string msg, ...) : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

OutOfRangeException::OutOfRangeException(string msg, ...) : Exception(ExceptionType::OUT_OF_RANGE, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

CatalogException::CatalogException(string msg, ...) : StandardException(ExceptionType::CATALOG, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

ParserException::ParserException(string msg, ...) : StandardException(ExceptionType::PARSER, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

SyntaxException::SyntaxException(string msg, ...) : Exception(ExceptionType::SYNTAX, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

ConstraintException::ConstraintException(string msg, ...) : Exception(ExceptionType::CONSTRAINT, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

BinderException::BinderException(string msg, ...) : StandardException(ExceptionType::BINDER, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

IOException::IOException(string msg, ...) : Exception(ExceptionType::IO, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

SerializationException::SerializationException(string msg, ...) : Exception(ExceptionType::SERIALIZATION, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

SequenceException::SequenceException(string msg, ...) : Exception(ExceptionType::SERIALIZATION, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

InterruptException::InterruptException() : Exception(ExceptionType::INTERRUPT, "Interrupted!") {
}

FatalException::FatalException(string msg, ...) : Exception(ExceptionType::FATAL, msg) {
	FORMAT_CONSTRUCTOR(msg);
}

InternalException::InternalException(string msg, ...) : Exception(ExceptionType::INTERNAL, msg) {
	FORMAT_CONSTRUCTOR(msg);
}
