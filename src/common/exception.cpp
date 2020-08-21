#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "fmt/format.h"
#include "fmt/printf.h"

namespace duckdb {
using namespace std;

Exception::Exception(string message) : std::exception(), type(ExceptionType::INVALID) {
	exception_message_ = message;
}

Exception::Exception(ExceptionType exception_type, string message) : std::exception(), type(exception_type) {
	exception_message_ = ExceptionTypeToString(exception_type) + ": " + message;
}

const char *Exception::what() const noexcept {
	return exception_message_.c_str();
}

template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(PhysicalType value) {
	return ExceptionFormatValue(TypeIdToString(value));
}
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(LogicalType value) {
	return ExceptionFormatValue(value.ToString());
}
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(float value) {
	return ExceptionFormatValue(double(value));
}
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(double value) {
	return ExceptionFormatValue(double(value));
}
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(string value) {
	return ExceptionFormatValue(string(value));
}
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const char *value) {
	return ExceptionFormatValue(string(value));
}
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(char *value) {
	return ExceptionFormatValue(string(value));
}

string Exception::ConstructMessageRecursive(string msg, vector<ExceptionFormatValue> &values) {
	std::vector<duckdb_fmt::basic_format_arg<duckdb_fmt::printf_context>> format_args;
	for (auto &val : values) {
		switch (val.type) {
		case ExceptionFormatValueType::FORMAT_VALUE_TYPE_DOUBLE:
			format_args.push_back(duckdb_fmt::internal::make_arg<duckdb_fmt::printf_context>(val.dbl_val));
			break;
		case ExceptionFormatValueType::FORMAT_VALUE_TYPE_INTEGER:
			format_args.push_back(duckdb_fmt::internal::make_arg<duckdb_fmt::printf_context>(val.int_val));
			break;
		case ExceptionFormatValueType::FORMAT_VALUE_TYPE_STRING:
			format_args.push_back(duckdb_fmt::internal::make_arg<duckdb_fmt::printf_context>(val.str_val));
			break;
		}
	}
	return duckdb_fmt::vsprintf(msg, duckdb_fmt::basic_format_args<duckdb_fmt::printf_context>(
	                                     format_args.data(), static_cast<int>(format_args.size())));
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

CastException::CastException(const PhysicalType origType, const PhysicalType newType)
    : Exception(ExceptionType::CONVERSION,
                "Type " + TypeIdToString(origType) + " can't be cast as " + TypeIdToString(newType)) {
}

CastException::CastException(const LogicalType origType, const LogicalType newType)
    : Exception(ExceptionType::CONVERSION, "Type " + origType.ToString() + " can't be cast as " + newType.ToString()) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const int64_t value, const PhysicalType origType,
                                                   const PhysicalType newType)
    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(origType) + " with value " +
                                               std::to_string((intmax_t)value) +
                                               " can't be cast because the value is out of range "
                                               "for the destination type " +
                                               TypeIdToString(newType)) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const double value, const PhysicalType origType,
                                                   const PhysicalType newType)
    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(origType) + " with value " + std::to_string(value) +
                                               " can't be cast because the value is out of range "
                                               "for the destination type " +
                                               TypeIdToString(newType)) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const hugeint_t value, const PhysicalType origType,
                                                   const PhysicalType newType)
    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(origType) + " with value " +
                                               value.ToString() +
                                               " can't be cast because the value is out of range "
                                               "for the destination type " +
                                               TypeIdToString(newType)) {
}


ValueOutOfRangeException::ValueOutOfRangeException(const PhysicalType varType, const idx_t length)
    : Exception(ExceptionType::OUT_OF_RANGE, "The value is too long to fit into type " + TypeIdToString(varType) + "(" +
                                                 std::to_string(length) + ")"){};

ConversionException::ConversionException(string msg) : Exception(ExceptionType::CONVERSION, msg) {
}

InvalidTypeException::InvalidTypeException(PhysicalType type, string msg)
    : Exception(ExceptionType::INVALID_TYPE, "Invalid Type [" + TypeIdToString(type) + "]: " + msg) {
}

InvalidTypeException::InvalidTypeException(LogicalType type, string msg)
    : Exception(ExceptionType::INVALID_TYPE, "Invalid Type [" + type.ToString() + "]: " + msg) {
}

TypeMismatchException::TypeMismatchException(const PhysicalType type_1, const PhysicalType type_2, string msg)
    : Exception(ExceptionType::MISMATCH_TYPE,
                "Type " + TypeIdToString(type_1) + " does not match with " + TypeIdToString(type_2) + ". " + msg) {
}

TypeMismatchException::TypeMismatchException(const LogicalType type_1, const LogicalType type_2, string msg)
    : Exception(ExceptionType::MISMATCH_TYPE,
                "Type " + type_1.ToString() + " does not match with " + type_2.ToString() + ". " + msg) {
}

TransactionException::TransactionException(string msg) : Exception(ExceptionType::TRANSACTION, msg) {
}

NotImplementedException::NotImplementedException(string msg) : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {
}

OutOfRangeException::OutOfRangeException(string msg) : Exception(ExceptionType::OUT_OF_RANGE, msg) {
}

CatalogException::CatalogException(string msg) : StandardException(ExceptionType::CATALOG, msg) {
}

ParserException::ParserException(string msg) : StandardException(ExceptionType::PARSER, msg) {
}

SyntaxException::SyntaxException(string msg) : Exception(ExceptionType::SYNTAX, msg) {
}

ConstraintException::ConstraintException(string msg) : Exception(ExceptionType::CONSTRAINT, msg) {
}

BinderException::BinderException(string msg) : StandardException(ExceptionType::BINDER, msg) {
}

IOException::IOException(string msg) : Exception(ExceptionType::IO, msg) {
}

SerializationException::SerializationException(string msg) : Exception(ExceptionType::SERIALIZATION, msg) {
}

SequenceException::SequenceException(string msg) : Exception(ExceptionType::SERIALIZATION, msg) {
}

InterruptException::InterruptException() : Exception(ExceptionType::INTERRUPT, "Interrupted!") {
}

FatalException::FatalException(string msg) : Exception(ExceptionType::FATAL, msg) {
}

InternalException::InternalException(string msg) : Exception(ExceptionType::INTERNAL, msg) {
}

InvalidInputException::InvalidInputException(string msg) : Exception(ExceptionType::INVALID_INPUT, msg) {
}

} // namespace duckdb
