#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {
// Default constructor
PreservedError::PreservedError() : initialized(false) {
}

// From Exception
PreservedError::PreservedError(const Exception &exception)
    : initialized(true), type(exception.type), message(exception.RawMessage()) {
}

// From std::exception
PreservedError::PreservedError(std::exception &exception)
    : initialized(true), type(ExceptionType::INVALID), message(exception.what()) {
}

// From std::string
PreservedError::PreservedError(const string &message)
    : initialized(true), type(ExceptionType::INVALID), message(message) {
}

Exception PreservedError::ToException() const {
	switch (type) {
	case ExceptionType::OUT_OF_RANGE:
		return OutOfRangeException(message);
	case ExceptionType::CONVERSION:
		return CastException(message);
	case ExceptionType::INVALID_TYPE:
		return InvalidTypeException(message);
	case ExceptionType::MISMATCH_TYPE:
		return TypeMismatchException(message);
	case ExceptionType::TRANSACTION:
		return TransactionException(message);
	case ExceptionType::NOT_IMPLEMENTED:
		return NotImplementedException(message);
	case ExceptionType::CATALOG:
		return CatalogException(message);
	case ExceptionType::CONNECTION:
		return ConnectionException(message);
	case ExceptionType::PARSER:
		return ParserException(message);
	case ExceptionType::PERMISSION:
		return PermissionException(message);
	case ExceptionType::SYNTAX:
		return SyntaxException(message);
	case ExceptionType::CONSTRAINT:
		return ConstraintException(message);
	case ExceptionType::BINDER:
		return BinderException(message);
	case ExceptionType::IO:
		return IOException(message);
	case ExceptionType::SERIALIZATION:
		return SerializationException(message);
	case ExceptionType::INTERRUPT:
		return InterruptException();
	case ExceptionType::INTERNAL:
		return InternalException(message);
	case ExceptionType::INVALID_INPUT:
		return InvalidInputException(message);
	case ExceptionType::OUT_OF_MEMORY:
		return OutOfMemoryException(message);
	case ExceptionType::PARAMETER_NOT_ALLOWED:
		return ParameterNotAllowedException(message);
	case ExceptionType::PARAMETER_NOT_RESOLVED:
		return ParameterNotResolvedException();
	default:
		return Exception(type, message);
	}
}

PreservedError::operator bool() {
	return initialized;
}

bool PreservedError::empty() const {
	return !initialized;
}

} // namespace duckdb
