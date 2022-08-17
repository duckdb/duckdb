#include "duckdb_python/exceptions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb_python/pybind_wrapper.hpp"

namespace py = pybind11;

namespace duckdb {

class Warning : public std::exception {};
class DatabaseError : public std::exception {};
class DataError : public std::exception {};
class OperationalError : public std::exception {};
class IntegrityError : public std::exception {};
class InternalError : public std::exception {};
class ProgrammingError : public std::exception {};
class NotSupportedError : public std::exception {};

/**
 * @see https://peps.python.org/pep-0249/#exceptions
 */
void RegisterExceptions(const py::module &m) {
	// This is the error structure defined in the DBAPI spec
	// StandardError
	// |__ Warning
	// |__ Error
	//    |__ InterfaceError
	//    |__ DatabaseError
	//       |__ DataError
	//       |__ OperationalError
	//       |__ IntegrityError
	//       |__ InternalError
	//       |__ ProgrammingError
	//       |__ NotSupportedError
	// The base class is mapped to Error in python to somewhat match the DBAPI 2.0 specifications
	auto warning_class = py::register_exception<Warning>(m, "Warning").ptr();
	auto error = py::register_exception<Exception>(m, "Error").ptr();

	// DataError
	auto data_error = py::register_exception<DataError>(m, "DataError", error).ptr();
	py::register_exception<OutOfRangeException>(m, "OutOfRangeException", data_error);
	py::register_exception<CastException>(m, "CastException", data_error);
	py::register_exception<ConversionException>(m, "ConversionException", data_error);
	// no unknown type error, or decimal type
	py::register_exception<TypeMismatchException>(m, "TypeMismatchException", data_error);
	// no divide by zero error
	py::register_exception<ValueOutOfRangeException>(m, "ValueOutOfRangeException", data_error);

	// OperationalError
	auto operational_error = py::register_exception<OperationalError>(m, "OperationalError", error).ptr();
	py::register_exception<TransactionException>(m, "TransactionException", operational_error);
	py::register_exception<OutOfMemoryException>(m, "OutOfMemoryException", operational_error);
	// no object size error
	// no null pointer error
	py::register_exception<IOException>(m, "IOException", operational_error);
	py::register_exception<SerializationException>(m, "SerializationException", operational_error);

	// IntegrityError
	auto integrity_error = py::register_exception<IntegrityError>(m, "IntegrityError", error).ptr();
	py::register_exception<ConstraintException>(m, "ConstraintException", integrity_error);

	// InternalError
	auto internal_error = py::register_exception<InternalError>(m, "InternalError", error).ptr();
	py::register_exception<InternalException>(m, "InternalException", internal_error);

	// ProgrammingError
	auto programming_error = py::register_exception<ProgrammingError>(m, "ProgrammingError", error).ptr();
	py::register_exception<ParserException>(m, "ParserException", programming_error);
	py::register_exception<SyntaxException>(m, "SyntaxException", programming_error);
	py::register_exception<BinderException>(m, "BinderException", programming_error);
	py::register_exception<InvalidInputException>(m, "InvalidInputException", programming_error);
	py::register_exception<InvalidTypeException>(m, "InvalidTypeException", programming_error);
	// no type for expression exceptions?
	py::register_exception<CatalogException>(m, "CatalogException", programming_error);

	// NotSupportedError
	auto not_supported_error = py::register_exception<NotSupportedError>(m, "NotSupportedError", error).ptr();
	py::register_exception<NotImplementedException>(m, "NotImplementedException", not_supported_error);

	// Unknown
	py::register_exception<FatalException>(m, "FatalException", error);
	py::register_exception<InterruptException>(m, "InterruptException", error);
	py::register_exception<PermissionException>(m, "PermissionException", error);
	py::register_exception<SequenceException>(m, "SequenceException", error);
	py::register_exception<StandardException>(m, "StandardException", error);
}
} // namespace duckdb
