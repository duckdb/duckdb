#include "duckdb_python/exceptions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb_python/pybind_wrapper.hpp"

namespace py = pybind11;

namespace duckdb {

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
	auto error_class = py::register_exception<Exception>(m, "Error").ptr();

	py::register_exception<NotImplementedException>(m, "NotSupportedError", error_class);

	py::register_exception<BinderException>(m, "BinderException", error_class);
	py::register_exception<CastException>(m, "CastException", error_class);
	py::register_exception<CatalogException>(m, "CatalogException", error_class);
	py::register_exception<ConstraintException>(m, "ConstraintException", error_class);
	py::register_exception<ConversionException>(m, "ConversionException", error_class);
	py::register_exception<FatalException>(m, "FatalException", error_class);
	py::register_exception<InternalException>(m, "InternalException", error_class);
	py::register_exception<InterruptException>(m, "InterruptException", error_class);
	py::register_exception<InvalidInputException>(m, "InvalidInputException", error_class);
	py::register_exception<InvalidTypeException>(m, "InvalidTypeException", error_class);
	py::register_exception<IOException>(m, "IOException", error_class);
	py::register_exception<OutOfMemoryException>(m, "OutOfMemoryException", error_class);
	py::register_exception<OutOfRangeException>(m, "OutOfRangeException", error_class);
	py::register_exception<ParserException>(m, "ParserException", error_class);
	py::register_exception<PermissionException>(m, "PermissionException", error_class);
	py::register_exception<SequenceException>(m, "SequenceException", error_class);
	py::register_exception<SerializationException>(m, "SerializationException", error_class);
	py::register_exception<SyntaxException>(m, "SyntaxException", error_class);
	py::register_exception<TransactionException>(m, "TransactionException", error_class);
	py::register_exception<TypeMismatchException>(m, "TypeMismatchException", error_class);
	py::register_exception<ValueOutOfRangeException>(m, "ValueOutOfRangeException", error_class);

	py::register_exception<StandardException>(m, "StandardException", error_class);
}
} // namespace duckdb
