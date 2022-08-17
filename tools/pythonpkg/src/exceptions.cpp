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

void	RegisterExceptionExplicit(const py::module& m) {
	static auto warning = py::exception<Warning>(m, "Warning");
	static auto error = py::exception<Exception>(m, "Error");
	static auto standard = py::exception<StandardException>(m, "StandardException", error);

	//! Error classifications
	static auto data_error = py::exception<DataError>(m, "DataError", error);
	static auto database_error = py::exception<DatabaseError>(m, "DatabaseError", error);
	static auto operational_error = py::exception<OperationalError>(m, "OperationalError", error);
	static auto integrity_error = py::exception<IntegrityError>(m, "IntegrityError", error);
	static auto internal_error = py::exception<InternalError>(m, "InternalError", error);
	static auto programming_error = py::exception<ProgrammingError>(m, "ProgrammingError", error);
	static auto not_supported_error = py::exception<NotSupportedError>(m, "NotSupportedError", error);

	// ! DataError
	static auto out_of_range = py::exception<OutOfRangeException>(m, "OutOfRangeException", data_error);
	static auto cast = py::exception<CastException>(m, "CastException", data_error);
	static auto conversion = py::exception<ConversionException>(m, "ConversionException", data_error);
	// no unknown type error, or decimal type
	static auto type_mismatch = py::exception<TypeMismatchException>(m, "TypeMismatchException", data_error);
	// no divide by zero error
	static auto value_out_of_range = py::exception<ValueOutOfRangeException>(m, "ValueOutOfRangeException", data_error);

	// OperationalError
	static auto transaction = py::exception<TransactionException>(m, "TransactionException", operational_error);
	static auto out_of_memory = py::exception<OutOfMemoryException>(m, "OutOfMemoryException", operational_error);
	static auto connection = py::exception<ConnectionException>(m, "ConnectionException", operational_error);
	// no object size error
	// no null pointer errors
	static auto io = py::exception<IOException>(m, "IOException", operational_error);
	static auto serialization = py::exception<SerializationException>(m, "SerializationException", operational_error);

	// IntegrityError
	static auto constraint = py::exception<ConstraintException>(m, "ConstraintException", integrity_error);

	// InternalError
	static auto internal = py::exception<InternalException>(m, "InternalException", internal_error);

	// ProgrammingError
	static auto parser = py::exception<ParserException>(m, "ParserException", programming_error);
	static auto syntax = py::exception<SyntaxException>(m, "SyntaxException", programming_error);
	static auto binder = py::exception<BinderException>(m, "BinderException", programming_error);
	static auto invalid_input = py::exception<InvalidInputException>(m, "InvalidInputException", programming_error);
	static auto invalid_type = py::exception<InvalidTypeException>(m, "InvalidTypeException", programming_error);
	// no type for expression exceptions?
	static auto catalog = py::exception<CatalogException>(m, "CatalogException", programming_error);

	// NotSupportedError
	static auto not_implemented = py::exception<NotImplementedException>(m, "NotImplementedException", not_supported_error);

	// Unknown
	static auto fatal = py::exception<FatalException>(m, "FatalException", error);
	static auto interrupt = py::exception<InterruptException>(m, "InterruptException", error);
	static auto permission = py::exception<PermissionException>(m, "PermissionException", error);
	static auto sequence = py::exception<SequenceException>(m, "SequenceException", error);

	py::register_exception_translator([](std::exception_ptr p) {
		try {
			if (p) {
				std::rethrow_exception(p);
			}
		} catch (const SequenceException & e) {
			sequence(e.what());
		} catch (const PermissionException& e) {
			permission(e.what());
		} catch (const InterruptException& e) {
			interrupt(e.what());
		} catch (const FatalException& e) {
			fatal(e.what());
		} catch (const NotImplementedException& e) {
			not_implemented(e.what());
		} catch (const CatalogException& e) {
			catalog(e.what());
		} catch (const InvalidTypeException& e) {
			invalid_type(e.what());
		} catch (const InvalidInputException& e) {
			invalid_input(e.what());
		} catch (const BinderException& e) {
			binder(e.what());
		} catch (const SyntaxException& e) {
			syntax(e.what());
		} catch (const ParserException& e) {
			parser(e.what());
		} catch (const InternalException& e) {
			internal(e.what());
		} catch (const ConstraintException& e) {
			constraint(e.what());
		} catch (const SerializationException& e) {
			serialization(e.what());
		} catch (const IOException& e) {
			io(e.what());
		} catch (const ConnectionException& e) {
			connection(e.what());
		} catch (const OutOfMemoryException& e) {
			out_of_memory(e.what());
		} catch (const TransactionException& e) {
			transaction(e.what());
		} catch (const ValueOutOfRangeException& e) {
			value_out_of_range(e.what());
		} catch (const TypeMismatchException& e) {
			type_mismatch(e.what());
		} catch (const ConversionException& e) {
			conversion(e.what());
		} catch (const CastException& e) {
			cast(e.what());
		} catch (const OutOfRangeException& e) {
			out_of_range(e.what());
		} catch (const NotSupportedError& e) {
			not_supported_error(e.what());
		} catch (const DataError& e) {
			data_error(e.what());
		} catch (const ProgrammingError& e) {
			programming_error(e.what());
		} catch (const InternalError& e) {
			internal_error(e.what());
		} catch (const IntegrityError& e) {
			integrity_error(e.what());
		} catch (const OperationalError& e) {
			operational_error(e.what());
		} catch (const DatabaseError& e) {
			database_error(e.what());
		} catch (const Warning& e) {
			warning(e.what());
		} catch (const StandardException& e) {
			standard(e.what());
		} catch (const Exception& e) {
			error(e.what());
		} catch (const std::exception& e) {
			invalid_input(e.what());
		}
	});
}

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
	//auto warning_class = py::register_exception<Warning>(m, "Warning").ptr();
	auto error = py::register_exception<Exception>(m, "Error").ptr();

	//// order of declaration matters, and this needs to be checked last
	//py::register_exception<StandardException>(m, "StandardException", error);
	//// Unknown
	//py::register_exception<FatalException>(m, "FatalException", error);
	//py::register_exception<InterruptException>(m, "InterruptException", error);
	//py::register_exception<PermissionException>(m, "PermissionException", error);
	//py::register_exception<SequenceException>(m, "SequenceException", error);

	//// DataError
	//auto data_error = py::register_exception<DataError>(m, "DataError", error).ptr();
	//py::register_exception<OutOfRangeException>(m, "OutOfRangeException", data_error);
	//py::register_exception<CastException>(m, "CastException", data_error);
	//py::register_exception<ConversionException>(m, "ConversionException", data_error);
	//// no unknown type error, or decimal type
	//py::register_exception<TypeMismatchException>(m, "TypeMismatchException", data_error);
	//// no divide by zero error
	//py::register_exception<ValueOutOfRangeException>(m, "ValueOutOfRangeException", data_error);

	//// OperationalError
	//auto operational_error = py::register_exception<OperationalError>(m, "OperationalError", error).ptr();
	//py::register_exception<TransactionException>(m, "TransactionException", operational_error);
	//py::register_exception<OutOfMemoryException>(m, "OutOfMemoryException", operational_error);
	//py::register_exception<ConnectionException>(m, "ConnectionException", operational_error);
	//// no object size error
	//// no null pointer errors
	//py::register_exception<IOException>(m, "IOException", operational_error);
	//py::register_exception<SerializationException>(m, "SerializationException", operational_error);

	//// IntegrityError
	//auto integrity_error = py::register_exception<IntegrityError>(m, "IntegrityError", error).ptr();
	//py::register_exception<ConstraintException>(m, "ConstraintException", integrity_error);

	//// InternalError
	//auto internal_error = py::register_exception<InternalError>(m, "InternalError", error).ptr();
	//py::register_exception<InternalException>(m, "InternalException", internal_error);

	//// ProgrammingError
	auto programming_error = py::register_exception<ProgrammingError>(m, "ProgrammingError", error).ptr();
	//py::register_exception<ParserException>(m, "ParserException", programming_error);
	//py::register_exception<SyntaxException>(m, "SyntaxException", programming_error);
	//py::register_exception<BinderException>(m, "BinderException", programming_error);
	py::register_exception<InvalidInputException>(m, "InvalidInputException", programming_error);
	//py::register_exception<InvalidTypeException>(m, "InvalidTypeException", programming_error);
	//// no type for expression exceptions?
	//py::register_exception<CatalogException>(m, "CatalogException", programming_error);

	//// NotSupportedError
	//auto not_supported_error = py::register_exception<NotSupportedError>(m, "NotSupportedError", error).ptr();
	//py::register_exception<NotImplementedException>(m, "NotImplementedException", not_supported_error);
}
} // namespace duckdb
