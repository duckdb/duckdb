#include "duckdb_python/pybind11/exceptions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/list.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace py = pybind11;

namespace duckdb {

class Warning : public std::exception {};

//===--------------------------------------------------------------------===//
// Base Error
//===--------------------------------------------------------------------===//
class PyError : public std::runtime_error {
public:
	explicit PyError(const string &err) : std::runtime_error(err) {
	}
};

//===--------------------------------------------------------------------===//
// Unknown Errors
//===--------------------------------------------------------------------===//
class PyFatalException : public PyError {
public:
	explicit PyFatalException(const string &err) : PyError(err) {
	}
};

class PyInterruptException : public PyError {
public:
	explicit PyInterruptException(const string &err) : PyError(err) {
	}
};

class PyPermissionException : public PyError {
public:
	explicit PyPermissionException(const string &err) : PyError(err) {
	}
};

class PySequenceException : public PyError {
public:
	explicit PySequenceException(const string &err) : PyError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Data Error
//===--------------------------------------------------------------------===//
class DataError : public std::runtime_error {
public:
	explicit DataError(const string &err) : std::runtime_error(err) {
	}
};

class PyOutOfRangeException : public DataError {
public:
	explicit PyOutOfRangeException(const string &err) : DataError(err) {
	}
};

class PyConversionException : public DataError {
public:
	explicit PyConversionException(const string &err) : DataError(err) {
	}
};

class PyTypeMismatchException : public DataError {
public:
	explicit PyTypeMismatchException(const string &err) : DataError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Operational Error
//===--------------------------------------------------------------------===//
class OperationalError : public std::runtime_error {
public:
	explicit OperationalError(const string &err) : std::runtime_error(err) {
	}
};

class PyTransactionException : public OperationalError {
public:
	explicit PyTransactionException(const string &err) : OperationalError(err) {
	}
};

class PyOutOfMemoryException : public OperationalError {
public:
	explicit PyOutOfMemoryException(const string &err) : OperationalError(err) {
	}
};

class PyConnectionException : public OperationalError {
public:
	explicit PyConnectionException(const string &err) : OperationalError(err) {
	}
};

class PySerializationException : public OperationalError {
public:
	explicit PySerializationException(const string &err) : OperationalError(err) {
	}
};

class PyIOException : public OperationalError {
public:
	explicit PyIOException(const string &err) : OperationalError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Integrity Error
//===--------------------------------------------------------------------===//
class IntegrityError : public std::runtime_error {
public:
	explicit IntegrityError(const string &err) : std::runtime_error(err) {
	}
};

class PyConstraintException : public IntegrityError {
public:
	explicit PyConstraintException(const string &err) : IntegrityError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Internal Error
//===--------------------------------------------------------------------===//
class InternalError : public std::runtime_error {
public:
	explicit InternalError(const string &err) : std::runtime_error(err) {
	}
};

class PyInternalException : public InternalError {
public:
	explicit PyInternalException(const string &err) : InternalError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Programming Error
//===--------------------------------------------------------------------===//
class ProgrammingError : public std::runtime_error {
public:
	explicit ProgrammingError(const string &err) : std::runtime_error(err) {
	}
};

class PyParserException : public ProgrammingError {
public:
	explicit PyParserException(const string &err) : ProgrammingError(err) {
	}
};

class PySyntaxException : public ProgrammingError {
public:
	explicit PySyntaxException(const string &err) : ProgrammingError(err) {
	}
};

class PyBinderException : public ProgrammingError {
public:
	explicit PyBinderException(const string &err) : ProgrammingError(err) {
	}
};

class PyInvalidInputException : public ProgrammingError {
public:
	explicit PyInvalidInputException(const string &err) : ProgrammingError(err) {
	}
};

class PyInvalidTypeException : public ProgrammingError {
public:
	explicit PyInvalidTypeException(const string &err) : ProgrammingError(err) {
	}
};

class PyCatalogException : public ProgrammingError {
public:
	explicit PyCatalogException(const string &err) : ProgrammingError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Not Supported Error
//===--------------------------------------------------------------------===//
class NotSupportedError : public std::runtime_error {
public:
	explicit NotSupportedError(const string &err) : std::runtime_error(err) {
	}
};

class PyNotImplementedException : public NotSupportedError {
public:
	explicit PyNotImplementedException(const string &err) : NotSupportedError(err) {
	}
};

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
	auto error = py::register_exception<PyError>(m, "Error").ptr();
	// FIXME: missing DatabaseError

	// order of declaration matters, and this needs to be checked last
	// Unknown
	py::register_exception<PyFatalException>(m, "FatalException", error);
	py::register_exception<PyInterruptException>(m, "InterruptException", error);
	py::register_exception<PyPermissionException>(m, "PermissionException", error);
	py::register_exception<PySequenceException>(m, "SequenceException", error);

	// DataError
	auto data_error = py::register_exception<DataError>(m, "DataError", error).ptr();
	py::register_exception<PyOutOfRangeException>(m, "OutOfRangeException", data_error);
	py::register_exception<PyConversionException>(m, "ConversionException", data_error);
	// no unknown type error, or decimal type
	py::register_exception<PyTypeMismatchException>(m, "TypeMismatchException", data_error);

	// OperationalError
	auto operational_error = py::register_exception<OperationalError>(m, "OperationalError", error).ptr();
	py::register_exception<PyTransactionException>(m, "TransactionException", operational_error);
	py::register_exception<PyOutOfMemoryException>(m, "OutOfMemoryException", operational_error);
	py::register_exception<PyConnectionException>(m, "ConnectionException", operational_error);
	// no object size error
	// no null pointer errors
	py::register_exception<PyIOException>(m, "IOException", operational_error);
	py::register_exception<PySerializationException>(m, "SerializationException", operational_error);

	static py::exception<HTTPException> HTTP_EXCEPTION(m, "HTTPException", operational_error);
	const auto string_type = py::type::of(py::str());
	const auto Dict = py::module_::import("typing").attr("Dict");
	HTTP_EXCEPTION.attr("__annotations__") =
	    py::dict(py::arg("status_code") = py::type::of(py::int_()), py::arg("body") = string_type,
	             py::arg("reason") = string_type, py::arg("headers") = Dict[py::make_tuple(string_type, string_type)]);
	HTTP_EXCEPTION.doc() = "Thrown when an error occurs in the httpfs extension, or whilst downloading an extension.";

	// IntegrityError
	auto integrity_error = py::register_exception<IntegrityError>(m, "IntegrityError", error).ptr();
	py::register_exception<PyConstraintException>(m, "ConstraintException", integrity_error);

	// InternalError
	auto internal_error = py::register_exception<InternalError>(m, "InternalError", error).ptr();
	py::register_exception<PyInternalException>(m, "InternalException", internal_error);

	//// ProgrammingError
	auto programming_error = py::register_exception<ProgrammingError>(m, "ProgrammingError", error).ptr();
	py::register_exception<PyParserException>(m, "ParserException", programming_error);
	py::register_exception<PySyntaxException>(m, "SyntaxException", programming_error);
	py::register_exception<PyBinderException>(m, "BinderException", programming_error);
	py::register_exception<PyInvalidInputException>(m, "InvalidInputException", programming_error);
	py::register_exception<PyInvalidTypeException>(m, "InvalidTypeException", programming_error);
	// no type for expression exceptions?
	py::register_exception<PyCatalogException>(m, "CatalogException", programming_error);

	// NotSupportedError
	auto not_supported_error = py::register_exception<NotSupportedError>(m, "NotSupportedError", error).ptr();
	py::register_exception<PyNotImplementedException>(m, "NotImplementedException", not_supported_error);

	py::register_exception_translator([](std::exception_ptr p) { // NOLINT(performance-unnecessary-value-param)
		try {
			if (p) {
				std::rethrow_exception(p);
			}
		} catch (const duckdb::Exception &ex) {
			duckdb::ErrorData error(ex);
			switch (error.Type()) {
			case ExceptionType::HTTP: {
				// construct exception object
				auto e = py::handle(HTTP_EXCEPTION.ptr())(py::str(error.Message()));

				auto headers = py::dict();
				for (auto &entry : error.ExtraInfo()) {
					if (entry.first == "status_code") {
						e.attr("status_code") = std::stoi(entry.second);
					} else if (entry.first == "response_body") {
						e.attr("body") = entry.second;
					} else if (entry.first == "reason") {
						e.attr("reason") = entry.second;
					} else if (StringUtil::StartsWith(entry.first, "header_")) {
						headers[py::str(entry.first.substr(7))] = entry.second;
					}
				}
				e.attr("headers") = std::move(headers);

				// "throw" exception object
				PyErr_SetObject(HTTP_EXCEPTION.ptr(), e.ptr());
				break;
			}
			case ExceptionType::CATALOG:
				throw PyCatalogException(error.Message());
			case ExceptionType::FATAL:
				throw PyFatalException(error.Message());
			case ExceptionType::INTERRUPT:
				throw PyInterruptException(error.Message());
			case ExceptionType::PERMISSION:
				throw PyPermissionException(error.Message());
			case ExceptionType::SEQUENCE:
				throw PySequenceException(error.Message());
			case ExceptionType::OUT_OF_RANGE:
				throw PyOutOfRangeException(error.Message());
			case ExceptionType::CONVERSION:
				throw PyConversionException(error.Message());
			case ExceptionType::MISMATCH_TYPE:
				throw PyTypeMismatchException(error.Message());
			case ExceptionType::TRANSACTION:
				throw PyTransactionException(error.Message());
			case ExceptionType::OUT_OF_MEMORY:
				throw PyOutOfMemoryException(error.Message());
			case ExceptionType::CONNECTION:
				throw PyConnectionException(error.Message());
			case ExceptionType::SERIALIZATION:
				throw PySerializationException(error.Message());
			case ExceptionType::CONSTRAINT:
				throw PyConstraintException(error.Message());
			case ExceptionType::INTERNAL:
				throw PyInternalException(error.Message());
			case ExceptionType::PARSER:
				throw PyParserException(error.Message());
			case ExceptionType::SYNTAX:
				throw PySyntaxException(error.Message());
			case ExceptionType::IO:
				throw PyIOException(error.Message());
			case ExceptionType::BINDER:
				throw PyBinderException(error.Message());
			case ExceptionType::INVALID_INPUT:
				throw PyInvalidInputException(error.Message());
			case ExceptionType::INVALID_TYPE:
				throw PyInvalidTypeException(error.Message());
			case ExceptionType::NOT_IMPLEMENTED:
				throw PyNotImplementedException(error.Message());
			default:
				throw std::runtime_error(error.RawMessage());
			}
		}
	});
}
} // namespace duckdb
