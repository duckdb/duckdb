#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/vector_conversion.hpp"
#include "duckdb/function/function.hpp"

namespace {
using namespace duckdb;

struct ParameterKind {
	enum class Type : uint8_t { POSITIONAL_ONLY, POSITIONAL_OR_KEYWORD, VAR_POSITIONAL, KEYWORD_ONLY, VAR_KEYWORD };
	static ParameterKind::Type FromString(const string &type_str) {
		if (type_str == "POSITIONAL_ONLY") {
			return Type::POSITIONAL_ONLY;
		} else if (type_str == "POSITIONAL_OR_KEYWORD") {
			return Type::POSITIONAL_OR_KEYWORD;
		} else if (type_str == "VAR_POSITIONAL") {
			return Type::VAR_POSITIONAL;
		} else if (type_str == "KEYWORD_ONLY") {
			return Type::KEYWORD_ONLY;
		} else if (type_str == "VAR_KEYWORD") {
			return Type::VAR_KEYWORD;
		} else {
			throw NotImplementedException("ParameterKindType not implemented for '%s'", type_str);
		}
	}
};

struct PythonUDFData {
public:
	PythonUDFData(const string &name, scalar_function_t func, bool varargs_p, FunctionNullHandling null_handling)
	    : name(name), func(func), null_handling(null_handling) {
		if (varargs_p) {
			varargs = LogicalType::ANY;
		}
		return_type = LogicalType::INVALID;
		param_count = DConstants::INVALID_INDEX;
	}

public:
	const string &name;
	vector<LogicalType> parameters;
	LogicalType return_type;
	LogicalType varargs = LogicalTypeId::INVALID;
	FunctionNullHandling null_handling;
	idx_t param_count;
	scalar_function_t func;

public:
	void Verify() {
		if (return_type == LogicalType::INVALID) {
			throw InvalidInputException("Could not infer the return type, please set it explicitly");
		}
	}

	void OverrideReturnType(shared_ptr<DuckDBPyType> type) {
		if (!type) {
			return;
		}
		return_type = type->Type();
	}

	void OverrideParameters(const py::object &parameters_p) {
		if (py::none().is(parameters_p)) {
			return;
		}
		if (!py::isinstance<py::list>(parameters_p)) {
			throw InvalidInputException("Either leave 'parameters' empty, or provide a list of DuckDBPyType objects");
		}

		auto params = py::list(parameters_p);
		if (params.size() != param_count) {
			throw InvalidInputException("%d types provided, but the provided function takes %d parameters",
			                            params.size(), param_count);
		}
		D_ASSERT(parameters.empty() || parameters.size() == param_count);
		if (parameters.empty()) {
			for (idx_t i = 0; i < param_count; i++) {
				parameters.push_back(LogicalType::ANY);
			}
		}
		idx_t i = 0;
		for (auto &param : params) {
			auto type = py::cast<shared_ptr<DuckDBPyType>>(param);
			parameters[i++] = type->Type();
		}
	}

	void AnalyzeSignature(const py::object &udf) {
		auto signature_func = py::module_::import("inspect").attr("signature");
		auto signature = signature_func(udf);
		auto sig_params = signature.attr("parameters");
		auto return_annotation = signature.attr("return_annotation");
		if (!py::none().is(return_annotation)) {
			shared_ptr<DuckDBPyType> pytype;
			if (py::try_cast<shared_ptr<DuckDBPyType>>(return_annotation, pytype)) {
				return_type = pytype->Type();
			}
		}
		param_count = py::len(sig_params);
		parameters.reserve(param_count);
		auto params = py::dict(sig_params);
		for (auto &item : params) {
			auto &key = item.first;
			auto &value = item.second;
			shared_ptr<DuckDBPyType> pytype;
			if (py::try_cast<shared_ptr<DuckDBPyType>>(value.attr("annotation"), pytype)) {
				parameters.push_back(pytype->Type());
			} else {
				std::string kind = py::str(value.attr("kind"));
				auto parameter_kind = ParameterKind::FromString(kind);
				if (parameter_kind == ParameterKind::Type::VAR_POSITIONAL) {
					varargs = LogicalType::ANY;
				}
				parameters.push_back(LogicalType::ANY);
			}
		}
	}

	ScalarFunction GetFunction() {
		ScalarFunction scalar_function(name, std::move(parameters), return_type, func, nullptr, nullptr, nullptr,
		                               nullptr, varargs, FunctionSideEffects::NO_SIDE_EFFECTS, null_handling);
		return scalar_function;
	}
};

}; // namespace

namespace duckdb {

static scalar_function_t CreateFunction(PyObject *function) {
	// Through the capture of the lambda, we have access to the function pointer
	// We just need to make sure that it doesn't get garbage collected
	scalar_function_t func = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
		py::gil_scoped_acquire gil;

		// owning references
		vector<py::handle> python_objects;
		vector<PyObject *> python_results;
		python_results.reserve(input.size());
		for (idx_t row = 0; row < input.size(); row++) {

			auto bundled_parameters = py::tuple(input.ColumnCount());
			for (idx_t i = 0; i < input.ColumnCount(); i++) {
				// Fill the tuple with the arguments for this row
				auto &column = input.data[i];
				auto value = column.GetValue(row);
				bundled_parameters[i] = PythonObject::FromValue(value, column.GetType());
			}

			// Call the function
			PyObject *ret = nullptr;
			ret = PyObject_CallObject(function, bundled_parameters.ptr());
			if (ret == nullptr && PyErr_Occurred()) {
				auto exception = py::error_already_set();
				throw InvalidInputException("Python exception occurred while executing the UDF: %s", exception.what());
			}
			python_objects.push_back(py::handle(ret));
			python_results.push_back(ret);
		}

		// Cast the resulting native python to DuckDB, using the return type
		// result.Resize(input.size());
		VectorConversion::ScanPandasObjectColumn(python_results.data(), input.size(), 0, result);
		if (input.AllConstant()) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	};
	return func;
}

ScalarFunction DuckDBPyConnection::CreateScalarUDF(const string &name, const py::object &udf,
                                                   const py::object &parameters, shared_ptr<DuckDBPyType> return_type,
                                                   bool varargs, FunctionNullHandling null_handling) {
	scalar_function_t func = CreateFunction(udf.ptr());

	PythonUDFData data(name, func, varargs, null_handling);

	data.AnalyzeSignature(udf);
	data.OverrideParameters(parameters);
	data.OverrideReturnType(return_type);
	data.Verify();

	return data.GetFunction();
}

} // namespace duckdb
