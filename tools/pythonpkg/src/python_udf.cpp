#include "duckdb/main/query_result.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pandas/pandas_scan.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb_python/arrow/arrow_array_stream.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb_python/numpy/numpy_scan.hpp"
#include "duckdb_python/arrow/arrow_export_utils.hpp"
#include "duckdb/common/types/arrow_aux_data.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

static py::list ConvertToSingleBatch(vector<LogicalType> &types, vector<string> &names, DataChunk &input,
                                     const ClientProperties &options) {
	ArrowSchema schema;
	ArrowConverter::ToArrowSchema(&schema, types, names, options);

	py::list single_batch;
	ArrowAppender appender(types, STANDARD_VECTOR_SIZE, options);
	appender.Append(input, 0, input.size(), input.size());
	auto array = appender.Finalize();
	TransformDuckToArrowChunk(schema, array, single_batch);
	return single_batch;
}

static py::object ConvertDataChunkToPyArrowTable(DataChunk &input, const ClientProperties &options) {
	auto types = input.GetTypes();
	vector<string> names;
	names.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		names.push_back(StringUtil::Format("c%d", i));
	}

	return pyarrow::ToArrowTable(types, names, ConvertToSingleBatch(types, names, input, options), options);
}

static void ConvertPyArrowToDataChunk(const py::object &table, Vector &out, ClientContext &context, idx_t count) {

	// Create the stream factory from the Table object
	auto stream_factory = make_uniq<PythonTableArrowArrayStreamFactory>(table.ptr(), context.GetClientProperties());
	auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
	auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;

	// Get the functions we need
	auto function = ArrowTableFunction::ArrowScanFunction;
	auto bind = ArrowTableFunction::ArrowScanBind;
	auto init_global = ArrowTableFunction::ArrowScanInitGlobal;
	auto init_local = ArrowTableFunction::ArrowScanInitLocalInternal;

	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(3);
	children.push_back(Value::POINTER(CastPointerToValue(stream_factory.get())));
	children.push_back(Value::POINTER(CastPointerToValue(stream_factory_produce)));
	children.push_back(Value::POINTER(CastPointerToValue(stream_factory_get_schema)));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = "ConvertPyArrowToDataChunk";
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);
	vector<LogicalType> return_types;
	vector<string> return_names;

	auto bind_data = bind(context, bind_input, return_types, return_names);

	if (return_types.size() != 1) {
		throw InvalidInputException(
		    "The returned table from a pyarrow scalar udf should only contain one column, found %d",
		    return_types.size());
	}
	// if (return_types[0] != out.GetType()) {
	//	throw InvalidInputException("The type of the returned array (%s) does not match the expected type: '%s'", )
	//}

	DataChunk result;
	// Reserve for STANDARD_VECTOR_SIZE instead of count, in case the returned table contains too many tuples
	result.Initialize(context, return_types, STANDARD_VECTOR_SIZE);

	vector<column_t> column_ids = {0};
	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	auto global_state = init_global(context, input);
	auto local_state = init_local(context, input, global_state.get());

	TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
	function(context, function_input, result);
	if (result.size() != count) {
		throw InvalidInputException("Returned pyarrow table should have %d tuples, found %d", count, result.size());
	}

	VectorOperations::Cast(context, result.data[0], out, count);
}

static scalar_function_t CreateVectorizedFunction(PyObject *function, PythonExceptionHandling exception_handling) {
	// Through the capture of the lambda, we have access to the function pointer
	// We just need to make sure that it doesn't get garbage collected
	scalar_function_t func = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
		py::gil_scoped_acquire gil;

		// owning references
		py::object python_object;
		// Convert the input datachunk to pyarrow
		ClientProperties options;

		if (state.HasContext()) {
			auto &context = state.GetContext();
			options = context.GetClientProperties();
		}

		auto pyarrow_table = ConvertDataChunkToPyArrowTable(input, options);
		py::tuple column_list = pyarrow_table.attr("columns");

		auto count = input.size();

		// Call the function
		auto ret = PyObject_CallObject(function, column_list.ptr());
		if (ret == nullptr && PyErr_Occurred()) {
			if (exception_handling == PythonExceptionHandling::FORWARD_ERROR) {
				auto exception = py::error_already_set();
				throw InvalidInputException("Python exception occurred while executing the UDF: %s", exception.what());
			} else if (exception_handling == PythonExceptionHandling::RETURN_NULL) {
				PyErr_Clear();
				python_object = py::module_::import("pyarrow").attr("nulls")(count);
			} else {
				throw NotImplementedException("Exception handling type not implemented");
			}
		} else {
			python_object = py::reinterpret_steal<py::object>(ret);
		}
		if (!py::isinstance(python_object, py::module_::import("pyarrow").attr("lib").attr("Table"))) {
			// Try to convert into a table
			py::list single_array(1);
			py::list single_name(1);

			single_array[0] = python_object;
			single_name[0] = "c0";
			try {
				python_object = py::module_::import("pyarrow").attr("lib").attr("Table").attr("from_arrays")(
				    single_array, py::arg("names") = single_name);
			} catch (py::error_already_set &) {
				throw InvalidInputException("Could not convert the result into an Arrow Table");
			}
		}
		// Convert the pyarrow result back to a DuckDB datachunk
		ConvertPyArrowToDataChunk(python_object, result, state.GetContext(), count);

		if (input.size() == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	};
	return func;
}

static scalar_function_t CreateNativeFunction(PyObject *function, PythonExceptionHandling exception_handling,
                                              const ClientProperties &client_properties) {
	// Through the capture of the lambda, we have access to the function pointer
	// We just need to make sure that it doesn't get garbage collected
	scalar_function_t func = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void { // NOLINT
		py::gil_scoped_acquire gil;

		// owning references
		vector<py::object> python_objects;
		vector<PyObject *> python_results;
		python_results.resize(input.size());
		for (idx_t row = 0; row < input.size(); row++) {

			auto bundled_parameters = py::tuple((int)input.ColumnCount());
			for (idx_t i = 0; i < input.ColumnCount(); i++) {
				// Fill the tuple with the arguments for this row
				auto &column = input.data[i];
				auto value = column.GetValue(row);
				bundled_parameters[i] = PythonObject::FromValue(value, column.GetType(), client_properties);
			}

			// Call the function
			auto ret = PyObject_CallObject(function, bundled_parameters.ptr());
			if (ret == nullptr && PyErr_Occurred()) {
				if (exception_handling == PythonExceptionHandling::FORWARD_ERROR) {
					auto exception = py::error_already_set();
					throw InvalidInputException("Python exception occurred while executing the UDF: %s",
					                            exception.what());
				} else if (exception_handling == PythonExceptionHandling::RETURN_NULL) {
					PyErr_Clear();
					ret = Py_None;
				} else {
					throw NotImplementedException("Exception handling type not implemented");
				}
			}
			python_objects.push_back(py::reinterpret_steal<py::object>(ret));
			python_results[row] = ret;
		}

		NumpyScan::ScanObjectColumn(python_results.data(), sizeof(PyObject *), input.size(), 0, result);
		if (input.size() == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	};
	return func;
}

namespace {

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
	PythonUDFData(const string &name, bool vectorized, FunctionNullHandling null_handling)
	    : name(name), null_handling(null_handling), vectorized(vectorized) {
		return_type = LogicalType::INVALID;
		param_count = DConstants::INVALID_INDEX;
	}

public:
	string name;
	vector<LogicalType> parameters;
	LogicalType return_type;
	LogicalType varargs = LogicalTypeId::INVALID;
	FunctionNullHandling null_handling;
	idx_t param_count;
	bool vectorized;

public:
	void Verify() {
		if (return_type == LogicalType::INVALID) {
			throw InvalidInputException("Could not infer the return type, please set it explicitly");
		}
	}

	void OverrideReturnType(const shared_ptr<DuckDBPyType> &type) {
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

	py::object GetSignature(const py::object &udf) {
		const int32_t PYTHON_3_10_HEX = 0x030a00f0;
		auto python_version = PY_VERSION_HEX;

		auto signature_func = py::module_::import("inspect").attr("signature");
		if (python_version >= PYTHON_3_10_HEX) {
			return signature_func(udf, py::arg("eval_str") = true);
		} else {
			return signature_func(udf);
		}
	}

	void AnalyzeSignature(const py::object &udf) {
		auto signature = GetSignature(udf);
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

	ScalarFunction GetFunction(const py::function &udf, PythonExceptionHandling exception_handling, bool side_effects,
	                           const ClientProperties &client_properties) {

		auto &import_cache = *DuckDBPyConnection::ImportCache();
		// Import this module, because importing this from a non-main thread causes a segfault
		(void)import_cache.numpy.core.multiarray();

		scalar_function_t func;
		if (vectorized) {
			func = CreateVectorizedFunction(udf.ptr(), exception_handling);
		} else {
			func = CreateNativeFunction(udf.ptr(), exception_handling, client_properties);
		}
		FunctionStability function_side_effects =
		    side_effects ? FunctionStability::VOLATILE : FunctionStability::CONSISTENT;
		ScalarFunction scalar_function(name, std::move(parameters), return_type, func, nullptr, nullptr, nullptr,
		                               nullptr, varargs, function_side_effects, null_handling);
		return scalar_function;
	}
};

} // namespace

ScalarFunction DuckDBPyConnection::CreateScalarUDF(const string &name, const py::function &udf,
                                                   const py::object &parameters,
                                                   const shared_ptr<DuckDBPyType> &return_type, bool vectorized,
                                                   FunctionNullHandling null_handling,
                                                   PythonExceptionHandling exception_handling, bool side_effects) {
	PythonUDFData data(name, vectorized, null_handling);

	data.AnalyzeSignature(udf);
	data.OverrideParameters(parameters);
	data.OverrideReturnType(return_type);
	data.Verify();
	return data.GetFunction(udf, exception_handling, side_effects, connection->context->GetClientProperties());
}

} // namespace duckdb
