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

// If these types are arrow canonical extensions, we must check if they are registered.
// If not, we should error.
void AreExtensionsRegistered(const LogicalType &arrow_type, const LogicalType &duckdb_type) {
	if (arrow_type != duckdb_type) {
		// Is it a UUID Registration?
		if (arrow_type.id() == LogicalTypeId::BLOB && duckdb_type.id() == LogicalTypeId::UUID) {
			throw InvalidConfigurationException(
			    "Mismatch on return type from Arrow object (%s) and DuckDB (%s). It seems that you are using the UUID "
			    "arrow canonical extension, but the same is not yet registered. Make sure to register it first with "
			    "e.g., pa.register_extension_type(UUIDType()). ",
			    arrow_type.ToString(), duckdb_type.ToString());
		}
		// Is it a JSON Registration
		if (!arrow_type.IsJSONType() && duckdb_type.IsJSONType()) {
			throw InvalidConfigurationException(
			    "Mismatch on return type from Arrow object (%s) and DuckDB (%s). It seems that you are using the JSON "
			    "arrow canonical extension, but the same is not yet registered. Make sure to register it first with "
			    "e.g., pa.register_extension_type(JSONType()). ",
			    arrow_type.ToString(), duckdb_type.ToString());
		}
	}
}
static void ConvertArrowTableToVector(const py::object &table, Vector &out, ClientContext &context, idx_t count) {
	// Create the stream factory from the Table object
	auto ptr = table.ptr();
	D_ASSERT(py::gil_check());
	py::gil_scoped_release gil;

	auto stream_factory = make_uniq<PythonTableArrowArrayStreamFactory>(ptr, context.GetClientProperties());
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
	dummy_table_function.name = "ConvertArrowTableToVector";
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

	AreExtensionsRegistered(return_types[0], out.GetType());

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
	out.Flatten(count);
}

static string NullHandlingError() {
	return R"(
The returned result contained NULL values, but the 'null_handling' was set to DEFAULT.
If you want more control over NULL values then 'null_handling' should be set to SPECIAL.

With DEFAULT all rows containing NULL have been filtered from the UDFs input.
Those rows are automatically set to NULL in the final result.
The UDF is not expected to return NULL values.
	)";
}

static ValidityMask &GetResultValidity(Vector &result) {
	auto vector_type = result.GetVectorType();
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		return ConstantVector::Validity(result);
	} else if (vector_type == VectorType::FLAT_VECTOR) {
		return FlatVector::Validity(result);
	} else {
		throw InternalException("VectorType %s was not expected here (GetResultValidity)",
		                        EnumUtil::ToString(vector_type));
	}
}

static void VerifyVectorizedNullHandling(Vector &result, idx_t count) {
	auto &validity = GetResultValidity(result);

	if (validity.AllValid()) {
		return;
	}

	throw InvalidInputException(NullHandlingError());
}

static scalar_function_t CreateVectorizedFunction(PyObject *function, PythonExceptionHandling exception_handling,
                                                  FunctionNullHandling null_handling) {
	// Through the capture of the lambda, we have access to the function pointer
	// We just need to make sure that it doesn't get garbage collected
	scalar_function_t func = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
		py::gil_scoped_acquire gil;

		const bool default_null_handling = null_handling == FunctionNullHandling::DEFAULT_NULL_HANDLING;

		// owning references
		py::object python_object;
		// Convert the input datachunk to pyarrow
		ClientProperties options;

		if (state.HasContext()) {
			auto &context = state.GetContext();
			options = context.GetClientProperties();
		}

		auto result_validity = FlatVector::Validity(result);
		SelectionVector selvec(input.size());
		idx_t input_size = input.size();
		if (default_null_handling) {
			vector<UnifiedVectorFormat> vec_data(input.ColumnCount());
			for (idx_t i = 0; i < input.ColumnCount(); i++) {
				input.data[i].ToUnifiedFormat(input.size(), vec_data[i]);
			}

			idx_t index = 0;
			for (idx_t i = 0; i < input.size(); i++) {
				bool any_null = false;
				for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
					auto &vec = vec_data[col_idx];
					if (!vec.validity.RowIsValid(vec.sel->get_index(i))) {
						any_null = true;
						break;
					}
				}
				if (any_null) {
					result_validity.SetInvalid(i);
					continue;
				}
				selvec.set_index(index++, i);
			}
			if (index != input.size()) {
				input.Slice(selvec, index);
			}
		}

		auto pyarrow_table = ConvertDataChunkToPyArrowTable(input, options);
		py::tuple column_list = pyarrow_table.attr("columns");

		auto count = input.size();

		// Call the function
		auto ret = PyObject_CallObject(function, column_list.ptr());
		bool exception_occurred = false;
		if (ret == nullptr && PyErr_Occurred()) {
			exception_occurred = true;
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
		if (count != input_size) {
			D_ASSERT(default_null_handling);
			// We filtered out some NULLs, now we need to reconstruct the final result by adding the nulls back
			Vector temp(result.GetType(), count);
			// Convert the table into a temporary Vector
			ConvertArrowTableToVector(python_object, temp, state.GetContext(), count);
			if (!exception_occurred) {
				VerifyVectorizedNullHandling(temp, count);
			}
			if (count) {
				SelectionVector inverted(input_size);
				// Create a SelVec that inverts the filtering
				// example: count: 6, null_indices: 1,3
				// input selvec: [0, 2, 4, 5]
				// inverted selvec: [0, 0, 1, 1, 2, 3]
				idx_t src_index = 0;
				for (idx_t i = 0; i < input_size; i++) {
					// Fill the gaps with the previous index
					inverted.set_index(i, src_index);
					if (src_index + 1 < count && selvec.get_index(src_index) == i) {
						src_index++;
					}
				}
				VectorOperations::Copy(temp, result, inverted, count, 0, 0, input_size);
			}
			for (idx_t i = 0; i < input_size; i++) {
				FlatVector::SetNull(result, i, !result_validity.RowIsValid(i));
			}
			result.Verify(input_size);
		} else {
			ConvertArrowTableToVector(python_object, result, state.GetContext(), count);
			if (default_null_handling && !exception_occurred) {
				VerifyVectorizedNullHandling(result, count);
			}
		}

		if (input_size == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	};
	return func;
}

static scalar_function_t CreateNativeFunction(PyObject *function, PythonExceptionHandling exception_handling,
                                              const ClientProperties &client_properties,
                                              FunctionNullHandling null_handling) {
	// Through the capture of the lambda, we have access to the function pointer
	// We just need to make sure that it doesn't get garbage collected
	scalar_function_t func = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void { // NOLINT
		py::gil_scoped_acquire gil;

		const bool default_null_handling = null_handling == FunctionNullHandling::DEFAULT_NULL_HANDLING;

		// owning references
		vector<py::object> python_objects;
		vector<PyObject *> python_results;
		python_results.resize(input.size());
		for (idx_t row = 0; row < input.size(); row++) {

			auto bundled_parameters = py::tuple((int)input.ColumnCount());
			bool contains_null = false;
			for (idx_t i = 0; i < input.ColumnCount(); i++) {
				// Fill the tuple with the arguments for this row
				auto &column = input.data[i];
				auto value = column.GetValue(row);
				if (value.IsNull() && default_null_handling) {
					contains_null = true;
					break;
				}
				bundled_parameters[i] = PythonObject::FromValue(value, column.GetType(), client_properties);
			}
			if (contains_null) {
				// Immediately insert None, no need to call the function
				python_objects.push_back(py::none());
				python_results[row] = py::none().ptr();
				continue;
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
			} else if ((!ret || ret == Py_None) && default_null_handling) {
				throw InvalidInputException(NullHandlingError());
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
		auto empty = py::module_::import("inspect").attr("Signature").attr("empty");
		if (!py::none().is(return_annotation) && !empty.is(return_annotation)) {
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
			func = CreateVectorizedFunction(udf.ptr(), exception_handling, null_handling);
		} else {
			func = CreateNativeFunction(udf.ptr(), exception_handling, client_properties, null_handling);
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
	auto &connection = con.GetConnection();

	data.AnalyzeSignature(udf);
	data.OverrideParameters(parameters);
	data.OverrideReturnType(return_type);
	data.Verify();
	return data.GetFunction(udf, exception_handling, side_effects, connection.context->GetClientProperties());
}

} // namespace duckdb
