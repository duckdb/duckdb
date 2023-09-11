#include "duckdb_python/map.hpp"
#include "duckdb_python/numpy/numpy_scan.hpp"
#include "duckdb_python/pandas/pandas_bind.hpp"
#include "duckdb_python/numpy/numpy_result_conversion.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pandas/column/pandas_numpy_column.hpp"
#include "duckdb_python/pandas/pandas_scan.hpp"
#include "duckdb_python/pybind11/dataframe.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb_python/pybind11/dataframe.hpp"

namespace duckdb {

MapFunction::MapFunction()
    : TableFunction("python_map_function", {LogicalType::TABLE, LogicalType::POINTER, LogicalType::POINTER}, nullptr,
                    MapFunctionBind) {
	in_out_function = MapFunctionExec;
}

struct MapFunctionData : public TableFunctionData {
	MapFunctionData() : function(nullptr) {
	}
	PyObject *function;
	vector<LogicalType> in_types, out_types;
	vector<string> in_names, out_names;
};

static py::object FunctionCall(NumpyResultConversion &conversion, const vector<string> &names, PyObject *function) {
	py::dict in_numpy_dict;
	for (idx_t col_idx = 0; col_idx < names.size(); col_idx++) {
		in_numpy_dict[names[col_idx].c_str()] = conversion.ToArray(col_idx);
	}
	auto in_df = py::module::import("pandas").attr("DataFrame").attr("from_dict")(in_numpy_dict);
	D_ASSERT(in_df.ptr());

	D_ASSERT(function);
	auto *df_obj = PyObject_CallObject(function, PyTuple_Pack(1, in_df.ptr()));
	if (!df_obj) {
		PyErr_PrintEx(1);
		throw InvalidInputException("Python error. See above for a stack trace.");
	}

	auto df = py::reinterpret_steal<py::object>(df_obj);
	if (df.is_none()) { // no return, probably modified in place
		throw InvalidInputException("No return value from Python function");
	}

	if (!py::isinstance<PandasDataFrame>(df)) {
		throw InvalidInputException(
		    "Expected the UDF to return an object of type 'pandas.DataFrame', found '%s' instead",
		    std::string(py::str(df.attr("__class__"))));
	}
	if (PandasDataFrame::IsPyArrowBacked(df)) {
		throw InvalidInputException(
		    "Produced DataFrame has columns that are backed by PyArrow, which is not supported yet in 'map'");
	}

	return df;
}

static bool ContainsNullType(const vector<LogicalType> &types) {
	for (auto &type : types) {
		if (type.id() == LogicalTypeId::SQLNULL) {
			return true;
		}
	}
	return false;
}

static void OverrideNullType(vector<LogicalType> &return_types, const vector<string> &return_names,
                             const vector<LogicalType> &original_types, const vector<string> &original_names) {
	if (!ContainsNullType(return_types)) {
		// Nothing to override, none of the returned types are NULL
		return;
	}
	if (return_types.size() != original_types.size()) {
		// FIXME: we can probably infer from the names in this case
		//  Cant infer what the type should be
		return;
	}
	for (idx_t i = 0; i < return_types.size(); i++) {
		auto &return_type = return_types[i];
		auto &original_type = original_types[i];

		if (return_type != LogicalTypeId::SQLNULL) {
			continue;
		}
		if (return_names[i] != original_names[i]) {
			throw InvalidInputException(
			    "Returned dataframe contains NULL type, and we could not infer the desired type");
		}
		// Override the NULL with the original type
		return_type = original_type;
	}
}

unique_ptr<FunctionData> BindExplicitSchema(unique_ptr<MapFunctionData> function_data, PyObject *schema_p,
                                            vector<LogicalType> &types, vector<string> &names) {
	D_ASSERT(schema_p != Py_None);

	auto schema_object = py::reinterpret_borrow<py::dict>(schema_p);
	if (!py::isinstance<py::dict>(schema_object)) {
		throw InvalidInputException("'schema' should be given as a Dict[str, DuckDBType]");
	}
	auto schema = py::dict(schema_object);

	auto column_count = schema.size();

	types.reserve(column_count);
	names.reserve(column_count);
	for (auto &item : schema) {
		auto name = item.first;
		auto type_p = item.second;
		names.push_back(std::string(py::str(name)));
		// TODO: replace with py::try_cast so we can catch the error and throw a better exception
		auto type = py::cast<shared_ptr<DuckDBPyType>>(type_p);
		types.push_back(type->Type());
	}

	function_data->out_names = names;
	function_data->out_types = types;

	return std::move(function_data);
}

// we call the passed function with a zero-row data frame to infer the output columns and their names.
// they better not change in the actual execution ^^
unique_ptr<FunctionData> MapFunction::MapFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	py::gil_scoped_acquire acquire;

	auto data_uptr = make_uniq<MapFunctionData>();
	auto &data = *data_uptr;
	data.function = reinterpret_cast<PyObject *>(input.inputs[1].GetPointer());
	auto explicit_schema = reinterpret_cast<PyObject *>(input.inputs[2].GetPointer());

	data.in_names = input.input_table_names;
	data.in_types = input.input_table_types;

	if (explicit_schema != Py_None) {
		return BindExplicitSchema(std::move(data_uptr), explicit_schema, return_types, names);
	}
	NumpyResultConversion conversion(data.in_types, 0, context.GetClientProperties());
	auto df = FunctionCall(conversion, data.in_names, data.function);
	vector<PandasColumnBindData> pandas_bind_data; // unused
	Pandas::Bind(context, df, pandas_bind_data, return_types, names);

	// output types are potentially NULL, this happens for types that map to 'object' dtype
	OverrideNullType(return_types, names, data.in_types, data.in_names);

	data.out_names = names;
	data.out_types = return_types;
	return std::move(data_uptr);
}

static string TypeVectorToString(const vector<LogicalType> &types) {
	return StringUtil::Join(types, types.size(), ", ", [](const LogicalType &argument) { return argument.ToString(); });
}

OperatorResultType MapFunction::MapFunctionExec(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                                DataChunk &output) {
	py::gil_scoped_acquire acquire;

	if (input.size() == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	auto &data = data_p.bind_data->Cast<MapFunctionData>();

	D_ASSERT(input.GetTypes() == data.in_types);
	NumpyResultConversion conversion(data.in_types, input.size(), context.client.GetClientProperties());
	conversion.Append(input);

	auto df = FunctionCall(conversion, data.in_names, data.function);

	vector<PandasColumnBindData> pandas_bind_data;
	vector<LogicalType> pandas_return_types;
	vector<string> pandas_names;

	Pandas::Bind(context.client, df, pandas_bind_data, pandas_return_types, pandas_names);
	if (pandas_return_types.size() != output.ColumnCount()) {
		throw InvalidInputException("Expected %llu columns from UDF, got %llu", output.ColumnCount(),
		                            pandas_return_types.size());
	}
	D_ASSERT(output.GetTypes() == data.out_types);
	if (pandas_return_types != output.GetTypes()) {
		throw InvalidInputException("UDF column type mismatch, expected [%s], got [%s]",
		                            TypeVectorToString(data.out_types), TypeVectorToString(pandas_return_types));
	}
	if (pandas_names != data.out_names) {
		throw InvalidInputException("UDF column name mismatch, expected [%s], got [%s]",
		                            StringUtil::Join(data.out_names, ", "), StringUtil::Join(pandas_names, ", "));
	}

	auto df_columns = py::list(df.attr("columns"));
	auto get_fun = df.attr("__getitem__");

	idx_t row_count = py::len(get_fun(df_columns[0]));
	if (row_count > STANDARD_VECTOR_SIZE) {
		throw InvalidInputException("UDF returned more than %llu rows, which is not allowed.", STANDARD_VECTOR_SIZE);
	}

	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &bind_data = pandas_bind_data[col_idx];
		PandasScanFunction::PandasBackendScanSwitch(bind_data, row_count, 0, output.data[col_idx]);
	}
	output.SetCardinality(row_count);
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
