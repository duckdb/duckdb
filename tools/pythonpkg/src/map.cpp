#include "duckdb_python/map.hpp"
#include "duckdb_python/vector_conversion.hpp"
#include "duckdb_python/array_wrapper.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

MapFunction::MapFunction()
    : TableFunction("python_map_function", {LogicalType::TABLE, LogicalType::POINTER}, nullptr, MapFunctionBind) {
	in_out_function = MapFunctionExec;
}

struct MapFunctionData : public TableFunctionData {
	MapFunctionData() : function(nullptr) {
	}
	PyObject *function;
	vector<LogicalType> in_types, out_types;
	vector<string> in_names, out_names;
};

static py::handle FunctionCall(NumpyResultConversion &conversion, vector<string> &names, PyObject *function) {
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

	py::handle df(df_obj);
	if (df.is_none()) { // no return, probably modified in place
		throw InvalidInputException("No return value from Python function");
	}

	return df;
}

// we call the passed function with a zero-row data frame to infer the output columns and their names.
// they better not change in the actual execution ^^
unique_ptr<FunctionData> MapFunction::MapFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	py::gil_scoped_acquire acquire;

	auto data_uptr = make_unique<MapFunctionData>();
	auto &data = *data_uptr;
	data.function = (PyObject *)input.inputs[0].GetPointer();
	data.in_names = input.input_table_names;
	data.in_types = input.input_table_types;

	NumpyResultConversion conversion(data.in_types, 0);
	auto df = FunctionCall(conversion, data.in_names, data.function);
	vector<PandasColumnBindData> pandas_bind_data; // unused
	VectorConversion::BindPandas(DBConfig::GetConfig(context), df, pandas_bind_data, return_types, names);

	data.out_names = names;
	data.out_types = return_types;
	return move(data_uptr);
}

static string TypeVectorToString(vector<LogicalType> &types) {
	return StringUtil::Join(types, types.size(), ", ", [](const LogicalType &argument) { return argument.ToString(); });
}

OperatorResultType MapFunction::MapFunctionExec(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                                DataChunk &output) {
	py::gil_scoped_acquire acquire;

	if (input.size() == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	auto &data = (MapFunctionData &)*data_p.bind_data;

	D_ASSERT(input.GetTypes() == data.in_types);
	NumpyResultConversion conversion(data.in_types, input.size());
	conversion.Append(input);

	auto df = FunctionCall(conversion, data.in_names, data.function);

	vector<PandasColumnBindData> pandas_bind_data;
	vector<LogicalType> pandas_return_types;
	vector<string> pandas_names;

	VectorConversion::BindPandas(DBConfig::GetConfig(context.client), df, pandas_bind_data, pandas_return_types,
	                             pandas_names);
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
		VectorConversion::NumpyToDuckDB(pandas_bind_data[col_idx], pandas_bind_data[col_idx].numpy_col, row_count, 0,
		                                output.data[col_idx]);
	}
	output.SetCardinality(row_count);
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
