#include "duckdb_python/pandas/pandas_bind.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb_python/pandas/pandas_analyzer.hpp"

namespace duckdb {

void Pandas::Bind(const DBConfig &config, py::handle df, vector<PandasColumnBindData> &bind_columns,
                  vector<LogicalType> &return_types, vector<string> &names) {
	// This performs a shallow copy that allows us to rename the dataframe
	auto df_columns = py::list(df.attr("columns"));
	auto df_types = py::list(df.attr("dtypes"));
	auto get_fun = df.attr("__getitem__");
	// TODO support masked arrays as well
	// TODO support dicts of numpy arrays as well
	if (py::len(df_columns) == 0 || py::len(df_types) == 0 || py::len(df_columns) != py::len(df_types)) {
		throw InvalidInputException("Need a DataFrame with at least one column");
	}

	py::array column_attributes = df.attr("columns").attr("values");

	// loop over every column
	for (idx_t col_idx = 0; col_idx < py::len(df_columns); col_idx++) {
		LogicalType duckdb_col_type;
		PandasColumnBindData bind_data;

		names.emplace_back(py::str(df_columns[col_idx]));
		bind_data.pandas_type = ConvertNumpyType(df_types[col_idx]);
		bool column_has_mask = py::hasattr(get_fun(df_columns[col_idx]).attr("array"), "_mask");

		if (column_has_mask) {
			// masked object, fetch the internal data and mask array
			bind_data.mask = make_unique<RegisteredArray>(get_fun(df_columns[col_idx]).attr("array").attr("_mask"));
		}

		auto column = get_fun(df_columns[col_idx]);
		if (bind_data.pandas_type == NumpyNullableType::CATEGORY) {
			// for category types, we create an ENUM type for string or use the converted numpy type for the rest
			D_ASSERT(py::hasattr(column, "cat"));
			D_ASSERT(py::hasattr(column.attr("cat"), "categories"));
			auto categories = py::array(column.attr("cat").attr("categories"));
			auto categories_pd_type = ConvertNumpyType(categories.attr("dtype"));
			if (categories_pd_type == NumpyNullableType::OBJECT) {
				// Let's hope the object type is a string.
				bind_data.pandas_type = NumpyNullableType::CATEGORY;
				auto enum_name = string(py::str(df_columns[col_idx]));
				vector<string> enum_entries = py::cast<vector<string>>(categories);
				idx_t size = enum_entries.size();
				Vector enum_entries_vec(LogicalType::VARCHAR, size);
				auto enum_entries_ptr = FlatVector::GetData<string_t>(enum_entries_vec);
				for (idx_t i = 0; i < size; i++) {
					enum_entries_ptr[i] = StringVector::AddStringOrBlob(enum_entries_vec, enum_entries[i]);
				}
				D_ASSERT(py::hasattr(column.attr("cat"), "codes"));
				duckdb_col_type = LogicalType::ENUM(enum_name, enum_entries_vec, size);
				bind_data.numpy_col = py::array(column.attr("cat").attr("codes"));
				D_ASSERT(py::hasattr(bind_data.numpy_col, "dtype"));
				bind_data.internal_categorical_type = string(py::str(bind_data.numpy_col.attr("dtype")));
			} else {
				bind_data.numpy_col = py::array(column.attr("to_numpy")());
				auto numpy_type = bind_data.numpy_col.attr("dtype");
				// for category types (non-strings), we use the converted numpy type
				bind_data.pandas_type = ConvertNumpyType(numpy_type);
				duckdb_col_type = NumpyToLogicalType(bind_data.pandas_type);
			}
		} else if (bind_data.pandas_type == NumpyNullableType::FLOAT_16) {
			auto pandas_array = get_fun(df_columns[col_idx]).attr("array");
			bind_data.numpy_col = py::array(column.attr("to_numpy")("float32"));
			bind_data.pandas_type = NumpyNullableType::FLOAT_32;
			duckdb_col_type = NumpyToLogicalType(bind_data.pandas_type);
		} else {
			auto pandas_array = get_fun(df_columns[col_idx]).attr("array");
			if (py::hasattr(pandas_array, "_data")) {
				// This means we can access the numpy array directly
				bind_data.numpy_col = get_fun(df_columns[col_idx]).attr("array").attr("_data");
			} else if (py::hasattr(pandas_array, "asi8")) {
				// This is a datetime object, has the option to get the array as int64_t's
				bind_data.numpy_col = py::array(pandas_array.attr("asi8"));
			} else {
				// Otherwise we have to get it through 'to_numpy()'
				bind_data.numpy_col = py::array(column.attr("to_numpy")());
			}
			duckdb_col_type = NumpyToLogicalType(bind_data.pandas_type);
		}
		// Analyze the inner data type of the 'object' column
		if (bind_data.pandas_type == NumpyNullableType::OBJECT) {
			PandasAnalyzer analyzer(config);
			if (analyzer.Analyze(get_fun(df_columns[col_idx]))) {
				duckdb_col_type = analyzer.AnalyzedType();
			}
		}

		D_ASSERT(py::hasattr(bind_data.numpy_col, "strides"));
		bind_data.numpy_stride = bind_data.numpy_col.attr("strides").attr("__getitem__")(0).cast<idx_t>();
		return_types.push_back(duckdb_col_type);
		bind_columns.push_back(std::move(bind_data));
	}
}

} // namespace duckdb
