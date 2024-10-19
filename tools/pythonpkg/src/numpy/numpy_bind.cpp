#include "duckdb_python/numpy/numpy_bind.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb_python/pandas/pandas_analyzer.hpp"
#include "duckdb_python/pandas/column/pandas_numpy_column.hpp"
#include "duckdb_python/pandas/pandas_bind.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"

namespace duckdb {

void NumpyBind::Bind(const ClientContext &context, py::handle df, vector<PandasColumnBindData> &bind_columns,
                     vector<LogicalType> &return_types, vector<string> &names) {

	auto df_columns = py::list(df.attr("keys")());
	auto df_types = py::list();
	for (auto item : py::cast<py::dict>(df)) {
		if (string(py::str(item.second.attr("dtype").attr("char"))) == "U") {
			df_types.attr("append")(py::str("string"));
			continue;
		}
		df_types.attr("append")(py::str(item.second.attr("dtype")));
	}
	auto get_fun = df.attr("__getitem__");
	if (py::len(df_columns) == 0 || py::len(df_types) == 0 || py::len(df_columns) != py::len(df_types)) {
		throw InvalidInputException("Need a DataFrame with at least one column");
	}
	for (idx_t col_idx = 0; col_idx < py::len(df_columns); col_idx++) {
		LogicalType duckdb_col_type;
		PandasColumnBindData bind_data;

		names.emplace_back(py::str(df_columns[col_idx]));
		bind_data.numpy_type = ConvertNumpyType(df_types[col_idx]);

		auto column = get_fun(df_columns[col_idx]);

		if (bind_data.numpy_type.type == NumpyNullableType::FLOAT_16) {
			bind_data.pandas_col = make_uniq<PandasNumpyColumn>(py::array(column.attr("astype")("float32")));
			bind_data.numpy_type.type = NumpyNullableType::FLOAT_32;
			duckdb_col_type = NumpyToLogicalType(bind_data.numpy_type);
		} else if (bind_data.numpy_type.type == NumpyNullableType::STRING) {
			bind_data.numpy_type.type = NumpyNullableType::CATEGORY;
			// here we call numpy.unique
			// this function call will return the unique values of a given array
			// together with the indices to reconstruct the given array
			auto uniq = py::cast<py::tuple>(py::module_::import("numpy").attr("unique")(column, false, true));
			vector<string> enum_entries = py::cast<vector<string>>(uniq.attr("__getitem__")(0));
			idx_t size = enum_entries.size();
			Vector enum_entries_vec(LogicalType::VARCHAR, size);
			auto enum_entries_ptr = FlatVector::GetData<string_t>(enum_entries_vec);
			for (idx_t i = 0; i < size; i++) {
				enum_entries_ptr[i] = StringVector::AddStringOrBlob(enum_entries_vec, enum_entries[i]);
			}
			duckdb_col_type = LogicalType::ENUM(enum_entries_vec, size);
			auto pandas_col = uniq.attr("__getitem__")(1);
			bind_data.internal_categorical_type = string(py::str(pandas_col.attr("dtype")));
			bind_data.pandas_col = make_uniq<PandasNumpyColumn>(pandas_col);
		} else {
			bind_data.pandas_col = make_uniq<PandasNumpyColumn>(column);
			duckdb_col_type = NumpyToLogicalType(bind_data.numpy_type);
		}

		if (bind_data.numpy_type.type == NumpyNullableType::OBJECT) {
			PandasAnalyzer analyzer(context);
			if (analyzer.Analyze(get_fun(df_columns[col_idx]))) {
				duckdb_col_type = analyzer.AnalyzedType();
			}
		}

		return_types.push_back(duckdb_col_type);
		bind_columns.push_back(std::move(bind_data));
	}
}

} // namespace duckdb
