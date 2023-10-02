//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pandas/pandas_analyzer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pybind11/gil_wrapper.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb_python/python_conversion.hpp"

namespace duckdb {

class PandasAnalyzer {
public:
	PandasAnalyzer(const DBConfig &config) {
		analyzed_type = LogicalType::SQLNULL;

		auto maximum_entry = config.options.set_variables.find("pandas_analyze_sample");
		D_ASSERT(maximum_entry != config.options.set_variables.end());
		sample_size = maximum_entry->second.GetValue<uint64_t>();
	}

public:
	LogicalType GetListType(py::object &ele, bool &can_convert);
	LogicalType DictToMap(const PyDictionary &dict, bool &can_convert);
	LogicalType DictToStruct(const PyDictionary &dict, bool &can_convert);
	LogicalType GetItemType(py::object ele, bool &can_convert);
	bool Analyze(py::object column);
	LogicalType AnalyzedType() {
		return analyzed_type;
	}

private:
	LogicalType InnerAnalyze(py::object column, bool &can_convert, bool sample = true, idx_t increment = 1);
	uint64_t GetSampleIncrement(idx_t rows);

private:
	uint64_t sample_size;
	//! Holds the gil to allow python object creation/destruction
	PythonGILWrapper gil;
	//! The resulting analyzed type
	LogicalType analyzed_type;
};

} // namespace duckdb
