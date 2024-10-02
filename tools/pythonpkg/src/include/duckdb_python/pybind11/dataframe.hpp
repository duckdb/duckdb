//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pybind11/dataframe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

class PandasDataFrame : public py::object {
public:
	PandasDataFrame(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool check_(const py::handle &object); // NOLINT
	static bool IsPyArrowBacked(const py::handle &df);
	static py::object ToArrowTable(const py::object &df);
};

class PolarsDataFrame : public py::object {
public:
	PolarsDataFrame(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool IsDataFrame(const py::handle &object);
	static bool IsLazyFrame(const py::handle &object);
	static bool check_(const py::handle &object); // NOLINT
};
} // namespace duckdb

namespace pybind11 {
namespace detail {
template <>
struct handle_type_name<duckdb::PandasDataFrame> {
	static constexpr auto name = _("pandas.DataFrame");
};
} // namespace detail
} // namespace pybind11
