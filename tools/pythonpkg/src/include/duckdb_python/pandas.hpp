#pragma once

#include "duckdb_python/pybind_wrapper.hpp"

namespace duckdb {
class data_frame : public py::object {
public:
	data_frame(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

	static bool check_(const py::handle &object) {
		return !py::none().is(object);
	}
};
} // namespace duckdb

namespace pybind11 {
namespace detail {
template <>
struct handle_type_name<duckdb::data_frame> {
	static constexpr auto name = _("pandas.DataFrame");
};
} // namespace detail
} // namespace pybind11
