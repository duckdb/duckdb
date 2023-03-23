#pragma once

#include "duckdb_python/pyconnection.hpp"
#include "duckdb/common/helper.hpp"

namespace py = pybind11;

namespace pybind11 {
namespace detail {

using duckdb::DuckDBPyConnection;
using duckdb::shared_ptr;

template <>
struct type_caster<shared_ptr<DuckDBPyConnection>> {
	using type = DuckDBPyConnection;
	PYBIND11_TYPE_CASTER(shared_ptr<type>, const_name("default_connection_holder"));

	using BaseCaster = copyable_holder_caster<type, shared_ptr<type>>;

	bool load(pybind11::handle src, bool b) {
		if (py::none().is(src)) {
			value = DuckDBPyConnection::DefaultConnection();
			return true;
		}
		BaseCaster bc;

		return bc.load(src, b);
	}

	static handle cast(shared_ptr<type> base, return_value_policy rvp, handle h) {
		return BaseCaster::cast(base, rvp, h);
	}
};

template <>
struct is_holder_type<DuckDBPyConnection, std::shared_ptr<DuckDBPyConnection>> : std::true_type {};

} // namespace detail
} // namespace pybind11
