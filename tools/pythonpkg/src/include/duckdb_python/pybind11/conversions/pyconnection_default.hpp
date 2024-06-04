#pragma once

#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb/common/helper.hpp"

using duckdb::DuckDBPyConnection;
using duckdb::shared_ptr;

namespace py = pybind11;

namespace PYBIND11_NAMESPACE {
namespace detail {

template <>
class type_caster<shared_ptr<DuckDBPyConnection>>
    : public copyable_holder_caster<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>> {
	using type = DuckDBPyConnection;
	using holder_caster = copyable_holder_caster<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>>;
	// This is used to generate documentation on duckdb-web
	PYBIND11_TYPE_CASTER(shared_ptr<type>, const_name("duckdb.DuckDBPyConnection"));

	bool load(handle src, bool convert) {
		if (py::none().is(src)) {
			value = DuckDBPyConnection::DefaultConnection();
			return true;
		}
		if (!holder_caster::load(src, convert)) {
			return false;
		}
		value = std::move(holder);
		return true;
	}

	static handle cast(shared_ptr<type> base, return_value_policy rvp, handle h) {
		return holder_caster::cast(base, rvp, h);
	}
};

template <>
struct is_holder_type<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>> : std::true_type {};

} // namespace detail
} // namespace PYBIND11_NAMESPACE
