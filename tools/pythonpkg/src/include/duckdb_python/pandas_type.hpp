#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/types.hpp"

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

// Pandas has two different sets of types
// NumPy dtypes (e.g., bool, int8,...)
// Pandas Specific Types (e.g., categorical, datetime_tz,...)
enum class PandasType : uint8_t {
	//! NumPy dtypes
	BOOL,      //! bool_, bool8
	INT_8,     //! byte, int8
	UINT_8,    //! ubyte, uint8
	INT_16,    //! int16, short
	UINT_16,   //! uint16, ushort
	INT_32,    //! int32, intc
	UINT_32,   //! uint32, uintc,
	INT_64,    //! int64, int0, int_, intp, matrix
	UINT_64,   //! uint64, uint, uint0, uintp
	FLOAT_32,  //! float32, single
	FLOAT_64,  //! float64, float_, double
	OBJECT,    //! object
	UNICODE,   //! <U1, unicode_, str_, str0
	DATETIME,  //! datetime64[D], datetime64
	TIMEDELTA, //! timedelta64[D], timedelta64

	//! ------------------------------------------------------------
	//! Pandas Specific Types
	//! ------------------------------------------------------------
	CATEGORY,    //! category
	DATETIME_TZ, //! datetime64[ns, TZ]

};

PandasType ConvertPandasType(const py::object &col_type);
LogicalType PandasToLogicalType(const PandasType &col_type);

} // namespace duckdb

namespace pybind11 {
namespace detail {
template <>
struct handle_type_name<duckdb::data_frame> {
	static constexpr auto name = _("pandas.DataFrame");
};
} // namespace detail
} // namespace pybind11
