//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/numpy/numpy_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

// Pandas has two different sets of types
// NumPy dtypes (e.g., bool, int8,...)
// Pandas Specific Types (e.g., categorical, datetime_tz,...)
enum class NumpyNullableType : uint8_t {
	//! NumPy dtypes
	BOOL,        //! bool_, bool8
	INT_8,       //! byte, int8
	UINT_8,      //! ubyte, uint8
	INT_16,      //! int16, short
	UINT_16,     //! uint16, ushort
	INT_32,      //! int32, intc
	UINT_32,     //! uint32, uintc,
	INT_64,      //! int64, int0, int_, intp, matrix
	UINT_64,     //! uint64, uint, uint0, uintp
	FLOAT_16,    //! float16, half
	FLOAT_32,    //! float32, single
	FLOAT_64,    //! float64, float_, double
	OBJECT,      //! object
	UNICODE,     //! <U1, unicode_, str_, str0
	DATETIME_S,  //! datetime64[s], <M8[s]
	DATETIME_MS, //! datetime64[ms], <M8[ms]
	DATETIME_NS, //! datetime64[ns], <M8[ns]
	DATETIME_US, //! datetime64[us], <M8[us]
	TIMEDELTA,   //! timedelta64[D], timedelta64

	//! ------------------------------------------------------------
	//! Extension Types
	//! ------------------------------------------------------------
	CATEGORY, //! category
};

struct NumpyType {
	NumpyNullableType type;
	//! Optionally if the type is a DATETIME,
	//! this indicates whether the type has timezone information
	bool has_timezone = false;
};

enum class NumpyObjectType : uint8_t {
	//! To identify supported Numpy objects for scaning
	INVALID,   //! unsupported numpy object
	NDARRAY1D, //! numpy array with shape (n, )
	NDARRAY2D, //! numpy array with shape (n_rows, n_cols)
	LIST,      //! list of numpy arrays of shape (n,)
	DICT,      //! dict of numpy arrays of shape (n,)
};

NumpyType ConvertNumpyType(const py::handle &col_type);
LogicalType NumpyToLogicalType(const NumpyType &col_type);

} // namespace duckdb
