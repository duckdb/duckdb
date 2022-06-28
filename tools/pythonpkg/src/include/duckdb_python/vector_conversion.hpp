//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/vector_conversion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind_wrapper.hpp"

#include "duckdb.hpp"
#include "duckdb_python/python_object_container.hpp"
namespace duckdb {

enum class ExtendedNumpyType : uint8_t {
	//! Original NumPy dtype nums

	BOOL = 0,              //! bool_, bool8
	INT_8 = 1,             //! byte, int8
	UINT_8 = 2,            //! ubyte, uint8
	INT_16 = 3,            //! int16, short
	UINT_16 = 4,           //! uint16, ushort
	INT_32 = 5,            //! int32, intc
	UINT_32 = 6,           //! uint32, uintc,
	INT_64 = 7,            //! int64, int0, int_, intp, matrix
	UINT_64 = 8,           //! uint64, uint, uint0, uintp
	LONG_INT_64 = 9,       //! longlong
	LONG_UINT_64 = 10,     //! unused
	FLOAT_32 = 11,         //! float32, single
	FLOAT_64 = 12,         //! float64, float_, double
	LONG_FLOAT_64 = 13,    //! longfloat, longdouble
	COMPLEX_64 = 14,       //! complex64, csingle
	COMPLEX_128 = 15,      //! complex128, complex_, cdouble, cfloat
	LONG_COMPLEX_128 = 16, //! clongdouble, clongfloat, longcomplex
	OBJECT = 17,           //! object
	BYTES = 18,            //! |S0, bytes0, bytes_, string_
	UNICODE = 19,          //! <U1, unicode_, str_, str0
	RECORD = 20,           //! |V1, record, void, void0
	DATETIME = 21,         //! datetime64[D], datetime64
	TIMEDELTA = 22,        //! timedelta64[D], timedelta64
	FLOAT_16 = 23,         //! float16, half

	//! ------------------------------------------------------------

	PANDA_BOOL,     //! boolean
	PANDA_CATEGORY, //! category
	PANDA_STRING,   //! string

	PANDA_INT8,  //! Int8
	PANDA_INT16, //! Int16
	PANDA_INT32, //! Int32
	PANDA_INT64, //! Int64

	PANDA_UINT8,  //! UInt8
	PANDA_UINT16, //! UInt16
	PANDA_UINT32, //! UInt32
	PANDA_UINT64, //! UInt64

	PANDA_FLOAT32, //! Float32
	PANDA_FLOAT64, //! Float64

	PANDA_EXTENSION_TYPE = 100, //! ExtensionDType base class num (category inherits from this)
	PANDA_DATETIME = 101,       //! datetime64[ns, UTC]
	PANDA_PERIOD = 102,         //! datetime64[ns, UTC]
	PANDA_INTERVAL = 103,       //! datetime64[ns, UTC]
};

// enum class PandasType : uint8_t {
//	BOOL,
//	PANDA_BOOL,
//	TINYINT,
//	SMALLINT,
//	INTEGER,
//	BIGINT,
//	UTINYINT,
//	USMALLINT,
//	UINTEGER,
//	UBIGINT,
//	FLOAT,
//	DOUBLE,
//	TIMESTAMP,
//	INTERVAL,
//	VARCHAR,
//	OBJECT,
//	CATEGORY
// };

struct NumPyArrayWrapper {
	explicit NumPyArrayWrapper(py::array numpy_array) : numpy_array(move(numpy_array)) {
	}

	py::array numpy_array;
};

struct PandasColumnBindData {
	ExtendedNumpyType pandas_type;
	py::array numpy_col;
	idx_t numpy_stride;
	unique_ptr<NumPyArrayWrapper> mask;
	// Only for categorical types
	string internal_categorical_type;
	// When object types are cast we must hold their data somewhere
	PythonObjectContainer<py::str> object_str_val;
};

class VectorConversion {
public:
	static void NumpyToDuckDB(PandasColumnBindData &bind_data, py::array &numpy_col, idx_t count, idx_t offset,
	                          Vector &out);

	static void BindPandas(py::handle df, vector<PandasColumnBindData> &out, vector<LogicalType> &return_types,
	                       vector<string> &names);

	static void Analyze(py::handle original_df);
};

} // namespace duckdb
