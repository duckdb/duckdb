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
	BOOL,             //! bool_, bool8
	INT_8,            //!	byte, int8
	UINT_8,           //! ubyte, uint8
	INT_16,           //! int16, short
	UINT_16,          //! uint16, ushort
	INT_32,           //! int32, intc
	UINT_32,          //! uint32, uintc,
	INT_64,           //! int64, int0, int_, intp, matrix
	UINT_64,          //! uint64, uint, uint0, uintp
	LONG_INT_64,      //! longlong
	LONG_UINT_64,     //! unused
	FLOAT_32,         //! float32, single
	FLOAT_64,         //! float64, float_, double
	LONG_FLOAT_64,    //! longfloat, longdouble
	COMPLEX_64,       //! complex64, csingle
	COMPLEX_128,      //! complex128, complex_, cdouble, cfloat
	LONG_COMPLEX_128, //! clongdouble, clongfloat, longcomplex
	OBJECT,           //! object
	BYTES,            //! |S0, bytes0, bytes_, string_
	UNICODE,          //! <U1, unicode_, str_, str0
	RECORD,           //! |V1, record, void, void0
	DATETIME,         //! datetime64[D], datetime64
	TIMEDELTA,        //! timedelta64[D], timedelta64
	FLOAT_16,         //! float16, half
	PANDA_BOOL,       //! boolean
	PANDA_CATEGORY,   //! category
	PANDA_INT8,       //! Int8
	PANDA_INT16,      //! Int16
	PANDA_INT32,      //! Int32
	PANDA_INT64,      //! Int64

	PANDA_UINT8,  //! UInt8
	PANDA_UINT16, //! UInt16
	PANDA_UINT32, //! UInt32
	PANDA_UINT64, //! UInt64

	PANDA_FLOAT32, //! Float32
	PANDA_FLOAT64, //! Float64

	PANDA_STRING,               //! string
	PANDA_EXTENSION_TYPE = 100, //! category (also, for some reason)
	PANDA_DATETIME = 101,       //! datetime64[ns, UTC]
	PANDA_PERIOD = 102,         //! datetime64[ns, UTC]
	PANDA_INTERVAL = 103,       //! datetime64[ns, UTC]
};

enum class PandasType : uint8_t {
	BOOL,
	BOOLEAN,
	TINYINT,
	SMALLINT,
	INTEGER,
	BIGINT,
	UTINYINT,
	USMALLINT,
	UINTEGER,
	UBIGINT,
	FLOAT,
	DOUBLE,
	TIMESTAMP,
	INTERVAL,
	VARCHAR,
	OBJECT,
	CATEGORY
};

struct NumPyArrayWrapper {
	explicit NumPyArrayWrapper(py::array numpy_array) : numpy_array(move(numpy_array)) {
	}

	py::array numpy_array;
};

struct PandasColumnBindData {
	PandasType pandas_type;
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
