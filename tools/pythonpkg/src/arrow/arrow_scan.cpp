//#include "duckdb_python/pyrelation.hpp"
//#include "duckdb_python/pyconnection/pyconnection.hpp"
//#include "duckdb_python/pyresult.hpp"
//#include "duckdb_python/python_conversion.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
//#include "utf8proc_wrapper.hpp"
//#include "duckdb/common/case_insensitive_map.hpp"
//#include "duckdb_python/pandas/pandas_bind.hpp"
//#include "duckdb_python/numpy/numpy_type.hpp"
//#include "duckdb_python/pandas/pandas_analyzer.hpp"
//#include "duckdb_python/numpy/numpy_type.hpp"
//#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb_python/arrow/arrow_scan.hpp"
#include "duckdb_python/pandas/column/pandas_arrow_column.hpp"
#include "duckdb_python/pandas/pandas_bind.hpp"

namespace duckdb {
// 'offset' is the offset within the column
// 'count' is the amount of values we will convert in this batch
void ArrowScan::Scan(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out) {
	D_ASSERT(bind_data.pandas_col->Backend() == PandasColumnBackend::NUMPY);
	auto &arrow_col = reinterpret_cast<PandasArrowColumn &>(*bind_data.pandas_col);
	auto &chunked_array = arrow_col.array;
	switch (bind_data.numpy_type.type) {
	case NumpyNullableType::
		ScanNumpyMasked<bool>(bind_data, count, offset, out);
		break;
	default:
		throw NotImplementedException("Unsupported Pandas-Arrow type");
	}
}

} // namespace duckdb