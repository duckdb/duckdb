#include "duckdb/function/scalar/compressed_materialization_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterCompressedMaterializationFunctions() {
	Register<CMIntegralCompressFun>();
	Register<CMIntegralDecompressFun>();
	Register<CMStringCompressFun>();
	Register<CMStringDecompressFun>();
}

} // namespace duckdb
