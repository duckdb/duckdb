#include "duckdb/function/scalar/statistical_functions.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {
	void BuiltinFunctions::RegisterStatisticalFunctions() {
		Register<StatisticalNormalPDFFun>();
	}
}