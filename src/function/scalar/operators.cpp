#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterOperators() {
	Register<AddFun>();
	Register<SubtractFun>();
	Register<MultiplyFun>();
	Register<DivideFun>();
	Register<ModFun>();
	Register<LeftShiftFun>();
	Register<RightShiftFun>();
	Register<BitwiseAndFun>();
	Register<BitwiseOrFun>();
	Register<BitwiseXorFun>();
	Register<BitwiseNotFun>();
}

} // namespace duckdb
