#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

void BuiltinFunctions::SET_ARCH(RegisterOperators)() {
	Register<SET_ARCH(AddFun)>();
	Register<SET_ARCH(SubtractFun)>();
	Register<SET_ARCH(MultiplyFun)>();
	Register<SET_ARCH(DivideFun)>();
	Register<SET_ARCH(ModFun)>();
	Register<SET_ARCH(LeftShiftFun)>();
	Register<SET_ARCH(RightShiftFun)>();
	Register<SET_ARCH(BitwiseAndFun)>();
	Register<SET_ARCH(BitwiseOrFun)>();
	Register<SET_ARCH(BitwiseXorFun)>();
	Register<SET_ARCH(BitwiseNotFun)>();
}

} // namespace duckdb
