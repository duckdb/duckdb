#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/exception.hpp"

using namespace duckdb;
using namespace std;

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
}
