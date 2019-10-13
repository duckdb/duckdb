#include "function/scalar/operators.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void BuiltinFunctions::RegisterOperators() {
	Register<Add>();
	Register<Subtract>();
	Register<Multiply>();
	Register<Divide>();
	Register<Mod>();
	Register<LeftShift>();
	Register<RightShift>();
	Register<BitwiseAnd>();
	Register<BitwiseOr>();
	Register<BitwiseXor>();
}
