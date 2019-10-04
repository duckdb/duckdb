#include "function/scalar/math_functions.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void BuiltinFunctions::RegisterMathFunctions() {
	Register<Abs>();
	Register<Sign>();

	Register<Ceil>();
	Register<Floor>();
	Register<Round>();

	Register<Degrees>();
	Register<Radians>();

	Register<Cbrt>();
	Register<Exp>();
	Register<Log2>();
	Register<Log10>();
	Register<Ln>();
	Register<Pow>();
	Register<Random>();
	Register<Setseed>();
	Register<Sqrt>();

	Register<Pi>();
}
