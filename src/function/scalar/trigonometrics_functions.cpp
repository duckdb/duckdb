#include "function/scalar/trigonometric_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/exception.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterTrigonometricsFunctions() {
	Register<Sin>();
	Register<Cos>();
	Register<Tan>();
	Register<Asin>();
	Register<Acos>();
	Register<Atan>();
	Register<Cot>();
	Register<Atan2>();
}

} // namespace duckdb
