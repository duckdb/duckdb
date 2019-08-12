#include "function/scalar/string_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterStringFunctions() {
	Register<Lower>();
	Register<Upper>();
	Register<Concat>();
	Register<Length>();
	Register<Regexp>();
	Register<Substring>();
}

}
