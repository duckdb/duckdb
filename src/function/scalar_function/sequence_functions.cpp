#include "function/scalar_function/sequence_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterSequenceFunctions() {
	AddFunction(ScalarFunction("nextval", nextval_matches_arguments, nextval_get_return_type, nextval_function, true, nextval_bind, nextval_dependency));
}

}
