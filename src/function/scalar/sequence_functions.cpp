#include "function/scalar/sequence_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterSequenceFunctions() {
	Register<Nextval>();
}

}
