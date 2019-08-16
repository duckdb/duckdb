#include "function/function_set.hpp"

using namespace duckdb;
using namespace std;

FunctionSet::FunctionSet(string name) :
	name(name) {

}

void FunctionSet::AddFunction(ScalarFunction function) {
	function.name = name;
	functions.push_back(function);
}
