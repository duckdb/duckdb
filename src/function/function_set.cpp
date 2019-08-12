#include "function/function_set.hpp"

using namespace duckdb;
using namespace std;

FunctionSet::FunctionSet(string name) :
	name(name) {

}

void FunctionSet::AddFunction(ScalarFunction function) {
	functions.push_back(function);
}
