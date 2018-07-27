
#include "common/types/operators.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

namespace operators {
	template<> double Modulo::Operation(double left, double right) {
		throw NotImplementedException("Modulo for double not implemented!");
	}
}