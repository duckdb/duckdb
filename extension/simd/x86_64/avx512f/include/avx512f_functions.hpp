#include "duckdb/function/function_set.hpp"

namespace duckdb {
class AVX512fFunctions {
public:
	static ScalarFunctionSet GetFunctions();
	static ScalarFunctionSet GetAddFunctions();
};
} // namespace duckdb