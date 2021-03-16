#include "duckdb/function/function_set.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
namespace duckdb {
class AVX2Functions {
public:
	static ScalarFunctionSet GetFunctions();
	static ScalarFunctionSet GetAddFunctions();
};

} // namespace duckdb