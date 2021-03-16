#include "duckdb/function/function_set.hpp"
namespace duckdb {
class Functions{
public:
    static ScalarFunctionSet GetFunctions();
    static ScalarFunctionSet GetAddFunctions();
};

}