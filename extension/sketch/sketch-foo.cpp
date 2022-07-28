#include "include/sketch-foo.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "utf8proc.hpp"


namespace duckdb {

struct FooOperator {
	static string_t Operation(const string_t &str, vector<char> &result) {		
        string_t foo_str = "foo";
        auto str_data = str.GetDataUnsafe();
        auto str_size = str.GetSize();
        result.insert(result.end(), str_data, str_data + str_size);
        result.insert(result.end(), foo_str.GetDataUnsafe(), foo_str.GetDataUnsafe() + foo_str.GetSize());
		return string_t(result.data(), result.size());
    }
};

static void FooFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &str_vector = args.data[0];
    vector<char> buffer;
    UnaryExecutor::Execute<string_t, string_t>(
	    str_vector, result, args.size(), [&](string_t str) {
		    return StringVector::AddString(result, FooOperator::Operation(str, buffer));
	    });
}

void SketchFoo::RegisterFunction(ClientContext &context) {
    ScalarFunctionSet set("sketchfoo");

    set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, FooFunction));

    auto &catalog = Catalog::GetCatalog(context);
    CreateScalarFunctionInfo func_info(move(set));
    catalog.AddFunction(context, &func_info);
}

} // namespace duckdb
