#include "duckdb/function/scalar/operator_functions.hpp"
#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/geometry_functions.hpp"

namespace duckdb {

// The canonical "element", "field" and "slice" functions back the `[]`, `.x` and `[m:n]` syntax forms.
// TODO: Make the implementations of these proper overloads

ScalarFunctionSet ElementFun::GetFunctions() {
	ScalarFunctionSet set("element");
	for (auto &function : ArrayExtractFun::GetFunctions().functions) {
		set.AddFunction(function);
	}
	for (auto &function : VariantExtractFun::GetFunctions().functions) {
		set.AddFunction(function);
	}
	return set;
}

ScalarFunctionSet FieldFun::GetFunctions() {
	ScalarFunctionSet set("field");
	for (auto &function : StructExtractFun::GetFunctions().functions) {
		set.AddFunction(function);
	}
	for (auto &function : VariantExtractFun::GetFunctions().functions) {
		set.AddFunction(function);
	}
	set.AddFunction(VertexExtractFun::GetFunction());
	return set;
}

ScalarFunctionSet SliceFun::GetFunctions() {
	// the list/string slice overloads live in the core_functions extension and are appended at load time
	ScalarFunctionSet set("slice");
	return set;
}

} // namespace duckdb
