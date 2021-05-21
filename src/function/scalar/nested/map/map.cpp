#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

static void MapFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::MAP);
	D_ASSERT(result.GetType().child_types().size() == 2 );

	//! Otherwise if its not a constant vector, this breaks the optimizer
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	if (args.data.empty()){
        child_list_t<LogicalType> child_types;
	    child_types.push_back({"", LogicalTypeId::SQLNULL});
	    LogicalType list_vector_type(LogicalType::LIST.id(), child_types);
	    auto list_vector = make_unique<Vector>(list_vector_type);
	    auto list_child = make_unique<Vector>(LogicalTypeId::SQLNULL);
	    ListVector::SetEntry(*list_vector, move(list_child));
	    ListVector::SetListSize(*list_vector, 0);
	    auto list_data = FlatVector::GetData<list_entry_t>(*list_vector);
	    list_data->offset = 0;
	    list_data->length = 0;
        StructVector::AddEntry(result, "key", move(list_vector));


        list_vector = make_unique<Vector>(list_vector_type);
        list_child = make_unique<Vector>(LogicalTypeId::SQLNULL);
	    ListVector::SetEntry(*list_vector, move(list_child));
	    ListVector::SetListSize(*list_vector, 0);
	    list_data = FlatVector::GetData<list_entry_t>(*list_vector);
	    list_data->offset = 0;
	    list_data->length = 0;
	    StructVector::AddEntry(result, "value", move(list_vector));
	    //! This is an empty map
	    return;
	}

	if (ListVector::GetListSize(args.data[0]) != ListVector::GetListSize(args.data[1])) {
		throw Exception("Key list has a different size from Value list");
	}
	unique_ptr<Vector> vec_ptr = make_unique<Vector>();
	vec_ptr->Reference(args.data[0]);
	StructVector::AddEntry(result, "key", move(vec_ptr));
	vec_ptr = make_unique<Vector>();
	vec_ptr->Reference(args.data[1]);
	StructVector::AddEntry(result, "value", move(vec_ptr));
	result.Verify(args.size());
}

static unique_ptr<FunctionData> MapBind(ClientContext &context, ScalarFunction &bound_function,
                                        vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> child_types;

	if (arguments.size() != 2 && !arguments.empty()) {
		throw Exception("We need exactly two lists for a map");
	}
	if (arguments.size() == 2){
	    if (arguments[0]->return_type.id() != LogicalTypeId::LIST) {
		    throw Exception("First argument is not a list");
	    }
	if (arguments[1]->return_type.id() != LogicalTypeId::LIST) {
		throw Exception("Second argument is not a list");
	}
		    child_types.push_back(make_pair("key", arguments[0]->return_type));
	    child_types.push_back(make_pair("value", arguments[1]->return_type));
	}

	if (arguments.empty()){
	    child_list_t<LogicalType> child;
	    child.push_back({"",LogicalTypeId::SQLNULL});
	    auto empty = LogicalType(LogicalTypeId::LIST,child);
	    child_types.push_back(make_pair("key",empty));
	    child_types.push_back(make_pair("value", empty));
	}



	//! this is more for completeness reasons
	bound_function.return_type = LogicalType(LogicalTypeId::MAP, move(child_types));
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void MapFun::RegisterFunction(BuiltinFunctions &set) {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map", {}, LogicalType::MAP, MapFunction, false, MapBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);

}

} // namespace duckdb
