#include "duckdb/planner/binder.hpp"

namespace duckdb {

unique_ptr<BoundCreateFunctionInfo> Binder::BindCreateFunctionInfo(unique_ptr<CreateInfo> info) {
    auto &base = (CreateMacroFunctionInfo &)*info;

    auto result = make_unique<BoundCreateFunctionInfo>(move(info));
    result->schema = BindSchema(*result->base);

	// arguments become dummy column names
    vector<string> dummy_column_names;
    vector<LogicalType> dummy_column_types;
    for (auto &arg : base.arguments) {
		string arg_str = arg->ToString();
        if (arg->expression_class != ExpressionClass::COLUMN_REF)
			throw BinderException("Invalid parameter \"%s\"", arg_str);
		dummy_column_names.push_back(arg_str);
		dummy_column_types.push_back(LogicalType::SQLNULL);
    }

    // check whether an unknown parameter is used in the function
	ExpressionBinder binder(*this, context);
	bind_context.AddGenericBinding(-1, "0_macro_arguments", dummy_column_names, dummy_column_types);
	binder.Bind(base.function, 0, true);

    return result;
}

} // namespace duckdb
