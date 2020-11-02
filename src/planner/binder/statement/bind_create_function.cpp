#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

unique_ptr<BoundCreateFunctionInfo> Binder::BindCreateFunctionInfo(unique_ptr<CreateInfo> info) {
    auto &base = (CreateMacroFunctionInfo &)*info;

    auto result = make_unique<BoundCreateFunctionInfo>(move(info));
    result->schema = BindSchema(*result->base);

	// check whether an unknown parameter is used in the function
    ParsedExpressionIterator::EnumerateChildren(*base.function, [&](const ParsedExpression &child) {
      // TODO: check whether an unknown parameter is used in base.function

	});
    return result;
}

} // namespace duckdb
