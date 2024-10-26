#include "duckdb/planner/expression_binder/index_binder.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

IndexBinder::IndexBinder(Binder &binder, ClientContext &context, optional_ptr<TableCatalogEntry> table,
                         optional_ptr<CreateIndexInfo> info)
    : ExpressionBinder(binder, context), table(table), info(info) {
}

unique_ptr<BoundIndex> IndexBinder::BindIndex(const UnboundIndex &unbound_index) {
	auto &index_type_name = unbound_index.GetIndexType();
	// Do we know the type of this index now?
	auto index_type = context.db->config.GetIndexTypes().FindByName(index_type_name);
	if (!index_type) {
		throw MissingExtensionException("Cannot bind index '%s', unknown index type '%s'. You need to load the "
		                                "extension that provides this index type before table '%s' can be modified.",
		                                unbound_index.GetTableName(), index_type_name, unbound_index.GetTableName());
	}

	auto &create_info = unbound_index.GetCreateInfo();
	auto &storage_info = unbound_index.GetStorageInfo();
	auto &parsed_expressions = unbound_index.GetParsedExpressions();

	// bind the parsed expressions to create unbound expressions
	vector<unique_ptr<Expression>> unbound_expressions;
	unbound_expressions.reserve(parsed_expressions.size());
	for (auto &expr : parsed_expressions) {
		auto copy = expr->Copy();
		unbound_expressions.push_back(Bind(copy));
	}

	CreateIndexInput input(unbound_index.table_io_manager, unbound_index.db, create_info.constraint_type,
	                       create_info.index_name, create_info.column_ids, unbound_expressions, storage_info,
	                       create_info.options);

	return index_type->create_instance(input);
}

BindResult IndexBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult(BinderException::Unsupported(expr, "window functions are not allowed in index expressions"));
	case ExpressionClass::SUBQUERY:
		return BindResult(BinderException::Unsupported(expr, "cannot use subquery in index expressions"));
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string IndexBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in index expressions";
}

} // namespace duckdb
