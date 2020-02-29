#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

namespace duckdb {

unique_ptr<BoundCreateInfo> Binder::BindCreateIndexInfo(unique_ptr<CreateInfo> info) {
	auto &base = (CreateIndexInfo&) *info;
	auto result = make_unique<BoundCreateIndexInfo>(move(info));

	// visit the table reference
	result->table = Bind(*base.table);
	if (result->table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Cannot create index on a view!");
	}
	IndexBinder binder(*this, context);
	for (auto &expr : base.expressions) {
		result->expressions.push_back(binder.Bind(expr));
	}
	return move(result);
}

}
