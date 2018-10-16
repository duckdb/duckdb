
#include "parser/statement/select_statement.hpp"

#include "common/assert.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

string SelectStatement::ToString() const { return "Select"; }

unique_ptr<SelectStatement> SelectStatement::Copy() {
	auto statement = make_unique<SelectStatement>();
	for (auto &child : select_list) {
		statement->select_list.push_back(child->Copy());
	}
	statement->from_table = from_table ? from_table->Copy() : nullptr;
	statement->where_clause = where_clause ? where_clause->Copy() : nullptr;
	statement->select_distinct = select_distinct;

	// groups
	for (auto &group : groupby.groups) {
		statement->groupby.groups.push_back(group->Copy());
	}
	statement->groupby.having =
	    groupby.having ? groupby.having->Copy() : nullptr;
	// order
	for (auto &order : orderby.orders) {
		statement->orderby.orders.push_back(
		    OrderByNode(order.type, order.expression->Copy()));
	}
	// limit
	statement->limit.limit = limit.limit;
	statement->limit.offset = limit.offset;

	statement->union_select = union_select ? union_select->Copy() : nullptr;
	statement->except_select = except_select ? except_select->Copy() : nullptr;

	return statement;
}

void SelectStatement::Serialize(Serializer &serializer) {
	// select_list
	serializer.Write<uint32_t>(select_list.size());
	for (auto &child : select_list) {
		child->Serialize(serializer);
	}

	// where_clause
	serializer.Write<bool>(where_clause ? true : false);
	if (where_clause) {
		where_clause->Serialize(serializer);
	}
}

unique_ptr<SelectStatement> SelectStatement::Deserialize(Deserializer &source) {
	auto statement = make_unique<SelectStatement>();
	bool failed = false;

	// select_list
	uint32_t select_count = source.Read<uint32_t>(failed);
	if (failed) {
		return nullptr;
	}
	for (size_t i = 0; i < select_count; i++) {
		auto child = Expression::Deserialize(source);
		if (!child) {
			return nullptr;
		}
		statement->select_list.push_back(move(child));
	}
	// FIXME FROM clause

	// where_clause
	auto has_where_clause = source.Read<bool>(failed);
	if (failed) {
		return nullptr;
	}
	if (has_where_clause) {
		statement->where_clause = Expression::Deserialize(source);
		if (!statement->where_clause) {
			return nullptr;
		}
	}

	// FIXME: the rest

	return nullptr;
}

bool SelectStatement::HasAggregation() {
	if (HasGroup()) {
		return true;
	}
	for (auto &expr : select_list) {
		if (expr->IsAggregate()) {
			return true;
		}
	}
	return false;
}
