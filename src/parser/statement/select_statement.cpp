
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
	// from clause
	serializer.Write<bool>(from_table ? true : false);
	if (from_table) {
		from_table->Serialize(serializer);
	}
	// where_clause
	serializer.Write<bool>(where_clause ? true : false);
	if (where_clause) {
		where_clause->Serialize(serializer);
	}
	// select_distinct
	serializer.Write<bool>(select_distinct);
	// group by
	serializer.Write<uint32_t>(groupby.groups.size());
	for (auto &group : groupby.groups) {
		group->Serialize(serializer);
	}
	// having
	serializer.Write<bool>(groupby.having ? true : false);
	if (groupby.having) {
		groupby.having->Serialize(serializer);
	}
	// order by
	serializer.Write<uint32_t>(orderby.orders.size());
	for (auto &order : orderby.orders) {
		serializer.Write<OrderType>(order.type);
		order.expression->Serialize(serializer);
	}
	// limit
	serializer.Write<int64_t>(limit.limit);
	serializer.Write<int64_t>(limit.offset);
	// union, except
	serializer.Write<bool>(union_select ? true : false);
	if (union_select) {
		union_select->Serialize(serializer);
	}
	serializer.Write<bool>(except_select ? true : false);
	if (except_select) {
		except_select->Serialize(serializer);
	}
}

unique_ptr<SelectStatement> SelectStatement::Deserialize(Deserializer &source) {
	auto statement = make_unique<SelectStatement>();
	bool failed = false;

	// select_list
	auto select_count = source.Read<uint32_t>(failed);
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
	// from clause
	auto has_from_clause = source.Read<bool>(failed);
	if (failed) {
		return nullptr;
	}
	if (has_from_clause) {
		statement->from_table = TableRef::Deserialize(source);
	}

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
	// select_distinct
	statement->select_distinct = source.Read<bool>(failed);
	// group by
	auto group_count = source.Read<uint32_t>(failed);
	if (failed) {
		return nullptr;
	}
	for (size_t i = 0; i < group_count; i++) {
		auto child = Expression::Deserialize(source);
		if (!child) {
			return nullptr;
		}
		statement->groupby.groups.push_back(move(child));
	}
	// having
	auto has_having_clause = source.Read<bool>(failed);
	if (failed) {
		return nullptr;
	}
	if (has_having_clause) {
		statement->groupby.having = Expression::Deserialize(source);
		if (!statement->groupby.having) {
			return nullptr;
		}
	}
	// order by
	auto order_count = source.Read<uint32_t>(failed);
	if (failed) {
		return nullptr;
	}
	for (size_t i = 0; i < order_count; i++) {
		auto order_type = source.Read<OrderType>(failed);
		auto expression = Expression::Deserialize(source);
		if (failed || !expression) {
			return nullptr;
		}
		statement->orderby.orders.push_back(
		    OrderByNode(order_type, move(expression)));
	}

	// limit
	statement->limit.limit = source.Read<int64_t>(failed);
	statement->limit.offset = source.Read<int64_t>(failed);
	if (failed) {
		return nullptr;
	}
	// union, except
	auto has_union_select = source.Read<bool>(failed);
	if (failed) {
		return nullptr;
	}
	if (has_union_select) {
		statement->union_select = SelectStatement::Deserialize(source);
		if (!statement->union_select) {
			return nullptr;
		}
	}
	auto has_except_select = source.Read<bool>(failed);
	if (failed) {
		return nullptr;
	}
	if (has_except_select) {
		statement->except_select = SelectStatement::Deserialize(source);
		if (!statement->except_select) {
			return nullptr;
		}
	}

	return statement;
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
