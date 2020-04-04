#include "duckdb/main/relation.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/relation/distinct_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/setop_relation.hpp"
#include "duckdb/main/relation/create_view_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

shared_ptr<Relation> Relation::Project(string select_list) {
	return Project(select_list, vector<string>());
}

shared_ptr<Relation> Relation::Project(string expression, string alias) {
	return Project(expression, vector<string>({alias}));
}

shared_ptr<Relation> Relation::Project(string select_list, vector<string> aliases) {
	auto expressions = Parser::ParseExpressionList(select_list);
	return make_shared<ProjectionRelation>(shared_from_this(), move(expressions), move(aliases));
}

shared_ptr<Relation> Relation::Project(vector<string> expressions, vector<string> aliases) {
	vector<unique_ptr<ParsedExpression>> result_list;
	for(auto &expr : expressions) {
		auto expression_list = Parser::ParseExpressionList(expr);
		if (expression_list.size() != 1) {
			throw ParserException("Expected a single expression in the expression list");
		}
		result_list.push_back(move(expression_list[0]));
	}
	return make_shared<ProjectionRelation>(shared_from_this(), move(result_list), move(aliases));
}

shared_ptr<Relation> Relation::Filter(string expression) {
	// if there are multiple expressions, we AND them together
	auto expression_list = Parser::ParseExpressionList(expression);
	if (expression_list.size() != 1) {
		throw ParserException("Expected a single expression as filter condition");
	}
	return make_shared<FilterRelation>(shared_from_this(), move(expression_list[0]));
}

shared_ptr<Relation> Relation::Limit(int64_t limit, int64_t offset) {
	return make_shared<LimitRelation>(shared_from_this(), limit, offset);
}

shared_ptr<Relation> Relation::Order(string expression) {
	auto order_list = Parser::ParseOrderList(expression);
	return make_shared<OrderRelation>(shared_from_this(), move(order_list));
}

shared_ptr<Relation> Relation::Union(shared_ptr<Relation> other) {
	return make_shared<SetOpRelation>(shared_from_this(), move(other), SetOperationType::UNION);
}

shared_ptr<Relation> Relation::Except(shared_ptr<Relation> other) {
	return make_shared<SetOpRelation>(shared_from_this(), move(other), SetOperationType::EXCEPT);
}

shared_ptr<Relation> Relation::Intersect(shared_ptr<Relation> other) {
	return make_shared<SetOpRelation>(shared_from_this(), move(other), SetOperationType::INTERSECT);
}

shared_ptr<Relation> Relation::Distinct() {
	return make_shared<DistinctRelation>(shared_from_this());
}

string Relation::GetAlias() {
	return "relation";
}

unique_ptr<QueryResult> Relation::Execute() {
	return context.Execute(shared_from_this());
}

BoundStatement Relation::Bind(Binder &binder) {
	SelectStatement stmt;
	stmt.node = GetQueryNode();
	return binder.Bind((SQLStatement&)stmt);
}

void Relation::Head(idx_t limit) {
	auto limit_node = Limit(limit);
	limit_node->Execute()->Print();
}

void Relation::CreateView(string name, bool replace) {
	auto view = make_shared<CreateViewRelation>(shared_from_this(), name, replace);
	view->Execute();
}

unique_ptr<QueryResult> Relation::SQL(string name, string sql) {
	CreateView(name);
	return context.Query(sql, false);
}

string Relation::ToString() {
	string str;
	str += "---------------------\n";
	str += "-- Expression Tree --\n";
	str += "---------------------\n";
	str += ToString(0);
	str += "\n\n";
	str += "---------------------\n";
	str += "-- Result Columns  --\n";
	str += "---------------------\n";
	auto &cols = Columns();
	for(idx_t i = 0; i < cols.size(); i++) {
		str += "- " + cols[i].name + " (" + SQLTypeToString(cols[i].type) + ")\n";
	}
	return str;
}

void Relation::Print() {
	Printer::Print(ToString());
}

string Relation::RenderWhitespace(idx_t depth) {
	return string(depth * 2, ' ');
}

}