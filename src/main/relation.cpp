#include "duckdb/main/relation.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/relation/projection_relation.hpp"

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
	throw NotImplementedException("FIXME:");
}

shared_ptr<Relation> Relation::Limit(idx_t n, idx_t offset) {
	throw NotImplementedException("FIXME:");
}

shared_ptr<Relation> Relation::Order(string expression) {
	throw NotImplementedException("FIXME:");
}

shared_ptr<Relation> Relation::Union(shared_ptr<Relation> other) {
	throw NotImplementedException("FIXME:");
}

unique_ptr<QueryResult> Relation::Execute() {
	return context.Execute(shared_from_this());
}

void Relation::Head(idx_t limit) {
	auto limit_node = Limit(limit);
	limit_node->Execute()->Print();
}

void Relation::CreateView(string name) {
	throw NotImplementedException("FIXME:");
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