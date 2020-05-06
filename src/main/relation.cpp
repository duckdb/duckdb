#include "duckdb/main/relation.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/distinct_relation.hpp"
#include "duckdb/main/relation/explain_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/insert_relation.hpp"
#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/setop_relation.hpp"
#include "duckdb/main/relation/subquery_relation.hpp"
#include "duckdb/main/relation/create_table_relation.hpp"
#include "duckdb/main/relation/create_view_relation.hpp"
#include "duckdb/main/relation/write_csv_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/relation/value_relation.hpp"

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

shared_ptr<Relation> Relation::Project(vector<string> expressions) {
	vector<string> aliases;
	return Project(move(expressions), aliases);
}

static vector<unique_ptr<ParsedExpression>> StringListToExpressionList(vector<string> expressions) {
	if (expressions.size() == 0) {
		throw ParserException("Zero expressions provided");
	}
	vector<unique_ptr<ParsedExpression>> result_list;
	for (auto &expr : expressions) {
		auto expression_list = Parser::ParseExpressionList(expr);
		if (expression_list.size() != 1) {
			throw ParserException("Expected a single expression in the expression list");
		}
		result_list.push_back(move(expression_list[0]));
	}
	return result_list;
}

shared_ptr<Relation> Relation::Project(vector<string> expressions, vector<string> aliases) {
	auto result_list = StringListToExpressionList(move(expressions));
	return make_shared<ProjectionRelation>(shared_from_this(), move(result_list), move(aliases));
}

shared_ptr<Relation> Relation::Filter(string expression) {
	auto expression_list = Parser::ParseExpressionList(expression);
	if (expression_list.size() != 1) {
		throw ParserException("Expected a single expression as filter condition");
	}
	return make_shared<FilterRelation>(shared_from_this(), move(expression_list[0]));
}

shared_ptr<Relation> Relation::Filter(vector<string> expressions) {
	// if there are multiple expressions, we AND them together
	auto expression_list = StringListToExpressionList(expressions);
	if (expression_list.size() == 0) {
		throw ParserException("Zero filter conditions provided");
	}
	auto expr = move(expression_list[0]);
	for (idx_t i = 1; i < expression_list.size(); i++) {
		expr =
		    make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(expr), move(expression_list[i]));
	}
	return make_shared<FilterRelation>(shared_from_this(), move(expr));
}

shared_ptr<Relation> Relation::Limit(int64_t limit, int64_t offset) {
	return make_shared<LimitRelation>(shared_from_this(), limit, offset);
}

shared_ptr<Relation> Relation::Order(string expression) {
	auto order_list = Parser::ParseOrderList(expression);
	return make_shared<OrderRelation>(shared_from_this(), move(order_list));
}

shared_ptr<Relation> Relation::Order(vector<string> expressions) {
	if (expressions.size() == 0) {
		throw ParserException("Zero ORDER BY expressions provided");
	}
	vector<OrderByNode> order_list;
	for (auto &expression : expressions) {
		auto inner_list = Parser::ParseOrderList(expression);
		if (inner_list.size() != 1) {
			throw ParserException("Expected a single ORDER BY expression in the expression list");
		}
		order_list.push_back(move(inner_list[0]));
	}
	return make_shared<OrderRelation>(shared_from_this(), move(order_list));
}

shared_ptr<Relation> Relation::Join(shared_ptr<Relation> other, string condition, JoinType type) {
	auto expression_list = Parser::ParseExpressionList(condition);
	if (expression_list.size() == 0) {
		throw ParserException("Expected a single expression as join condition");
	}
	if (expression_list.size() > 1 || expression_list[0]->type == ExpressionType::COLUMN_REF) {
		// multiple columns or single column ref: the condition is a USING list
		vector<string> using_columns;
		for (auto &expr : expression_list) {
			if (expr->type != ExpressionType::COLUMN_REF) {
				throw ParserException("Expected a single expression as join condition");
			}
			auto &colref = (ColumnRefExpression &)*expr;
			if (!colref.table_name.empty()) {
				throw ParserException("Expected empty table name for column in USING clause");
			}
			using_columns.push_back(colref.column_name);
		}
		return make_shared<JoinRelation>(shared_from_this(), other, move(using_columns), type);
	} else {
		// single expression that is not a column reference: use the expression as a join condition
		return make_shared<JoinRelation>(shared_from_this(), other, move(expression_list[0]), type);
	}
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

shared_ptr<Relation> Relation::Alias(string alias) {
	return make_shared<SubqueryRelation>(shared_from_this(), alias);
}

shared_ptr<Relation> Relation::Aggregate(string aggregate_list) {
	auto expression_list = Parser::ParseExpressionList(aggregate_list);
	return make_shared<AggregateRelation>(shared_from_this(), move(expression_list));
}

shared_ptr<Relation> Relation::Aggregate(string aggregate_list, string group_list) {
	auto expression_list = Parser::ParseExpressionList(aggregate_list);
	auto groups = Parser::ParseExpressionList(group_list);
	return make_shared<AggregateRelation>(shared_from_this(), move(expression_list), move(groups));
}

shared_ptr<Relation> Relation::Aggregate(vector<string> aggregates) {
	auto aggregate_list = StringListToExpressionList(move(aggregates));
	return make_shared<AggregateRelation>(shared_from_this(), move(aggregate_list));
}

shared_ptr<Relation> Relation::Aggregate(vector<string> aggregates, vector<string> groups) {
	auto aggregate_list = StringListToExpressionList(move(aggregates));
	auto group_list = StringListToExpressionList(move(groups));
	return make_shared<AggregateRelation>(shared_from_this(), move(aggregate_list), move(group_list));
}

string Relation::GetAlias() {
	return "relation";
}

unique_ptr<TableRef> Relation::GetTableRef() {
	return make_unique<SubqueryRef>(GetQueryNode(), GetAlias());
}

unique_ptr<QueryResult> Relation::Execute() {
	return context.Execute(shared_from_this());
}

BoundStatement Relation::Bind(Binder &binder) {
	SelectStatement stmt;
	stmt.node = GetQueryNode();
	return binder.Bind((SQLStatement &)stmt);
}

void Relation::Insert(string table_name) {
	Insert(DEFAULT_SCHEMA, table_name);
}

void Relation::Insert(string schema_name, string table_name) {
	auto insert = make_shared<InsertRelation>(shared_from_this(), schema_name, table_name);
	insert->Execute();
}

void Relation::Insert(vector<vector<Value>> values){
	vector<string> column_names;
	auto rel = make_shared<ValueRelation>(context, move(values), move(column_names), "values");
	rel->Insert(GetAlias());
}

void Relation::Create(string table_name) {
	Create(DEFAULT_SCHEMA, table_name);
}

void Relation::Create(string schema_name, string table_name) {
	auto create = make_shared<CreateTableRelation>(shared_from_this(), schema_name, table_name);
	create->Execute();
}

void Relation::WriteCSV(string csv_file) {
	auto write_csv = make_shared<WriteCSVRelation>(shared_from_this(), csv_file);
	write_csv->Execute();
}

void Relation::Head(idx_t limit) {
	auto limit_node = Limit(limit);
	limit_node->Execute()->Print();
}

shared_ptr<Relation> Relation::CreateView(string name, bool replace) {
	auto view = make_shared<CreateViewRelation>(shared_from_this(), name, replace);
	view->Execute();
	return shared_from_this();
}

unique_ptr<QueryResult> Relation::Query(string sql) {
	return context.Query(sql, false);
}

unique_ptr<QueryResult> Relation::Query(string name, string sql) {
	CreateView(name);
	return Query(sql);
}

unique_ptr<QueryResult> Relation::Explain() {
	auto explain = make_shared<ExplainRelation>(shared_from_this());
	return explain->Execute();
}

void Relation::Update(string update, string condition) {
	throw Exception("UPDATE can only be used on base tables!");
}

void Relation::Delete(string condition) {
	throw Exception("DELETE can only be used on base tables!");
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
	for (idx_t i = 0; i < cols.size(); i++) {
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

} // namespace duckdb
