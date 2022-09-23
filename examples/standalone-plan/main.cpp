#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#endif

using namespace duckdb;

// in this example we build a simple volcano model executor on top of the DuckDB logical plans
// for simplicity, the executor only handles integer values and doesn't handle null values

void ExecuteQuery(Connection &con, const string &query);
void CreateFunction(Connection &con, string name, vector<LogicalType> arguments, LogicalType return_type);
void CreateAggregateFunction(Connection &con, string name, vector<LogicalType> arguments, LogicalType return_type);

//===--------------------------------------------------------------------===//
// Example Using DuckDB Catalog/Tables
//===--------------------------------------------------------------------===//
void RunExampleDuckDBCatalog() {
	// in this example we use the DuckDB CREATE TABLE syntax to create tables to link against
	// this works and the tables are easy to define, but since the tables are empty there are no statistics available
	// we can use our own table functions (see RunExampleTableScan), but this is slightly more involved

	DBConfig config;
	config.initialize_default_database = false;

	// disable the statistics propagator optimizer
	// this is required since the statistics propagator will truncate our plan
	// (e.g. it will recognize the table is empty that satisfy the predicate i=3
	//       and then prune the entire plan)
	config.disabled_optimizers.insert(OptimizerType::STATISTICS_PROPAGATION);
	// we don't support filter pushdown yet in our toy example
	config.disabled_optimizers.insert(OptimizerType::FILTER_PUSHDOWN);

	DuckDB db(nullptr, &config);
	Connection con(db);

	// we perform an explicit BEGIN TRANSACTION here
	// since "CreateFunction" will directly poke around in the catalog
	// which requires an active transaction
	con.Query("BEGIN TRANSACTION");

	// register dummy tables (for our binding purposes)
	con.Query("CREATE TABLE mytable(i INTEGER, j INTEGER)");
	con.Query("CREATE TABLE myothertable(k INTEGER)");
	// contents of the tables
	// mytable:
	// i: 1, 2, 3, 4, 5
	// j: 2, 3, 4, 5, 6
	// myothertable
	// k: 1, 10, 20
	// (see MyScanNode)

	// register functions and aggregates (for our binding purposes)
	CreateFunction(con, "+", {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER);
	CreateAggregateFunction(con, "count_star", {}, LogicalType::BIGINT);
	CreateAggregateFunction(con, "sum", {LogicalType::INTEGER}, LogicalType::INTEGER);

	con.Query("COMMIT");

	// standard projections
	ExecuteQuery(con, "SELECT * FROM mytable");
	ExecuteQuery(con, "SELECT i FROM mytable");
	ExecuteQuery(con, "SELECT j FROM mytable");
	ExecuteQuery(con, "SELECT k FROM myothertable");
	// some simple filter + projection
	ExecuteQuery(con, "SELECT i+1 FROM mytable WHERE i=3 OR i=4");
	// more complex filters
	ExecuteQuery(con, "SELECT i+1 FROM mytable WHERE (i<=2 AND j<=3) OR (i=4 AND j=5)");
	// aggregate
	ExecuteQuery(con, "SELECT COUNT(*), SUM(i) + 1, SUM(j) + 2 FROM mytable WHERE i>2");
	// with a subquery
	ExecuteQuery(con,
	             "SELECT a, b + 1, c + 2 FROM (SELECT COUNT(*), SUM(i), SUM(j) FROM mytable WHERE i > 2) tbl(a, b, c)");
}

//===--------------------------------------------------------------------===//
// Example Using Custom Scan Function
//===--------------------------------------------------------------------===//
void CreateMyScanFunction(Connection &con);

unique_ptr<TableFunctionRef> MyReplacementScan(ClientContext &context, const string &table_name,
                                               ReplacementScanData *data) {
	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value(table_name)));
	table_function->function = make_unique<FunctionExpression>("my_scan", move(children));
	return table_function;
}

void RunExampleTableScan() {
	// in this example we use our own TableFunction instead of the built-in "seq_scan"
	// this allows us to emit our own statistics without needing to insert them into the DuckDB tables
	// it also allows us to define ourselves what we do/do not support
	// (e.g. we can disable projection or filter pushdown in the table function if desired)
	// this means we don't need to disable optimizers anymore

	DBConfig config;
	config.initialize_default_database = false;
	config.replacement_scans.push_back(ReplacementScan(MyReplacementScan));

	DuckDB db(nullptr, &config);
	Connection con(db);

	// we perform an explicit BEGIN TRANSACTION here
	// since "CreateFunction" will directly poke around in the catalog
	// which requires an active transaction
	con.Query("BEGIN TRANSACTION");

	// register functions and aggregates (for our binding purposes)
	CreateFunction(con, "+", {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER);
	CreateAggregateFunction(con, "count_star", {}, LogicalType::BIGINT);
	CreateAggregateFunction(con, "sum", {LogicalType::INTEGER}, LogicalType::INTEGER);

	CreateMyScanFunction(con);

	con.Query("COMMIT");

	// standard projections
	ExecuteQuery(con, "SELECT * FROM mytable");
	ExecuteQuery(con, "SELECT i FROM mytable");
	ExecuteQuery(con, "SELECT j FROM mytable");
	ExecuteQuery(con, "SELECT k FROM myothertable");
	// some simple filter + projection
	ExecuteQuery(con, "SELECT i+1 FROM mytable WHERE i=3 OR i=4");
	// more complex filters
	ExecuteQuery(con, "SELECT i+1 FROM mytable WHERE (i<=2 AND j<=3) OR (i=4 AND j=5)");
	// aggregate
	ExecuteQuery(con, "SELECT COUNT(*), SUM(i) + 1, SUM(j) + 2 FROM mytable WHERE i>2");
	// with a subquery
	ExecuteQuery(con,
	             "SELECT a, b + 1, c + 2 FROM (SELECT COUNT(*), SUM(i), SUM(j) FROM mytable WHERE i > 2) tbl(a, b, c)");
}

int main() {
	RunExampleDuckDBCatalog();
	RunExampleTableScan();
}

//===--------------------------------------------------------------------===//
// Create Dummy Scalar/Aggregate Functions in the Catalog
//===--------------------------------------------------------------------===//
void CreateFunction(Connection &con, string name, vector<LogicalType> arguments, LogicalType return_type) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	// we can register multiple functions here if we want overloads
	// you may also want to set has_side_effects or varargs in the ScalarFunction (if required)
	ScalarFunctionSet set(name);
	set.AddFunction(ScalarFunction(move(arguments), move(return_type), nullptr));

	CreateScalarFunctionInfo info(move(set));
	catalog.CreateFunction(context, &info);
}

void CreateAggregateFunction(Connection &con, string name, vector<LogicalType> arguments, LogicalType return_type) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	// we can register multiple functions here if we want overloads
	AggregateFunctionSet set(name);
	set.AddFunction(AggregateFunction(move(arguments), move(return_type), nullptr, nullptr, nullptr, nullptr, nullptr));

	CreateAggregateFunctionInfo info(move(set));
	catalog.CreateFunction(context, &info);
}

//===--------------------------------------------------------------------===//
// Custom Table Scan Function
//===--------------------------------------------------------------------===//
struct MyBindData : public TableFunctionData {
	MyBindData(string name_p) : table_name(move(name_p)) {
	}

	string table_name;
};

// contents of the tables
// mytable:
// i: 1, 2, 3, 4, 5
// j: 2, 3, 4, 5, 6
// myothertable
// k: 1, 10, 20
// (see MyScanNode)
static unique_ptr<FunctionData> MyScanBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto table_name = input.inputs[0].ToString();
	if (table_name == "mytable") {
		names.emplace_back("i");
		return_types.emplace_back(LogicalType::INTEGER);

		names.emplace_back("j");
		return_types.emplace_back(LogicalType::INTEGER);
	} else if (table_name == "myothertable") {
		names.emplace_back("k");
		return_types.emplace_back(LogicalType::INTEGER);
	} else {
		throw std::runtime_error("Unknown table " + table_name);
	}
	auto result = make_unique<MyBindData>(table_name);
	return move(result);
}

static unique_ptr<BaseStatistics> MyScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                   column_t column_id) {
	auto &bind_data = (MyBindData &)*bind_data_p;
	if (bind_data.table_name == "mytable") {
		if (column_id == 0) {
			// i: 1, 2, 3, 4, 5
			return make_unique<NumericStatistics>(LogicalType::INTEGER, Value::INTEGER(1), Value::INTEGER(5));
		} else if (column_id == 1) {
			// j: 2, 3, 4, 5, 6
			return make_unique<NumericStatistics>(LogicalType::INTEGER, Value::INTEGER(2), Value::INTEGER(6));
		}
	} else if (bind_data.table_name == "myothertable") {
		// k: 1, 10, 20
		return make_unique<NumericStatistics>(LogicalType::INTEGER, Value::INTEGER(1), Value::INTEGER(20));
	}
	return nullptr;
}

unique_ptr<NodeStatistics> MyScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (MyBindData &)*bind_data_p;
	if (bind_data.table_name == "mytable") {
		// 5 tuples
		return make_unique<NodeStatistics>(5, 5);
	} else if (bind_data.table_name == "myothertable") {
		return make_unique<NodeStatistics>(3, 3);
	}
	return nullptr;
}

void CreateMyScanFunction(Connection &con) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	TableFunction my_scan("my_scan", {LogicalType::VARCHAR}, nullptr, MyScanBind, nullptr, MyScanStatistics, nullptr,
	                      nullptr, MyScanCardinality);
	my_scan.projection_pushdown = true;
	my_scan.filter_pushdown = false;

	CreateTableFunctionInfo info(move(my_scan));
	catalog.CreateTableFunction(context, &info);
}

//===--------------------------------------------------------------------===//
// Example Execution Engine: Row-based volcano style that only supports int32
//===--------------------------------------------------------------------===//
class MyNode {
public:
	virtual ~MyNode() {
	}
	virtual vector<int> GetNextRow() = 0;

	unique_ptr<MyNode> child;
};

class MyPlanGenerator {
public:
	unique_ptr<MyNode> TransformPlan(LogicalOperator &op);
};

void ExecuteQuery(Connection &con, const string &query) {
	// create the logical plan
	auto plan = con.ExtractPlan(query);
	plan->Print();

	// transform the logical plan into our own plan
	MyPlanGenerator generator;
	auto my_plan = generator.TransformPlan(*plan);

	// execute the plan and print the result
	printf("Executing query: %s\n", query.c_str());
	printf("----------------------\n");
	vector<int> result;
	while (true) {
		result = my_plan->GetNextRow();
		if (result.empty()) {
			break;
		}
		string str;
		for (size_t i = 0; i < result.size(); i++) {
			if (i > 0) {
				str += ", ";
			}
			str += std::to_string(result[i]);
		}
		printf("%s\n", str.c_str());
	}
	printf("----------------------\n");
}

//===--------------------------------------------------------------------===//
// Table Scan Node
//===--------------------------------------------------------------------===//
class MyScanNode : public MyNode {
public:
	MyScanNode(string name_p, vector<column_t> column_ids_p) : name(move(name_p)), column_ids(move(column_ids_p)) {
		// fill up the data based on which table we are scanning
		if (name == "mytable") {
			// i
			data.push_back({1, 2, 3, 4, 5});
			// j
			data.push_back({2, 3, 4, 5, 6});
		} else if (name == "myothertable") {
			// k
			data.push_back({1, 10, 20});
		} else {
			throw std::runtime_error("Unsupported table!");
		}
	}

	string name;
	vector<column_t> column_ids;
	vector<vector<int>> data;
	int index = 0;

	vector<int> GetNextRow() override {
		vector<int> result;
		if (index >= data[0].size()) {
			return result;
		}
		// fill the result based on the projection list (column_ids)
		for (size_t i = 0; i < column_ids.size(); i++) {
			result.push_back(data[column_ids[i]][index]);
		}
		index++;
		return result;
	};
};

//===--------------------------------------------------------------------===//
// Expression Execution
//===--------------------------------------------------------------------===//

// note that we run expression execution directly on top of DuckDB expressions
// it is also possible to transform the expressions into our own expressions (MyExpression)
class MyExpressionExecutor {
public:
	MyExpressionExecutor(vector<int> current_row_p) : current_row(move(current_row_p)) {
	}

	vector<int> current_row;

	int Execute(Expression &expression);

protected:
	int Execute(BoundReferenceExpression &expr);
	int Execute(BoundCastExpression &expr);
	int Execute(BoundComparisonExpression &expr);
	int Execute(BoundConjunctionExpression &expr);
	int Execute(BoundConstantExpression &expr);
	int Execute(BoundFunctionExpression &expr);
};

//===--------------------------------------------------------------------===//
// Filter
//===--------------------------------------------------------------------===//
class MyFilterNode : public MyNode {
public:
	MyFilterNode(unique_ptr<Expression> filter_node) : filter(move(filter_node)) {
	}

	unique_ptr<Expression> filter;

	bool ExecuteFilter(Expression &expr, const vector<int> &current_row) {
		MyExpressionExecutor executor(current_row);
		auto val = executor.Execute(expr);
		return val != 0;
	}

	vector<int> GetNextRow() override {
		D_ASSERT(child);
		while (true) {
			auto next = child->GetNextRow();
			if (next.empty()) {
				return next;
			}
			// check if the filter passes, if it does we return the row
			// if not we return the next row
			if (ExecuteFilter(*filter, next)) {
				return next;
			}
		}
	};
};

//===--------------------------------------------------------------------===//
// Projection
//===--------------------------------------------------------------------===//
class MyProjectionNode : public MyNode {
public:
	MyProjectionNode(vector<unique_ptr<Expression>> projections_p) : projections(move(projections_p)) {
	}

	vector<unique_ptr<Expression>> projections;

	vector<int> GetNextRow() override {
		auto next = child->GetNextRow();
		if (next.empty()) {
			return next;
		}
		MyExpressionExecutor executor(next);
		vector<int> result;
		for (size_t i = 0; i < projections.size(); i++) {
			result.push_back(executor.Execute(*projections[i]));
		}
		return result;
	};
};

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
class MyAggregateNode : public MyNode {
public:
	MyAggregateNode(vector<unique_ptr<Expression>> aggregates_p) : aggregates(move(aggregates_p)) {
		// initialize aggregate states to 0
		aggregate_states.resize(aggregates.size(), 0);
	}

	vector<unique_ptr<Expression>> aggregates;
	vector<int> aggregate_states;

	void ExecuteAggregate(MyExpressionExecutor &executor, int index, BoundAggregateExpression &expr) {
		if (expr.function.name == "sum") {
			int child = executor.Execute(*expr.children[0]);
			aggregate_states[index] += child;
		} else if (expr.function.name == "count_star") {
			aggregate_states[index]++;
		} else {
			throw std::runtime_error("Unsupported aggregate function " + expr.function.name);
		}
	}

	vector<int> GetNextRow() override {
		if (aggregate_states.empty()) {
			// finished aggregating
			return aggregate_states;
		}
		while (true) {
			auto next = child->GetNextRow();
			if (next.empty()) {
				return move(aggregate_states);
			}
			MyExpressionExecutor executor(next);
			for (size_t i = 0; i < aggregates.size(); i++) {
				ExecuteAggregate(executor, i, (BoundAggregateExpression &)*aggregates[i]);
			}
		}
	};
};

//===--------------------------------------------------------------------===//
// Plan Transformer - Transform a DuckDB logical plan into a custom plan (MyNode)
//===--------------------------------------------------------------------===//
unique_ptr<MyNode> MyPlanGenerator::TransformPlan(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// projection
		auto child = TransformPlan(*op.children[0]);
		auto node = make_unique<MyProjectionNode>(move(op.expressions));
		node->child = move(child);
		return move(node);
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		// filter
		auto child = TransformPlan(*op.children[0]);
		auto node = make_unique<MyFilterNode>(move(op.expressions[0]));
		node->child = move(child);
		return move(node);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = (LogicalAggregate &)op;
		if (!aggr.groups.empty()) {
			throw std::runtime_error("Grouped aggregate not supported");
		}
		auto child = TransformPlan(*op.children[0]);
		auto node = make_unique<MyAggregateNode>(move(op.expressions));
		node->child = move(child);
		return move(node);
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = (LogicalGet &)op;
		// table scan or table function

		// get nodes have two properties: table_filters (filter pushdown) and column_ids (projection pushdown)
		// table_filters are only generated if optimizers are enabled (through the filter pushdown optimizer)
		// column_ids are always generated
		// the column_ids specify which columns should be emitted and in which order
		// e.g. if we have a table "i, j, k" and the column_ids are {0, 2} we should emit ONLY "i, k" and in that order
		if (get.function.name == "seq_scan") {
			// built-in table scan
			auto &table = (TableScanBindData &)*get.bind_data;
			if (!get.table_filters.filters.empty()) {
				// note: filter pushdown will only be triggered if optimizers are enabled
				throw std::runtime_error("Filter pushdown unsupported");
			}
			return make_unique<MyScanNode>(table.table->name, get.column_ids);
		} else if (get.function.name == "my_scan") {
			// our own scan
			auto &my_bind_data = (MyBindData &)*get.bind_data;
			return make_unique<MyScanNode>(my_bind_data.table_name, get.column_ids);
		} else {
			throw std::runtime_error("Unsupported table function");
		}
	}
	default:
		throw std::runtime_error("Unsupported logical operator for transformation");
	}
}

//===--------------------------------------------------------------------===//
// Expression Execution for various built-in expressions
//===--------------------------------------------------------------------===//
int MyExpressionExecutor::Execute(BoundReferenceExpression &expr) {
	// column references (e.g. "SELECT a FROM tbl") are turned into BoundReferences
	// these refer to an index within the row they come from
	// because of that it is important to correctly handle the get.column_ids
	return current_row[expr.index];
}

int MyExpressionExecutor::Execute(BoundCastExpression &expr) {
	return Execute(*expr.child);
}

int MyExpressionExecutor::Execute(BoundConjunctionExpression &expr) {
	int result;
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		result = 1;
		for (size_t i = 0; i < expr.children.size(); i++) {
			result = result && Execute(*expr.children[i]);
		}
	} else if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		result = 0;
		for (size_t i = 0; i < expr.children.size(); i++) {
			result = result || Execute(*expr.children[i]);
		}
	} else {
		throw std::runtime_error("Unrecognized conjunction (this shouldn't be possible)");
	}
	return result;
}

int MyExpressionExecutor::Execute(BoundConstantExpression &expr) {
	return expr.value.GetValue<int32_t>();
}

int MyExpressionExecutor::Execute(BoundComparisonExpression &expr) {
	auto lchild = Execute(*expr.left);
	auto rchild = Execute(*expr.right);
	bool cmp;
	switch (expr.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
		cmp = lchild == rchild;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		cmp = lchild != rchild;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		cmp = lchild < rchild;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		cmp = lchild > rchild;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		cmp = lchild <= rchild;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		cmp = lchild >= rchild;
		break;
	default:
		throw std::runtime_error("Unsupported comparison");
	}
	return cmp ? 1 : 0;
}

//===--------------------------------------------------------------------===//
// Expression Execution for built-in functions
//===--------------------------------------------------------------------===//
int MyExpressionExecutor::Execute(BoundFunctionExpression &expr) {
	if (expr.function.name == "+") {
		auto lchild = Execute(*expr.children[0]);
		auto rchild = Execute(*expr.children[1]);
		return lchild + rchild;
	}
	throw std::runtime_error("Unsupported function " + expr.function.name);
}

int MyExpressionExecutor::Execute(Expression &expression) {
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
		return Execute((BoundReferenceExpression &)expression);
	case ExpressionClass::BOUND_CAST:
		return Execute((BoundCastExpression &)expression);
	case ExpressionClass::BOUND_COMPARISON:
		return Execute((BoundComparisonExpression &)expression);
	case ExpressionClass::BOUND_CONJUNCTION:
		return Execute((BoundConjunctionExpression &)expression);
	case ExpressionClass::BOUND_CONSTANT:
		return Execute((BoundConstantExpression &)expression);
	case ExpressionClass::BOUND_FUNCTION:
		return Execute((BoundFunctionExpression &)expression);
	default:
		throw std::runtime_error("Unsupported expression for expression executor " + expression.ToString());
	}
}
