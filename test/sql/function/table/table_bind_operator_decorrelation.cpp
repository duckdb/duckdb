#include "catch.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// This dummy extension operator, which meets the minimum requirements of an operator, is intended to demonstrate
// that using DecorrelationMode does not produce any unexpected errors or behaivor
// The operator passes chunks through unchanged
class PhysicalExtensionOperatorPassthrough : public PhysicalOperator {
public:
	PhysicalExtensionOperatorPassthrough(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
	                                     idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality) {
	}

public:
	string GetName() const override {
		return "physical_operator_extension_passthrough";
	}

	OperatorResultType Execute(ExecutionContext &, DataChunk &input, DataChunk &chunk, GlobalOperatorState &,
	                           OperatorState &) const override {
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	bool ParallelOperator() const override {
		return true;
	}
};

class LogicalExtensionOperatorPassthrough : public LogicalExtensionOperator {
public:
	explicit LogicalExtensionOperatorPassthrough(string name_p, const DecorrelationMode mode_p)
	    : name(std::move(name_p)), mode(mode_p) {
	}

public:
	PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
		D_ASSERT(children.size() == 1);

		auto &child = planner.CreatePlan(*children[0]);
		auto &physical_op = planner.Make<PhysicalExtensionOperatorPassthrough>(types, estimated_cardinality);
		physical_op.children.push_back(child);

		return physical_op;
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

	DecorrelationMode GetDecorrelationMode() const override {
		return mode;
	}

	string GetExtensionName() const override {
		return name;
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}

private:
	string name;
	DecorrelationMode mode;
};

//! Create a projection (below the operator) with the input columns
static unique_ptr<LogicalOperator> BindPassthrough(TableFunctionBindInput &input, TableIndex table_bind_index,
                                                   vector<Identifier> &return_names, const string &name,
                                                   DecorrelationMode mode) {
	return_names = input.input_table_names;

	auto &child = *input.input_plan;
	auto child_bindings = child->GetColumnBindings();

	D_ASSERT(child_bindings.size() == input.input_table_types.size());

	vector<unique_ptr<Expression>> select_list;
	select_list.reserve(child_bindings.size());

	// prepare columns for the projection
	for (idx_t i = 0; i < child_bindings.size(); i++) {
		select_list.push_back(
		    make_uniq<BoundColumnRefExpression>(return_names[i], input.input_table_types[i], child_bindings[i]));
	}

	auto projection = make_uniq<LogicalProjection>(table_bind_index, std::move(select_list));
	projection->children.push_back(std::move(child));

	auto logical_op = make_uniq<LogicalExtensionOperatorPassthrough>(name, mode);
	logical_op->children.push_back(std::move(projection));

	return std::move(logical_op);
}

static unique_ptr<LogicalOperator> BindSingleChild(ClientContext &, TableFunctionBindInput &input,
                                                   TableIndex table_bind_index, vector<Identifier> &return_names) {
	return BindPassthrough(input, table_bind_index, return_names, "logical_single_child_passthrough",
	                       DecorrelationMode::SINGLE_CHILD_PASSTHROUGH);
}

static unique_ptr<LogicalOperator> BindNotSupported(ClientContext &, TableFunctionBindInput &input,
                                                    TableIndex table_bind_index, vector<Identifier> &return_names) {
	return BindPassthrough(input, table_bind_index, return_names, "logical_not_supported_passthrough",
	                       DecorrelationMode::NOT_SUPPORTED);
}

static void Register(Connection &con) {
	// Create two test table functions, one for each DecorrelationMode
	con.BeginTransaction();
	auto &client_context = *con.context;
	auto &catalog = Catalog::GetSystemCatalog(client_context);
	TableFunction single_child("decorrelating_single_child", {LogicalType::TABLE}, nullptr, nullptr);
	single_child.bind_operator = BindSingleChild;
	CreateTableFunctionInfo single_child_info(single_child);
	catalog.CreateTableFunction(*con.context, single_child_info);
	TableFunction not_supported("decorrelating_not_supported", {LogicalType::TABLE}, nullptr, nullptr);
	not_supported.bind_operator = BindNotSupported;
	CreateTableFunctionInfo not_supported_info(not_supported);
	catalog.CreateTableFunction(*con.context, not_supported_info);
	con.Commit();
}

static void CreateTestTable(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 5), (2, 10), (3, 15)"));
}

TEST_CASE("Extension operator without correlation", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	Register(con);
	CreateTestTable(con);

	// since both subqueries are uncorrelated, the dependent join is never reached
	auto result = con.Query("SELECT i, j FROM decorrelating_single_child((FROM integers)) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 10, 15}));

	auto result2 = con.Query("SELECT i, j FROM decorrelating_not_supported((FROM integers)) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result2, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result2, 1, {5, 10, 15}));
}

TEST_CASE("Extension operator that reaches dependent join pushdown", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	Register(con);
	CreateTestTable(con);

	auto result = con.Query("SELECT i, (SELECT sum(j) "
	                        "           FROM decorrelating_single_child((FROM integers AS inner_i "
	                        "                                            WHERE inner_i.i <= outer_i.i))) AS running "
	                        "FROM integers AS outer_i ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 15, 30}));
}

TEST_CASE("Extension operator that doesn't support decorrelation", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	Register(con);
	CreateTestTable(con);

	// should fail
	auto result = con.Query("SELECT i, (SELECT sum(j) "
	                        "           FROM decorrelating_not_supported((FROM integers AS inner_i "
	                        "                                             WHERE inner_i.i <= outer_i.i))) "
	                        "FROM integers AS outer_i");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "logical_not_supported_passthrough"));
	REQUIRE(StringUtil::Contains(result->GetError(), "not (yet) supported"));
}

TEST_CASE("Extension operator in a correlated scalar subquery", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	Register(con);

	auto result = con.Query("SELECT t1.* "
	                        "FROM range(0,20,2) AS t1(a) "
	                        "WHERE t1.a =    (SELECT * FROM decorrelating_single_child((SELECT t2.a "
	                        "                FROM range(0,20,1) AS t2(a) "
	                        "                WHERE t1.a = t2.a))) "
	                        "ORDER BY t1.a");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 2, 4, 6, 8, 10, 12, 14, 16, 18}));
}

TEST_CASE("Extension operator in a correlated recursive CTE", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	Register(con);

	auto result =
	    con.Query("SELECT * FROM range(3) AS _(x), "
	              "              (WITH RECURSIVE tbl(a) AS (SELECT x UNION SELECT a + 1 "
	              "                                         FROM decorrelating_single_child((SELECT * FROM tbl)) "
	              "                                         WHERE a < 10) TABLE tbl ) AS __(y) "
	              "ORDER BY x, y");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2}));
	REQUIRE(CHECK_COLUMN(
	    result, 1, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
}

TEST_CASE("Extension operator in a correlated recursive CTE without decorrelation", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	Register(con);

	// should fail
	auto result =
	    con.Query("SELECT * FROM range(3) AS _(x), "
	              "              (WITH RECURSIVE tbl(a) AS (SELECT x UNION SELECT a + 1 "
	              "                                         FROM decorrelating_not_supported((SELECT * FROM tbl)) "
	              "                                         WHERE a < 10) TABLE tbl ) AS __(y)");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "logical_not_supported_passthrough"));
	REQUIRE(StringUtil::Contains(result->GetError(), "not (yet) supported"));
}
