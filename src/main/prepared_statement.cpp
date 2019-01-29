#include "main/prepared_statement.hpp"

#include "execution/executor.hpp"
#include "execution/physical_plan_generator.hpp"
#include "main/connection.hpp"
#include "parser/expression/parameter_expression.hpp"

using namespace duckdb;
using namespace std;

class FindParameters : public LogicalOperatorVisitor {
public:
	FindParameters(DuckDBPreparedStatement &stmt) : stmt(stmt) {
	}

protected:
	using SQLNodeVisitor::Visit;
	void Visit(ParameterExpression &expr) override {
		throw Exception("eek");
	}

private:
	DuckDBPreparedStatement &stmt;
};

// TODO find the exprs that are params and store maps
DuckDBPreparedStatement::DuckDBPreparedStatement(unique_ptr<LogicalOperator> pplan) : plan(move(pplan)) {
	FindParameters v(*this);
	v.VisitOperator(*plan);
}

void DuckDBPreparedStatement::Bind(size_t param_idx, Value val) {
}

unique_ptr<DuckDBResult> DuckDBPreparedStatement::Execute(DuckDBConnection &conn) {
	auto result = make_unique<DuckDBResult>();
	result->names = plan->GetNames();

	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(conn.context);
	physical_planner.CreatePlan(move(plan));

	// finally execute the plan and return the result
	Executor executor;
	result->collection = executor.Execute(conn.context, move(physical_planner.plan));
	result->success = true;

	return result;
}
