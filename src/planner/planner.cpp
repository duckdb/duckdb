
#include "parser/statement/select_statement.hpp"

#include "planner/binder.hpp"
#include "planner/planner.hpp"

#include "planner/logicalplangenerator.hpp"

using namespace duckdb;
using namespace std;

void Planner::CreatePlan(Catalog &catalog, SelectStatement &statement) {
	// first bind the tables and columns to the catalog
	Binder binder(catalog);
	binder.Visit(statement);

	// now create a logical query plan from the query
	LogicalPlanGenerator logical_planner(catalog);
	logical_planner.Visit(statement);
	logical_planner.Print();

	this->plan = move(logical_planner.root);
}

bool Planner::CreatePlan(Catalog &catalog, unique_ptr<SQLStatement> statement) {
	this->success = false;
	try {
		switch (statement->GetType()) {
		case StatementType::SELECT:
			CreatePlan(catalog,
			           *reinterpret_cast<SelectStatement *>(statement.get()));
			this->success = true;
			break;
		default:
			this->message = StringUtil::Format(
			    "Statement of type %d not implemented!", statement->GetType());
		}
	} catch (Exception ex) {
		this->message = ex.GetMessage();
	} catch (...) {
		this->message = "UNHANDLED EXCEPTION TYPE THROWN IN PLANNER!";
	}
	return this->success;
}
