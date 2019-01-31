#include "main/prepared_statement.hpp"

#include "execution/executor.hpp"
#include "execution/physical_plan_generator.hpp"
#include "main/connection.hpp"
#include "main/database.hpp"
#include "parser/expression/parameter_expression.hpp"

using namespace duckdb;
using namespace std;

class FindParameters : public SQLNodeVisitor {
public:
	FindParameters(DuckDBPreparedStatement &stmt) : stmt(stmt) {
	}

protected:
	using SQLNodeVisitor::Visit;
	void Visit(ParameterExpression &expr) override {
		if (expr.return_type == TypeId::INVALID) {
			throw Exception("Could not determine type for prepared statement parameter. Conder using a CAST on it.");
		}
		auto it = stmt.parameter_expression_map.find(expr.parameter_nr);
		if (it != stmt.parameter_expression_map.end()) {
			throw Exception("Duplicate parameter index. Use $1, $2 etc. to differentiate.");
		}
		stmt.parameter_expression_map[expr.parameter_nr] = &expr;
	}

private:
	DuckDBPreparedStatement &stmt;
};

DuckDBPreparedStatement::DuckDBPreparedStatement(unique_ptr<PhysicalOperator> pplan, vector<string> names)
    : plan(move(pplan)), names(names) {
	FindParameters v(*this);
	plan->Accept(&v);
}

DuckDBPreparedStatement *DuckDBPreparedStatement::Bind(size_t param_idx, Value val) {
	auto it = parameter_expression_map.find(param_idx);
	if (it == parameter_expression_map.end() || it->second == nullptr) {
		throw Exception("Could not find parameter with this index");
	}
	ParameterExpression *expr = it->second;
	if (expr->return_type != val.type) {
		val = val.CastAs(expr->return_type);
	}
	expr->value = val;
	return this;
}

unique_ptr<DuckDBResult> DuckDBPreparedStatement::Execute(DuckDBConnection &conn) {
	auto result = make_unique<DuckDBResult>();

	result->names = names;
	result->success = false;
	conn.context.interrupted = false;

	// finally execute the plan and return the result

	if (conn.context.transaction.IsAutoCommit() && !conn.context.transaction.HasActiveTransaction()) {
		conn.context.transaction.BeginTransaction();
	}

	conn.context.ActiveTransaction().active_query = conn.context.db.transaction_manager.GetQueryNumber();

	try {
		//
		//			if (statement->type == StatementType::UPDATE || statement->type == StatementType::DELETE ||
		//			    statement->type == StatementType::ALTER || statement->type == StatementType::CREATE_INDEX) {
		//				// log query in UNDO buffer so it can be saved in the WAL on commit
		//				auto &transaction = context.transaction.ActiveTransaction();
		//				transaction.PushQuery(query);
		//			}
		//
		//

		// finally execute the plan and return the result
		Executor executor;
		result->collection = executor.Execute(conn.context, plan.get());
		result->success = true;

	} catch (Exception &ex) {
		result->error = ex.GetMessage();
	} catch (...) {
		result->error = "UNHANDLED EXCEPTION TYPE THROWN!";
	}

	// destroy any data held in the query allocator
	conn.context.allocator.Destroy();

	if (conn.context.transaction.HasActiveTransaction()) {
		conn.context.ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
		try {
			if (conn.context.transaction.IsAutoCommit()) {
				if (result->GetSuccess()) {
					conn.context.transaction.Commit();
				} else {
					conn.context.transaction.Rollback();
				}
			}
		} catch (Exception &ex) {
			result->success = false;
			result->error = ex.GetMessage();
		} catch (...) {
			result->success = false;
			result->error = "UNHANDLED EXCEPTION TYPE THROWN IN TRANSACTION COMMIT!";
		}
	}
	return result;

	return result;
}
