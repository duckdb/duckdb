#include <string>
#include <utility>

#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_set.hpp"
#include "duckdb/planner/operator/logical_reset.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/common/enums/set_type.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

BoundStatement Binder::Bind(SetVariableStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	// evaluate the scalar value
	Value value;
	unique_ptr<LogicalOperator> op;
	if (stmt.scope != SetScope::VARIABLE) {
		ConstantBinder default_binder(*this, context, "SET value");
		auto bound_value = default_binder.Bind(stmt.value);
		if (bound_value->HasParameter()) {
			throw NotImplementedException("SET statements cannot have parameters");
		}
		value = ExpressionExecutor::EvaluateScalar(context, *bound_value, true);
	} else {
		auto select_node = make_uniq<SelectNode>();
		select_node->select_list.push_back(std::move(stmt.value));
		select_node->from_table = make_uniq<EmptyTableRef>();
		auto bound_select = Bind(*select_node);
		if (bound_select.types.size() > 1) {
			throw BinderException("SET variable expected a single input");
		}
		op = std::move(bound_select.plan);
	}
	result.plan = make_uniq<LogicalSet>(stmt.name, std::move(value), stmt.scope);
	if (op) {
		result.plan->children.push_back(std::move(op));
	}
	auto &properties = GetStatementProperties();
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

BoundStatement Binder::Bind(ResetVariableStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	result.plan = make_uniq<LogicalReset>(stmt.name, stmt.scope);

	auto &properties = GetStatementProperties();
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

BoundStatement Binder::Bind(SetStatement &stmt) {
	switch (stmt.set_type) {
	case SetType::SET: {
		auto &set_stmt = stmt.Cast<SetVariableStatement>();
		return Bind(set_stmt);
	}
	case SetType::RESET: {
		auto &set_stmt = stmt.Cast<ResetVariableStatement>();
		return Bind(set_stmt);
	}
	default:
		throw NotImplementedException("Type not implemented for SetType");
	}
}

} // namespace duckdb
