#include "duckdb_python/pystatement.hpp"

namespace duckdb {

enum class ExpectedResultType : uint8_t { QUERY_RESULT, NOTHING, CHANGED_ROWS, UNKNOWN };

static void InitializeReadOnlyProperties(py::class_<DuckDBPyStatement, unique_ptr<DuckDBPyStatement>> &m) {
	m.def_property_readonly("type", &DuckDBPyStatement::Type, "Get the type of the statement.")
	    .def_property_readonly("query", &DuckDBPyStatement::Query, "Get the query equivalent to this statement.")
	    .def_property_readonly("named_parameters", &DuckDBPyStatement::NamedParameters,
	                           "Get the map of named parameters this statement has.")
	    .def_property_readonly("expected_result_type", &DuckDBPyStatement::ExpectedResultType,
	                           "Get the expected type of result produced by this statement, actual type may vary "
	                           "depending on the statement.");
}

void DuckDBPyStatement::Initialize(py::handle &m) {
	auto relation_module =
	    py::class_<DuckDBPyStatement, unique_ptr<DuckDBPyStatement>>(m, "Statement", py::module_local());
	InitializeReadOnlyProperties(relation_module);
}

DuckDBPyStatement::DuckDBPyStatement(unique_ptr<SQLStatement> statement) : statement(std::move(statement)) {
}

unique_ptr<SQLStatement> DuckDBPyStatement::GetStatement() {
	return statement->Copy();
}

string DuckDBPyStatement::Query() const {
	auto &loc = statement->stmt_location;
	auto &length = statement->stmt_length;
	return statement->query.substr(loc, length);
}

py::set DuckDBPyStatement::NamedParameters() const {
	py::set result;
	auto &named_parameters = statement->named_param_map;
	for (auto &param : named_parameters) {
		result.add(param.first);
	}
	return result;
}

py::list DuckDBPyStatement::ExpectedResultType() const {
	py::list possibilities;
	switch (statement->type) {
	case StatementType::PREPARE_STATEMENT:
	case StatementType::VACUUM_STATEMENT:
	case StatementType::ALTER_STATEMENT:
	case StatementType::TRANSACTION_STATEMENT:
	case StatementType::SET_STATEMENT:
	case StatementType::LOAD_STATEMENT:
	case StatementType::EXPORT_STATEMENT:
	case StatementType::DROP_STATEMENT:
	case StatementType::DETACH_STATEMENT:
	case StatementType::CREATE_FUNC_STATEMENT:
	case StatementType::COPY_DATABASE_STATEMENT:
	case StatementType::ATTACH_STATEMENT: {
		possibilities.append(StatementReturnType::NOTHING);
		break;
	}
	case StatementType::PRAGMA_STATEMENT:
	case StatementType::EXPLAIN_STATEMENT:
	case StatementType::LOGICAL_PLAN_STATEMENT:
	case StatementType::CALL_STATEMENT:
	case StatementType::SELECT_STATEMENT:
	case StatementType::EXECUTE_STATEMENT: {
		possibilities.append(StatementReturnType::QUERY_RESULT);
		break;
	}
	case StatementType::COPY_STATEMENT: {
		possibilities.append(StatementReturnType::CHANGED_ROWS);
		possibilities.append(StatementReturnType::QUERY_RESULT);
		break;
	}
	case StatementType::DELETE_STATEMENT:
	case StatementType::UPDATE_STATEMENT:
	case StatementType::INSERT_STATEMENT: {
		possibilities.append(StatementReturnType::CHANGED_ROWS);
		possibilities.append(StatementReturnType::QUERY_RESULT);
		break;
	}
	case StatementType::ANALYZE_STATEMENT:
	case StatementType::VARIABLE_SET_STATEMENT:
	case StatementType::RELATION_STATEMENT:
	case StatementType::EXTENSION_STATEMENT:
	case StatementType::CREATE_STATEMENT:
	case StatementType::MULTI_STATEMENT: {
		possibilities.append(StatementReturnType::CHANGED_ROWS);
		possibilities.append(StatementReturnType::QUERY_RESULT);
		possibilities.append(StatementReturnType::NOTHING);
		break;
	}
	default: {
		throw InternalException("Unrecognized StatementType in ExpectedResultType: %s",
		                        StatementTypeToString(statement->type));
	}
	}
	return possibilities;
}

StatementType DuckDBPyStatement::Type() const {
	return statement->type;
}

} // namespace duckdb
