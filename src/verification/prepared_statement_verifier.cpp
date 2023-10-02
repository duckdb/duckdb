#include "duckdb/verification/prepared_statement_verifier.hpp"

#include "duckdb/common/preserved_error.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"

namespace duckdb {

PreparedStatementVerifier::PreparedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::PREPARED, "Prepared", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> PreparedStatementVerifier::Create(const SQLStatement &statement) {
	return make_uniq<PreparedStatementVerifier>(statement.Copy());
}

void PreparedStatementVerifier::Extract() {
	auto &select = *statement;
	// replace all the constants from the select statement and replace them with parameter expressions
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    *select.node, [&](unique_ptr<ParsedExpression> &child) { ConvertConstants(child); });
	statement->n_param = values.size();
	for (auto &kv : values) {
		statement->named_param_map[kv.first] = 0;
	}
	// create the PREPARE and EXECUTE statements
	string name = "__duckdb_verification_prepared_statement";
	auto prepare = make_uniq<PrepareStatement>();
	prepare->name = name;
	prepare->statement = std::move(statement);

	auto execute = make_uniq<ExecuteStatement>();
	execute->name = name;
	execute->named_values = std::move(values);

	auto dealloc = make_uniq<DropStatement>();
	dealloc->info->type = CatalogType::PREPARED_STATEMENT;
	dealloc->info->name = string(name);

	prepare_statement = std::move(prepare);
	execute_statement = std::move(execute);
	dealloc_statement = std::move(dealloc);
}

void PreparedStatementVerifier::ConvertConstants(unique_ptr<ParsedExpression> &child) {
	if (child->type == ExpressionType::VALUE_CONSTANT) {
		// constant: extract the constant value
		auto alias = child->alias;
		child->alias = string();
		// check if the value already exists
		idx_t index = values.size();
		auto identifier = std::to_string(index + 1);
		const auto predicate = [&](const std::pair<const string, unique_ptr<ParsedExpression>> &pair) {
			return pair.second->Equals(*child.get());
		};
		auto result = std::find_if(values.begin(), values.end(), predicate);
		if (result == values.end()) {
			// If it doesn't exist yet, add it
			values[identifier] = std::move(child);
		} else {
			identifier = result->first;
		}

		// replace it with an expression
		auto parameter = make_uniq<ParameterExpression>();
		parameter->identifier = identifier;
		parameter->alias = alias;
		child = std::move(parameter);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(*child,
	                                            [&](unique_ptr<ParsedExpression> &child) { ConvertConstants(child); });
}

bool PreparedStatementVerifier::Run(
    ClientContext &context, const string &query,
    const std::function<unique_ptr<QueryResult>(const string &, unique_ptr<SQLStatement>)> &run) {
	bool failed = false;
	// verify that we can extract all constants from the query and run the query as a prepared statement
	// create the PREPARE and EXECUTE statements
	Extract();
	// execute the prepared statements
	try {
		auto prepare_result = run(string(), std::move(prepare_statement));
		if (prepare_result->HasError()) {
			prepare_result->ThrowError("Failed prepare during verify: ");
		}
		auto execute_result = run(string(), std::move(execute_statement));
		if (execute_result->HasError()) {
			execute_result->ThrowError("Failed execute during verify: ");
		}
		materialized_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(execute_result));
	} catch (const Exception &ex) {
		if (ex.type != ExceptionType::PARAMETER_NOT_ALLOWED) {
			materialized_result = make_uniq<MaterializedQueryResult>(PreservedError(ex));
		}
		failed = true;
	} catch (std::exception &ex) {
		materialized_result = make_uniq<MaterializedQueryResult>(PreservedError(ex));
		failed = true;
	}
	run(string(), std::move(dealloc_statement));
	context.interrupted = false;

	return failed;
}

} // namespace duckdb
