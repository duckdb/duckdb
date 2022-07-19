#include "duckdb/verification/prepared_statement_verifier.hpp"

#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"

namespace duckdb {

PreparedStatementVerifier::PreparedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::PREPARED, "Prepared", move(statement_p)) {
}

unique_ptr<StatementVerifier> PreparedStatementVerifier::Create(const SQLStatement &statement) {
	return make_unique<PreparedStatementVerifier>(statement.Copy());
}

void PreparedStatementVerifier::Extract() {
	auto &select = *statement;
	// replace all the constants from the select statement and replace them with parameter expressions
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    *select.node, [&](unique_ptr<ParsedExpression> &child) { ConvertConstants(child); });
	statement->n_param = values.size();
	// create the PREPARE and EXECUTE statements
	string name = "__duckdb_verification_prepared_statement";
	auto prepare = make_unique<PrepareStatement>();
	prepare->name = name;
	prepare->statement = move(statement);

	auto execute = make_unique<ExecuteStatement>();
	execute->name = name;
	execute->values = move(values);

	auto dealloc = make_unique<DropStatement>();
	dealloc->info->type = CatalogType::PREPARED_STATEMENT;
	dealloc->info->name = string(name);

	prepare_statement = move(prepare);
	execute_statement = move(execute);
	dealloc_statement = move(dealloc);
}

void PreparedStatementVerifier::ConvertConstants(unique_ptr<ParsedExpression> &child) {
	if (child->type == ExpressionType::VALUE_CONSTANT) {
		// constant: extract the constant value
		auto alias = child->alias;
		child->alias = string();
		// check if the value already exists
		idx_t index = values.size();
		for (idx_t v_idx = 0; v_idx < values.size(); v_idx++) {
			if (values[v_idx]->Equals(child.get())) {
				// duplicate value! refer to the original value
				index = v_idx;
				break;
			}
		}
		if (index == values.size()) {
			values.push_back(move(child));
		}
		// replace it with an expression
		auto parameter = make_unique<ParameterExpression>();
		parameter->parameter_nr = index + 1;
		parameter->alias = alias;
		child = move(parameter);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(*child,
	                                            [&](unique_ptr<ParsedExpression> &child) { ConvertConstants(child); });
}

bool PreparedStatementVerifier::Run(
    ClientContext &context, const string &query,
    const std::function<unique_ptr<QueryResult>(const string &, unique_ptr<SQLStatement>)> &run) {
	// verify that we can extract all constants from the query and run the query as a prepared statement
	// create the PREPARE and EXECUTE statements
	Extract();
	// execute the prepared statements
	try {
		auto prepare_result = run(string(), move(prepare_statement));
		if (!prepare_result->success) {
			throw std::runtime_error("Failed prepare during verify: " + prepare_result->error);
		}
		auto execute_result = run(string(), move(execute_statement));
		if (!execute_result->success) {
			throw std::runtime_error("Failed execute during verify: " + execute_result->error);
		}
		materialized_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(execute_result));
	} catch (std::exception &ex) {
		if (!StringUtil::Contains(ex.what(), "Parameter Not Allowed Error")) {
			materialized_result = make_unique<MaterializedQueryResult>(ex.what());
		}
	}
	run(string(), move(dealloc_statement));
	context.interrupted = false;

	return true;
}

} // namespace duckdb
