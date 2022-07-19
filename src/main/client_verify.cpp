#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct VerifyStatement {
	explicit VerifyStatement(unique_ptr<SelectStatement> statement_p, bool require_equality = true,
	                         bool disable_optimizer = false)
	    : statement(move(statement_p)), require_equality(require_equality), disable_optimizer(disable_optimizer),
	      select_list(statement->node->GetSelectList()) {
	}

	unique_ptr<SelectStatement> statement;
	bool require_equality;
	bool disable_optimizer;
	const vector<unique_ptr<ParsedExpression>> &select_list;
};

struct PreparedStatementVerifier {
public:
	vector<unique_ptr<ParsedExpression>> values;
	unique_ptr<SQLStatement> prepare_statement;
	unique_ptr<SQLStatement> execute_statement;
	unique_ptr<SQLStatement> dealloc_statement;

public:
	void ConvertConstants(unique_ptr<ParsedExpression> &child) {
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
		ParsedExpressionIterator::EnumerateChildren(
		    *child, [&](unique_ptr<ParsedExpression> &child) { ConvertConstants(child); });
	}

	void Extract(unique_ptr<SQLStatement> statement) {
		auto &select = (SelectStatement &)*statement;
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
};

// TODO: stuff in ClientContext::PendingStatementOrPreparedStatementInternal too
string ClientContext::VerifyQuery(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement) {
	D_ASSERT(statement->type == StatementType::SELECT_STATEMENT);
	// aggressive query verification

	// the purpose of this function is to test correctness of otherwise hard to test features:
	// Copy() of statements and expressions
	// Serialize()/Deserialize() of expressions
	// Hash() of expressions
	// Equality() of statements and expressions
	// ToString() of statements and expressions
	// Correctness of plans both with and without optimizers

	vector<VerifyStatement> verify_statements;

	// copy the statement
	auto select_stmt = (SelectStatement *)statement.get();
	auto copied_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(select_stmt->Copy());
	auto unoptimized_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(select_stmt->Copy());
	auto prepared_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(select_stmt->Copy());

	BufferedSerializer serializer;
	select_stmt->Serialize(serializer);
	BufferedDeserializer source(serializer);
	auto deserialized_stmt = SelectStatement::Deserialize(source);

	auto query_str = select_stmt->ToString();
	Parser parser;
	parser.ParseQuery(query_str);
	D_ASSERT(parser.statements.size() == 1);
	D_ASSERT(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	auto parsed_statement = move(parser.statements[0]);

	vector<string> names = {"Original statement", "Copied statement", "Deserialized statement",
	                        "Parsed statement",   "Unoptimized",      "Prepared statement"};
	verify_statements.emplace_back(unique_ptr_cast<SQLStatement, SelectStatement>(move(statement)));
	verify_statements.emplace_back(move(copied_stmt));
	verify_statements.emplace_back(move(deserialized_stmt));
	verify_statements.emplace_back(unique_ptr_cast<SQLStatement, SelectStatement>(move(parsed_statement)), false);
	verify_statements.emplace_back(unique_ptr_cast<SQLStatement, SelectStatement>(move(unoptimized_stmt)), true, true);

	// all the statements should be equal
	for (idx_t i = 1; i < verify_statements.size(); i++) {
		if (!verify_statements[i].require_equality) {
			continue;
		}
		D_ASSERT(verify_statements[i].statement->Equals(verify_statements[0].statement.get()));
	}

	// now perform checking on the expressions
#ifdef DEBUG
	for (idx_t i = 1; i < verify_statements.size(); i++) {
		D_ASSERT(verify_statements[i].select_list.size() == verify_statements[0].select_list.size());
	}
	auto expr_count = verify_statements[0].select_list.size();
	auto &orig_expr_list = verify_statements[0].select_list;
	for (idx_t i = 0; i < expr_count; i++) {
		// run the ToString, to verify that it doesn't crash
		auto str = orig_expr_list[i]->ToString();
		for (idx_t v_idx = 0; v_idx < verify_statements.size(); v_idx++) {
			if (!verify_statements[v_idx].require_equality && orig_expr_list[i]->HasSubquery()) {
				continue;
			}
			// check that the expressions are equivalent
			D_ASSERT(orig_expr_list[i]->Equals(verify_statements[v_idx].select_list[i].get()));
			// check that the hashes are equivalent too
			D_ASSERT(orig_expr_list[i]->Hash() == verify_statements[v_idx].select_list[i]->Hash());

			verify_statements[v_idx].select_list[i]->Verify();
		}
		D_ASSERT(!orig_expr_list[i]->Equals(nullptr));

		if (orig_expr_list[i]->HasSubquery()) {
			continue;
		}
		// ToString round trip
		auto parsed_list = Parser::ParseExpressionList(str);
		D_ASSERT(parsed_list.size() == 1);
		D_ASSERT(parsed_list[0]->Equals(orig_expr_list[i].get()));
	}
	// perform additional checking within the expressions
	for (idx_t outer_idx = 0; outer_idx < orig_expr_list.size(); outer_idx++) {
		auto hash = orig_expr_list[outer_idx]->Hash();
		for (idx_t inner_idx = 0; inner_idx < orig_expr_list.size(); inner_idx++) {
			auto hash2 = orig_expr_list[inner_idx]->Hash();
			if (hash != hash2) {
				// if the hashes are not equivalent, the expressions should not be equivalent
				D_ASSERT(!orig_expr_list[outer_idx]->Equals(orig_expr_list[inner_idx].get()));
			}
		}
	}
#endif

	// disable profiling if it is enabled
	auto &config = ClientConfig::GetConfig(*this);
	bool profiling_is_enabled = config.enable_profiler;
	if (profiling_is_enabled) {
		config.enable_profiler = false;
	}

	// see below
	auto statement_copy_for_explain = select_stmt->Copy();

	// execute the original statement
	bool any_failed = false;
	auto optimizer_enabled = config.enable_optimizer;
	vector<unique_ptr<MaterializedQueryResult>> results;
	for (idx_t i = 0; i < verify_statements.size(); i++) {
		interrupted = false;
		config.enable_optimizer = !verify_statements[i].disable_optimizer;
		try {
			auto result = RunStatementInternal(lock, query, move(verify_statements[i].statement), false, false);
			if (!result->success) {
				any_failed = true;
			}
			results.push_back(unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result)));
		} catch (std::exception &ex) {
			any_failed = true;
			results.push_back(make_unique<MaterializedQueryResult>(ex.what()));
		}
		interrupted = false;
	}
	if (!any_failed) {
		// verify that we can extract all constants from the query and run the query as a prepared statement
		// create the PREPARE and EXECUTE statements
		PreparedStatementVerifier verifier;
		verifier.Extract(move(prepared_stmt));
		// execute the prepared statements
		try {
			auto prepare_result = RunStatementInternal(lock, string(), move(verifier.prepare_statement), false, false);
			if (!prepare_result->success) {
				throw std::runtime_error("Failed prepare during verify: " + prepare_result->error);
			}
			auto execute_result = RunStatementInternal(lock, string(), move(verifier.execute_statement), false, false);
			if (!execute_result->success) {
				throw std::runtime_error("Failed execute during verify: " + execute_result->error);
			}
			results.push_back(unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(execute_result)));
		} catch (std::exception &ex) {
			if (!StringUtil::Contains(ex.what(), "Parameter Not Allowed Error")) {
				results.push_back(make_unique<MaterializedQueryResult>(ex.what()));
			}
		}
		RunStatementInternal(lock, string(), move(verifier.dealloc_statement), false, false);

		interrupted = false;
	}
	config.enable_optimizer = optimizer_enabled;

	// check explain, only if q does not already contain EXPLAIN
	if (results[0]->success) {
		auto explain_q = "EXPLAIN " + query;
		auto explain_stmt = make_unique<ExplainStatement>(move(statement_copy_for_explain));
		try {
			RunStatementInternal(lock, explain_q, move(explain_stmt), false, false);
		} catch (std::exception &ex) { // LCOV_EXCL_START
			interrupted = false;
			return "EXPLAIN failed but query did not (" + string(ex.what()) + ")";
		} // LCOV_EXCL_STOP
	}

	if (profiling_is_enabled) {
		config.enable_profiler = true;
	}

	// now compare the results
	// the results of all runs should be identical
	D_ASSERT(names.size() >= results.size());
	for (idx_t i = 1; i < results.size(); i++) {
		auto name = names[i];
		if (results[0]->success != results[i]->success) { // LCOV_EXCL_START
			string result = name + " differs from original result!\n";
			result += "Original Result:\n" + results[0]->ToString();
			result += name + ":\n" + results[i]->ToString();
			return result;
		}                                                             // LCOV_EXCL_STOP
		if (!results[0]->collection.Equals(results[i]->collection)) { // LCOV_EXCL_START
			string result = name + " differs from original result!\n";
			result += "Original Result:\n" + results[0]->ToString();
			result += name + ":\n" + results[i]->ToString();
			return result;
		} // LCOV_EXCL_STOP
	}

	return "";
}

} // namespace duckdb
