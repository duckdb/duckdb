#include "duckdb/verification/statement_verifier.hpp"

#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/verification/copied_statement_verifier.hpp"
#include "duckdb/verification/deserialized_statement_verifier.hpp"
#include "duckdb/verification/external_statement_verifier.hpp"
#include "duckdb/verification/parsed_statement_verifier.hpp"
#include "duckdb/verification/prepared_statement_verifier.hpp"
#include "duckdb/verification/unoptimized_statement_verifier.hpp"
#include "duckdb/verification/no_operator_caching_verifier.hpp"
#include "duckdb/verification/fetch_row_verifier.hpp"
#include "duckdb/verification/explain_statement_verifier.hpp"

namespace duckdb {

const vector<unique_ptr<ParsedExpression>> &StatementVerifier::GetSelectList(QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		return node.Cast<SelectNode>().select_list;
	case QueryNodeType::SET_OPERATION_NODE:
		return GetSelectList(*node.Cast<SetOperationNode>().children[0]);
	default:
		return empty_select_list;
	}
}

StatementVerifier::StatementVerifier(VerificationType type, string name, unique_ptr<SQLStatement> statement_p,
                                     optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters_p)
    : type(type), name(std::move(name)), statement(std::move(statement_p)),
      select_statement(statement->type == StatementType::SELECT_STATEMENT ? &statement->Cast<SelectStatement>()
                                                                          : nullptr),
      parameters(parameters_p),
      select_list(select_statement ? GetSelectList(*select_statement->node) : empty_select_list) {
}

StatementVerifier::StatementVerifier(unique_ptr<SQLStatement> statement_p,
                                     optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters)
    : StatementVerifier(VerificationType::ORIGINAL, "Original", std::move(statement_p), parameters) {
}

StatementVerifier::~StatementVerifier() noexcept {
}

unique_ptr<StatementVerifier>
StatementVerifier::Create(VerificationType type, const SQLStatement &statement_p,
                          optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters) {
	switch (type) {
	case VerificationType::COPIED:
		return CopiedStatementVerifier::Create(statement_p, parameters);
	case VerificationType::DESERIALIZED:
		return DeserializedStatementVerifier::Create(statement_p, parameters);
	case VerificationType::PARSED:
		return ParsedStatementVerifier::Create(statement_p, parameters);
	case VerificationType::UNOPTIMIZED:
		return UnoptimizedStatementVerifier::Create(statement_p, parameters);
	case VerificationType::NO_OPERATOR_CACHING:
		return NoOperatorCachingVerifier::Create(statement_p, parameters);
	case VerificationType::PREPARED:
		return PreparedStatementVerifier::Create(statement_p, parameters);
	case VerificationType::EXTERNAL:
		return ExternalStatementVerifier::Create(statement_p, parameters);
	case VerificationType::EXPLAIN:
		return ExplainStatementVerifier::Create(statement_p, parameters);
	case VerificationType::FETCH_ROW_AS_SCAN:
		return FetchRowVerifier::Create(statement_p, parameters);
	case VerificationType::INVALID:
	default:
		throw InternalException("Invalid statement verification type!");
	}
}

void StatementVerifier::CheckExpressions(const StatementVerifier &other) const {
	// Only the original statement should check other statements
	D_ASSERT(type == VerificationType::ORIGINAL);

	// Check equality
	if (other.RequireEquality() && select_statement) {
		D_ASSERT(select_statement->Equals(*other.select_statement));
	}

#ifdef DEBUG
	// Now perform checking on the expressions
	D_ASSERT(select_list.size() == other.select_list.size());
	const auto expr_count = select_list.size();
	if (other.RequireEquality()) {
		for (idx_t i = 0; i < expr_count; i++) {
			// Run the ToString, to verify that it doesn't crash
			select_list[i]->ToString();

			if (select_list[i]->HasSubquery()) {
				continue;
			}

			// Check that the expressions are equivalent
			D_ASSERT(select_list[i]->Equals(*other.select_list[i]));
			// Check that the hashes are equivalent too
			D_ASSERT(select_list[i]->Hash() == other.select_list[i]->Hash());

			other.select_list[i]->Verify();
		}
	}
#endif
}

void StatementVerifier::CheckExpressions() const {
#ifdef DEBUG
	D_ASSERT(type == VerificationType::ORIGINAL);
	// Perform additional checking within the expressions
	const auto expr_count = select_list.size();
	for (idx_t outer_idx = 0; outer_idx < expr_count; outer_idx++) {
		auto hash = select_list[outer_idx]->Hash();
		for (idx_t inner_idx = 0; inner_idx < expr_count; inner_idx++) {
			auto hash2 = select_list[inner_idx]->Hash();
			if (hash != hash2) {
				// if the hashes are not equivalent, the expressions should not be equivalent
				D_ASSERT(!select_list[outer_idx]->Equals(*select_list[inner_idx]));
			}
		}
	}
#endif
}

bool StatementVerifier::Run(
    ClientContext &context, const string &query,
    const std::function<unique_ptr<QueryResult>(const string &, unique_ptr<SQLStatement>,
                                                optional_ptr<case_insensitive_map_t<BoundParameterData>>)> &run) {
	bool failed = false;

	context.interrupted = false;
	context.config.enable_optimizer = !DisableOptimizer();
	context.config.enable_caching_operators = !DisableOperatorCaching();
	context.config.force_external = ForceExternal();
	context.config.force_fetch_row = ForceFetchRow();
	try {
		auto result = run(query, std::move(statement), parameters);
		if (result->HasError()) {
			failed = true;
		}
		materialized_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
	} catch (std::exception &ex) {
		failed = true;
		materialized_result = make_uniq<MaterializedQueryResult>(ErrorData(ex));
	}
	context.interrupted = false;

	return failed;
}

string StatementVerifier::CompareResults(const StatementVerifier &other) {
	D_ASSERT(type == VerificationType::ORIGINAL);
	string error;
	if (materialized_result->HasError() != other.materialized_result->HasError()) { // LCOV_EXCL_START
		string result = other.name + " statement differs from original result!\n";
		result += "Original Result:\n" + materialized_result->ToString();
		result += other.name + ":\n" + other.materialized_result->ToString();
		return result;
	} // LCOV_EXCL_STOP
	if (materialized_result->HasError()) {
		return "";
	}
	if (!ColumnDataCollection::ResultEquals(materialized_result->Collection(), other.materialized_result->Collection(),
	                                        error)) { // LCOV_EXCL_START
		string result = other.name + " statement differs from original result!\n";
		result += "Original Result:\n" + materialized_result->ToString();
		result += other.name + ":\n" + other.materialized_result->ToString();
		result += "\n\n---------------------------------\n" + error;
		return result;
	} // LCOV_EXCL_STOP

	return "";
}

} // namespace duckdb
