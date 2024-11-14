//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/verification/prepared_statement_verifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/verification/statement_verifier.hpp"

namespace duckdb {

class PreparedStatementVerifier : public StatementVerifier {
public:
	explicit PreparedStatementVerifier(unique_ptr<SQLStatement> statement_p,
	                                   optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);
	static unique_ptr<StatementVerifier> Create(const SQLStatement &statement_p,
	                                            optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);

	bool Run(ClientContext &context, const string &query,
	         const std::function<unique_ptr<QueryResult>(const string &, unique_ptr<SQLStatement>,
	                                                     optional_ptr<case_insensitive_map_t<BoundParameterData>>)>
	             &run) override;

private:
	case_insensitive_map_t<unique_ptr<ParsedExpression>> values;
	unique_ptr<SQLStatement> prepare_statement;
	unique_ptr<SQLStatement> execute_statement;
	unique_ptr<SQLStatement> dealloc_statement;

private:
	void Extract();
	void ConvertConstants(unique_ptr<ParsedExpression> &child);
};

} // namespace duckdb
