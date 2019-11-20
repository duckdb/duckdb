#include "duckdb/common/exception.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

void Transformer::TransformCTE(PGWithClause *de_with_clause, SelectStatement &select) {
	// TODO: might need to update in case of future lawsuit
	assert(de_with_clause);

	if (de_with_clause->recursive) {
		throw NotImplementedException("Recursive CTEs not supported");
	}
	assert(de_with_clause->ctes);
	for (auto cte_ele = de_with_clause->ctes->head; cte_ele != NULL; cte_ele = cte_ele->next) {
		auto cte = reinterpret_cast<PGCommonTableExpr *>(cte_ele->data.ptr_value);
		// lets throw some errors on unsupported features early
		if (cte->cterecursive) {
			throw NotImplementedException("Recursive CTEs not supported");
		}
		if (cte->aliascolnames) {
			throw NotImplementedException("Column name aliases not supported in CTEs");
		}
		if (cte->ctecolnames) {
			throw NotImplementedException("Column name setting not supported in CTEs");
		}
		if (cte->ctecoltypes) {
			throw NotImplementedException("Column type setting not supported in CTEs");
		}
		if (cte->ctecoltypmods) {
			throw NotImplementedException("Column type modification not supported in CTEs");
		}
		if (cte->ctecolcollations) {
			throw NotImplementedException("CTE collations not supported");
		}
		// we need a query
		if (!cte->ctequery || cte->ctequery->type != T_PGSelectStmt) {
			throw Exception("A CTE needs a SELECT");
		}

		auto cte_select = TransformSelectNode((PGSelectStmt *)cte->ctequery);
		if (!cte_select) {
			throw Exception("A CTE needs a SELECT");
		}
		auto cte_name = string(cte->ctename);

		auto it = select.cte_map.find(cte_name);
		if (it != select.cte_map.end()) {
			// can't have two CTEs with same name
			throw Exception("A CTE needs an unique name");
		}
		select.cte_map[cte_name] = move(cte_select);
	}
}
