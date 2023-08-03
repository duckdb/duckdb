#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ExplainStatement> Transformer::TransformExplain(duckdb_libpgquery::PGExplainStmt &stmt) {
	auto explain_type = ExplainType::EXPLAIN_STANDARD;
	if (stmt.options) {
		for (auto n = stmt.options->head; n; n = n->next) {
			auto def_elem = PGPointerCast<duckdb_libpgquery::PGDefElem>(n->data.ptr_value)->defname;
			string elem(def_elem);
			if (elem == "analyze") {
				explain_type = ExplainType::EXPLAIN_ANALYZE;
			} else {
				throw NotImplementedException("Unimplemented explain type: %s", elem);
			}
		}
	}
	return make_uniq<ExplainStatement>(TransformStatement(*stmt.query), explain_type);
}

} // namespace duckdb
