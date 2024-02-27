#include "duckdb/parser/statement/materialized_cte_statement.hpp"

namespace duckdb {

MaterializedCTEStatement::MaterializedCTEStatement(const MaterializedCTEStatement &other)
    : SQLStatement(other), ctename(other.ctename), query(other.query->Copy()), child(other.child->Copy()),
      cte_map(other.cte_map.Copy()) {
}

string MaterializedCTEStatement::ToString() const {
	string result;
	result = cte_map.ToString();
	result += child->ToString();
	return result;
}

unique_ptr<SQLStatement> MaterializedCTEStatement::Copy() const {
	return unique_ptr<MaterializedCTEStatement>(new MaterializedCTEStatement(*this));
}

} // namespace duckdb
