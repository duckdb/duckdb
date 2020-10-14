#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<PragmaStatement> Transformer::TransformShow(PGNode *node) {
	// we transform SHOW x into PRAGMA SHOW('x')

	auto stmt = reinterpret_cast<PGVariableShowStmt *>(node);

	auto result = make_unique<PragmaStatement>();
	auto &info = *result->info;

	if (string(stmt->name) == "tables") {
		// show all tables
		info.name = "show_tables";
	} else {
		// show one specific table
		info.name = "show";
		info.parameters.push_back(Value(stmt->name));
	}

	return result;
}

} // namespace duckdb
