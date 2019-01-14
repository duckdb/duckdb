#include "parser/statement/drop_index_statement.hpp"
#include "parser/statement/drop_schema_statement.hpp"
#include "parser/statement/drop_table_statement.hpp"
#include "parser/statement/drop_view_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<SQLStatement> Transformer::TransformDrop(Node *node) {
	DropStmt *stmt = reinterpret_cast<DropStmt *>(node);
	assert(stmt);
	if (stmt->objects->length != 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}

	switch (stmt->removeType) {
	case OBJECT_TABLE:
		return TransformDropTable(stmt);
	case OBJECT_SCHEMA:
		return TransformDropSchema(stmt);
	case OBJECT_INDEX:
		return TransformDropIndex(stmt);
	case OBJECT_VIEW:
		return TransformDropView(stmt);
	default:
		throw NotImplementedException("Cannot drop this type yet");
	}
}
