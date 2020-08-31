#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<TableRef> Transformer::TransformRangeVar(PGRangeVar *root) {
	auto result = make_unique<BaseTableRef>();

	result->alias = TransformAlias(root->alias);
	if (root->relname) {
		result->table_name = root->relname;
	}
	if (root->schemaname) {
		result->schema_name = root->schemaname;
	}
	return move(result);
}

QualifiedName Transformer::TransformQualifiedName(duckdb_libpgquery::PGRangeVar *root) {
	QualifiedName qname;
	if (root->relname) {
		qname.name = root->relname;
	} else {
		qname.name = string();
	}
	if (root->schemaname) {
		qname.schema = root->schemaname;
	} else {
		qname.schema = INVALID_SCHEMA;
	}
	return qname;
}

} // namespace duckdb
