#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static IndexType StringToIndexType(const string &str) {
	string upper_str = StringUtil::Upper(str);
	if (upper_str == "INVALID") {
		return IndexType::INVALID;
	} else if (upper_str == "ART") {
		return IndexType::ART;
	} else {
		throw ConversionException("No IndexType conversion from string '%s'", upper_str);
	}
	return IndexType::INVALID;
}

unique_ptr<CreateStatement> Transformer::TransformCreateIndex(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGIndexStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateIndexInfo>();
	if (stmt->unique) {
		info->constraint_type = IndexConstraintType::UNIQUE;
	} else {
		info->constraint_type = IndexConstraintType::NONE;
	}

	info->on_conflict = TransformOnConflict(stmt->onconflict);

	for (auto cell = stmt->indexParams->head; cell != nullptr; cell = cell->next) {
		auto index_element = (duckdb_libpgquery::PGIndexElem *)cell->data.ptr_value;
		if (index_element->collation) {
			throw NotImplementedException("Index with collation not supported yet!");
		}
		if (index_element->opclass) {
			throw NotImplementedException("Index with opclass not supported yet!");
		}

		if (index_element->name) {
			// create a column reference expression
			info->expressions.push_back(make_unique<ColumnRefExpression>(index_element->name, stmt->relation->relname));
		} else {
			// parse the index expression
			D_ASSERT(index_element->expr);
			info->expressions.push_back(TransformExpression(index_element->expr));
		}
	}

	info->index_type = StringToIndexType(string(stmt->accessMethod));
	auto tableref = make_unique<BaseTableRef>();
	tableref->table_name = stmt->relation->relname;
	if (stmt->relation->schemaname) {
		tableref->schema_name = stmt->relation->schemaname;
	}
	info->table = move(tableref);
	if (stmt->idxname) {
		info->index_name = stmt->idxname;
	} else {
		throw NotImplementedException("Index wout a name not supported yet!");
	}
	for (auto &expr : info->expressions) {
		info->parsed_expressions.emplace_back(expr->Copy());
	}
	result->info = move(info);
	return result;
}

} // namespace duckdb
