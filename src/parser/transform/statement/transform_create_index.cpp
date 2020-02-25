#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/statement/create_index_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

static IndexType StringToIndexType(const string &str) {
	string upper_str = StringUtil::Upper(str);
	if (upper_str == "INVALID") {
		return IndexType::INVALID;
	} else if (upper_str == "ART") {
		return IndexType::ART;
	} else {
		throw ConversionException(StringUtil::Format("No IndexType conversion from string '%s'", upper_str.c_str()));
	}
	return IndexType::INVALID;
}

unique_ptr<CreateIndexStatement> Transformer::TransformCreateIndex(PGNode *node) {
	auto stmt = reinterpret_cast<PGIndexStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateIndexStatement>();
	auto &info = *result->info.get();

	info.unique = stmt->unique;
	info.if_not_exists = stmt->if_not_exists;

	for (auto cell = stmt->indexParams->head; cell != nullptr; cell = cell->next) {
		auto index_element = (PGIndexElem *)cell->data.ptr_value;
		if (index_element->collation) {
			throw NotImplementedException("Index with collation not supported yet!");
		}
		if (index_element->opclass) {
			throw NotImplementedException("Index with opclass not supported yet!");
		}

		if (index_element->name) {
			// create a column reference expression
			result->expressions.push_back(
			    make_unique<ColumnRefExpression>(index_element->name, stmt->relation->relname));
		} else {
			// parse the index expression
			assert(index_element->expr);
			result->expressions.push_back(TransformExpression(index_element->expr));
		}
	}

	info.idx_type = StringToIndexType(string(stmt->accessMethod));
	auto tableref = make_unique<BaseTableRef>();
	tableref->table_name = stmt->relation->relname;
	if (stmt->relation->schemaname) {
		tableref->schema_name = stmt->relation->schemaname;
	}
	result->table = move(tableref);
	if (stmt->idxname) {
		info.index_name = stmt->idxname;
	} else {
		throw NotImplementedException("Index wout a name not supported yet!");
	}
	return result;
}
