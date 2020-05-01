#include "duckdb/parser/statement/alter_table_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

using namespace std;

namespace duckdb {

unique_ptr<AlterTableStatement> Transformer::TransformAlter(PGNode *node) {
	auto stmt = reinterpret_cast<PGAlterTableStmt *>(node);
	assert(stmt);
	assert(stmt->relation);

	auto result = make_unique<AlterTableStatement>();

	auto table = TransformRangeVar(stmt->relation);
	assert(table->type == TableReferenceType::BASE_TABLE);

	auto &basetable = (BaseTableRef&) *table;
	// first we check the type of ALTER
	for (auto c = stmt->cmds->head; c != NULL; c = c->next) {
		auto command = reinterpret_cast<PGAlterTableCmd *>(lfirst(c));
		//TODO: Include more options for command->subtype
		switch (command->subtype) {
		case PG_AT_AddColumn: {
			auto cdef = (PGColumnDef *)command->def;
			auto centry = TransformColumnDefinition(cdef);
			if (cdef->constraints) {
				for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
					auto constraint = TransformConstraint(constr, centry, 0);
					if (constraint) {
						throw ParserException("Adding columns with constraints not yet supported");
					}
				}
			}
			result->info = make_unique<AddColumnInfo>(basetable.schema_name, basetable.table_name, move(centry));
			break;
		}
		case PG_AT_DropColumn:
		case PG_AT_AlterColumnType:
		default:
			throw NotImplementedException(
				"ALTER TABLE option not supported yet!");
		}
	}

	return result;
}

}
