#include "duckdb/parser/statement/alter_table_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<AlterTableStatement> Transformer::TransformAlter(PGNode *node) {
	throw NotImplementedException("Alter table not supported yet!");
	// auto stmt = reinterpret_cast<AlterTableStmt *>(node);
	// assert(stmt);
	// assert(stmt->relation);

	// auto result = make_unique<AlterTableStatement>();
	// auto &info = *result->info.get();
	// auto new_alter_cmd = make_unique<AlterTableCmd>();
	// result->table = TransformRangeVar(stmt->relation);

	// info.table = stmt->relation->relname;

	// // first we check the type of ALTER
	// for (auto c = stmt->cmds->head; c != NULL; c = c->next) {
	// 	auto command = reinterpret_cast<PGAlterTableCmd *>(lfirst(c));
	// 	//TODO: Include more options for command->subtype
	// 	switch (command->subtype) {
	// 		case PG_AT_AddColumn: {
	//                auto cdef = (ColumnDef *)command->def;
	//                char *name = (reinterpret_cast<PGValue *>(
	//                        cdef->typeName->names->tail->data.ptr_value)
	//                        ->val.str);
	//                auto centry =
	//                        ColumnDefinition(cdef->colname,
	//                        TransformStringToTypeId(name));
	//                info.new_columns.push_back(centry);
	//                break;
	//            }
	// 		case PG_AT_DropColumn:
	// 		case PG_AT_AlterColumnType:
	// 		default:
	// 			throw NotImplementedException(
	// 			    "ALTER TABLE option not supported yet!");
	// 	}
	// }

	// return result;
}
