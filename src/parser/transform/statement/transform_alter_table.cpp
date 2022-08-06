#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

unique_ptr<AlterStatement> Transformer::TransformAlter(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGAlterTableStmt *>(node);
	D_ASSERT(stmt);
	D_ASSERT(stmt->relation);

	if (stmt->cmds->length != 1) {
		throw ParserException("Only one ALTER command per statement is supported");
	}

	auto result = make_unique<AlterStatement>();
	auto qname = TransformQualifiedName(stmt->relation);

	// first we check the type of ALTER
	for (auto c = stmt->cmds->head; c != nullptr; c = c->next) {
		auto command = reinterpret_cast<duckdb_libpgquery::PGAlterTableCmd *>(lfirst(c));
		// TODO: Include more options for command->subtype
		switch (command->subtype) {
		case duckdb_libpgquery::PG_AT_AddColumn: {
			auto cdef = (duckdb_libpgquery::PGColumnDef *)command->def;

			if (stmt->relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
				throw ParserException("Adding columns is only supported for tables");
			}
			if (cdef->category == duckdb_libpgquery::COL_GENERATED) {
				throw ParserException("Adding generated columns after table creation is not supported yet");
			}
			auto centry = TransformColumnDefinition(cdef);

			if (cdef->constraints) {
				for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
					auto constraint = TransformConstraint(constr, centry, 0);
					if (!constraint) {
						continue;
					}
					throw ParserException("Adding columns with constraints not yet supported");
				}
			}
			result->info = make_unique<AddColumnInfo>(qname.schema, qname.name, move(centry));
			break;
		}
		case duckdb_libpgquery::PG_AT_DropColumn: {
			bool cascade = command->behavior == duckdb_libpgquery::PG_DROP_CASCADE;

			if (stmt->relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
				throw ParserException("Dropping columns is only supported for tables");
			}
			result->info =
			    make_unique<RemoveColumnInfo>(qname.schema, qname.name, command->name, command->missing_ok, cascade);
			break;
		}
		case duckdb_libpgquery::PG_AT_ColumnDefault: {
			auto expr = TransformExpression(command->def);

			if (stmt->relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
				throw ParserException("Alter column's default is only supported for tables");
			}
			result->info = make_unique<SetDefaultInfo>(qname.schema, qname.name, command->name, move(expr));
			break;
		}
		case duckdb_libpgquery::PG_AT_AlterColumnType: {
			auto cdef = (duckdb_libpgquery::PGColumnDef *)command->def;
			auto column_definition = TransformColumnDefinition(cdef);
			unique_ptr<ParsedExpression> expr;

			if (stmt->relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
				throw ParserException("Alter column's type is only supported for tables");
			}
			if (cdef->raw_default) {
				expr = TransformExpression(cdef->raw_default);
			} else {
				auto colref = make_unique<ColumnRefExpression>(command->name);
				expr = make_unique<CastExpression>(column_definition.Type(), move(colref));
			}
			result->info = make_unique<ChangeColumnTypeInfo>(qname.schema, qname.name, command->name,
			                                                 column_definition.Type(), move(expr));
			break;
		}
		case duckdb_libpgquery::PG_AT_DropConstraint:
		case duckdb_libpgquery::PG_AT_DropNotNull:
		default:
			throw NotImplementedException("ALTER TABLE option not supported yet!");
		}
	}
	result->info->if_exists = stmt->missing_ok;

	return result;
}

} // namespace duckdb
