#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

OnEntryNotFound Transformer::TransformOnEntryNotFound(bool missing_ok) {
	return missing_ok ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
}

unique_ptr<AlterStatement> Transformer::TransformAlter(duckdb_libpgquery::PGAlterTableStmt &stmt) {
	D_ASSERT(stmt.relation);

	if (stmt.cmds->length != 1) {
		throw ParserException("Only one ALTER command per statement is supported");
	}

	auto result = make_uniq<AlterStatement>();
	auto qname = TransformQualifiedName(*stmt.relation);

	// first we check the type of ALTER
	for (auto c = stmt.cmds->head; c != nullptr; c = c->next) {
		auto command = reinterpret_cast<duckdb_libpgquery::PGAlterTableCmd *>(lfirst(c));
		AlterEntryData data(qname.catalog, qname.schema, qname.name, TransformOnEntryNotFound(stmt.missing_ok));
		// TODO: Include more options for command->subtype
		switch (command->subtype) {
		case duckdb_libpgquery::PG_AT_AddColumn: {
			auto cdef = PGPointerCast<duckdb_libpgquery::PGColumnDef>(command->def);

			if (stmt.relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
				throw ParserException("Adding columns is only supported for tables");
			}
			if (cdef->category == duckdb_libpgquery::COL_GENERATED) {
				throw ParserException("Adding generated columns after table creation is not supported yet");
			}
			auto centry = TransformColumnDefinition(*cdef);

			if (cdef->constraints) {
				for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
					auto constraint = TransformConstraint(constr, centry, 0);
					if (!constraint) {
						continue;
					}
					throw ParserException("Adding columns with constraints not yet supported");
				}
			}
			result->info = make_uniq<AddColumnInfo>(std::move(data), std::move(centry), command->missing_ok);
			break;
		}
		case duckdb_libpgquery::PG_AT_DropColumn: {
			bool cascade = command->behavior == duckdb_libpgquery::PG_DROP_CASCADE;

			if (stmt.relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
				throw ParserException("Dropping columns is only supported for tables");
			}
			result->info = make_uniq<RemoveColumnInfo>(std::move(data), command->name, command->missing_ok, cascade);
			break;
		}
		case duckdb_libpgquery::PG_AT_ColumnDefault: {
			auto expr = TransformExpression(command->def);

			if (stmt.relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
				throw ParserException("Alter column's default is only supported for tables");
			}
			result->info = make_uniq<SetDefaultInfo>(std::move(data), command->name, std::move(expr));
			break;
		}
		case duckdb_libpgquery::PG_AT_AlterColumnType: {
			auto cdef = PGPointerCast<duckdb_libpgquery::PGColumnDef>(command->def);
			auto column_definition = TransformColumnDefinition(*cdef);
			unique_ptr<ParsedExpression> expr;

			if (stmt.relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
				throw ParserException("Alter column's type is only supported for tables");
			}
			if (cdef->raw_default) {
				expr = TransformExpression(cdef->raw_default);
			} else {
				auto colref = make_uniq<ColumnRefExpression>(command->name);
				expr = make_uniq<CastExpression>(column_definition.Type(), std::move(colref));
			}
			result->info = make_uniq<ChangeColumnTypeInfo>(std::move(data), command->name, column_definition.Type(),
			                                               std::move(expr));
			break;
		}
		case duckdb_libpgquery::PG_AT_SetNotNull: {
			result->info = make_uniq<SetNotNullInfo>(std::move(data), command->name);
			break;
		}
		case duckdb_libpgquery::PG_AT_DropNotNull: {
			result->info = make_uniq<DropNotNullInfo>(std::move(data), command->name);
			break;
		}
		case duckdb_libpgquery::PG_AT_DropConstraint:
		default:
			throw NotImplementedException("ALTER TABLE option not supported yet!");
		}
	}

	return result;
}

} // namespace duckdb
