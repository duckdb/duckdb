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
	auto qualified_name = TransformQualifiedName(*stmt.relation);

	// Check the ALTER type.
	for (auto c = stmt.cmds->head; c != nullptr; c = c->next) {

		auto command = PGPointerCast<duckdb_libpgquery::PGAlterTableCmd>(c->data.ptr_value);
		AlterEntryData data(qualified_name.catalog, qualified_name.schema, qualified_name.name,
		                    TransformOnEntryNotFound(stmt.missing_ok));

		switch (command->subtype) {
		case duckdb_libpgquery::PG_AT_AddColumn: {
			auto column_def = PGPointerCast<duckdb_libpgquery::PGColumnDef>(command->def);
			if (stmt.relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
				throw ParserException("Adding columns is only supported for tables");
			}
			if (column_def->category == duckdb_libpgquery::COL_GENERATED) {
				throw ParserException("Adding generated columns after table creation is not supported yet");
			}

			auto column_entry = TransformColumnDefinition(*column_def);
			if (column_def->constraints) {
				for (auto cell = column_def->constraints->head; cell != nullptr; cell = cell->next) {
					auto pg_constraint = PGPointerCast<duckdb_libpgquery::PGConstraint>(cell->data.ptr_value);
					auto constraint = TransformConstraint(*pg_constraint, column_entry, 0);
					if (!constraint) {
						continue;
					}
					throw ParserException("Adding columns with constraints not yet supported");
				}
			}
			result->info = make_uniq<AddColumnInfo>(std::move(data), std::move(column_entry), command->missing_ok);
			break;
		}
		case duckdb_libpgquery::PG_AT_DropColumn: {
			auto cascade = command->behavior == duckdb_libpgquery::PG_DROP_CASCADE;
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
			auto column_def = PGPointerCast<duckdb_libpgquery::PGColumnDef>(command->def);
			auto column_entry = TransformColumnDefinition(*column_def);

			unique_ptr<ParsedExpression> expr;
			if (stmt.relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
				throw ParserException("Alter column's type is only supported for tables");
			}

			if (column_entry.GetType() == LogicalType::UNKNOWN && !column_def->raw_default) {
				throw ParserException("Omitting the type is only possible in combination with USING");
			}

			if (column_def->raw_default) {
				expr = TransformExpression(column_def->raw_default);
			} else {
				auto col_ref = make_uniq<ColumnRefExpression>(command->name);
				expr = make_uniq<CastExpression>(column_entry.Type(), std::move(col_ref));
			}
			result->info =
			    make_uniq<ChangeColumnTypeInfo>(std::move(data), command->name, column_entry.Type(), std::move(expr));
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
		case duckdb_libpgquery::PG_AT_AddConstraint: {
			auto pg_constraint = PGCast<duckdb_libpgquery::PGConstraint>(*command->def);
			if (pg_constraint.contype != duckdb_libpgquery::PGConstrType::PG_CONSTR_PRIMARY) {
				throw NotImplementedException("No support for that ALTER TABLE option yet!");
			}

			auto constraint = TransformConstraint(pg_constraint);
			result->info = make_uniq<AddConstraintInfo>(std::move(data), std::move(constraint));
			break;
		}
		case duckdb_libpgquery::PG_AT_SetPartitionedBy: {
			vector<unique_ptr<ParsedExpression>> partition_keys;
			TransformExpressionList(*command->def_list, partition_keys);
			result->info = make_uniq<SetPartitionedByInfo>(std::move(data), std::move(partition_keys));
			break;
		}
		default:
			throw NotImplementedException("No support for that ALTER TABLE option yet!");
		}
	}
	return result;
}

} // namespace duckdb
