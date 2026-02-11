#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/multi_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/set_statement.hpp"

namespace duckdb {

OnEntryNotFound Transformer::TransformOnEntryNotFound(bool missing_ok) {
	return missing_ok ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
}

vector<string> Transformer::TransformNameList(duckdb_libpgquery::PGList &list) {
	vector<string> result;
	for (auto c = list.head; c != nullptr; c = c->next) {
		result.emplace_back(static_cast<char *>(c->data.ptr_value));
	}
	return result;
}

void AddToMultiStatement(const unique_ptr<MultiStatement> &multi_statement, unique_ptr<AlterInfo> alter_info) {
	auto alter_statement = make_uniq<AlterStatement>();
	alter_statement->info = std::move(alter_info);
	multi_statement->statements.push_back(std::move(alter_statement));
}

void AddUpdateToMultiStatement(const unique_ptr<MultiStatement> &multi_statement, const string &column_name,
                               const AlterEntryData &table_data,
                               const unique_ptr<ParsedExpression> &original_expression) {
	auto update_statement = make_uniq<UpdateStatement>();
	update_statement->prioritize_table_when_binding = true;

	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->catalog_name = table_data.catalog;
	table_ref->schema_name = table_data.schema;
	table_ref->table_name = table_data.name;
	update_statement->table = std::move(table_ref);

	auto set_info = make_uniq<UpdateSetInfo>();
	set_info->columns.push_back(column_name);
	set_info->expressions.push_back(original_expression->Copy());
	update_statement->set_info = std::move(set_info);

	multi_statement->statements.push_back(std::move(update_statement));
}

unique_ptr<MultiStatement> TransformAndMaterializeAlter(const duckdb_libpgquery::PGAlterTableStmt &stmt,
                                                        AlterEntryData &data,
                                                        unique_ptr<AlterInfo> info_with_null_placeholder,
                                                        const string &column_name,
                                                        unique_ptr<ParsedExpression> expression) {
	auto multi_statement = make_uniq<MultiStatement>();
	/* Here we do a workaround that consists of the following statements:
	 *	 1. `ALTER TABLE t ADD COLUMN col <type> DEFAULT NULL;`
	 *	 2. `UPDATE t SET col = <expression>;`
	 *	 3. `ALTER TABLE t ALTER col SET DEFAULT <expression>;`

	 *
	 * This workaround exists because, when statements like this were executed:
	 *	`ALTER TABLE ... ADD COLUMN ... DEFAULT <expression>`
	 * the WAL replay would re-run the default expression, and with expressions such as RANDOM or CURRENT_TIMESTAMP, the
	 * value would be different from that of the original run. By now doing an UPDATE in statement 2, we force
	 * materialization of these values for all existing rows, which makes WAL replays consistent.
	 */

	// 1. `ALTER TABLE t ADD COLUMN col <type> DEFAULT NULL;`
	AddToMultiStatement(multi_statement, std::move(info_with_null_placeholder));

	// 2. `UPDATE t SET u = <expression>;`
	AddUpdateToMultiStatement(multi_statement, column_name, data, expression);

	// 3. `ALTER TABLE t ALTER u SET DEFAULT <expression>;`
	// Reinstate the original default expression.
	AddToMultiStatement(multi_statement, make_uniq<SetDefaultInfo>(data, column_name, std::move(expression)));

	return multi_statement;
}

unique_ptr<SQLStatement> Transformer::TransformAlter(duckdb_libpgquery::PGAlterTableStmt &stmt) {
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
			auto column_name_list = PGPointerCast<duckdb_libpgquery::PGList>(command->def_list->head->data.ptr_value);
			auto column_def = PGPointerCast<duckdb_libpgquery::PGColumnDef>(command->def_list->tail->data.ptr_value);
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
			auto column_names = TransformNameList(*column_name_list);
			if (column_names.empty()) {
				throw InternalException("Expected a name");
			}
			column_entry.SetName(column_names.back());
			if (column_names.size() == 1) {
				// ADD COLUMN
				if (!column_entry.HasDefaultValue() ||
				    column_entry.DefaultValue().GetExpressionClass() == ExpressionClass::CONSTANT) {
					result->info =
					    make_uniq<AddColumnInfo>(std::move(data), std::move(column_entry), command->missing_ok);
					break;
				}
				auto null_column = column_entry.Copy();
				null_column.SetDefaultValue(make_uniq<ConstantExpression>(ConstantExpression(Value(nullptr))));
				return unique_ptr<SQLStatement>(std::move(TransformAndMaterializeAlter(
				    stmt, data, make_uniq<AddColumnInfo>(data, std::move(null_column), command->missing_ok),
				    column_entry.GetName(), column_entry.DefaultValue().Copy())));

			} else {
				// ADD FIELD
				column_names.pop_back();
				result->info = make_uniq<AddFieldInfo>(std::move(data), std::move(column_names),
				                                       std::move(column_entry), command->missing_ok);
			}
			break;
		}
		case duckdb_libpgquery::PG_AT_DropColumn: {
			auto cascade = command->behavior == duckdb_libpgquery::PG_DROP_CASCADE;
			if (stmt.relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
				throw ParserException("Dropping columns is only supported for tables");
			}
			auto column_names = TransformNameList(*command->def_list);
			if (column_names.empty()) {
				throw InternalException("Expected a name");
			}
			if (column_names.size() == 1) {
				result->info =
				    make_uniq<RemoveColumnInfo>(std::move(data), column_names[0], command->missing_ok, cascade);
			} else {
				result->info =
				    make_uniq<RemoveFieldInfo>(std::move(data), std::move(column_names), command->missing_ok, cascade);
			}
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
			if (command->def_list) {
				TransformExpressionList(*command->def_list, partition_keys);
			}
			result->info = make_uniq<SetPartitionedByInfo>(std::move(data), std::move(partition_keys));
			break;
		}
		case duckdb_libpgquery::PG_AT_SetSortedBy: {
			vector<OrderByNode> orders;
			if (command->def_list) {
				TransformOrderBy(command->def_list, orders);
			}
			result->info = make_uniq<SetSortedByInfo>(std::move(data), std::move(orders));
			break;
		}
		case duckdb_libpgquery::PG_AT_SetRelOptions: {
			case_insensitive_map_t<unique_ptr<ParsedExpression>> options;
			if (command->options) {
				TransformTableOptions(options, command->options);
			}
			result->info = make_uniq<SetTableOptionsInfo>(std::move(data), std::move(options));
			break;
		}
		case duckdb_libpgquery::PG_AT_ResetRelOptions: {
			case_insensitive_map_t<unique_ptr<ParsedExpression>> rel_options_map;
			case_insensitive_set_t options;
			if (command->options) {
				// TransformTableOptions will throw if key values are parsed
				TransformTableOptions(rel_options_map, command->options, true);
				// make sure RESET only supports single value options
				// default of true is allowed
				for (auto &option : rel_options_map) {
					options.insert(option.first);
				}
			}
			result->info = make_uniq<ResetTableOptionsInfo>(std::move(data), std::move(options));
			break;
		}
		default:
			throw NotImplementedException("No support for that ALTER TABLE option yet!");
		}
	}
	return unique_ptr<SQLStatement>(std::move(result));
}

} // namespace duckdb
