#include "duckdb/catalog/catalog_entry/table_column_type.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

string Transformer::TransformCollation(optional_ptr<duckdb_libpgquery::PGCollateClause> collate) {
	if (!collate) {
		return string();
	}
	string collation;
	for (auto c = collate->collname->head; c != nullptr; c = lnext(c)) {
		auto pgvalue = PGPointerCast<duckdb_libpgquery::PGValue>(c->data.ptr_value);
		if (pgvalue->type != duckdb_libpgquery::T_PGString) {
			throw ParserException("Expected a string as collation type!");
		}
		auto collation_argument = string(pgvalue->val.str);
		if (collation.empty()) {
			collation = collation_argument;
		} else {
			collation += "." + collation_argument;
		}
	}
	return collation;
}

OnCreateConflict Transformer::TransformOnConflict(duckdb_libpgquery::PGOnCreateConflict conflict) {
	switch (conflict) {
	case duckdb_libpgquery::PG_ERROR_ON_CONFLICT:
		return OnCreateConflict::ERROR_ON_CONFLICT;
	case duckdb_libpgquery::PG_IGNORE_ON_CONFLICT:
		return OnCreateConflict::IGNORE_ON_CONFLICT;
	case duckdb_libpgquery::PG_REPLACE_ON_CONFLICT:
		return OnCreateConflict::REPLACE_ON_CONFLICT;
	default:
		throw InternalException("Unrecognized OnConflict type");
	}
}

unique_ptr<ParsedExpression> Transformer::TransformCollateExpr(duckdb_libpgquery::PGCollateClause &collate) {
	auto child = TransformExpression(collate.arg);
	auto collation = TransformCollation(&collate);
	return make_uniq<CollateExpression>(collation, std::move(child));
}

ColumnDefinition Transformer::TransformColumnDefinition(duckdb_libpgquery::PGColumnDef &cdef) {
	string name;
	if (cdef.colname) {
		name = cdef.colname;
	}

	auto optional_type = cdef.category == duckdb_libpgquery::COL_GENERATED;
	LogicalType target_type;
	if (optional_type && !cdef.typeName) {
		target_type = LogicalType::ANY;
	} else if (!cdef.typeName) {
		// ALTER TABLE tbl ALTER TYPE USING ...
		target_type = LogicalType::UNKNOWN;
	} else if (cdef.collClause) {
		if (cdef.category == duckdb_libpgquery::COL_GENERATED) {
			throw ParserException("Collations are not supported on generated columns");
		}

		auto typename_expr = TransformTypeExpressionInternal(*cdef.typeName);
		if (typename_expr->type != ExpressionType::TYPE) {
			throw ParserException("Only type names can have collations!");
		}

		auto &type_expr = typename_expr->Cast<TypeExpression>();
		if (!StringUtil::CIEquals(type_expr.GetTypeName(), "VARCHAR")) {
			throw ParserException("Only VARCHAR columns can have collations!");
		}

		// Push back collation as a parameter of the type expression
		auto collation = TransformCollation(cdef.collClause);
		auto collation_expr = make_uniq<ConstantExpression>(Value(collation));
		collation_expr->SetAlias("collation");
		type_expr.GetChildren().push_back(std::move(collation_expr));

		target_type = LogicalType::UNBOUND(std::move(typename_expr));
	} else {
		target_type = TransformTypeName(*cdef.typeName);
	}

	return ColumnDefinition(name, target_type);
}

unique_ptr<CreateStatement> Transformer::TransformCreateTable(duckdb_libpgquery::PGCreateStmt &stmt) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTableInfo>();

	if (stmt.inhRelations) {
		throw NotImplementedException("inherited relations not implemented");
	}
	D_ASSERT(stmt.relation);

	info->catalog = INVALID_CATALOG;
	auto qname = TransformQualifiedName(*stmt.relation);
	info->catalog = qname.catalog;
	info->schema = qname.schema;
	info->table = qname.name;
	info->on_conflict = TransformOnConflict(stmt.onconflict);
	info->temporary =
	    stmt.relation->relpersistence == duckdb_libpgquery::PGPostgresRelPersistence::PG_RELPERSISTENCE_TEMP;

	if (info->temporary && stmt.oncommit != duckdb_libpgquery::PGOnCommitAction::PG_ONCOMMIT_PRESERVE_ROWS &&
	    stmt.oncommit != duckdb_libpgquery::PGOnCommitAction::PG_ONCOMMIT_NOOP) {
		throw NotImplementedException("Only ON COMMIT PRESERVE ROWS is supported");
	}
	if (!stmt.tableElts) {
		throw ParserException("Table must have at least one column!");
	}

	idx_t column_count = 0;
	for (auto c = stmt.tableElts->head; c != nullptr; c = lnext(c)) {
		auto node = PGPointerCast<duckdb_libpgquery::PGNode>(c->data.ptr_value);
		switch (node->type) {
		case duckdb_libpgquery::T_PGColumnDef: {
			auto pg_col_def = PGPointerCast<duckdb_libpgquery::PGColumnDef>(c->data.ptr_value);
			auto col_def = TransformColumnDefinition(*pg_col_def);

			if (pg_col_def->constraints) {
				for (auto cell = pg_col_def->constraints->head; cell != nullptr; cell = cell->next) {
					auto pg_constraint = PGPointerCast<duckdb_libpgquery::PGConstraint>(cell->data.ptr_value);
					auto constraint = TransformConstraint(*pg_constraint, col_def, info->columns.LogicalColumnCount());
					if (constraint) {
						info->constraints.push_back(std::move(constraint));
					}
				}
			}

			info->columns.AddColumn(std::move(col_def));
			column_count++;
			break;
		}
		case duckdb_libpgquery::T_PGConstraint: {
			auto pg_constraint = PGPointerCast<duckdb_libpgquery::PGConstraint>(c->data.ptr_value);
			info->constraints.push_back(TransformConstraint(*pg_constraint));
			break;
		}
		default:
			throw NotImplementedException("ColumnDef type not handled yet");
		}
	}

	vector<unique_ptr<ParsedExpression>> partition_keys;
	if (stmt.partition_list) {
		TransformExpressionList(*stmt.partition_list, partition_keys);
	}
	info->partition_keys = std::move(partition_keys);

	vector<unique_ptr<ParsedExpression>> order_keys;
	if (stmt.sort_list) {
		TransformExpressionList(*stmt.sort_list, order_keys);
	}
	info->sort_keys = std::move(order_keys);

	if (stmt.options) {
		TransformTableOptions(info->options, stmt.options);
	}

	if (!column_count) {
		throw ParserException("Table must have at least one column!");
	}

	result->info = std::move(info);
	return result;
}

} // namespace duckdb
