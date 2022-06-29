#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<Constraint> Transformer::TransformConstraint(duckdb_libpgquery::PGListCell *cell) {
	auto constraint = reinterpret_cast<duckdb_libpgquery::PGConstraint *>(cell->data.ptr_value);
	switch (constraint->contype) {
	case duckdb_libpgquery::PG_CONSTR_UNIQUE:
	case duckdb_libpgquery::PG_CONSTR_PRIMARY: {
		bool is_primary_key = constraint->contype == duckdb_libpgquery::PG_CONSTR_PRIMARY;
		vector<string> columns;
		for (auto kc = constraint->keys->head; kc; kc = kc->next) {
			columns.emplace_back(reinterpret_cast<duckdb_libpgquery::PGValue *>(kc->data.ptr_value)->val.str);
		}
		return make_unique<UniqueConstraint>(columns, is_primary_key);
	}
	case duckdb_libpgquery::PG_CONSTR_CHECK: {
		auto expression = TransformExpression(constraint->raw_expr);
		if (expression->HasSubquery()) {
			throw ParserException("subqueries prohibited in CHECK constraints");
		}
		return make_unique<CheckConstraint>(TransformExpression(constraint->raw_expr));
	}
	case duckdb_libpgquery::PG_CONSTR_FOREIGN: {
		ForeignKeyInfo fk_info;
		fk_info.type = ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
		if (constraint->pktable->schemaname) {
			fk_info.schema = constraint->pktable->schemaname;
		} else {
			fk_info.schema = "";
		}
		fk_info.table = constraint->pktable->relname;
		vector<string> pk_columns, fk_columns;
		for (auto kc = constraint->fk_attrs->head; kc; kc = kc->next) {
			fk_columns.emplace_back(reinterpret_cast<duckdb_libpgquery::PGValue *>(kc->data.ptr_value)->val.str);
		}
		if (constraint->pk_attrs) {
			for (auto kc = constraint->pk_attrs->head; kc; kc = kc->next) {
				pk_columns.emplace_back(reinterpret_cast<duckdb_libpgquery::PGValue *>(kc->data.ptr_value)->val.str);
			}
		}
		if (!pk_columns.empty() && pk_columns.size() != fk_columns.size()) {
			throw ParserException("The number of referencing and referenced columns for foreign keys must be the same");
		}
		if (fk_columns.empty()) {
			throw ParserException("The set of referencing and referenced columns for foreign keys must be not empty");
		}
		return make_unique<ForeignKeyConstraint>(pk_columns, fk_columns, move(fk_info));
	}
	default:
		throw NotImplementedException("Constraint type not handled yet!");
	}
}

unique_ptr<Constraint> Transformer::TransformConstraint(duckdb_libpgquery::PGListCell *cell, ColumnDefinition &column,
                                                        idx_t index) {
	auto constraint = reinterpret_cast<duckdb_libpgquery::PGConstraint *>(cell->data.ptr_value);
	D_ASSERT(constraint);
	switch (constraint->contype) {
	case duckdb_libpgquery::PG_CONSTR_NOTNULL:
		return make_unique<NotNullConstraint>(index);
	case duckdb_libpgquery::PG_CONSTR_CHECK:
		return TransformConstraint(cell);
	case duckdb_libpgquery::PG_CONSTR_PRIMARY:
		return make_unique<UniqueConstraint>(index, true);
	case duckdb_libpgquery::PG_CONSTR_UNIQUE:
		return make_unique<UniqueConstraint>(index, false);
	case duckdb_libpgquery::PG_CONSTR_NULL:
		return nullptr;
	case duckdb_libpgquery::PG_CONSTR_GENERATED_VIRTUAL: {
		if (column.DefaultValue()) {
			throw InvalidInputException("DEFAULT constraint on GENERATED column \"%s\" is not allowed", column.Name());
		}
		column.SetGeneratedExpression(TransformExpression(constraint->raw_expr));
		return nullptr;
	}
	case duckdb_libpgquery::PG_CONSTR_GENERATED_STORED:
		throw InvalidInputException("Can not create a STORED generated column!");
	case duckdb_libpgquery::PG_CONSTR_DEFAULT:
		column.SetDefaultValue(TransformExpression(constraint->raw_expr));
		return nullptr;
	case duckdb_libpgquery::PG_CONSTR_COMPRESSION:
		column.SetCompressionType(CompressionTypeFromString(constraint->compression_name));
		if (column.CompressionType() == CompressionType::COMPRESSION_AUTO) {
			throw ParserException("Unrecognized option for column compression, expected none, uncompressed, rle, "
			                      "dictionary, pfor, bitpacking or fsst");
		}
		return nullptr;
	case duckdb_libpgquery::PG_CONSTR_FOREIGN: {
		ForeignKeyInfo fk_info;
		fk_info.type = ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
		if (constraint->pktable->schemaname) {
			fk_info.schema = constraint->pktable->schemaname;
		} else {
			fk_info.schema = "";
		}
		fk_info.table = constraint->pktable->relname;
		vector<string> pk_columns, fk_columns;

		fk_columns.emplace_back(column.Name().c_str());
		if (constraint->pk_attrs) {
			for (auto kc = constraint->pk_attrs->head; kc; kc = kc->next) {
				pk_columns.emplace_back(reinterpret_cast<duckdb_libpgquery::PGValue *>(kc->data.ptr_value)->val.str);
			}
		}
		if (pk_columns.size() != fk_columns.size()) {
			throw ParserException("The number of referencing and referenced columns for foreign keys must be the same");
		}
		return make_unique<ForeignKeyConstraint>(pk_columns, fk_columns, move(fk_info));
	}
	default:
		throw NotImplementedException("Constraint not implemented!");
	}
}

} // namespace duckdb
