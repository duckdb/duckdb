#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

static void ParseSchemaTableNameFK(duckdb_libpgquery::PGRangeVar &input, ForeignKeyInfo &fk_info) {
	if (input.catalogname) {
		throw ParserException("FOREIGN KEY constraints cannot be defined cross-database");
	}
	fk_info.schema = input.schemaname ? input.schemaname : "";
	fk_info.table = input.relname;
}

static bool ForeignKeyActionSupported(char action) {
	switch (action) {
	case PG_FKCONSTR_ACTION_NOACTION:
	case PG_FKCONSTR_ACTION_RESTRICT:
		return true;
	case PG_FKCONSTR_ACTION_CASCADE:
	case PG_FKCONSTR_ACTION_SETDEFAULT:
	case PG_FKCONSTR_ACTION_SETNULL:
		return false;
	default:
		D_ASSERT(false);
	}
	return false;
}

static unique_ptr<ForeignKeyConstraint>
TransformForeignKeyConstraint(duckdb_libpgquery::PGConstraint &constraint,
                              optional_ptr<const string> override_fk_column = nullptr) {
	if (!ForeignKeyActionSupported(constraint.fk_upd_action) || !ForeignKeyActionSupported(constraint.fk_del_action)) {
		throw ParserException("FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT");
	}

	ForeignKeyInfo fk_info;
	fk_info.type = ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
	ParseSchemaTableNameFK(*constraint.pktable, fk_info);

	vector<string> pk_columns;
	vector<string> fk_columns;

	if (override_fk_column) {
		D_ASSERT(!constraint.fk_attrs);
		fk_columns.emplace_back(*override_fk_column);

	} else if (constraint.fk_attrs) {
		for (auto kc = constraint.fk_attrs->head; kc; kc = kc->next) {
			auto value = Transformer::PGPointerCast<duckdb_libpgquery::PGValue>(kc->data.ptr_value);
			fk_columns.emplace_back(value->val.str);
		}
	}

	if (constraint.pk_attrs) {
		for (auto kc = constraint.pk_attrs->head; kc; kc = kc->next) {
			auto value = Transformer::PGPointerCast<duckdb_libpgquery::PGValue>(kc->data.ptr_value);
			pk_columns.emplace_back(value->val.str);
		}
	}
	if (!pk_columns.empty() && pk_columns.size() != fk_columns.size()) {
		throw ParserException("The number of referencing and referenced columns for foreign keys must be the same");
	}
	if (fk_columns.empty()) {
		throw ParserException("The set of referencing and referenced columns for foreign keys must be not empty");
	}
	return make_uniq<ForeignKeyConstraint>(pk_columns, fk_columns, std::move(fk_info));
}

unique_ptr<Constraint> Transformer::TransformConstraint(duckdb_libpgquery::PGConstraint &constraint) {
	switch (constraint.contype) {
	case duckdb_libpgquery::PG_CONSTR_UNIQUE:
	case duckdb_libpgquery::PG_CONSTR_PRIMARY: {
		bool is_primary_key = constraint.contype == duckdb_libpgquery::PG_CONSTR_PRIMARY;
		if (!constraint.keys) {
			throw ParserException("UNIQUE USING INDEX is not supported");
		}
		vector<string> columns;
		for (auto kc = constraint.keys->head; kc; kc = kc->next) {
			auto value = PGPointerCast<duckdb_libpgquery::PGValue>(kc->data.ptr_value);
			columns.emplace_back(value->val.str);
		}
		return make_uniq<UniqueConstraint>(columns, is_primary_key);
	}
	case duckdb_libpgquery::PG_CONSTR_CHECK: {
		auto expression = TransformExpression(constraint.raw_expr);
		if (expression->HasSubquery()) {
			throw ParserException("subqueries prohibited in CHECK constraints");
		}
		return make_uniq<CheckConstraint>(TransformExpression(constraint.raw_expr));
	}
	case duckdb_libpgquery::PG_CONSTR_FOREIGN:
		return TransformForeignKeyConstraint(constraint);
	default:
		throw NotImplementedException("Constraint type not handled yet!");
	}
}

unique_ptr<Constraint> Transformer::TransformConstraint(duckdb_libpgquery::PGConstraint &constraint,
                                                        ColumnDefinition &column, idx_t index) {
	switch (constraint.contype) {
	case duckdb_libpgquery::PG_CONSTR_NOTNULL:
		return make_uniq<NotNullConstraint>(LogicalIndex(index));
	case duckdb_libpgquery::PG_CONSTR_CHECK:
		return TransformConstraint(constraint);
	case duckdb_libpgquery::PG_CONSTR_PRIMARY:
		return make_uniq<UniqueConstraint>(LogicalIndex(index), column.GetName(), true);
	case duckdb_libpgquery::PG_CONSTR_UNIQUE:
		return make_uniq<UniqueConstraint>(LogicalIndex(index), column.GetName(), false);
	case duckdb_libpgquery::PG_CONSTR_NULL:
		return nullptr;
	case duckdb_libpgquery::PG_CONSTR_GENERATED_VIRTUAL: {
		if (column.HasDefaultValue()) {
			throw InvalidInputException("\"%s\" has a DEFAULT value set, it can not become a GENERATED column",
			                            column.Name());
		}
		column.SetGeneratedExpression(TransformExpression(constraint.raw_expr));
		return nullptr;
	}
	case duckdb_libpgquery::PG_CONSTR_GENERATED_STORED:
		throw InvalidInputException("Can not create a STORED generated column!");
	case duckdb_libpgquery::PG_CONSTR_DEFAULT:
		column.SetDefaultValue(TransformExpression(constraint.raw_expr));
		return nullptr;
	case duckdb_libpgquery::PG_CONSTR_COMPRESSION:
		column.SetCompressionType(CompressionTypeFromString(constraint.compression_name));
		if (column.CompressionType() == CompressionType::COMPRESSION_AUTO) {
			throw ParserException("Unrecognized option for column compression, expected none, uncompressed, rle, "
			                      "dictionary, pfor, bitpacking or fsst");
		}
		return nullptr;
	case duckdb_libpgquery::PG_CONSTR_FOREIGN:
		return TransformForeignKeyConstraint(constraint, &column.Name());
	default:
		throw NotImplementedException("Constraint not implemented!");
	}
}

} // namespace duckdb
