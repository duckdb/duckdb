#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_column_type.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include <random>
#include <iterator>

namespace duckdb {

string Transformer::TransformCollation(duckdb_libpgquery::PGCollateClause *collate) {
	if (!collate) {
		return string();
	}
	string collation;
	for (auto c = collate->collname->head; c != nullptr; c = lnext(c)) {
		auto pgvalue = (duckdb_libpgquery::PGValue *)c->data.ptr_value;
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

unique_ptr<ParsedExpression> Transformer::TransformCollateExpr(duckdb_libpgquery::PGCollateClause *collate) {
	auto child = TransformExpression(collate->arg);
	auto collation = TransformCollation(collate);
	return make_unique<CollateExpression>(collation, move(child));
}

ColumnDefinition Transformer::TransformColumnDefinition(duckdb_libpgquery::PGColumnDef *cdef) {
	string colname;
	if (cdef->colname) {
		colname = cdef->colname;
	}
	bool optional_type = cdef->category == duckdb_libpgquery::COL_GENERATED;
	LogicalType target_type = (optional_type && !cdef->typeName) ? LogicalType::ANY : TransformTypeName(cdef->typeName);
	if (cdef->collClause) {
		if (cdef->category == duckdb_libpgquery::COL_GENERATED) {
			throw ParserException("Collations are not supported on generated columns");
		}
		if (target_type.id() != LogicalTypeId::VARCHAR) {
			throw ParserException("Only VARCHAR columns can have collations!");
		}
		target_type = LogicalType::VARCHAR_COLLATION(TransformCollation(cdef->collClause));
	}

	return ColumnDefinition(colname, target_type);
}

static std::string GenerateRandomName(idx_t size) {
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<> dis(0, 15);

	std::stringstream ss;
	idx_t i;
	ss << std::hex;
	for (i = 0; i < size; i++) {
		ss << dis(gen);
	}
	return ss.str();
}

void Transformer::AddRandomGeneratedColumn(unordered_set<string> &columns, data_ptr_t info_p) {
	// Get the minimum size for the random column, as to not collide with any existing column
	idx_t size = 10;
	for (auto &column : columns) {
		if (column.size() > size) {
			size = column.size() + 1;
		}
	}
	// Random name for the generated column ('a' to make it start with a letter)
	auto random_name = "a" + GenerateRandomName(size);
	auto info = (CreateTableInfo *)info_p;

	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<> dis(0, columns.size() - 1);

	// The column it will reference
	auto chosen_column = columns.begin();
	std::advance(chosen_column, (int)dis(gen));

	duckdb_libpgquery::PGColumnDef cdef;
	cdef.colname = (char *)random_name.c_str();
	cdef.category = duckdb_libpgquery::COL_GENERATED;
	cdef.typeName = NULL;
	cdef.collClause = NULL;

	auto gcol_reference = make_unique_base<ParsedExpression, ColumnRefExpression>(*chosen_column);
	auto centry = TransformColumnDefinition(&cdef);
	centry.SetGeneratedExpression(move(gcol_reference));
	info->columns.insert(info->columns.begin(), move(centry));
}

unique_ptr<CreateStatement> Transformer::TransformCreateTable(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateTableInfo>();

	if (stmt->inhRelations) {
		throw NotImplementedException("inherited relations not implemented");
	}
	D_ASSERT(stmt->relation);

	info->schema = INVALID_SCHEMA;
	if (stmt->relation->schemaname) {
		info->schema = stmt->relation->schemaname;
	}
	info->table = stmt->relation->relname;
	info->on_conflict = TransformOnConflict(stmt->onconflict);
	info->temporary =
	    stmt->relation->relpersistence == duckdb_libpgquery::PGPostgresRelPersistence::PG_RELPERSISTENCE_TEMP;

	if (info->temporary && stmt->oncommit != duckdb_libpgquery::PGOnCommitAction::PG_ONCOMMIT_PRESERVE_ROWS &&
	    stmt->oncommit != duckdb_libpgquery::PGOnCommitAction::PG_ONCOMMIT_NOOP) {
		throw NotImplementedException("Only ON COMMIT PRESERVE ROWS is supported");
	}
	if (!stmt->tableElts) {
		throw ParserException("Table must have at least one column!");
	}

	idx_t column_count = 0;
	std::unordered_set<string> column_names;
	for (auto c = stmt->tableElts->head; c != nullptr; c = lnext(c)) {
		auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(c->data.ptr_value);
		switch (node->type) {
		case duckdb_libpgquery::T_PGColumnDef: {
			auto cdef = (duckdb_libpgquery::PGColumnDef *)c->data.ptr_value;
			auto centry = TransformColumnDefinition(cdef);
			if (cdef->constraints) {
				for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
					// Verify generated columns adds a GENERATED COLUMN at index 0, so we offset everything by 1
					auto constraint =
					    TransformConstraint(constr, centry, verify_generated_columns + info->columns.size());
					if (constraint) {
						info->constraints.push_back(move(constraint));
					}
				}
			}
			column_names.insert(centry.GetName());
			info->columns.push_back(move(centry));
			column_count++;
			break;
		}
		case duckdb_libpgquery::T_PGConstraint: {
			info->constraints.push_back(TransformConstraint(c));
			break;
		}
		default:
			throw NotImplementedException("ColumnDef type not handled yet");
		}
	}

	if (!column_count) {
		throw ParserException("Table must have at least one column!");
	}
	if (verify_generated_columns) {
		// Add a single generated column to the front to test if generated columns are properly supported
		AddRandomGeneratedColumn(column_names, (data_ptr_t)info.get());
	}

	result->info = move(info);
	return result;
}

} // namespace duckdb
