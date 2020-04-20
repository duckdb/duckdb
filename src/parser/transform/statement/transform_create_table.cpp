#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/constraint.hpp"

using namespace duckdb;
using namespace std;

CollationType Transformer::TransformCollation(PGCollateClause *collate) {
	if (!collate) {
		return CollationType::COLLATE_NONE;
	}
	CollationType collation = CollationType::COLLATE_NONE;
	for (auto c = collate->collname->head; c != NULL; c = lnext(c)) {
		auto pgvalue = (PGValue*) c->data.ptr_value;
		if (pgvalue->type != T_PGString) {
			throw ParserException("Expected a string as collation type!");
		}
		auto collation_argument = string(pgvalue->val.str);
		if (collation_argument== "nocase") {
			switch(collation) {
			case CollationType::COLLATE_NONE:
				collation = CollationType::COLLATE_NOCASE;
				break;
			case CollationType::COLLATE_NOACCENT:
				collation = CollationType::COLLATE_NOCASE_NOACCENT;
				break;
			default:
				throw ParserException("Unexpected NOCASE collation!");
			}
		} else if (collation_argument == "noaccent") {
			switch(collation) {
			case CollationType::COLLATE_NONE:
				collation = CollationType::COLLATE_NOACCENT;
				break;
			case CollationType::COLLATE_NOCASE:
				collation = CollationType::COLLATE_NOCASE_NOACCENT;
				break;
			default:
				throw ParserException("Unexpected NOACCENT collation!");
			}
		} else {
			throw ParserException("Unsupported collation type %s", collation_argument.c_str());
		}
	}
	return collation;
}

unique_ptr<CreateStatement> Transformer::TransformCreateTable(PGNode *node) {
	auto stmt = reinterpret_cast<PGCreateStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateTableInfo>();

	if (stmt->inhRelations) {
		throw NotImplementedException("inherited relations not implemented");
	}
	assert(stmt->relation);

	info->schema = INVALID_SCHEMA;
	if (stmt->relation->schemaname) {
		info->schema = stmt->relation->schemaname;
	}
	info->table = stmt->relation->relname;
	info->on_conflict = stmt->if_not_exists ? OnCreateConflict::IGNORE : OnCreateConflict::ERROR;
	info->temporary = stmt->relation->relpersistence == PGPostgresRelPersistence::PG_RELPERSISTENCE_TEMP;

	if (info->temporary && stmt->oncommit != PGOnCommitAction::PG_ONCOMMIT_PRESERVE_ROWS &&
	    stmt->oncommit != PGOnCommitAction::PG_ONCOMMIT_NOOP) {
		throw NotImplementedException("Only ON COMMIT PRESERVE ROWS is supported");
	}
	if (!stmt->tableElts) {
		throw ParserException("Table must have at least one column!");
	}

	for (auto c = stmt->tableElts->head; c != NULL; c = lnext(c)) {
		auto node = reinterpret_cast<PGNode *>(c->data.ptr_value);
		switch (node->type) {
		case T_PGColumnDef: {
			auto cdef = (PGColumnDef *)c->data.ptr_value;
			SQLType target_type = TransformTypeName(cdef->typeName);
			target_type.collation = TransformCollation(cdef->collClause);

			auto centry = ColumnDefinition(cdef->colname, target_type);

			if (cdef->constraints) {
				for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
					auto constraint = TransformConstraint(constr, centry, info->columns.size());
					if (constraint) {
						info->constraints.push_back(move(constraint));
					}
				}
			}
			info->columns.push_back(move(centry));
			break;
		}
		case T_PGConstraint: {
			info->constraints.push_back(TransformConstraint(c));
			break;
		}
		default:
			throw NotImplementedException("ColumnDef type not handled yet");
		}
	}
	result->info = move(info);
	return result;
}
