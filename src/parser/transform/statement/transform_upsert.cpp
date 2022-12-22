#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

OnConflictAction TransformOnConflictAction(duckdb_libpgquery::PGOnConflictClause *on_conflict) {
	if (!on_conflict) {
		return OnConflictAction::THROW;
	}
	switch (on_conflict->action) {
	case duckdb_libpgquery::PG_ONCONFLICT_NONE:
		return OnConflictAction::THROW;
	case duckdb_libpgquery::PG_ONCONFLICT_NOTHING:
		return OnConflictAction::NOTHING;
	case duckdb_libpgquery::PG_ONCONFLICT_UPDATE:
		return OnConflictAction::UPDATE;
	default:
		throw InternalException("Type not implemented for OnConflictAction");
	}
}

vector<string> TransformConflictTarget(duckdb_libpgquery::PGList *list) {
	vector<string> columns;
	for (auto cell = list->head; cell != nullptr; cell = cell->next) {
		auto index_element = (duckdb_libpgquery::PGIndexElem *)cell->data.ptr_value;
		if (index_element->collation) {
			throw NotImplementedException("Index with collation not supported yet!");
		}
		if (index_element->opclass) {
			throw NotImplementedException("Index with opclass not supported yet!");
		}
		if (!index_element->name) {
			throw NotImplementedException("Non-column index element not supported yet!");
		}
		if (index_element->nulls_ordering) {
			throw NotImplementedException("Index with null_ordering not supported yet!");
		}
		if (index_element->ordering) {
			throw NotImplementedException("Index with ordering not supported yet!");
		}
		columns.push_back(index_element->name);
	}
	return columns;
}

unique_ptr<OnConflictInfo> Transformer::TransformOnConflictClause(duckdb_libpgquery::PGOnConflictClause *node,
                                                                  const string &relname) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGOnConflictClause *>(node);
	D_ASSERT(stmt);

	auto result = make_unique<OnConflictInfo>();
	result->action_type = TransformOnConflictAction(stmt);
	if (stmt->infer) {
		// A filter for the DO ... is specified
		if (stmt->infer->indexElems) {
			// Columns are specified
			result->indexed_columns = TransformConflictTarget(stmt->infer->indexElems);
			if (stmt->infer->whereClause) {
				result->condition = TransformExpression(stmt->infer->whereClause);
			}
		} else {
			// A constraint name is specified
			result->constraint_name = stmt->infer->conname;
		}
	} else if (result->action_type == OnConflictAction::UPDATE) {
		// This is SQLite behavior, though I'm not entirely sure why they do this
		// 'When the conflict target is omitted, the upsert behavior is triggered by a violation of any uniqueness
		// constraint on the table of the INSERT' Which sounds like it could function just fine for DO UPDATE.
		throw InvalidInputException("Empty conflict target is not supported for DO UPDATE");
	}

	if (result->action_type == OnConflictAction::UPDATE) {
		result->set_info = TransformUpdateSetInfo(stmt->targetList, stmt->whereClause);
	}
	return result;
}

} // namespace duckdb
