#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/statement_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<CommonTableExpressionInfo> CommonTableExpressionInfo::Copy() {
	auto result = make_uniq<CommonTableExpressionInfo>();
	result->aliases = aliases;
	result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());

	for (auto &key : result->key_targets) {
		result->key_targets.push_back(key->Copy());
	}

	result->materialized = materialized;
	return result;
}

CommonTableExpressionInfo::~CommonTableExpressionInfo() {
}

CTEMaterialize CommonTableExpressionInfo::GetMaterializedForSerialization(Serializer &serializer) const {
	if (serializer.ShouldSerialize(7)) {
		return materialized;
	}
	return CTEMaterialize::CTE_MATERIALIZE_DEFAULT;
}

void Transformer::ExtractCTEsRecursive(CommonTableExpressionMap &cte_map) {
	for (auto &cte_entry : stored_cte_map) {
		for (auto &entry : cte_entry.get().map) {
			auto found_entry = cte_map.map.find(entry.first);
			if (found_entry != cte_map.map.end()) {
				// entry already present - use top-most entry
				continue;
			}
			cte_map.map[entry.first] = entry.second->Copy();
		}
	}
	if (parent) {
		parent->ExtractCTEsRecursive(cte_map);
	}
}

void Transformer::TransformCTE(duckdb_libpgquery::PGWithClause &de_with_clause, CommonTableExpressionMap &cte_map) {
	stored_cte_map.push_back(cte_map);

	// TODO: might need to update in case of future lawsuit
	D_ASSERT(de_with_clause.ctes);
	for (auto cte_ele = de_with_clause.ctes->head; cte_ele != nullptr; cte_ele = cte_ele->next) {
		auto info = make_uniq<CommonTableExpressionInfo>();

		auto &cte = *PGPointerCast<duckdb_libpgquery::PGCommonTableExpr>(cte_ele->data.ptr_value);
		if (cte.recursive_keys) {
			auto key_target = PGPointerCast<duckdb_libpgquery::PGNode>(cte.recursive_keys->head->data.ptr_value);
			if (key_target) {
				TransformExpressionList(*cte.recursive_keys, info->key_targets);
			}
		}

		if (cte.aliascolnames) {
			for (auto node = cte.aliascolnames->head; node != nullptr; node = node->next) {
				auto value = PGPointerCast<duckdb_libpgquery::PGValue>(node->data.ptr_value);
				info->aliases.emplace_back(value->val.str);
			}
		}
		// lets throw some errors on unsupported features early
		if (cte.ctecolnames) {
			throw NotImplementedException("Column name setting not supported in CTEs");
		}
		if (cte.ctecoltypes) {
			throw NotImplementedException("Column type setting not supported in CTEs");
		}
		if (cte.ctecoltypmods) {
			throw NotImplementedException("Column type modification not supported in CTEs");
		}
		if (cte.ctecolcollations) {
			throw NotImplementedException("CTE collations not supported");
		}
		// we need a query
		if (!cte.ctequery) {
			throw ParserException("A CTE needs a query");
		}

		bool is_dml_cte = false;
		switch (cte.ctequery->type) {
		case duckdb_libpgquery::T_PGSelectStmt: {
			// CTE transformation can either result in inlining for non recursive CTEs, or in recursive CTE bindings
			// otherwise.
			if (cte.cterecursive || de_with_clause.recursive) {
				info->query = TransformRecursiveCTE(cte, *info);
			} else {
				Transformer cte_transformer(*this);
				info->query = cte_transformer.TransformSelectStmt(*cte.ctequery);
			}
			break;
		}
		case duckdb_libpgquery::T_PGInsertStmt: {
			// Recursive CTEs cannot contain data-modifying statements
			if (cte.cterecursive || de_with_clause.recursive) {
				throw ParserException("Recursive CTEs cannot contain data-modifying statements");
			}
			Transformer cte_transformer(*this);
			auto &insert_stmt = *PGPointerCast<duckdb_libpgquery::PGInsertStmt>(cte.ctequery);
			auto insert = cte_transformer.TransformInsert(insert_stmt);
			if (insert->returning_list.empty()) {
				throw ParserException("INSERT in a CTE must have a RETURNING clause");
			}
			// Wrap the DML statement in a SelectStatement via StatementNode
			info->query = make_uniq<SelectStatement>();
			// Copy inner CTEs before moving the statement (for WITH ... INSERT ... syntax)
			auto inner_ctes = insert->cte_map.Copy();
			info->query->node = make_uniq<StatementNode>(std::move(insert));
			info->query->node->cte_map = std::move(inner_ctes);
			is_dml_cte = true;
			break;
		}
		case duckdb_libpgquery::T_PGUpdateStmt: {
			// Recursive CTEs cannot contain data-modifying statements
			if (cte.cterecursive || de_with_clause.recursive) {
				throw ParserException("Recursive CTEs cannot contain data-modifying statements");
			}
			Transformer cte_transformer(*this);
			auto &update_stmt = *PGPointerCast<duckdb_libpgquery::PGUpdateStmt>(cte.ctequery);
			auto update = cte_transformer.TransformUpdate(update_stmt);
			if (update->returning_list.empty()) {
				throw ParserException("UPDATE in a CTE must have a RETURNING clause");
			}
			// Wrap the DML statement in a SelectStatement via StatementNode
			info->query = make_uniq<SelectStatement>();
			// Copy inner CTEs before moving the statement (for WITH ... UPDATE ... syntax)
			auto inner_ctes = update->cte_map.Copy();
			info->query->node = make_uniq<StatementNode>(std::move(update));
			info->query->node->cte_map = std::move(inner_ctes);
			is_dml_cte = true;
			break;
		}
		case duckdb_libpgquery::T_PGDeleteStmt: {
			// Recursive CTEs cannot contain data-modifying statements
			if (cte.cterecursive || de_with_clause.recursive) {
				throw ParserException("Recursive CTEs cannot contain data-modifying statements");
			}
			Transformer cte_transformer(*this);
			auto &delete_stmt = *PGPointerCast<duckdb_libpgquery::PGDeleteStmt>(cte.ctequery);
			auto del = cte_transformer.TransformDelete(delete_stmt);
			if (del->returning_list.empty()) {
				throw ParserException("DELETE in a CTE must have a RETURNING clause");
			}
			// Wrap the DML statement in a SelectStatement via StatementNode
			info->query = make_uniq<SelectStatement>();
			// Copy inner CTEs before moving the statement (for WITH ... DELETE ... syntax)
			auto inner_ctes = del->cte_map.Copy();
			info->query->node = make_uniq<StatementNode>(std::move(del));
			info->query->node->cte_map = std::move(inner_ctes);
			is_dml_cte = true;
			break;
		}
		default:
			throw ParserException("A CTE needs a SELECT, INSERT, UPDATE, or DELETE statement");
		}
		D_ASSERT(info->query);
		auto cte_name = string(cte.ctename);

		auto it = cte_map.map.find(cte_name);
		if (it != cte_map.map.end()) {
			// can't have two CTEs with same name
			throw ParserException("Duplicate CTE name \"%s\"", cte_name);
		}

		// DML CTEs must always be materialized to ensure they execute exactly once
		if (is_dml_cte) {
			info->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
		} else if (cte.ctematerialized == duckdb_libpgquery::PGCTEMaterializeDefault) {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
			info->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
#else
			info->materialized = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;
#endif
		} else if (cte.ctematerialized == duckdb_libpgquery::PGCTEMaterializeAlways) {
			info->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
		} else if (cte.ctematerialized == duckdb_libpgquery::PGCTEMaterializeNever) {
			info->materialized = CTEMaterialize::CTE_MATERIALIZE_NEVER;
		}

		cte_map.map[cte_name] = std::move(info);
	}
}

unique_ptr<SelectStatement> Transformer::TransformRecursiveCTE(duckdb_libpgquery::PGCommonTableExpr &cte,
                                                               CommonTableExpressionInfo &info) {
	auto &stmt = *PGPointerCast<duckdb_libpgquery::PGSelectStmt>(cte.ctequery);

	unique_ptr<SelectStatement> select;
	switch (stmt.op) {
	case duckdb_libpgquery::PG_SETOP_UNION: {
		select = make_uniq<SelectStatement>();
		select->node = make_uniq_base<QueryNode, RecursiveCTENode>();
		auto &result = select->node->Cast<RecursiveCTENode>();
		result.ctename = string(cte.ctename);
		result.union_all = stmt.all;
		if (stmt.withClause) {
			auto with_clause = PGPointerCast<duckdb_libpgquery::PGWithClause>(stmt.withClause);
			TransformCTE(*with_clause, result.cte_map);
		}
		result.left = TransformSelectNode(*stmt.larg);
		result.right = TransformSelectNode(*stmt.rarg);
		result.aliases = info.aliases;
		for (auto &key : info.key_targets) {
			result.key_targets.emplace_back(key->Copy());
		}
		break;
	}
	case duckdb_libpgquery::PG_SETOP_EXCEPT:
	case duckdb_libpgquery::PG_SETOP_INTERSECT:
	default: {
		// This CTE is not recursive. Fallback to regular query transformation.
		auto node = TransformSelectNode(*cte.ctequery);
		auto result = make_uniq<SelectStatement>();
		result->node = std::move(node);
		return result;
	}
	}

	if (stmt.limitCount || stmt.limitOffset) {
		throw ParserException("LIMIT or OFFSET in a recursive query is not allowed");
	}
	if (stmt.sortClause) {
		throw ParserException("ORDER BY in a recursive query is not allowed");
	}
	return select;
}

} // namespace duckdb
