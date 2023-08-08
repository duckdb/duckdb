#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"

namespace duckdb {

unique_ptr<CommonTableExpressionInfo> CommonTableExpressionInfo::Copy() {
	auto result = make_uniq<CommonTableExpressionInfo>();
	result->aliases = aliases;
	result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	result->materialized = materialized;
	return result;
}

void Transformer::ExtractCTEsRecursive(CommonTableExpressionMap &cte_map) {
	for (auto &cte_entry : stored_cte_map) {
		for (auto &entry : cte_entry->map) {
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

void Transformer::TransformCTE(duckdb_libpgquery::PGWithClause &de_with_clause, CommonTableExpressionMap &cte_map,
                               vector<unique_ptr<CTENode>> &materialized_ctes) {
	// TODO: might need to update in case of future lawsuit
	stored_cte_map.push_back(&cte_map);

	D_ASSERT(de_with_clause.ctes);
	for (auto cte_ele = de_with_clause.ctes->head; cte_ele != nullptr; cte_ele = cte_ele->next) {
		auto info = make_uniq<CommonTableExpressionInfo>();

		auto &cte = *PGPointerCast<duckdb_libpgquery::PGCommonTableExpr>(cte_ele->data.ptr_value);
		if (cte.aliascolnames) {
			for (auto node = cte.aliascolnames->head; node != nullptr; node = node->next) {
				info->aliases.emplace_back(
				    reinterpret_cast<duckdb_libpgquery::PGValue *>(node->data.ptr_value)->val.str);
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
		if (!cte.ctequery || cte.ctequery->type != duckdb_libpgquery::T_PGSelectStmt) {
			throw NotImplementedException("A CTE needs a SELECT");
		}

		// CTE transformation can either result in inlining for non recursive CTEs, or in recursive CTE bindings
		// otherwise.
		if (cte.cterecursive || de_with_clause.recursive) {
			info->query = TransformRecursiveCTE(cte, *info);
		} else {
			Transformer cte_transformer(*this);
			info->query =
			    cte_transformer.TransformSelect(*PGPointerCast<duckdb_libpgquery::PGSelectStmt>(cte.ctequery));
		}
		D_ASSERT(info->query);
		auto cte_name = string(cte.ctename);

		auto it = cte_map.map.find(cte_name);
		if (it != cte_map.map.end()) {
			// can't have two CTEs with same name
			throw ParserException("Duplicate CTE name \"%s\"", cte_name);
		}

#ifdef DUCKDB_ALTERNATIVE_VERIFY
		if (cte.ctematerialized == duckdb_libpgquery::PGCTEMaterializeDefault) {
#else
		if (cte.ctematerialized == duckdb_libpgquery::PGCTEMaterializeAlways) {
#endif
			auto materialize = make_uniq<CTENode>();
			materialize->query = info->query->node->Copy();
			materialize->ctename = cte_name;
			materialize->aliases = info->aliases;
			materialized_ctes.push_back(std::move(materialize));

			info->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
		}

		cte_map.map[cte_name] = std::move(info);
	}
}

unique_ptr<SelectStatement> Transformer::TransformRecursiveCTE(duckdb_libpgquery::PGCommonTableExpr &cte,
                                                               CommonTableExpressionInfo &info) {
	auto &stmt = *PGPointerCast<duckdb_libpgquery::PGSelectStmt>(cte.ctequery);

	unique_ptr<SelectStatement> select;
	switch (stmt.op) {
	case duckdb_libpgquery::PG_SETOP_UNION:
	case duckdb_libpgquery::PG_SETOP_EXCEPT:
	case duckdb_libpgquery::PG_SETOP_INTERSECT: {
		select = make_uniq<SelectStatement>();
		select->node = make_uniq_base<QueryNode, RecursiveCTENode>();
		auto &result = select->node->Cast<RecursiveCTENode>();
		result.ctename = string(cte.ctename);
		result.union_all = stmt.all;
		result.left = TransformSelectNode(*PGPointerCast<duckdb_libpgquery::PGSelectStmt>(stmt.larg));
		result.right = TransformSelectNode(*PGPointerCast<duckdb_libpgquery::PGSelectStmt>(stmt.rarg));
		result.aliases = info.aliases;
		if (stmt.op != duckdb_libpgquery::PG_SETOP_UNION) {
			throw ParserException("Unsupported setop type for recursive CTE: only UNION or UNION ALL are supported");
		}
		break;
	}
	default:
		// This CTE is not recursive. Fallback to regular query transformation.
		return TransformSelect(*PGPointerCast<duckdb_libpgquery::PGSelectStmt>(cte.ctequery));
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
