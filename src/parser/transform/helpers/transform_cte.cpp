#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<CommonTableExpressionInfo> CommonTableExpressionInfo::Copy() {
	auto result = make_uniq<CommonTableExpressionInfo>();
	result->aliases = aliases;
	if (query_node) {
		result->query_node = query_node->Copy();
	}
	for (auto &key : key_targets) {
		result->key_targets.push_back(key->Copy());
	}
	for (auto &agg : payload_aggregates) {
		result->payload_aggregates.push_back(agg->Copy());
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

unique_ptr<SelectStatement> CommonTableExpressionInfo::GetQueryForSerialization(Serializer &serializer) const {
	// Field 101 is only written for old storage versions (v7 and earlier).
	// For v8+ this method is not called; the QueryNode is written directly as field 106.
	if (!query_node ||
	    (query_node->type != QueryNodeType::SELECT_NODE && query_node->type != QueryNodeType::SET_OPERATION_NODE &&
	     query_node->type != QueryNodeType::RECURSIVE_CTE_NODE)) {
		throw SerializationException(
		    "DML CTEs (INSERT/UPDATE/DELETE) require storage version v2.0.0 or higher and cannot be "
		    "serialized to older storage formats");
	}
	auto select = make_uniq<SelectStatement>();
	select->node = query_node->Copy();
	return select;
}

// Serialization field layout for CommonTableExpressionInfo:
//   100  aliases            vector<string>        (all versions)
//   101  query              SelectStatement*      (v7 and older; written via GetQueryForSerialization)
//   102  materialized       CTEMaterialize        (all versions)
//   103  key_targets        vector<ParsedExpr*>   (all versions)
//   104  payload_aggregates vector<ParsedExpr*>   (all versions)
//   105  dml_query          QueryNode*            (deleted; tombstoned, never shipped)
//   106  query_node         QueryNode*            (v8+; unified field for SELECT and DML CTEs)
void CommonTableExpressionInfo::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<vector<string>>(100, "aliases", aliases);
	if (!serializer.ShouldSerialize(8)) {
		// Old format (v7 and earlier): wrap the QueryNode back in a SelectStatement.
		serializer.WritePropertyWithDefault<unique_ptr<SelectStatement>>(101, "query",
		                                                                 GetQueryForSerialization(serializer));
	}
	serializer.WriteProperty<CTEMaterialize>(102, "materialized", GetMaterializedForSerialization(serializer));
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(103, "key_targets", key_targets);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(104, "payload_aggregates",
	                                                                          payload_aggregates);
	if (serializer.ShouldSerialize(8)) {
		// New format (v8+): write the QueryNode directly.
		D_ASSERT(query_node);
		serializer.WriteProperty<unique_ptr<QueryNode>>(106, "query_node", query_node->Copy());
	}
}

unique_ptr<CommonTableExpressionInfo> CommonTableExpressionInfo::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<CommonTableExpressionInfo>();
	deserializer.ReadPropertyWithDefault<vector<string>>(100, "aliases", result->aliases);
	// Field 101 (SelectStatement) is only written in old format (v7 and earlier).
	auto select = deserializer.ReadPropertyWithDefault<unique_ptr<SelectStatement>>(101, "query");
	if (select) {
		result->query_node = std::move(select->node);
	}
	deserializer.ReadProperty<CTEMaterialize>(102, "materialized", result->materialized);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(103, "key_targets", result->key_targets);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(104, "payload_aggregates",
	                                                                           result->payload_aggregates);
	// Field 106 (QueryNode) is only present in new format (v8+); overrides field 101 if present.
	auto query_node = deserializer.ReadPropertyWithDefault<unique_ptr<QueryNode>>(106, "query_node");
	if (query_node) {
		result->query_node = std::move(query_node);
	}
	return result;
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
	idx_t dml_cte_count = 0;
	for (auto cte_ele = de_with_clause.ctes->head; cte_ele != nullptr; cte_ele = cte_ele->next) {
		auto info = make_uniq<CommonTableExpressionInfo>();

		auto &cte = *PGPointerCast<duckdb_libpgquery::PGCommonTableExpr>(cte_ele->data.ptr_value);

		if (cte.using_key_list) {
			for (auto key_ele = cte.using_key_list->head; key_ele != nullptr; key_ele = key_ele->next) {
				auto expr_ele = TransformExpression(*PGPointerCast<duckdb_libpgquery::PGNode>(key_ele->data.ptr_value));
				info->key_targets.push_back(std::move(expr_ele));
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
			throw ParserException("A CTE body must be a SELECT, INSERT, UPDATE, or DELETE statement");
		}

		bool is_recursive = cte.cterecursive || de_with_clause.recursive;

		// CTE transformation can either result in inlining for non recursive CTEs, or in recursive CTE bindings
		// otherwise.
		if (cte.ctequery->type == duckdb_libpgquery::T_PGSelectStmt) {
			if (is_recursive) {
				info->query_node = TransformRecursiveCTE(cte, *info);
			} else {
				Transformer cte_transformer(*this);
				auto select = cte_transformer.TransformSelectStmt(*cte.ctequery);
				info->query_node = std::move(select->node);
			}
		} else {
			// DML body (INSERT / UPDATE / DELETE)
			if (is_recursive) {
				throw ParserException("Recursive CTEs with DML statements are not supported");
			}
			if (++dml_cte_count > 1) {
				throw ParserException("Only a single DML statement (INSERT/UPDATE/DELETE) is allowed per WITH clause");
			}
			Transformer cte_transformer(*this);
			if (cte.ctequery->type == duckdb_libpgquery::T_PGInsertStmt) {
				auto stmt = cte_transformer.TransformInsert(PGCast<duckdb_libpgquery::PGInsertStmt>(*cte.ctequery));
				info->query_node = unique_ptr_cast<InsertQueryNode, QueryNode>(std::move(stmt->node));
			} else if (cte.ctequery->type == duckdb_libpgquery::T_PGUpdateStmt) {
				auto stmt = cte_transformer.TransformUpdate(PGCast<duckdb_libpgquery::PGUpdateStmt>(*cte.ctequery));
				info->query_node = unique_ptr_cast<UpdateQueryNode, QueryNode>(std::move(stmt->node));
			} else if (cte.ctequery->type == duckdb_libpgquery::T_PGDeleteStmt) {
				auto stmt = cte_transformer.TransformDelete(PGCast<duckdb_libpgquery::PGDeleteStmt>(*cte.ctequery));
				info->query_node = unique_ptr_cast<DeleteQueryNode, QueryNode>(std::move(stmt->node));
			} else {
				throw ParserException("A CTE body must be a SELECT, INSERT, UPDATE, or DELETE statement");
			}
		}
		D_ASSERT(info->query_node);
		auto cte_name = string(cte.ctename);

		auto it = cte_map.map.find(cte_name);
		if (it != cte_map.map.end()) {
			// can't have two CTEs with same name
			throw ParserException("Duplicate CTE name \"%s\"", cte_name);
		}

		if (cte.ctematerialized == duckdb_libpgquery::PGCTEMaterializeDefault) {
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

unique_ptr<QueryNode> Transformer::TransformRecursiveCTE(duckdb_libpgquery::PGCommonTableExpr &cte,
                                                         CommonTableExpressionInfo &info) {
	auto &stmt = *PGPointerCast<duckdb_libpgquery::PGSelectStmt>(cte.ctequery);

	switch (stmt.op) {
	case duckdb_libpgquery::PG_SETOP_UNION: {
		auto node = make_uniq_base<QueryNode, RecursiveCTENode>();
		auto &result = node->Cast<RecursiveCTENode>();
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
		if (stmt.limitCount || stmt.limitOffset) {
			throw ParserException("LIMIT or OFFSET in a recursive query is not allowed");
		}
		if (stmt.sortClause) {
			throw ParserException("ORDER BY in a recursive query is not allowed");
		}
		return node;
	}
	default:
		// This CTE is not recursive. Fallback to regular query transformation.
		return TransformSelectNode(*cte.ctequery);
	}
}

} // namespace duckdb
