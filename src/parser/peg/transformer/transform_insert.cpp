#include "duckdb/parser/peg/ast/insert_values.hpp"
#include "duckdb/parser/peg/ast/on_conflict_expression_target.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformInsertStatement(
    PEGTransformer &transformer, CommonTableExpressionMap with_clause, const OnConflictAction &or_action,
    unique_ptr<BaseTableRef> insert_target, const InsertColumnOrder &by_name_or_position,
    const vector<string> &insert_column_list, InsertValues insert_values, unique_ptr<OnConflictInfo> on_conflict_clause,
    vector<unique_ptr<ParsedExpression>> returning_clause) {
	auto result = make_uniq<InsertStatement>();
	auto &node = *result->node;
	node.cte_map = std::move(with_clause);
	node.catalog = insert_target->catalog_name;
	node.schema = insert_target->schema_name;
	node.table = insert_target->table_name;
	node.column_order = by_name_or_position;
	node.columns = StringsToIdentifiers(insert_column_list);
	if (!node.columns.empty() && insert_values.default_values) {
		throw ParserException(
		    "You can not provide both a column list and DEFAULT VALUES, please remove one of the two");
	}
	if (insert_values.default_values) {
		node.default_values = true;
	}
	if (insert_values.select_statement) {
		node.select_statement = std::move(insert_values.select_statement);
	}
	if (on_conflict_clause) {
		if (or_action != OnConflictAction::THROW) {
			// OR REPLACE | OR IGNORE are shorthands for the ON CONFLICT clause
			throw ParserException("You can not provide both OR REPLACE|IGNORE and an ON CONFLICT clause, please remove "
			                      "the first if you want to have more granular control");
		}
		node.on_conflict_info = std::move(on_conflict_clause);
		node.table_ref = std::move(insert_target);
	} else if (or_action != OnConflictAction::THROW) {
		auto on_conflict_info = make_uniq<OnConflictInfo>();
		on_conflict_info->action_type = or_action;
		node.on_conflict_info = std::move(on_conflict_info);
		node.table_ref = std::move(insert_target);
	}
	node.returning_list = std::move(returning_clause);
	return std::move(result);
}

OnConflictAction PEGTransformerFactory::TransformInsertOrReplace(PEGTransformer &transformer) {
	return OnConflictAction::REPLACE;
}

OnConflictAction PEGTransformerFactory::TransformInsertOrIgnore(PEGTransformer &transformer) {
	return OnConflictAction::NOTHING;
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformInsertTarget(PEGTransformer &transformer,
                                                                      unique_ptr<BaseTableRef> base_table_name,
                                                                      const Identifier &insert_alias) {
	base_table_name->alias = insert_alias;
	return base_table_name;
}

Identifier PEGTransformerFactory::TransformInsertAlias(PEGTransformer &transformer, const Identifier &identifier) {
	return identifier;
}

unique_ptr<OnConflictInfo>
PEGTransformerFactory::TransformOnConflictClause(PEGTransformer &transformer,
                                                 OnConflictExpressionTarget on_conflict_target,
                                                 unique_ptr<OnConflictInfo> on_conflict_action) {
	on_conflict_action->indexed_columns = on_conflict_target.indexed_columns;
	if (on_conflict_target.where_clause) {
		on_conflict_action->condition = std::move(on_conflict_target.where_clause);
	}
	return on_conflict_action;
}

OnConflictExpressionTarget PEGTransformerFactory::TransformOnConflictExpressionTarget(
    PEGTransformer &transformer, const vector<string> &column_id_list, unique_ptr<ParsedExpression> where_clause) {
	OnConflictExpressionTarget result;
	result.indexed_columns = StringsToIdentifiers(column_id_list);
	result.where_clause = std::move(where_clause);
	return result;
}

OnConflictExpressionTarget PEGTransformerFactory::TransformOnConflictIndexTarget(PEGTransformer &transformer,
                                                                                 const Identifier &constraint_name) {
	throw NotImplementedException("ON CONSTRAINT conflict target is not supported yet");
}

unique_ptr<OnConflictInfo> PEGTransformerFactory::TransformOnConflictUpdate(PEGTransformer &transformer,
                                                                            unique_ptr<UpdateSetInfo> update_set_clause,
                                                                            unique_ptr<ParsedExpression> where_clause) {
	auto result = make_uniq<OnConflictInfo>();
	result->action_type = OnConflictAction::UPDATE;
	result->set_info = std::move(update_set_clause);
	result->set_info->condition = std::move(where_clause);
	return result;
}

unique_ptr<OnConflictInfo> PEGTransformerFactory::TransformOnConflictNothing(PEGTransformer &transformer) {
	auto result = make_uniq<OnConflictInfo>();
	result->action_type = OnConflictAction::NOTHING;
	return result;
}

InsertValues PEGTransformerFactory::TransformSelectInsertValues(PEGTransformer &transformer,
                                                                unique_ptr<SelectStatement> select_statement_internal) {
	InsertValues result;
	result.select_statement = std::move(select_statement_internal);
	return result;
}

InsertValues PEGTransformerFactory::TransformDefaultValues(PEGTransformer &transformer) {
	InsertValues result;
	result.default_values = true;
	return result;
}

InsertColumnOrder PEGTransformerFactory::TransformInsertByName(PEGTransformer &transformer) {
	return InsertColumnOrder::INSERT_BY_NAME;
}

InsertColumnOrder PEGTransformerFactory::TransformInsertByPosition(PEGTransformer &transformer) {
	return InsertColumnOrder::INSERT_BY_POSITION;
}

vector<string> PEGTransformerFactory::TransformInsertColumnList(PEGTransformer &transformer,
                                                                const vector<string> &column_list) {
	return column_list;
}

vector<string> PEGTransformerFactory::TransformColumnList(PEGTransformer &transformer,
                                                          const vector<Identifier> &col_id) {
	return IdentifiersToStrings(col_id);
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformReturningClause(PEGTransformer &transformer,
                                                vector<unique_ptr<ParsedExpression>> target_list) {
	return target_list;
}

} // namespace duckdb
