#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/insert_binder.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression_binder/update_binder.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

static void CheckInsertColumnCountMismatch(int64_t expected_columns, int64_t result_columns, bool columns_provided,
                                           const char *tname) {
	if (result_columns != expected_columns) {
		string msg = StringUtil::Format(!columns_provided ? "table %s has %lld columns but %lld values were supplied"
		                                                  : "Column name/value mismatch for insert on %s: "
		                                                    "expected %lld columns but %lld values were supplied",
		                                tname, expected_columns, result_columns);
		throw BinderException(msg);
	}
}

BoundStatement Binder::Bind(InsertStatement &stmt) {
	BoundStatement result;
	if (stmt.returning_list.empty()) {
		result.names = {"Count"};
		result.types = {LogicalType::BIGINT};
	}


	auto table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context, stmt.schema, stmt.table);
	D_ASSERT(table);
	if (!table->temporary) {
		// inserting into a non-temporary table: alters underlying database
		this->read_only = false;
	}

	auto insert = make_unique<LogicalInsert>(table);


	vector<idx_t> named_column_map;
	if (!stmt.columns.empty()) {
		// insertion statement specifies column list

		// create a mapping of (list index) -> (column index)
		case_insensitive_map_t<idx_t> column_name_map;
		for (idx_t i = 0; i < stmt.columns.size(); i++) {
			column_name_map[stmt.columns[i]] = i;
			auto entry = table->name_map.find(stmt.columns[i]);
			if (entry == table->name_map.end()) {
				throw BinderException("Column %s not found in table %s", stmt.columns[i], table->name);
			}
			if (entry->second == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot explicitly insert values into rowid column");
			}
			insert->expected_types.push_back(table->columns[entry->second].type);
			named_column_map.push_back(entry->second);
		}
		for (idx_t i = 0; i < table->columns.size(); i++) {
			auto &col = table->columns[i];
			auto entry = column_name_map.find(col.name);
			if (entry == column_name_map.end()) {
				// column not specified, set index to DConstants::INVALID_INDEX
				insert->column_index_map.push_back(DConstants::INVALID_INDEX);
			} else {
				// column was specified, set to the index
				insert->column_index_map.push_back(entry->second);
			}
		}
	} else {
		for (idx_t i = 0; i < table->columns.size(); i++) {
			insert->expected_types.push_back(table->columns[i].type);
		}
	}

	// bind the default values
	BindDefaultValues(table->columns, insert->bound_defaults);

	if (!stmt.select_statement) {
		result.plan = move(insert);
		return result;
	}

	idx_t expected_columns = stmt.columns.empty() ? table->columns.size() : stmt.columns.size();
	// special case: check if we are inserting from a VALUES statement
	if (stmt.select_statement->node->type == QueryNodeType::SELECT_NODE) {
		auto &node = (SelectNode &)*stmt.select_statement->node;
		if (node.from_table->type == TableReferenceType::EXPRESSION_LIST) {
			auto &expr_list = (ExpressionListRef &)*node.from_table;
			expr_list.expected_types.resize(expected_columns);
			expr_list.expected_names.resize(expected_columns);

			D_ASSERT(expr_list.values.size() > 0);
			CheckInsertColumnCountMismatch(expected_columns, expr_list.values[0].size(), !stmt.columns.empty(),
			                               table->name.c_str());

			// VALUES list!
			for (idx_t col_idx = 0; col_idx < expected_columns; col_idx++) {
				idx_t table_col_idx = stmt.columns.empty() ? col_idx : named_column_map[col_idx];
				D_ASSERT(table_col_idx < table->columns.size());

				// set the expected types as the types for the INSERT statement
				auto &column = table->columns[table_col_idx];
				expr_list.expected_types[col_idx] = column.type;
				expr_list.expected_names[col_idx] = column.name;

				// now replace any DEFAULT values with the corresponding default expression
				for (idx_t list_idx = 0; list_idx < expr_list.values.size(); list_idx++) {
					if (expr_list.values[list_idx][col_idx]->type == ExpressionType::VALUE_DEFAULT) {
						// DEFAULT value! replace the entry
						if (column.default_value) {
							expr_list.values[list_idx][col_idx] = column.default_value->Copy();
						} else {
							expr_list.values[list_idx][col_idx] = make_unique<ConstantExpression>(Value(column.type));
						}
					}
				}
			}
		}
	}

	// parse select statement and add to logical plan
	auto root_select = Bind(*stmt.select_statement);
	// ------------------------------------------------------------------------------------------------------------
	// This is to bind the returning list
	// visit the retuning list and expand any "*" statements
	vector<unique_ptr<ParsedExpression>> new_returning_list;
	auto returning_result = make_unique<BoundSelectNode>();;
	for (auto &returning_element : stmt.returning_list) {
		if (returning_element->GetExpressionType() == ExpressionType::STAR) {
			// * statement, expand to all columns from the FROM clause
			bind_context.GenerateAllColumnExpressions((StarExpression &)*returning_element, new_returning_list);
		} else {
			// regular statement, add it to the list
			new_returning_list.push_back(move(returning_element));
		}
	}
	stmt.returning_list = move(new_returning_list);


	// create a mapping of (alias -> index) and a mapping of (Expression -> index) for the RETURNING list
	case_insensitive_map_t<idx_t> alias_map;
	expression_map_t<idx_t> projection_map;
	for (idx_t i = 0; i < stmt.returning_list.size(); i++) {
		auto &expr = stmt.returning_list[i];
		result.names.push_back(expr->GetName());
		ExpressionBinder::QualifyColumnNames(*this, expr);
		if (!expr->alias.empty()) {
			alias_map[expr->alias] = i;
			result.names[i] = expr->alias;
		}
		projection_map[expr.get()] = i;
		returning_result->original_expressions.push_back(expr->Copy());
	}

	returning_result->column_count = stmt.returning_list.size();

	// after that, bind the returning statement to the context of the INSERT statement
	BoundGroupInformation info;
	SelectBinder select_binder(*this, context, *returning_result, info);
	for (idx_t i = 0; i < stmt.returning_list.size(); i++) {
		LogicalType result_type;
		unique_ptr<Expression> expr = select_binder.Bind(stmt.returning_list[i], &result_type);

		// TODO: FIX THIS!!!!
		if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
			// https://stackoverflow.com/questions/36120424/alternatives-of-static-pointer-cast-for-unique-ptr
			BoundColumnRefExpression& expr2 = *(static_cast<BoundColumnRefExpression*>(expr.get()));
			expr2.binding.table_index = 0;
		}

		returning_result->select_list.push_back(move(expr));
		if (i < result.names.size()) {
			result.types.push_back(result_type);
		}
	}

	// ------------------------------------------------------------------------------------------------------------
	// TODO: bind the returning values similar to how select values are bound in Bind selectStatement.
	// TODO: once the returning values are bound, update the result.names and result.types
	// TODO (maybe?): update the plan to return the returning_list values.
	// TODO:          the value_map in the planner uses those expressions.
	// TODO: here you need to change result.types.size()
	CheckInsertColumnCountMismatch(expected_columns, root_select.types.size(), !stmt.columns.empty(),
	                               table->name.c_str());

	auto root = CastLogicalOperatorToTypes(root_select.types, insert->expected_types, move(root_select.plan));
	// TODO: what is this root pointer? how should I use my bound returning list to make sure it's properly updated?
	// TODO: the root variable is a logical plan. So eventually it should ideally contain the return info in some way so
	// TODO: that it the return plan can be executed properly. Why do we need the plan? Because the returning statement
	// TODO: may also need special executing functions.

	insert->AddChild(move(root));

	for (auto i = returning_result->select_list.begin(); i != returning_result->select_list.end(); i++) {
		insert->returning_list.push_back(move(*i));
	}

	// TODO: check either that you are returning the RETURNING statements or the effected rows.
	//	D_ASSERT(insert->returning_list.size() == result.names.size());
	//	D_ASSERT(insert->returning_list.size() == result.types.size());

	if (!insert->returning_list.empty()) {
		this->allow_stream_result = true;
	} else {
		this->allow_stream_result = false;
	}

	result.plan = move(insert);

	return result;
}

} // namespace duckdb
