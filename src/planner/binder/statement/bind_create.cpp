#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/operator/logical_create.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/parsed_data/bound_create_function_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

void Binder::BindSchemaOrCatalog(ClientContext &context, string &catalog, string &schema) {
	if (catalog.empty() && !schema.empty()) {
		// schema is specified - but catalog is not
		// try searching for the catalog instead
		auto &db_manager = DatabaseManager::Get(context);
		auto database = db_manager.GetDatabase(context, schema);
		if (database) {
			// we have a database with this name
			// check if there is a schema
			auto schema_obj = Catalog::GetSchema(context, INVALID_CATALOG, schema, true);
			if (schema_obj) {
				auto &attached = schema_obj->catalog->GetAttached();
				throw BinderException(
				    "Ambiguous reference to catalog or schema \"%s\" - use a fully qualified path like \"%s.%s\"",
				    schema, attached.GetName(), schema);
			}
			catalog = schema;
			schema = string();
		}
	}
}

void Binder::BindSchemaOrCatalog(string &catalog, string &schema) {
	BindSchemaOrCatalog(context, catalog, schema);
}

SchemaCatalogEntry *Binder::BindSchema(CreateInfo &info) {
	BindSchemaOrCatalog(info.catalog, info.schema);
	if (IsInvalidCatalog(info.catalog) && info.temporary) {
		info.catalog = TEMP_CATALOG;
	}
	auto &search_path = ClientData::Get(context).catalog_search_path;
	if (IsInvalidCatalog(info.catalog) && IsInvalidSchema(info.schema)) {
		auto &default_entry = search_path->GetDefault();
		info.catalog = default_entry.catalog;
		info.schema = default_entry.schema;
	} else if (IsInvalidSchema(info.schema)) {
		info.schema = search_path->GetDefaultSchema(info.catalog);
	} else if (IsInvalidCatalog(info.catalog)) {
		info.catalog = search_path->GetDefaultCatalog(info.schema);
	}
	if (IsInvalidCatalog(info.catalog)) {
		info.catalog = DatabaseManager::GetDefaultDatabase(context);
	}
	if (!info.temporary) {
		// non-temporary create: not read only
		if (info.catalog == TEMP_CATALOG) {
			throw ParserException("Only TEMPORARY table names can use the \"%s\" catalog", TEMP_CATALOG);
		}
	} else {
		if (info.catalog != TEMP_CATALOG) {
			throw ParserException("TEMPORARY table names can *only* use the \"%s\" catalog", TEMP_CATALOG);
		}
	}
	// fetch the schema in which we want to create the object
	auto schema_obj = Catalog::GetSchema(context, info.catalog, info.schema);
	D_ASSERT(schema_obj->type == CatalogType::SCHEMA_ENTRY);
	info.schema = schema_obj->name;
	if (!info.temporary) {
		properties.modified_databases.insert(schema_obj->catalog->GetName());
	}
	return schema_obj;
}

SchemaCatalogEntry *Binder::BindCreateSchema(CreateInfo &info) {
	auto schema = BindSchema(info);
	if (schema->catalog->IsSystemCatalog()) {
		throw BinderException("Cannot create entry in system catalog");
	}
	return schema;
}

void Binder::BindCreateViewInfo(CreateViewInfo &base) {
	// bind the view as if it were a query so we can catch errors
	// note that we bind the original, and replace the original with a copy
	auto view_binder = Binder::CreateBinder(context);
	view_binder->can_contain_nulls = true;

	auto copy = base.query->Copy();
	auto query_node = view_binder->Bind(*base.query);
	base.query = unique_ptr_cast<SQLStatement, SelectStatement>(move(copy));
	if (base.aliases.size() > query_node.names.size()) {
		throw BinderException("More VIEW aliases than columns in query result");
	}
	// fill up the aliases with the remaining names of the bound query
	base.aliases.reserve(query_node.names.size());
	for (idx_t i = base.aliases.size(); i < query_node.names.size(); i++) {
		base.aliases.push_back(query_node.names[i]);
	}
	base.types = query_node.types;
}

SchemaCatalogEntry *Binder::BindCreateFunctionInfo(CreateInfo &info) {
	auto &base = (CreateMacroInfo &)info;
	auto &scalar_function = (ScalarMacroFunction &)*base.function;

	if (scalar_function.expression->HasParameter()) {
		throw BinderException("Parameter expressions within macro's are not supported!");
	}

	// create macro binding in order to bind the function
	vector<LogicalType> dummy_types;
	vector<string> dummy_names;
	// positional parameters
	for (idx_t i = 0; i < base.function->parameters.size(); i++) {
		auto param = (ColumnRefExpression &)*base.function->parameters[i];
		if (param.IsQualified()) {
			throw BinderException("Invalid parameter name '%s': must be unqualified", param.ToString());
		}
		dummy_types.emplace_back(LogicalType::SQLNULL);
		dummy_names.push_back(param.GetColumnName());
	}
	// default parameters
	for (auto it = base.function->default_parameters.begin(); it != base.function->default_parameters.end(); it++) {
		auto &val = (ConstantExpression &)*it->second;
		dummy_types.push_back(val.value.type());
		dummy_names.push_back(it->first);
	}
	auto this_macro_binding = make_unique<DummyBinding>(dummy_types, dummy_names, base.name);
	macro_binding = this_macro_binding.get();
	ExpressionBinder::QualifyColumnNames(*this, scalar_function.expression);

	// create a copy of the expression because we do not want to alter the original
	auto expression = scalar_function.expression->Copy();

	// bind it to verify the function was defined correctly
	string error;
	auto sel_node = make_unique<BoundSelectNode>();
	auto group_info = make_unique<BoundGroupInformation>();
	SelectBinder binder(*this, context, *sel_node, *group_info);
	error = binder.Bind(&expression, 0, false);

	if (!error.empty()) {
		throw BinderException(error);
	}

	return BindCreateSchema(info);
}

void Binder::BindLogicalType(ClientContext &context, LogicalType &type, const string &catalog, const string &schema) {
	if (type.id() == LogicalTypeId::LIST) {
		auto child_type = ListType::GetChildType(type);
		BindLogicalType(context, child_type, catalog, schema);
		auto alias = type.GetAlias();
		type = LogicalType::LIST(child_type);
		type.SetAlias(alias);
	} else if (type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::MAP) {
		auto child_types = StructType::GetChildTypes(type);
		for (auto &child_type : child_types) {
			BindLogicalType(context, child_type.second, catalog, schema);
		}
		// Generate new Struct/Map Type
		auto alias = type.GetAlias();
		if (type.id() == LogicalTypeId::STRUCT) {
			type = LogicalType::STRUCT(child_types);
		} else {
			type = LogicalType::MAP(child_types);
		}
		type.SetAlias(alias);
	} else if (type.id() == LogicalTypeId::UNION) {
		auto member_types = UnionType::CopyMemberTypes(type);
		for (auto &member_type : member_types) {
			BindLogicalType(context, member_type.second, catalog, schema);
		}
		// Generate new Union Type
		auto alias = type.GetAlias();
		type = LogicalType::UNION(member_types);
		type.SetAlias(alias);
	} else if (type.id() == LogicalTypeId::USER) {
		type = Catalog::GetType(context, catalog, schema, UserType::GetTypeName(type));
	} else if (type.id() == LogicalTypeId::ENUM) {
		auto &enum_type_name = EnumType::GetTypeName(type);
		auto enum_type_catalog = Catalog::GetEntry<TypeCatalogEntry>(context, catalog, schema, enum_type_name, true);
		LogicalType::SetCatalog(type, enum_type_catalog);
	}
}

static void FindMatchingPrimaryKeyColumns(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints,
                                          ForeignKeyConstraint &fk) {
	// find the matching primary key constraint
	bool found_constraint = false;
	// if no columns are defined, we will automatically try to bind to the primary key
	bool find_primary_key = fk.pk_columns.empty();
	for (auto &constr : constraints) {
		if (constr->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = (UniqueConstraint &)*constr;
		if (find_primary_key && !unique.is_primary_key) {
			continue;
		}
		found_constraint = true;

		vector<string> pk_names;
		if (unique.index.index != DConstants::INVALID_INDEX) {
			pk_names.push_back(columns.GetColumn(LogicalIndex(unique.index)).Name());
		} else {
			pk_names = unique.columns;
		}
		if (pk_names.size() != fk.fk_columns.size()) {
			// the number of referencing and referenced columns for foreign keys must be the same
			continue;
		}
		if (find_primary_key) {
			// found matching primary key
			fk.pk_columns = pk_names;
			return;
		}
		if (fk.pk_columns != pk_names) {
			// Name mismatch
			continue;
		}
		// found match
		return;
	}
	// no match found! examine why
	if (!found_constraint) {
		// no unique constraint or primary key
		string search_term = find_primary_key ? "primary key" : "primary key or unique constraint";
		throw BinderException("Failed to create foreign key: there is no %s for referenced table \"%s\"", search_term,
		                      fk.info.table);
	}
	// check if all the columns exist
	for (auto &name : fk.pk_columns) {
		bool found = columns.ColumnExists(name);
		if (!found) {
			throw BinderException(
			    "Failed to create foreign key: referenced table \"%s\" does not have a column named \"%s\"",
			    fk.info.table, name);
		}
	}
	auto fk_names = StringUtil::Join(fk.pk_columns, ",");
	throw BinderException("Failed to create foreign key: referenced table \"%s\" does not have a primary key or unique "
	                      "constraint on the columns %s",
	                      fk.info.table, fk_names);
}

static void FindForeignKeyIndexes(const ColumnList &columns, const vector<string> &names,
                                  vector<PhysicalIndex> &indexes) {
	D_ASSERT(indexes.empty());
	D_ASSERT(!names.empty());
	for (auto &name : names) {
		if (!columns.ColumnExists(name)) {
			throw BinderException("column \"%s\" named in key does not exist", name);
		}
		auto &column = columns.GetColumn(name);
		if (column.Generated()) {
			throw BinderException("Failed to create foreign key: referenced column \"%s\" is a generated column",
			                      column.Name());
		}
		indexes.push_back(column.Physical());
	}
}

static void CheckForeignKeyTypes(const ColumnList &pk_columns, const ColumnList &fk_columns, ForeignKeyConstraint &fk) {
	D_ASSERT(fk.info.pk_keys.size() == fk.info.fk_keys.size());
	for (idx_t c_idx = 0; c_idx < fk.info.pk_keys.size(); c_idx++) {
		auto &pk_col = pk_columns.GetColumn(fk.info.pk_keys[c_idx]);
		auto &fk_col = fk_columns.GetColumn(fk.info.fk_keys[c_idx]);
		if (pk_col.Type() != fk_col.Type()) {
			throw BinderException("Failed to create foreign key: incompatible types between column \"%s\" (\"%s\") and "
			                      "column \"%s\" (\"%s\")",
			                      pk_col.Name(), pk_col.Type().ToString(), fk_col.Name(), fk_col.Type().ToString());
		}
	}
}

void ExpressionContainsGeneratedColumn(const ParsedExpression &expr, const unordered_set<string> &gcols,
                                       bool &contains_gcol) {
	if (contains_gcol) {
		return;
	}
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &column_ref = (ColumnRefExpression &)expr;
		auto &name = column_ref.GetColumnName();
		if (gcols.count(name)) {
			contains_gcol = true;
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { ExpressionContainsGeneratedColumn(child, gcols, contains_gcol); });
}

static bool AnyConstraintReferencesGeneratedColumn(CreateTableInfo &table_info) {
	unordered_set<string> generated_columns;
	for (auto &col : table_info.columns.Logical()) {
		if (!col.Generated()) {
			continue;
		}
		generated_columns.insert(col.Name());
	}
	if (generated_columns.empty()) {
		return false;
	}

	for (auto &constr : table_info.constraints) {
		switch (constr->type) {
		case ConstraintType::CHECK: {
			auto &constraint = (CheckConstraint &)*constr;
			auto &expr = constraint.expression;
			bool contains_generated_column = false;
			ExpressionContainsGeneratedColumn(*expr, generated_columns, contains_generated_column);
			if (contains_generated_column) {
				return true;
			}
			break;
		}
		case ConstraintType::NOT_NULL: {
			auto &constraint = (NotNullConstraint &)*constr;
			if (table_info.columns.GetColumn(constraint.index).Generated()) {
				return true;
			}
			break;
		}
		case ConstraintType::UNIQUE: {
			auto &constraint = (UniqueConstraint &)*constr;
			auto index = constraint.index;
			if (index.index == DConstants::INVALID_INDEX) {
				for (auto &col : constraint.columns) {
					if (generated_columns.count(col)) {
						return true;
					}
				}
			} else {
				if (table_info.columns.GetColumn(index).Generated()) {
					return true;
				}
			}
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			// If it contained a generated column, an exception would have been thrown inside AddDataTableIndex earlier
			break;
		}
		default: {
			throw NotImplementedException("ConstraintType not implemented");
		}
		}
	}
	return false;
}

BoundStatement Binder::Bind(CreateStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};
	properties.return_type = StatementReturnType::NOTHING;

	auto catalog_type = stmt.info->type;
	switch (catalog_type) {
	case CatalogType::SCHEMA_ENTRY:
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_SCHEMA, move(stmt.info));
		break;
	case CatalogType::VIEW_ENTRY: {
		auto &base = (CreateViewInfo &)*stmt.info;
		// bind the schema
		auto schema = BindCreateSchema(*stmt.info);
		BindCreateViewInfo(base);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_VIEW, move(stmt.info), schema);
		break;
	}
	case CatalogType::SEQUENCE_ENTRY: {
		auto schema = BindCreateSchema(*stmt.info);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_SEQUENCE, move(stmt.info), schema);
		break;
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto schema = BindCreateSchema(*stmt.info);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_MACRO, move(stmt.info), schema);
		break;
	}
	case CatalogType::MACRO_ENTRY: {
		auto schema = BindCreateFunctionInfo(*stmt.info);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_MACRO, move(stmt.info), schema);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		auto &base = (CreateIndexInfo &)*stmt.info;

		// visit the table reference
		auto bound_table = Bind(*base.table);
		if (bound_table->type != TableReferenceType::BASE_TABLE) {
			throw BinderException("Can only delete from base table!");
		}
		auto &table_binding = (BoundBaseTableRef &)*bound_table;
		auto table = table_binding.table;

		// bind the index expressions
		vector<unique_ptr<Expression>> expressions;
		IndexBinder binder(*this, context);
		for (auto &expr : base.expressions) {
			expressions.push_back(binder.Bind(expr));
		}

		auto plan = CreatePlan(*bound_table);
		if (plan->type != LogicalOperatorType::LOGICAL_GET) {
			throw BinderException("Cannot create index on a view!");
		}

		auto &get = (LogicalGet &)*plan;
		for (auto &column_id : get.column_ids) {
			if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot create an index on the rowid!");
			}
		}
		if (table->temporary) {
			stmt.info->temporary = true;
		}

		auto create_index_info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(move(stmt.info));
		for (auto &index : get.column_ids) {
			create_index_info->scan_types.push_back(get.returned_types[index]);
		}
		create_index_info->scan_types.emplace_back(LogicalType::ROW_TYPE);
		create_index_info->names = get.names;
		create_index_info->column_ids = get.column_ids;

		// the logical CREATE INDEX also needs all fields to scan the referenced table
		result.plan = make_unique<LogicalCreateIndex>(move(get.bind_data), move(create_index_info), move(expressions),
		                                              *table, move(get.function));
		break;
	}
	case CatalogType::TABLE_ENTRY: {
		auto &create_info = (CreateTableInfo &)*stmt.info;
		// If there is a foreign key constraint, resolve primary key column's index from primary key column's name
		unordered_set<SchemaCatalogEntry *> fk_schemas;
		for (idx_t i = 0; i < create_info.constraints.size(); i++) {
			auto &cond = create_info.constraints[i];
			if (cond->type != ConstraintType::FOREIGN_KEY) {
				continue;
			}
			auto &fk = (ForeignKeyConstraint &)*cond;
			if (fk.info.type != ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				continue;
			}
			D_ASSERT(fk.info.pk_keys.empty());
			D_ASSERT(fk.info.fk_keys.empty());
			FindForeignKeyIndexes(create_info.columns, fk.fk_columns, fk.info.fk_keys);
			if (create_info.table == fk.info.table) {
				// self-referential foreign key constraint
				fk.info.type = ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE;
				FindMatchingPrimaryKeyColumns(create_info.columns, create_info.constraints, fk);
				FindForeignKeyIndexes(create_info.columns, fk.pk_columns, fk.info.pk_keys);
				CheckForeignKeyTypes(create_info.columns, create_info.columns, fk);
			} else {
				// have to resolve referenced table
				auto pk_table_entry_ptr =
				    Catalog::GetEntry<TableCatalogEntry>(context, INVALID_CATALOG, fk.info.schema, fk.info.table);
				fk_schemas.insert(pk_table_entry_ptr->schema);
				FindMatchingPrimaryKeyColumns(pk_table_entry_ptr->columns, pk_table_entry_ptr->constraints, fk);
				FindForeignKeyIndexes(pk_table_entry_ptr->columns, fk.pk_columns, fk.info.pk_keys);
				CheckForeignKeyTypes(pk_table_entry_ptr->columns, create_info.columns, fk);
				auto index = pk_table_entry_ptr->storage->info->indexes.FindForeignKeyIndex(
				    fk.info.pk_keys, ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE);
				if (!index) {
					auto fk_column_names = StringUtil::Join(fk.pk_columns, ",");
					throw BinderException("Failed to create foreign key on %s(%s): no UNIQUE or PRIMARY KEY constraint "
					                      "present on these columns",
					                      pk_table_entry_ptr->name, fk_column_names);
				}
			}
			D_ASSERT(fk.info.pk_keys.size() == fk.info.fk_keys.size());
			D_ASSERT(fk.info.pk_keys.size() == fk.pk_columns.size());
			D_ASSERT(fk.info.fk_keys.size() == fk.fk_columns.size());
		}
		if (AnyConstraintReferencesGeneratedColumn(create_info)) {
			throw BinderException("Constraints on generated columns are not supported yet");
		}
		auto bound_info = BindCreateTableInfo(move(stmt.info));
		auto root = move(bound_info->query);
		for (auto &fk_schema : fk_schemas) {
			if (fk_schema != bound_info->schema) {
				throw BinderException("Creating foreign keys across different schemas or catalogs is not supported");
			}
		}

		// create the logical operator
		auto &schema = bound_info->schema;
		auto create_table = make_unique<LogicalCreateTable>(schema, move(bound_info));
		if (root) {
			// CREATE TABLE AS
			properties.return_type = StatementReturnType::CHANGED_ROWS;
			create_table->children.push_back(move(root));
		}
		result.plan = move(create_table);
		break;
	}
	case CatalogType::TYPE_ENTRY: {
		auto schema = BindCreateSchema(*stmt.info);
		auto &create_type_info = (CreateTypeInfo &)(*stmt.info);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_TYPE, move(stmt.info), schema);
		if (create_type_info.query) {
			// CREATE TYPE mood AS ENUM (SELECT 'happy')
			auto &select_stmt = (SelectStatement &)*create_type_info.query;
			auto &query_node = *select_stmt.node;

			// We always add distinct modifier implicitly
			bool need_to_add = true;
			if (!query_node.modifiers.empty()) {
				if (query_node.modifiers[0]->type == ResultModifierType::DISTINCT_MODIFIER) {
					// There are cases where the same column is grouped repeatedly
					// CREATE TYPE mood AS ENUM (SELECT DISTINCT ON(x) x FROM test);
					// When we push into a constant expression
					// => CREATE TYPE mood AS ENUM (SELECT DISTINCT ON(x, x) x FROM test);
					auto &distinct_modifier = (DistinctModifier &)*query_node.modifiers[0];
					distinct_modifier.distinct_on_targets.push_back(make_unique<ConstantExpression>(Value::INTEGER(1)));
					need_to_add = false;
				}
			}

			// Add distinct modifier
			if (need_to_add) {
				auto distinct_modifier = make_unique<DistinctModifier>();
				distinct_modifier->distinct_on_targets.push_back(make_unique<ConstantExpression>(Value::INTEGER(1)));
				query_node.modifiers.emplace(query_node.modifiers.begin(), move(distinct_modifier));
			}

			auto query_obj = Bind(*create_type_info.query);
			auto query = move(query_obj.plan);

			auto &sql_types = query_obj.types;
			if (sql_types.size() != 1 || sql_types[0].id() != LogicalType::VARCHAR) {
				// add cast expression?
				throw BinderException("The query must return one varchar column");
			}

			result.plan->AddChild(move(query));
		} else if (create_type_info.type.id() == LogicalTypeId::USER) {
			// two cases:
			// 1: create a type with a non-existant type as source, catalog.GetType(...) will throw exception.
			// 2: create a type alias with a custom type.
			// eg. CREATE TYPE a AS INT; CREATE TYPE b AS a;
			// We set b to be an alias for the underlying type of a
			auto &catalog = Catalog::GetCatalog(context);
			auto inner_type = catalog.GetType(context, "", UserType::GetTypeName(create_type_info.type));
			// clear to nullptr, we don't need this
			LogicalType::SetCatalog(inner_type, nullptr);
			inner_type.SetAlias(create_type_info.name);
			create_type_info.type = inner_type;
		}
		break;
	}
	default:
		throw Exception("Unrecognized type!");
	}
	properties.allow_stream_result = false;
	return result;
}

} // namespace duckdb
