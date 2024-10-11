#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/operator/logical_create.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression_binder/select_bind_state.hpp"

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
			auto &search_path = *context.client_data->catalog_search_path;
			auto catalog_names = search_path.GetCatalogsForSchema(schema);
			if (catalog_names.empty()) {
				catalog_names.push_back(DatabaseManager::GetDefaultDatabase(context));
			}
			for (auto &catalog_name : catalog_names) {
				auto &catalog = Catalog::GetCatalog(context, catalog_name);
				if (catalog.CheckAmbiguousCatalogOrSchema(context, schema)) {
					throw BinderException(
					    "Ambiguous reference to catalog or schema \"%s\" - use a fully qualified path like \"%s.%s\"",
					    schema, catalog_name, schema);
				}
			}
			catalog = schema;
			schema = string();
		}
	}
}

void Binder::BindSchemaOrCatalog(string &catalog, string &schema) {
	BindSchemaOrCatalog(context, catalog, schema);
}

const string Binder::BindCatalog(string &catalog) {
	auto &db_manager = DatabaseManager::Get(context);
	optional_ptr<AttachedDatabase> database = db_manager.GetDatabase(context, catalog);
	if (database) {
		return db_manager.GetDatabase(context, catalog).get()->GetName();
	} else {
		return db_manager.GetDefaultDatabase(context);
	}
}

SchemaCatalogEntry &Binder::BindSchema(CreateInfo &info) {
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
	auto &schema_obj = Catalog::GetSchema(context, info.catalog, info.schema);
	D_ASSERT(schema_obj.type == CatalogType::SCHEMA_ENTRY);
	info.schema = schema_obj.name;
	if (!info.temporary) {
		auto &properties = GetStatementProperties();
		properties.RegisterDBModify(schema_obj.catalog, context);
	}
	return schema_obj;
}

SchemaCatalogEntry &Binder::BindCreateSchema(CreateInfo &info) {
	auto &schema = BindSchema(info);
	if (schema.catalog.IsSystemCatalog()) {
		throw BinderException("Cannot create entry in system catalog");
	}
	return schema;
}

void Binder::SetCatalogLookupCallback(catalog_entry_callback_t callback) {
	entry_retriever.SetCallback(std::move(callback));
}

void Binder::BindCreateViewInfo(CreateViewInfo &base) {
	// bind the view as if it were a query so we can catch errors
	// note that we bind the original, and replace the original with a copy
	auto view_binder = Binder::CreateBinder(context);
	auto &dependencies = base.dependencies;
	auto &catalog = Catalog::GetCatalog(context, base.catalog);

	auto &db_config = DBConfig::GetConfig(context);
	auto should_create_dependencies = db_config.options.enable_view_dependencies;

	if (should_create_dependencies) {
		view_binder->SetCatalogLookupCallback([&dependencies, &catalog](CatalogEntry &entry) {
			if (&catalog != &entry.ParentCatalog()) {
				// Don't register dependencies between catalogs
				return;
			}
			dependencies.AddDependency(entry);
		});
	}
	view_binder->can_contain_nulls = true;

	auto copy = base.query->Copy();
	auto query_node = view_binder->Bind(*base.query);
	base.query = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(copy));
	if (base.aliases.size() > query_node.names.size()) {
		throw BinderException("More VIEW aliases than columns in query result");
	}
	base.types = query_node.types;
	base.names = query_node.names;
}

SchemaCatalogEntry &Binder::BindCreateFunctionInfo(CreateInfo &info) {
	auto &base = info.Cast<CreateMacroInfo>();

	auto &dependencies = base.dependencies;
	auto &catalog = Catalog::GetCatalog(context, info.catalog);
	auto &db_config = DBConfig::GetConfig(context);
	// try to bind each of the included functions
	unordered_set<idx_t> positional_parameters;
	for (auto &function : base.macros) {
		auto &scalar_function = function->Cast<ScalarMacroFunction>();
		if (scalar_function.expression->HasParameter()) {
			throw BinderException("Parameter expressions within macro's are not supported!");
		}
		vector<LogicalType> dummy_types;
		vector<string> dummy_names;
		auto parameter_count = function->parameters.size();
		if (positional_parameters.find(parameter_count) != positional_parameters.end()) {
			throw BinderException(
			    "Ambiguity in macro overloads - macro \"%s\" has multiple definitions with %llu parameters", base.name,
			    parameter_count);
		}
		positional_parameters.insert(parameter_count);

		// positional parameters
		for (auto &param_expr : function->parameters) {
			auto param = param_expr->Cast<ColumnRefExpression>();
			if (param.IsQualified()) {
				throw BinderException("Invalid parameter name '%s': must be unqualified", param.ToString());
			}
			dummy_types.emplace_back(LogicalType::SQLNULL);
			dummy_names.push_back(param.GetColumnName());
		}
		// default parameters
		for (auto &entry : function->default_parameters) {
			auto &val = entry.second->Cast<ConstantExpression>();
			dummy_types.push_back(val.value.type());
			dummy_names.push_back(entry.first);
		}
		auto this_macro_binding = make_uniq<DummyBinding>(dummy_types, dummy_names, base.name);
		macro_binding = this_macro_binding.get();

		// create a copy of the expression because we do not want to alter the original
		auto expression = scalar_function.expression->Copy();
		ExpressionBinder::QualifyColumnNames(*this, expression);

		// bind it to verify the function was defined correctly
		BoundSelectNode sel_node;
		BoundGroupInformation group_info;
		SelectBinder binder(*this, context, sel_node, group_info);
		auto should_create_dependencies = db_config.options.enable_macro_dependencies;

		if (should_create_dependencies) {
			binder.SetCatalogLookupCallback([&dependencies, &catalog](CatalogEntry &entry) {
				if (&catalog != &entry.ParentCatalog()) {
					// Don't register any cross-catalog dependencies
					return;
				}
				// Register any catalog entry required to bind the macro function
				dependencies.AddDependency(entry);
			});
		}
		ErrorData error;
		try {
			error = binder.Bind(expression, 0, false);
			if (error.HasError()) {
				error.Throw();
			}
		} catch (const std::exception &ex) {
			error = ErrorData(ex);
		}
		// if we cannot resolve parameters we postpone binding until the macro function is used
		if (error.HasError() && error.Type() != ExceptionType::PARAMETER_NOT_RESOLVED) {
			error.Throw();
		}
	}

	return BindCreateSchema(info);
}

static bool IsValidUserType(optional_ptr<CatalogEntry> entry) {
	if (!entry) {
		return false;
	}
	return entry->Cast<TypeCatalogEntry>().user_type.id() != LogicalTypeId::INVALID;
}

void Binder::BindLogicalType(LogicalType &type, optional_ptr<Catalog> catalog, const string &schema) {
	if (type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::MAP) {
		auto child_type = ListType::GetChildType(type);
		BindLogicalType(child_type, catalog, schema);
		auto alias = type.GetAlias();
		auto modifiers = type.GetModifiersCopy();
		if (type.id() == LogicalTypeId::LIST) {
			type = LogicalType::LIST(child_type);
		} else {
			D_ASSERT(child_type.id() == LogicalTypeId::STRUCT); // map must be list of structs
			type = LogicalType::MAP(child_type);
		}

		type.SetAlias(alias);
		type.SetModifiers(modifiers);
	} else if (type.id() == LogicalTypeId::STRUCT) {
		auto child_types = StructType::GetChildTypes(type);
		for (auto &child_type : child_types) {
			BindLogicalType(child_type.second, catalog, schema);
		}
		// Generate new Struct Type
		auto alias = type.GetAlias();
		auto modifiers = type.GetModifiersCopy();
		type = LogicalType::STRUCT(child_types);
		type.SetAlias(alias);
		type.SetModifiers(modifiers);
	} else if (type.id() == LogicalTypeId::ARRAY) {
		auto child_type = ArrayType::GetChildType(type);
		auto array_size = ArrayType::GetSize(type);
		BindLogicalType(child_type, catalog, schema);
		auto alias = type.GetAlias();
		auto modifiers = type.GetModifiersCopy();
		type = LogicalType::ARRAY(child_type, array_size);
		type.SetAlias(alias);
		type.SetModifiers(modifiers);
	} else if (type.id() == LogicalTypeId::UNION) {
		auto member_types = UnionType::CopyMemberTypes(type);
		for (auto &member_type : member_types) {
			BindLogicalType(member_type.second, catalog, schema);
		}
		// Generate new Union Type
		auto alias = type.GetAlias();
		auto modifiers = type.GetModifiersCopy();
		type = LogicalType::UNION(member_types);
		type.SetAlias(alias);
		type.SetModifiers(modifiers);
	} else if (type.id() == LogicalTypeId::USER) {
		auto user_type_name = UserType::GetTypeName(type);
		auto user_type_schema = UserType::GetSchema(type);
		auto user_type_mods = UserType::GetTypeModifiers(type);

		bind_type_modifiers_function_t user_bind_modifiers_func = nullptr;

		if (catalog) {
			// The search order is:
			// 1) In the explicitly set schema (my_schema.my_type)
			// 2) In the same schema as the table
			// 3) In the same catalog
			// 4) System catalog

			optional_ptr<CatalogEntry> entry = nullptr;
			if (!user_type_schema.empty()) {
				entry = entry_retriever.GetEntry(CatalogType::TYPE_ENTRY, *catalog, user_type_schema, user_type_name,
				                                 OnEntryNotFound::RETURN_NULL);
			}
			if (!IsValidUserType(entry)) {
				entry = entry_retriever.GetEntry(CatalogType::TYPE_ENTRY, *catalog, schema, user_type_name,
				                                 OnEntryNotFound::RETURN_NULL);
			}
			if (!IsValidUserType(entry)) {
				entry = entry_retriever.GetEntry(CatalogType::TYPE_ENTRY, *catalog, INVALID_SCHEMA, user_type_name,
				                                 OnEntryNotFound::RETURN_NULL);
			}
			if (!IsValidUserType(entry)) {
				entry = entry_retriever.GetEntry(CatalogType::TYPE_ENTRY, INVALID_CATALOG, INVALID_SCHEMA,
				                                 user_type_name, OnEntryNotFound::THROW_EXCEPTION);
			}
			auto &type_entry = entry->Cast<TypeCatalogEntry>();
			type = type_entry.user_type;
			user_bind_modifiers_func = type_entry.bind_modifiers;
		} else {
			string type_catalog = UserType::GetCatalog(type);
			string type_schema = UserType::GetSchema(type);

			BindSchemaOrCatalog(context, type_catalog, type_schema);
			auto entry = entry_retriever.GetEntry(CatalogType::TYPE_ENTRY, type_catalog, type_schema, user_type_name);
			auto &type_entry = entry->Cast<TypeCatalogEntry>();
			type = type_entry.user_type;
			user_bind_modifiers_func = type_entry.bind_modifiers;
		}

		BindLogicalType(type, catalog, schema);

		// Apply the type modifiers (if any)
		if (user_bind_modifiers_func) {
			// If an explicit bind_modifiers function was provided, use that to set the type modifier
			BindTypeModifiersInput input {context, type, user_type_mods};
			type = user_bind_modifiers_func(input);
		} else if (type.HasModifiers()) {
			// If the type already has modifiers, try to replace them with the user-provided ones if they are compatible
			// This enables registering custom types with "default" type modifiers that can be overridden, without
			// having to provide a custom bind_modifiers function
			auto type_mods_size = type.GetModifiers()->size();

			// Are we trying to pass more type modifiers than the type has?
			if (user_type_mods.size() > type_mods_size) {
				throw BinderException(
				    "Cannot apply '%d' type modifier(s) to type '%s' taking at most '%d' type modifier(s)",
				    user_type_mods.size(), user_type_name, type_mods_size);
			}

			// Deep copy the type so that we can replace the type modifiers
			type = type.DeepCopy();

			// Re-fetch the type modifiers now that we've deduplicated the ExtraTypeInfo
			auto &type_mods = *type.GetModifiers();

			// Replace them in order, casting if necessary
			for (idx_t i = 0; i < MinValue(type_mods.size(), user_type_mods.size()); i++) {
				auto &type_mod = type_mods[i];
				auto user_type_mod = user_type_mods[i];
				if (type_mod.type() == user_type_mod.type()) {
					type_mod = std::move(user_type_mod);
				} else if (user_type_mod.DefaultTryCastAs(type_mod.type())) {
					type_mod = std::move(user_type_mod);
				} else {
					throw BinderException("Cannot apply type modifier '%s' to type '%s', expected value of type '%s'",
					                      user_type_mod.ToString(), user_type_name, type_mod.type().ToString());
				}
			}
		} else if (!user_type_mods.empty()) {
			// We're trying to pass type modifiers to a type that doesnt have any
			throw BinderException("Type '%s' does not take any type modifiers", user_type_name);
		}
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
		auto &unique = constr->Cast<UniqueConstraint>();
		if (find_primary_key && !unique.IsPrimaryKey()) {
			continue;
		}
		found_constraint = true;

		vector<string> pk_names;
		if (unique.HasIndex()) {
			pk_names.push_back(columns.GetColumn(LogicalIndex(unique.GetIndex())).Name());
		} else {
			pk_names = unique.GetColumnNames();
		}
		if (find_primary_key) {
			// found matching primary key
			if (pk_names.size() != fk.fk_columns.size()) {
				auto pk_name_str = StringUtil::Join(pk_names, ",");
				auto fk_name_str = StringUtil::Join(fk.fk_columns, ",");
				throw BinderException(
				    "Failed to create foreign key: number of referencing (%s) and referenced columns (%s) differ",
				    fk_name_str, pk_name_str);
			}
			fk.pk_columns = pk_names;
			return;
		}
		if (pk_names.size() != fk.fk_columns.size()) {
			// the number of referencing and referenced columns for foreign keys must be the same
			continue;
		}
		bool equals = true;
		for (idx_t i = 0; i < fk.pk_columns.size(); i++) {
			if (!StringUtil::CIEquals(fk.pk_columns[i], pk_names[i])) {
				equals = false;
				break;
			}
		}
		if (!equals) {
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
		auto &column_ref = expr.Cast<ColumnRefExpression>();
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
			auto &constraint = constr->Cast<CheckConstraint>();
			auto &expr = constraint.expression;
			bool contains_generated_column = false;
			ExpressionContainsGeneratedColumn(*expr, generated_columns, contains_generated_column);
			if (contains_generated_column) {
				return true;
			}
			break;
		}
		case ConstraintType::NOT_NULL: {
			auto &constraint = constr->Cast<NotNullConstraint>();
			if (table_info.columns.GetColumn(constraint.index).Generated()) {
				return true;
			}
			break;
		}
		case ConstraintType::UNIQUE: {
			auto &constraint = constr->Cast<UniqueConstraint>();
			if (!constraint.HasIndex()) {
				for (auto &col : constraint.GetColumnNames()) {
					if (generated_columns.count(col)) {
						return true;
					}
				}
			} else {
				if (table_info.columns.GetColumn(constraint.GetIndex()).Generated()) {
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

unique_ptr<LogicalOperator> DuckCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                         TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	D_ASSERT(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &base = stmt.info->Cast<CreateIndexInfo>();

	auto &get = plan->Cast<LogicalGet>();
	// bind the index expressions
	IndexBinder index_binder(binder, binder.context);
	auto &dependencies = base.dependencies;
	auto &catalog = Catalog::GetCatalog(binder.context, base.catalog);
	index_binder.SetCatalogLookupCallback([&dependencies, &catalog](CatalogEntry &entry) {
		if (&catalog != &entry.ParentCatalog()) {
			// Don't register any cross-catalog dependencies
			return;
		}
		dependencies.AddDependency(entry);
	});
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(base.expressions.size());
	for (auto &expr : base.expressions) {
		expressions.push_back(index_binder.Bind(expr));
	}

	auto create_index_info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(stmt.info));
	auto &column_ids = get.GetColumnIds();
	for (auto &column_id : column_ids) {
		if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
			throw BinderException("Cannot create an index on the rowid!");
		}
		create_index_info->scan_types.push_back(get.returned_types[column_id]);
	}
	create_index_info->scan_types.emplace_back(LogicalType::ROW_TYPE);
	create_index_info->names = get.names;
	create_index_info->column_ids = column_ids;
	create_index_info->schema = table.schema.name;
	auto &bind_data = get.bind_data->Cast<TableScanBindData>();
	bind_data.is_create_index = true;
	get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);

	// the logical CREATE INDEX also needs all fields to scan the referenced table
	auto result = make_uniq<LogicalCreateIndex>(std::move(create_index_info), std::move(expressions), table);
	result->children.push_back(std::move(plan));
	return std::move(result);
}

BoundStatement Binder::Bind(CreateStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	auto catalog_type = stmt.info->type;
	auto &properties = GetStatementProperties();
	switch (catalog_type) {
	case CatalogType::SCHEMA_ENTRY: {
		auto &base = stmt.info->Cast<CreateInfo>();
		auto catalog = BindCatalog(base.catalog);
		properties.RegisterDBModify(Catalog::GetCatalog(context, catalog), context);
		result.plan = make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_SCHEMA, std::move(stmt.info));
		break;
	}
	case CatalogType::VIEW_ENTRY: {
		auto &base = stmt.info->Cast<CreateViewInfo>();
		// bind the schema
		auto &schema = BindCreateSchema(*stmt.info);
		BindCreateViewInfo(base);
		result.plan = make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_VIEW, std::move(stmt.info), &schema);
		break;
	}
	case CatalogType::SEQUENCE_ENTRY: {
		auto &schema = BindCreateSchema(*stmt.info);
		result.plan =
		    make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_SEQUENCE, std::move(stmt.info), &schema);
		break;
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto &schema = BindCreateSchema(*stmt.info);
		result.plan =
		    make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_MACRO, std::move(stmt.info), &schema);
		break;
	}
	case CatalogType::MACRO_ENTRY: {
		auto &schema = BindCreateFunctionInfo(*stmt.info);
		auto logical_create =
		    make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_MACRO, std::move(stmt.info), &schema);
		result.plan = std::move(logical_create);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		auto &base = stmt.info->Cast<CreateIndexInfo>();

		// visit the table reference
		auto table_ref = make_uniq<BaseTableRef>();
		table_ref->catalog_name = base.catalog;
		table_ref->schema_name = base.schema;
		table_ref->table_name = base.table;

		auto bound_table = Bind(*table_ref);
		if (bound_table->type != TableReferenceType::BASE_TABLE) {
			throw BinderException("Can only create an index over a base table!");
		}
		auto &table_binding = bound_table->Cast<BoundBaseTableRef>();
		auto &table = table_binding.table;
		if (table.temporary) {
			stmt.info->temporary = true;
		}
		properties.RegisterDBModify(table.catalog, context);

		// create a plan over the bound table
		auto plan = CreatePlan(*bound_table);
		if (plan->type != LogicalOperatorType::LOGICAL_GET) {
			throw BinderException("Cannot create index on a view!");
		}
		result.plan = table.catalog.BindCreateIndex(*this, stmt, table, std::move(plan));
		break;
	}
	case CatalogType::TABLE_ENTRY: {
		auto &create_info = stmt.info->Cast<CreateTableInfo>();
		// If there is a foreign key constraint, resolve primary key column's index from primary key column's name
		reference_set_t<SchemaCatalogEntry> fk_schemas;
		for (idx_t i = 0; i < create_info.constraints.size(); i++) {
			auto &cond = create_info.constraints[i];
			if (cond->type != ConstraintType::FOREIGN_KEY) {
				continue;
			}
			auto &fk = cond->Cast<ForeignKeyConstraint>();
			if (fk.info.type != ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				continue;
			}
			D_ASSERT(fk.info.pk_keys.empty());
			D_ASSERT(fk.info.fk_keys.empty());
			FindForeignKeyIndexes(create_info.columns, fk.fk_columns, fk.info.fk_keys);
			if (StringUtil::CIEquals(create_info.table, fk.info.table)) {
				// self-referential foreign key constraint
				fk.info.type = ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE;
				FindMatchingPrimaryKeyColumns(create_info.columns, create_info.constraints, fk);
				FindForeignKeyIndexes(create_info.columns, fk.pk_columns, fk.info.pk_keys);
				CheckForeignKeyTypes(create_info.columns, create_info.columns, fk);
			} else {
				// have to resolve referenced table
				auto table_entry =
				    entry_retriever.GetEntry(CatalogType::TABLE_ENTRY, INVALID_CATALOG, fk.info.schema, fk.info.table);
				auto &pk_table_entry_ptr = table_entry->Cast<TableCatalogEntry>();
				fk_schemas.insert(pk_table_entry_ptr.schema);
				FindMatchingPrimaryKeyColumns(pk_table_entry_ptr.GetColumns(), pk_table_entry_ptr.GetConstraints(), fk);
				FindForeignKeyIndexes(pk_table_entry_ptr.GetColumns(), fk.pk_columns, fk.info.pk_keys);
				CheckForeignKeyTypes(pk_table_entry_ptr.GetColumns(), create_info.columns, fk);
				auto &storage = pk_table_entry_ptr.GetStorage();

				if (!storage.HasForeignKeyIndex(fk.info.pk_keys, ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE)) {
					auto fk_column_names = StringUtil::Join(fk.pk_columns, ",");
					throw BinderException("Failed to create foreign key on %s(%s): no UNIQUE or PRIMARY KEY constraint "
					                      "present on these columns",
					                      pk_table_entry_ptr.name, fk_column_names);
				}
			}
			D_ASSERT(fk.info.pk_keys.size() == fk.info.fk_keys.size());
			D_ASSERT(fk.info.pk_keys.size() == fk.pk_columns.size());
			D_ASSERT(fk.info.fk_keys.size() == fk.fk_columns.size());
		}
		if (AnyConstraintReferencesGeneratedColumn(create_info)) {
			throw BinderException("Constraints on generated columns are not supported yet");
		}
		auto bound_info = BindCreateTableInfo(std::move(stmt.info));
		auto root = std::move(bound_info->query);
		for (auto &fk_schema : fk_schemas) {
			if (&fk_schema.get() != &bound_info->schema) {
				throw BinderException("Creating foreign keys across different schemas or catalogs is not supported");
			}
		}

		// create the logical operator
		auto &schema = bound_info->schema;
		auto create_table = make_uniq<LogicalCreateTable>(schema, std::move(bound_info));
		if (root) {
			// CREATE TABLE AS
			properties.return_type = StatementReturnType::CHANGED_ROWS;
			create_table->children.push_back(std::move(root));
		}
		result.plan = std::move(create_table);
		break;
	}
	case CatalogType::TYPE_ENTRY: {
		auto &schema = BindCreateSchema(*stmt.info);
		auto &create_type_info = stmt.info->Cast<CreateTypeInfo>();
		result.plan = make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_TYPE, std::move(stmt.info), &schema);

		auto &catalog = Catalog::GetCatalog(context, create_type_info.catalog);
		auto &dependencies = create_type_info.dependencies;
		auto dependency_callback = [&dependencies, &catalog](CatalogEntry &entry) {
			if (&catalog != &entry.ParentCatalog()) {
				// Don't register any cross-catalog dependencies
				return;
			}
			dependencies.AddDependency(entry);
		};
		if (create_type_info.query) {
			// CREATE TYPE mood AS ENUM (SELECT 'happy')
			auto query_obj = Bind(*create_type_info.query);
			auto query = std::move(query_obj.plan);
			create_type_info.query.reset();

			auto &sql_types = query_obj.types;
			if (sql_types.size() != 1) {
				// add cast expression?
				throw BinderException("The query must return a single column");
			}
			if (sql_types[0].id() != LogicalType::VARCHAR) {
				// push a projection casting to varchar
				vector<unique_ptr<Expression>> select_list;
				auto ref = make_uniq<BoundColumnRefExpression>(sql_types[0], query->GetColumnBindings()[0]);
				auto cast_expr = BoundCastExpression::AddCastToType(context, std::move(ref), LogicalType::VARCHAR);
				select_list.push_back(std::move(cast_expr));
				auto proj = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(select_list));
				proj->AddChild(std::move(query));
				query = std::move(proj);
			}

			result.plan->AddChild(std::move(query));
		} else if (create_type_info.type.id() == LogicalTypeId::USER) {
			SetCatalogLookupCallback(dependency_callback);
			// two cases:
			// 1: create a type with a non-existent type as source, Binder::BindLogicalType(...) will throw exception.
			// 2: create a type alias with a custom type.
			// eg. CREATE TYPE a AS INT; CREATE TYPE b AS a;
			// We set b to be an alias for the underlying type of a
			auto type_entry_p = entry_retriever.GetEntry(CatalogType::TYPE_ENTRY, schema.catalog.GetName(), schema.name,
			                                             UserType::GetTypeName(create_type_info.type));
			D_ASSERT(type_entry_p);
			auto &type_entry = type_entry_p->Cast<TypeCatalogEntry>();
			create_type_info.type = type_entry.user_type;
		} else {
			SetCatalogLookupCallback(dependency_callback);
			// This is done so that if the type contains a USER type,
			// we register this dependency
			auto preserved_type = create_type_info.type;
			BindLogicalType(create_type_info.type);
			create_type_info.type = preserved_type;
		}
		break;
	}
	case CatalogType::SECRET_ENTRY: {
		CatalogTransaction transaction = CatalogTransaction(Catalog::GetSystemCatalog(context), context);
		properties.return_type = StatementReturnType::QUERY_RESULT;
		return SecretManager::Get(context).BindCreateSecret(transaction, stmt.info->Cast<CreateSecretInfo>());
	}
	default:
		throw InternalException("Unrecognized type!");
	}
	properties.return_type = StatementReturnType::NOTHING;
	properties.allow_stream_result = false;
	return result;
}

} // namespace duckdb
