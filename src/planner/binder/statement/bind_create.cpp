#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/operator/logical_create.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

void Binder::BindSchemaOrCatalog(CatalogEntryRetriever &retriever, string &catalog, string &schema) {
	auto &context = retriever.GetContext();
	if (catalog.empty() && !schema.empty()) {
		// schema is specified - but catalog is not
		// try searching for the catalog instead
		auto &db_manager = DatabaseManager::Get(context);
		auto database = db_manager.GetDatabase(context, schema);
		if (database) {
			// we have a database with this name
			// check if there is a schema
			auto &search_path = retriever.GetSearchPath();
			auto catalog_names = search_path.GetCatalogsForSchema(schema);
			if (catalog_names.empty()) {
				catalog_names.push_back(DatabaseManager::GetDefaultDatabase(context));
			}
			for (auto &catalog_name : catalog_names) {
				auto catalog_ptr = Catalog::GetCatalogEntry(retriever, catalog_name);
				if (!catalog_ptr) {
					continue;
				}
				if (catalog_ptr->CheckAmbiguousCatalogOrSchema(context, schema)) {
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

void Binder::BindSchemaOrCatalog(ClientContext &context, string &catalog, string &schema) {
	CatalogEntryRetriever retriever(context);
	BindSchemaOrCatalog(retriever, catalog, schema);
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

void Binder::SearchSchema(CreateInfo &info) {
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
		info.schema = search_path->GetDefaultSchema(context, info.catalog);
	} else if (IsInvalidCatalog(info.catalog)) {
		info.catalog = search_path->GetDefaultCatalog(info.schema);
	}
	if (IsInvalidCatalog(info.catalog)) {
		info.catalog = DatabaseManager::GetDefaultDatabase(context);
	}
	if (!info.temporary) {
		// non-temporary create: not read only
		if (info.catalog == TEMP_CATALOG) {
			throw ParserException("Only TEMPORARY table names can use the \"%s\" catalog", std::string(TEMP_CATALOG));
		}
	} else {
		if (info.catalog != TEMP_CATALOG) {
			throw ParserException("TEMPORARY table names can *only* use the \"%s\" catalog", std::string(TEMP_CATALOG));
		}
	}
}

SchemaCatalogEntry &Binder::BindSchema(CreateInfo &info) {
	SearchSchema(info);
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

	bool should_create_dependencies = DBConfig::GetSetting<EnableViewDependenciesSetting>(context);
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

	auto view_search_path = GetSearchPath(catalog, base.schema);
	view_binder->entry_retriever.SetSearchPath(std::move(view_search_path));

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
	//! Set to identify exact matches in macro overloads
	struct VectorOfLogicalTypeHash {
		std::size_t operator()(const vector<LogicalType> &k) const {
			auto hash = std::hash<size_t>()(k.size());
			for (auto &type : k) {
				hash = CombineHash(hash, type.Hash());
			}
			return hash;
		}
	};

	struct VectorOfLogicalTypeEquality {
		bool operator()(const vector<LogicalType> &a, const vector<LogicalType> &b) const {
			if (a.size() != b.size()) {
				return false;
			}
			for (idx_t i = 0; i < a.size(); i++) {
				if (a[i] != b[i]) {
					return false;
				}
			}
			return true;
		}
	};

	using vector_of_logical_type_set_t =
	    unordered_set<vector<LogicalType>, VectorOfLogicalTypeHash, VectorOfLogicalTypeEquality>;

	// Bind the catalog/schema
	SearchSchema(info);
	auto &catalog = Catalog::GetCatalog(context, info.catalog);

	// Figure out if we can store typed macro parameters
	auto &attached = catalog.GetAttached();
	auto store_types = true;
	if (attached.HasStorageManager()) {
		// If DuckDB is used as a storage, we must check the version.
		auto &storage_manager = attached.GetStorageManager();
		const auto since = SerializationCompatibility::FromString("v1.4.0").serialization_version;
		store_types = info.temporary || attached.IsTemporary() || storage_manager.InMemory() ||
		              storage_manager.GetStorageVersion() >= since;
	}
	// try to bind each of the included functions
	vector_of_logical_type_set_t type_overloads;
	auto &base = info.Cast<CreateMacroInfo>();
	for (auto &function : base.macros) {
		if (!store_types) {
			for (const auto &type : function->types) {
				if (type.id() != LogicalTypeId::UNKNOWN) {
					string msg = "Typed macro parameters are only supported for storage versions v1.4.0 and higher.\n";
					msg += "Use an in-memory database, ATTACH with (STORAGE_VERSION v1.4.0), or create a TEMP macro";
					throw BinderException(msg);
				}
			}
		}

		if (info.type == CatalogType::MACRO_ENTRY) {
			auto &scalar_function = function->Cast<ScalarMacroFunction>();
			if (scalar_function.expression->HasParameter()) {
				throw BinderException("Parameter expressions within macro's are not supported!");
			}
		} else {
			D_ASSERT(info.type == CatalogType::TABLE_MACRO_ENTRY);
			auto &table_function = function->Cast<TableMacroFunction>();
			ParsedExpressionIterator::EnumerateQueryNodeChildren(
			    *table_function.query_node, [](unique_ptr<ParsedExpression> &child) {
				    if (child->HasParameter()) {
					    throw BinderException("Parameter expressions within macro's are not supported!");
				    }
			    });
		}

		// Resolve any user type arguments
		for (idx_t param_idx = 0; param_idx < function->types.size(); param_idx++) {
			auto &type = function->types[param_idx];
			if (type.id() == LogicalTypeId::UNKNOWN) {
				continue;
			}
			if (type.id() == LogicalTypeId::USER) {
				type = TransformStringToLogicalType(type.ToString(), context);
			}
			const auto &param_name = function->parameters[param_idx]->Cast<ColumnRefExpression>().GetColumnName();
			auto it = function->default_parameters.find(param_name);
			if (it != function->default_parameters.end()) {
				const auto &val_type = it->second->Cast<ConstantExpression>().value.type();
				if (CastFunctionSet::ImplicitCastCost(context, val_type, type) < 0) {
					auto msg =
					    StringUtil::Format("Default value '%s' for parameter '%s' cannot be implicitly cast to '%s'.",
					                       it->second->ToString(), param_name, type.ToString());
					throw BinderException(msg + " Please add an explicit type cast.");
				}
			}
		}

		vector<LogicalType> dummy_types;
		vector<string> dummy_names;
		// positional parameters
		for (idx_t param_idx = 0; param_idx < function->parameters.size(); param_idx++) {
			dummy_types.emplace_back(function->types.empty() ? LogicalType::UNKNOWN : function->types[param_idx]);
			dummy_names.push_back(function->parameters[param_idx]->Cast<ColumnRefExpression>().GetColumnName());
		}

		if (!type_overloads.insert(dummy_types).second) {
			throw BinderException(
			    "Ambiguity in macro overloads - macro %s() has multiple definitions with the same parameters",
			    base.name);
		}

		auto this_macro_binding = make_uniq<DummyBinding>(dummy_types, dummy_names, base.name);
		macro_binding = this_macro_binding.get();

		auto &dependencies = base.dependencies;
		const auto should_create_dependencies = DBConfig::GetSetting<EnableMacroDependenciesSetting>(context);
		const auto binder_callback = [&dependencies, &catalog](CatalogEntry &entry) {
			if (&catalog != &entry.ParentCatalog()) {
				// Don't register any cross-catalog dependencies
				return;
			}
			// Register any catalog entry required to bind the macro function
			dependencies.AddDependency(entry);
		};

		// bind it to verify the function was defined correctly
		ErrorData error;
		if (info.type == CatalogType::MACRO_ENTRY) {
			BoundSelectNode sel_node;
			BoundGroupInformation group_info;
			SelectBinder binder(*this, context, sel_node, group_info);
			if (should_create_dependencies) {
				binder.SetCatalogLookupCallback(binder_callback);
			}

			// create a copy of the expression because we do not want to alter the original
			auto expression = function->Cast<ScalarMacroFunction>().expression->Copy();
			ExpressionBinder::QualifyColumnNames(*this, expression);
			try {
				error = binder.Bind(expression, 0, false);
				if (error.HasError()) {
					error.Throw();
				}
			} catch (const std::exception &ex) {
				error = ErrorData(ex);
			}
		} else {
			D_ASSERT(info.type == CatalogType::TABLE_MACRO_ENTRY);
			auto dummy_binder = CreateBinder(context, this);
			if (should_create_dependencies) {
				dummy_binder->SetCatalogLookupCallback(binder_callback);
			}

			// create a copy of the query node because we do not want to alter the original
			auto query_node = function->Cast<TableMacroFunction>().query_node->Copy();
			ParsedExpressionIterator::EnumerateQueryNodeChildren(
			    *query_node, [&dummy_binder](unique_ptr<ParsedExpression> &child) {
				    ExpressionBinder::QualifyColumnNames(*dummy_binder, child);
			    });
			try {
				dummy_binder->Bind(*query_node);
			} catch (const std::exception &ex) {
				error = ErrorData(ex);
			}
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

LogicalType Binder::BindLogicalTypeInternal(const LogicalType &type, optional_ptr<Catalog> catalog,
                                            const string &schema) {
	if (type.id() != LogicalTypeId::USER) {
		// Nested type, make sure to bind any nested user types recursively
		LogicalType result;
		switch (type.id()) {
		case LogicalTypeId::LIST: {
			auto child_type = BindLogicalTypeInternal(ListType::GetChildType(type), catalog, schema);
			result = LogicalType::LIST(child_type);
			break;
		}
		case LogicalTypeId::MAP: {
			auto key_type = BindLogicalTypeInternal(MapType::KeyType(type), catalog, schema);
			auto value_type = BindLogicalTypeInternal(MapType::ValueType(type), catalog, schema);
			result = LogicalType::MAP(std::move(key_type), std::move(value_type));
			break;
		}
		case LogicalTypeId::ARRAY: {
			auto child_type = BindLogicalTypeInternal(ArrayType::GetChildType(type), catalog, schema);
			auto array_size = ArrayType::GetSize(type);
			result = LogicalType::ARRAY(child_type, array_size);
			break;
		}
		case LogicalTypeId::STRUCT: {
			auto child_types = StructType::GetChildTypes(type);
			child_list_t<LogicalType> new_child_types;
			for (auto &entry : child_types) {
				new_child_types.emplace_back(entry.first, BindLogicalTypeInternal(entry.second, catalog, schema));
			}
			result = LogicalType::STRUCT(std::move(new_child_types));
			break;
		}
		case LogicalTypeId::UNION: {
			child_list_t<LogicalType> member_types;
			for (idx_t i = 0; i < UnionType::GetMemberCount(type); i++) {
				auto child_type = BindLogicalTypeInternal(UnionType::GetMemberType(type, i), catalog, schema);
				member_types.emplace_back(UnionType::GetMemberName(type, i), std::move(child_type));
			}
			result = LogicalType::UNION(std::move(member_types));
			break;
		}
		default:
			return type;
		}

		// Set the alias and extension info back
		result.SetAlias(type.GetAlias());
		auto ext_info = type.HasExtensionInfo() ? make_uniq<ExtensionTypeInfo>(*type.GetExtensionInfo()) : nullptr;
		result.SetExtensionInfo(std::move(ext_info));
		return result;
	}

	// User type, bind the user type
	auto user_type_name = UserType::GetTypeName(type);
	auto user_type_schema = UserType::GetSchema(type);
	auto user_type_mods = UserType::GetTypeModifiers(type);

	bind_logical_type_function_t user_bind_modifiers_func = nullptr;

	LogicalType result;
	EntryLookupInfo type_lookup(CatalogType::TYPE_ENTRY, user_type_name);
	if (catalog) {
		// The search order is:
		// 1) In the explicitly set schema (my_schema.my_type)
		// 2) In the same schema as the table
		// 3) In the same catalog
		// 4) System catalog

		optional_ptr<CatalogEntry> entry = nullptr;
		if (!user_type_schema.empty()) {
			entry = entry_retriever.GetEntry(*catalog, user_type_schema, type_lookup, OnEntryNotFound::RETURN_NULL);
		}
		if (!IsValidUserType(entry)) {
			entry = entry_retriever.GetEntry(*catalog, schema, type_lookup, OnEntryNotFound::RETURN_NULL);
		}
		if (!IsValidUserType(entry)) {
			entry = entry_retriever.GetEntry(*catalog, INVALID_SCHEMA, type_lookup, OnEntryNotFound::RETURN_NULL);
		}
		if (!IsValidUserType(entry)) {
			entry = entry_retriever.GetEntry(INVALID_CATALOG, INVALID_SCHEMA, type_lookup,
			                                 OnEntryNotFound::THROW_EXCEPTION);
		}
		auto &type_entry = entry->Cast<TypeCatalogEntry>();
		result = type_entry.user_type;
		user_bind_modifiers_func = type_entry.bind_function;
	} else {
		string type_catalog = UserType::GetCatalog(type);
		string type_schema = UserType::GetSchema(type);

		BindSchemaOrCatalog(context, type_catalog, type_schema);
		auto entry = entry_retriever.GetEntry(type_catalog, type_schema, type_lookup);
		auto &type_entry = entry->Cast<TypeCatalogEntry>();
		result = type_entry.user_type;
		user_bind_modifiers_func = type_entry.bind_function;
	}

	// Now we bind the inner user type
	BindLogicalType(result, catalog, schema);

	// Apply the type modifiers (if any)
	if (user_bind_modifiers_func) {
		// If an explicit bind_modifiers function was provided, use that to construct the type

		BindLogicalTypeInput input {context, result, user_type_mods};
		result = user_bind_modifiers_func(input);
	} else {
		if (!user_type_mods.empty()) {
			throw BinderException("Type '%s' does not take any type modifiers", user_type_name);
		}
	}
	return result;
}

void Binder::BindLogicalType(LogicalType &type, optional_ptr<Catalog> catalog, const string &schema) {
	// check if we need to bind this type at all
	if (!TypeVisitor::Contains(type, LogicalTypeId::USER)) {
		return;
	}
	type = BindLogicalTypeInternal(type, catalog, schema);
}

unique_ptr<LogicalOperator> DuckCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                         TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	D_ASSERT(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto create_index_info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(stmt.info));
	IndexBinder index_binder(binder, binder.context);
	return index_binder.BindCreateIndex(binder.context, std::move(create_index_info), table, std::move(plan), nullptr);
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
		auto &schema = BindCreateFunctionInfo(*stmt.info);
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
		auto &create_index_info = stmt.info->Cast<CreateIndexInfo>();

		// Plan the table scan.
		TableDescription table_description(create_index_info.catalog, create_index_info.schema,
		                                   create_index_info.table);
		auto table_ref = make_uniq<BaseTableRef>(table_description);
		auto bound_table = Bind(*table_ref);
		auto plan = std::move(bound_table.plan);
		if (plan->type != LogicalOperatorType::LOGICAL_GET) {
			throw BinderException("can only create an index on a base table");
		}
		auto &get = plan->Cast<LogicalGet>();
		auto table_ptr = get.GetTable();
		if (!table_ptr) {
			throw BinderException("can only create an index on a base table");
		}

		auto &table = *table_ptr;
		if (table.temporary) {
			stmt.info->temporary = true;
		}
		properties.RegisterDBModify(table.catalog, context);
		result.plan = table.catalog.BindCreateIndex(*this, stmt, table, std::move(plan));
		break;
	}
	case CatalogType::TABLE_ENTRY: {
		auto bound_info = BindCreateTableInfo(std::move(stmt.info));
		auto root = std::move(bound_info->query);

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

			EntryLookupInfo type_lookup(CatalogType::TYPE_ENTRY, UserType::GetTypeName(create_type_info.type));
			auto type_entry_p = entry_retriever.GetEntry(schema.catalog.GetName(), schema.name, type_lookup);
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

		auto &info = stmt.info->Cast<CreateSecretInfo>();

		// We need to execute all expressions in the CreateSecretInfo to construct a CreateSecretInput
		ConstantBinder default_binder(*this, context, "Secret Parameter");

		string provider_string, type_string;
		vector<string> scope_strings;

		if (info.provider) {
			auto bound_provider = default_binder.Bind(info.provider);
			if (bound_provider->HasParameter()) {
				throw InvalidInputException("Create Secret expressions can not have parameters!");
			}
			provider_string =
			    StringUtil::Lower(ExpressionExecutor::EvaluateScalar(context, *bound_provider, true).ToString());
		}
		if (info.type) {
			auto bound_type = default_binder.Bind(info.type);
			if (bound_type->HasParameter()) {
				throw InvalidInputException("Create Secret expressions can not have parameters!");
			}
			type_string = StringUtil::Lower(ExpressionExecutor::EvaluateScalar(context, *bound_type, true).ToString());
		}
		if (info.scope) {
			auto bound_scope = default_binder.Bind(info.scope);
			if (bound_scope->HasParameter()) {
				throw InvalidInputException("Create Secret expressions can not have parameters!");
			}
			// Execute all scope expressions
			Value scope = ExpressionExecutor::EvaluateScalar(context, *bound_scope, true);
			if (scope.type() == LogicalType::VARCHAR) {
				scope_strings.push_back(scope.ToString());
			} else if (scope.type() == LogicalType::LIST(LogicalType::VARCHAR)) {
				for (const auto &item : ListValue::GetChildren(scope)) {
					scope_strings.push_back(item.GetValue<string>());
				}
			} else if (scope.type().InternalType() == PhysicalType::STRUCT) {
				// struct expression with empty keys is also allowed for backwards compatibility to when the create
				// secret statement would be parsed differently: this allows CREATE SECRET (TYPE x, SCOPE ('bla',
				// 'bloe'))
				for (const auto &child : StructValue::GetChildren(scope)) {
					if (child.type() != LogicalType::VARCHAR) {
						throw InvalidInputException(
						    "Invalid input to scope parameter of create secret: only struct of VARCHARs is allowed");
					}
					scope_strings.push_back(child.GetValue<string>());
				}
			} else {
				throw InvalidInputException("Create Secret scope must be of type VARCHAR or LIST(VARCHAR)");
			}
		}

		// Execute all options expressions
		case_insensitive_map_t<Value> bound_options;
		for (auto &option : info.options) {
			auto bound_value = default_binder.Bind(option.second);
			if (bound_value->HasParameter()) {
				throw InvalidInputException("Create Secret expressions can not have parameters!");
			}
			bound_options.insert({option.first, ExpressionExecutor::EvaluateScalar(context, *bound_value, true)});
		}

		CreateSecretInput create_secret_input {type_string,   provider_string, info.storage_type, info.name,
		                                       scope_strings, bound_options,   info.on_conflict,  info.persist_type};

		return SecretManager::Get(context).BindCreateSecret(transaction, create_secret_input);
	}
	default:
		throw InternalException("Unrecognized type!");
	}
	properties.return_type = StatementReturnType::NOTHING;
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	return result;
}

} // namespace duckdb
