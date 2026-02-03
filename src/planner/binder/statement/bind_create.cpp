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
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

void Binder::BindSchemaOrCatalog(CatalogEntryRetriever &retriever, string &catalog, string &schema) {
	auto &context = retriever.GetContext();
	if (schema.empty()) {
		return;
	}
	if (!catalog.empty()) {
		return;
	}
	// schema is specified - but catalog is not
	// try searching for the catalog instead
	auto &db_manager = DatabaseManager::Get(context);
	auto database = db_manager.GetDatabase(context, schema);
	if (!database) {
		//! No database by that name was found
		return;
	}
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
			    "Ambiguous reference to catalog or schema \"%s\" - use a fully qualified path like \"%s.%s\"", schema,
			    catalog_name, schema);
		}
	}
	catalog = schema;
	schema = string();
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
		properties.RegisterDBModify(schema_obj.catalog, context, DatabaseModificationType::CREATE_CATALOG_ENTRY);
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

void Binder::BindView(ClientContext &context, const SelectStatement &stmt, const string &catalog_name,
                      const string &schema_name, optional_ptr<LogicalDependencyList> dependencies,
                      const vector<string> &aliases, vector<LogicalType> &result_types, vector<string> &result_names) {
	auto view_binder = Binder::CreateBinder(context);
	auto &catalog = Catalog::GetCatalog(context, catalog_name);

	if (dependencies) {
		view_binder->SetCatalogLookupCallback([&dependencies, &catalog](CatalogEntry &entry) {
			if (&catalog != &entry.ParentCatalog()) {
				// Don't register dependencies between catalogs
				return;
			}
			dependencies->AddDependency(entry);
		});
	}
	view_binder->can_contain_nulls = true;

	auto view_search_path = view_binder->GetSearchPath(catalog, schema_name);
	view_binder->entry_retriever.SetSearchPath(std::move(view_search_path));

	auto copy = stmt.Copy();
	auto query_node = view_binder->Bind(*copy);
	if (aliases.size() > query_node.names.size()) {
		throw BinderException("More VIEW aliases than columns in query result");
	}
	result_types = query_node.types;
	result_names = query_node.names;
}

void Binder::BindCreateViewInfo(CreateViewInfo &base) {
	optional_ptr<LogicalDependencyList> dependencies;
	if (Settings::Get<EnableViewDependenciesSetting>(context)) {
		dependencies = base.dependencies;
	}
	BindView(context, *base.query, base.catalog, base.schema, dependencies, base.aliases, base.types, base.names);
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

		// Constant-fold all default parameter expressions
		for (auto &it : function->default_parameters) {
			auto &param_name = it.first;
			auto &param_expr = it.second;

			if (param_expr->type == ExpressionType::VALUE_CONSTANT) {
				continue;
			}

			ConstantBinder binder(*this, context, StringUtil::Format("Default value for parameter '%s'", param_name));
			auto default_expr = param_expr->Copy();
			auto bound_default = binder.Bind(default_expr);
			if (!bound_default->IsFoldable()) {
				auto msg = StringUtil::Format("Default value '%s' for parameter '%s' is not a constant expression.",
				                              param_expr->ToString(), param_name);
				throw BinderException(msg);
			}

			auto default_val = ExpressionExecutor::EvaluateScalar(context, *bound_default);

			// Save this back as a constant expression
			auto const_expr = make_uniq<ConstantExpression>(default_val);
			const_expr->alias = param_name;
			it.second = std::move(const_expr);
		}

		// Resolve any user type arguments
		for (idx_t param_idx = 0; param_idx < function->types.size(); param_idx++) {
			auto &type = function->types[param_idx];
			if (type.id() == LogicalTypeId::UNKNOWN) {
				continue;
			}
			if (type.id() == LogicalTypeId::UNBOUND) {
				BindLogicalType(type);
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
		const auto should_create_dependencies = Settings::Get<EnableMacroDependenciesSetting>(context);
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

LogicalType Binder::BindLogicalTypeInternal(const unique_ptr<ParsedExpression> &type_expr) {
	ConstantBinder binder(*this, context, "Type binding");
	auto copy = type_expr->Copy();
	auto expr = binder.Bind(copy);

	if (!expr->IsFoldable()) {
		throw BinderException(*type_expr, "Type expression is not constant");
	}

	if (expr->return_type != LogicalTypeId::TYPE) {
		throw BinderException(*type_expr, "Expected a type returning expression, but got expression of type '%s'",
		                      expr->return_type.ToString());
	}

	// Shortcut for constant expressions
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		auto &const_expr = expr->Cast<BoundConstantExpression>();
		return TypeValue::GetType(const_expr.value);
	}

	// Else, evaluate the type expression
	auto type_value = ExpressionExecutor::EvaluateScalar(context, *expr);
	D_ASSERT(type_value.type().id() == LogicalTypeId::TYPE);
	return TypeValue::GetType(type_value);
}

void Binder::BindLogicalType(LogicalType &type) {
	// Check if we need to bind this type at all
	if (!TypeVisitor::Contains(type, LogicalTypeId::UNBOUND)) {
		return;
	}

	// Replace all unbound types within the type
	//   Normally, the unbound type is the root type, but it can also be nested within other types if we e.g.
	//   alter-table and change a struct field.
	type = TypeVisitor::VisitReplace(type, [&](const LogicalType &ty) {
		if (ty.id() == LogicalTypeId::UNBOUND) {
			auto &type_expr = UnboundType::GetTypeExpression(ty);
			return BindLogicalTypeInternal(type_expr);
		}

		return ty;
	});
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
		properties.RegisterDBModify(Catalog::GetCatalog(context, catalog), context,
		                            DatabaseModificationType::CREATE_CATALOG_ENTRY);
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
		properties.RegisterDBModify(table.catalog, context, DatabaseModificationType::CREATE_INDEX);
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
		} else {
			SetCatalogLookupCallback(dependency_callback);
			// Bind the underlying type
			BindLogicalType(create_type_info.type);
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
