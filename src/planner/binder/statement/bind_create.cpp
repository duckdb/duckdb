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
#include "duckdb/planner/operator/logical_create_matview.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/parsed_data/bound_create_function_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

namespace duckdb {

SchemaCatalogEntry *Binder::BindSchema(CreateInfo &info) {
	if (info.schema.empty()) {
		info.schema = info.temporary ? TEMP_SCHEMA : ClientData::Get(context).catalog_search_path->GetDefault();
	}

	if (!info.temporary) {
		// non-temporary create: not read only
		if (info.schema == TEMP_SCHEMA) {
			throw ParserException("Only TEMPORARY table names can use the \"temp\" schema");
		}
		properties.read_only = false;
	} else {
		if (info.schema != TEMP_SCHEMA) {
			throw ParserException("TEMPORARY table names can *only* use the \"%s\" schema", TEMP_SCHEMA);
		}
	}
	// fetch the schema in which we want to create the object
	auto schema_obj = Catalog::GetCatalog(context).GetSchema(context, info.schema);
	D_ASSERT(schema_obj->type == CatalogType::SCHEMA_ENTRY);
	info.schema = schema_obj->name;
	return schema_obj;
}

void Binder::BindCreateViewInfo(CreateViewInfo &base) {
	// bind the view as if it were a query so we can catch errors
	// note that we bind the original, and replace the original with a copy
	// this is because the original has
	this->can_contain_nulls = true;

	auto copy = base.query->Copy();
	auto query_node = Bind(*base.query);
	base.query = unique_ptr_cast<SQLStatement, SelectStatement>(move(copy));
	if (base.aliases.size() > query_node.names.size()) {
		throw BinderException("More VIEW aliases than columns in query result");
	}
	// fill up the aliases with the remaining names of the bound query
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
	auto this_macro_binding = make_unique<MacroBinding>(dummy_types, dummy_names, base.name);
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

	return BindSchema(info);
}

void Binder::BindLogicalType(ClientContext &context, LogicalType &type, const string &schema) {
	if (type.id() == LogicalTypeId::LIST) {
		auto child_type = ListType::GetChildType(type);
		BindLogicalType(context, child_type, schema);
		type = LogicalType::LIST(child_type);
	} else if (type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::MAP) {
		auto child_types = StructType::GetChildTypes(type);
		for (auto &child_type : child_types) {
			BindLogicalType(context, child_type.second, schema);
		}
		// Generate new Struct/Map Type
		if (type.id() == LogicalTypeId::STRUCT) {
			type = LogicalType::STRUCT(child_types);
		} else {
			type = LogicalType::MAP(child_types);
		}
	} else if (type.id() == LogicalTypeId::USER) {
		auto &catalog = Catalog::GetCatalog(context);
		type = catalog.GetType(context, schema, UserType::GetTypeName(type));
	} else if (type.id() == LogicalTypeId::ENUM) {
		auto &enum_type_name = EnumType::GetTypeName(type);
		auto enum_type_catalog = (TypeCatalogEntry *)context.db->GetCatalog().GetEntry(context, CatalogType::TYPE_ENTRY,
		                                                                               schema, enum_type_name, true);
		EnumType::SetCatalog(type, enum_type_catalog);
	}
}

static void FindMatchingPrimaryKeyColumns(vector<unique_ptr<Constraint>> &constraints, ForeignKeyConstraint &fk) {
	if (!fk.pk_columns.empty()) {
		return;
	}
	// find the matching primary key constraint
	for (auto &constr : constraints) {
		if (constr->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = (UniqueConstraint &)*constr;
		if (!unique.is_primary_key) {
			continue;
		}
		idx_t column_count;
		if (unique.index != DConstants::INVALID_INDEX) {
			fk.info.pk_keys.push_back(unique.index);
			column_count = 1;
		} else {
			fk.pk_columns = unique.columns;
			column_count = unique.columns.size();
		}
		if (column_count != fk.fk_columns.size()) {
			throw BinderException("The number of referencing and referenced columns for foreign keys must be the same");
		}
		return;
	}
	throw BinderException("there is no primary key for referenced table \"%s\"", fk.info.table);
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
		auto schema = BindSchema(*stmt.info);
		BindCreateViewInfo(base);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_VIEW, move(stmt.info), schema);
		break;
	}
	case CatalogType::SEQUENCE_ENTRY: {
		auto schema = BindSchema(*stmt.info);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_SEQUENCE, move(stmt.info), schema);
		break;
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto schema = BindSchema(*stmt.info);
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
		// this gives us a logical table scan
		// we take the required columns from here
		// create the logical operator
		result.plan = make_unique<LogicalCreateIndex>(*table, get.column_ids, move(expressions),
		                                              unique_ptr_cast<CreateInfo, CreateIndexInfo>(move(stmt.info)));
		break;
	}
	case CatalogType::MATVIEW_ENTRY:
	case CatalogType::TABLE_ENTRY: {
        BindTable(catalog_type, result, stmt);
	}
	case CatalogType::TYPE_ENTRY: {
		auto schema = BindSchema(*stmt.info);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_TYPE, move(stmt.info), schema);
		break;
	}
	default:
		throw Exception("Unrecognized type!");
	}
	properties.allow_stream_result = false;
	return result;
}

void Binder::BindTable(CatalogType catalog_type, BoundStatement &result, CreateStatement &stmt) {
    // If there is a foreign key constraint, resolve primary key column's index from primary key column's name
    auto &create_info = (CreateTableInfo &)*stmt.info;
    auto &catalog = Catalog::GetCatalog(context);
    for (idx_t i = 0; i < create_info.constraints.size(); i++) {
        auto &cond = create_info.constraints[i];
        if (cond->type != ConstraintType::FOREIGN_KEY) {
            continue;
        }
        auto &fk = (ForeignKeyConstraint &)*cond;
        if (fk.info.type != ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
            continue;
        }
        D_ASSERT(fk.info.pk_keys.empty() && !fk.pk_columns.empty());
        if (create_info.table == fk.info.table) {
            fk.info.type = ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE;
        } else {
            // have to resolve referenced table
            auto pk_table_entry_ptr = catalog.GetEntry<TableCatalogEntry>(context, fk.info.schema, fk.info.table);
            D_ASSERT(!fk.pk_columns.empty() && fk.info.pk_keys.empty());
            for (auto &keyname : fk.pk_columns) {
                auto entry = pk_table_entry_ptr->name_map.find(keyname);
                if (entry == pk_table_entry_ptr->name_map.end()) {
                    throw BinderException("column \"%s\" named in key does not exist", keyname);
                }
                fk.info.pk_keys.push_back(entry->second);
            }
            auto index = pk_table_entry_ptr->storage->info->indexes.FindForeignKeyIndex(
                fk.info.pk_keys, ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE);
            if (!index) {
                auto fk_column_names = StringUtil::Join(fk.pk_columns, ",");
                throw BinderException("Failed to create foreign key on %s(%s): no UNIQUE or PRIMARY KEY constraint "
                                      "present on these columns",
                                      pk_table_entry_ptr->name, fk_column_names);
            }
        }
    }
    // We first check if there are any user types, if yes we check to which custom types they refer.
    auto bound_info = BindCreateTableInfo(move(stmt.info));
    auto root = move(bound_info->query);

    // create the logical operator
    auto &schema = bound_info->schema;

    switch (catalog_type) {
    case CatalogType::TABLE_ENTRY: {
        auto create_table = make_unique<LogicalCreateTable>(schema, move(bound_info));
        if (root) {
            create_table->children.push_back(move(root));
        }
        result.plan = move(create_table);
        break;
    }
    case CatalogType::MATVIEW_ENTRY: {
        auto create_matview = make_unique<LogicalCreateMatView>(schema, move(bound_info));
        if (root) {
            create_matview->children.push_back(move(root));
        }
        result.plan = move(create_matview);
        break;
    }
    default:
        throw BinderException("Failed to bind table or matview %s", catalog_type);
    }
}

} // namespace duckdb
