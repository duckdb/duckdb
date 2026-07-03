#include "duckdb/parser/peg/ast/add_column_entry.hpp"
#include "duckdb/parser/peg/ast/column_constraint_entry.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/parsed_data/alter_database_info.hpp"
#include "duckdb/parser/statement/multi_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformAlterStatement(PEGTransformer &transformer,
                                                                        unique_ptr<AlterInfo> alter_options) {
	auto result = make_uniq<AlterStatement>();
	result->info = std::move(alter_options);
	if (result->info->type != AlterType::ALTER_TABLE) {
		return std::move(result);
	}
	auto &alter_table = result->info->Cast<AlterTableInfo>();
	if (alter_table.alter_table_type != AlterTableType::ADD_COLUMN) {
		return std::move(result);
	}
	auto &add_column = alter_table.Cast<AddColumnInfo>();
	if (!add_column.new_column.HasDefaultValue() ||
	    add_column.new_column.DefaultValue().GetExpressionClass() == ExpressionClass::CONSTANT) {
		return std::move(result);
	}
	auto &column_entry = add_column.new_column;
	auto null_column = column_entry.Copy();
	null_column.SetDefaultValue(make_uniq<ConstantExpression>(ConstantExpression(Value(nullptr))));
	auto alter_entry_data = add_column.GetAlterEntryData();
	return unique_ptr<SQLStatement>(std::move(
	    TransformAndMaterializeAlter(alter_entry_data,
	                                 make_uniq<AddColumnInfo>(add_column.GetAlterEntryData(), std::move(null_column),
	                                                          add_column.if_column_not_exists),
	                                 column_entry.GetName().GetIdentifierName(), column_entry.DefaultValue().Copy())));
}

unique_ptr<AlterInfo>
PEGTransformerFactory::TransformAlterTableStmt(PEGTransformer &transformer, const optional<bool> &if_exists,
                                               unique_ptr<BaseTableRef> base_table_name,
                                               vector<unique_ptr<AlterTableInfo>> alter_table_options) {
	if (alter_table_options.size() > 1) {
		throw ParserException("Only one ALTER command per statement is supported");
	}
	auto result = std::move(alter_table_options[0]);
	result->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	result->SetQualifiedName(base_table_name->GetQualifiedName());

	return std::move(result);
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterDatabaseStmt(PEGTransformer &transformer,
                                                                        const optional<bool> &if_exists,
                                                                        const Identifier &identifier,
                                                                        const Identifier &identifier_1) {
	OnEntryNotFound not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	auto catalog_name = identifier;
	auto new_name = identifier_1;
	auto result = make_uniq<RenameDatabaseInfo>(catalog_name, new_name, not_found);
	return std::move(result);
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterViewStmt(PEGTransformer &transformer,
                                                                    const optional<bool> &if_exists,
                                                                    unique_ptr<BaseTableRef> base_table_name,
                                                                    unique_ptr<AlterTableInfo> rename_alter) {
	auto rename_table = unique_ptr_cast<AlterTableInfo, RenameTableInfo>(std::move(rename_alter));
	auto result = make_uniq<RenameViewInfo>(AlterEntryData(), rename_table->new_table_name);
	result->SetQualifiedName(base_table_name->GetQualifiedName());
	result->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	return std::move(result);
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterSchemaStmt(PEGTransformer &transformer,
                                                                      const optional<bool> &if_exists,
                                                                      const QualifiedName &qualified_name,
                                                                      unique_ptr<AlterTableInfo> rename_alter) {
	throw NotImplementedException("Altering schemas is not yet supported");
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterSequenceStmt(PEGTransformer &transformer,
                                                                        const optional<bool> &if_exists,
                                                                        const QualifiedName &qualified_sequence_name,
                                                                        unique_ptr<AlterInfo> alter_sequence_options) {
	alter_sequence_options->SetQualifiedName(qualified_sequence_name);
	alter_sequence_options->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	return alter_sequence_options;
}

QualifiedName PEGTransformerFactory::TransformQualifiedSequenceName(PEGTransformer &transformer,
                                                                    const optional<Identifier> &catalog_qualification,
                                                                    const optional<Identifier> &schema_qualification,
                                                                    const Identifier &sequence_name) {
	// The grammar greedily fills the catalog qualifier first, so a 2-part name "a.b" arrives as catalog="a"
	// with no schema. Store the qualifiers as a plain schema path: a lone qualifier becomes the schema, and the
	// binder's BindSchemaOrCatalog promotes it to a catalog if it names an attached database.
	vector<Identifier> schema_path;
	if (catalog_qualification) {
		schema_path.push_back(*catalog_qualification);
	}
	if (schema_qualification) {
		schema_path.push_back(*schema_qualification);
	}
	return QualifiedName(std::move(schema_path), sequence_name);
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterSequenceOptions(PEGTransformer &transformer,
                                                                           ParseResult &choice_result) {
	if (choice_result.name == "RenameAlter") {
		return transformer.Transform<unique_ptr<AlterTableInfo>>(choice_result);
	}
	return transformer.Transform<unique_ptr<AlterInfo>>(choice_result);
}

unique_ptr<AlterInfo>
PEGTransformerFactory::TransformSetSequenceOption(PEGTransformer &transformer,
                                                  vector<pair<string, unique_ptr<SequenceOption>>> sequence_option) {
	bool has_owned = false;
	unique_ptr<AlterInfo> owned_info;
	for (auto &seq_option : sequence_option) {
		if (seq_option.first == "owned") {
			if (has_owned) {
				throw ParserException("Owned by value should be passed at most once");
			}
			has_owned = true;
			auto owned_by = unique_ptr_cast<SequenceOption, QualifiedSequenceOption>(std::move(seq_option.second));
			auto schema = owned_by->qualified_name.Schema().empty() ? Identifier::DefaultSchema()
			                                                        : owned_by->qualified_name.Schema();
			owned_info =
			    make_uniq<ChangeOwnershipInfo>(CatalogType::SEQUENCE_ENTRY, "", "", "", schema,
			                                   owned_by->qualified_name.Name(), OnEntryNotFound::THROW_EXCEPTION);
		}
	}
	if (owned_info) {
		return owned_info;
	}
	throw NotImplementedException("ALTER SEQUENCE option not yet supported");
}

void PEGTransformerFactory::AddToMultiStatement(const unique_ptr<MultiStatement> &multi_statement,
                                                unique_ptr<AlterInfo> alter_info) {
	auto alter_statement = make_uniq<AlterStatement>();
	alter_statement->info = std::move(alter_info);
	alter_statement->query = alter_statement->ToString();
	multi_statement->statements.push_back(std::move(alter_statement));
}

void PEGTransformerFactory::AddUpdateToMultiStatement(const unique_ptr<MultiStatement> &multi_statement,
                                                      const string &column_name, const AlterEntryData &table_data,
                                                      const unique_ptr<ParsedExpression> &original_expression) {
	auto update_statement = make_uniq<UpdateStatement>();
	auto &node = *update_statement->node;
	node.prioritize_table_when_binding = true;

	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->SetQualifiedName(table_data.GetQualifiedName());
	node.table = std::move(table_ref);

	auto set_info = make_uniq<UpdateSetInfo>();
	set_info->columns.emplace_back(column_name);
	set_info->expressions.push_back(original_expression->Copy());
	node.set_info = std::move(set_info);

	update_statement->query = update_statement->ToString() + ";";
	multi_statement->statements.push_back(std::move(update_statement));
}

unique_ptr<MultiStatement> PEGTransformerFactory::TransformAndMaterializeAlter(
    AlterEntryData &data, unique_ptr<AlterInfo> info_with_null_placeholder, const string &column_name,
    unique_ptr<ParsedExpression> expression) {
	auto multi_statement = make_uniq<MultiStatement>();
	/* Here we do a workaround that consists of the following statements:
	 *	 1. `ALTER TABLE t ADD COLUMN col <type> DEFAULT NULL;`
	 *	 2. `UPDATE t SET col = <expression>;`
	 *	 3. `ALTER TABLE t ALTER col SET DEFAULT <expression>;`

	 *
	 * This workaround exists because, when statements like this were executed:
	 *	`ALTER TABLE ... ADD COLUMN ... DEFAULT <expression>`
	 * the WAL replay would re-run the default expression, and with expressions such as RANDOM or CURRENT_TIMESTAMP, the
	 * value would be different from that of the original run. By now doing an UPDATE in statement 2, we force
	 * materialization of these values for all existing rows, which makes WAL replays consistent.
	 */

	// 1. `ALTER TABLE t ADD COLUMN col <type> DEFAULT NULL;`
	AddToMultiStatement(multi_statement, std::move(info_with_null_placeholder));

	// 2. `UPDATE t SET u = <expression>;`
	AddUpdateToMultiStatement(multi_statement, column_name, data, expression);

	// 3. `ALTER TABLE t ALTER u SET DEFAULT <expression>;`
	// Reinstate the original default expression.
	AddToMultiStatement(multi_statement,
	                    make_uniq<SetDefaultInfo>(data, Identifier(column_name), std::move(expression)));

	return multi_statement;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAddColumn(PEGTransformer &transformer,
                                                                     const bool &has_result,
                                                                     const optional<bool> &if_not_exists,
                                                                     AddColumnEntry add_column_entry) {
	auto column_definition = ColumnDefinition(add_column_entry.column_path.back(), add_column_entry.type);
	if (add_column_entry.default_value) {
		column_definition.SetDefaultValue(std::move(add_column_entry.default_value));
	}

	unique_ptr<AlterTableInfo> result;
	auto if_not_exists_value = if_not_exists.has_value();

	if (add_column_entry.column_path.size() == 1) {
		result = make_uniq<AddColumnInfo>(AlterEntryData(), std::move(column_definition), if_not_exists_value);
	} else {
		const auto parent_path =
		    vector<Identifier>(add_column_entry.column_path.begin(), add_column_entry.column_path.end() - 1);
		result =
		    make_uniq<AddFieldInfo>(AlterEntryData(), parent_path, std::move(column_definition), if_not_exists_value);
	}
	return result;
}

AddColumnEntry PEGTransformerFactory::TransformAddColumnEntry(
    PEGTransformer &transformer, const vector<string> &dotted_identifier, const optional<LogicalType> &type,
    optional<GeneratedColumnDefinition> generated_column, optional<vector<ColumnConstraintEntry>> column_constraint) {
	AddColumnEntry new_column;
	new_column.column_path = StringsToIdentifiers(dotted_identifier);
	bool has_type = type.has_value();
	bool has_generated = generated_column && generated_column->expr != nullptr;
	// TODO(Dtenwolde) this checking logic should be moved to the binder
	if (!has_type && !has_generated) {
		throw ParserException("Column definition requires a type or generated expression");
	}
	if (has_generated) {
		throw ParserException("Adding generated columns after table creation is not supported yet");
	}
	if (type) {
		new_column.type = *type;
	}
	if (column_constraint) {
		for (auto &constraint : *column_constraint) {
			if (constraint.constraint_name == "DefaultValue") {
				if (new_column.default_value) {
					throw ParserException("Cannot define a default value twice");
				}
				new_column.default_value = std::move(constraint.expression);
			}
		}
	}
	return new_column;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformDropColumn(
    PEGTransformer &transformer, const bool &has_result, const optional<bool> &if_exists,
    unique_ptr<ColumnRefExpression> nested_column_name, const optional<bool> &drop_behavior) {
	auto if_exists_value = if_exists.has_value();
	auto drop_behavior_value = drop_behavior ? *drop_behavior : false;
	if (nested_column_name->ColumnNames().size() == 1) {
		auto result =
		    make_uniq<RemoveColumnInfo>(AlterEntryData(), nested_column_name->ColumnNames()[0].GetIdentifierName(),
		                                if_exists_value, drop_behavior_value);
		return std::move(result);
	}
	auto result = make_uniq<RemoveFieldInfo>(AlterEntryData(), nested_column_name->ColumnNames(), if_exists_value,
	                                         drop_behavior_value);
	return std::move(result);
}

unique_ptr<AlterTableInfo>
PEGTransformerFactory::TransformAlterColumn(PEGTransformer &transformer, const bool &has_result,
                                            unique_ptr<ColumnRefExpression> nested_column_name,
                                            unique_ptr<AlterTableInfo> alter_column_entry) {
	if (alter_column_entry->alter_table_type == AlterTableType::SET_DEFAULT) {
		auto set_default_entry = unique_ptr_cast<AlterTableInfo, SetDefaultInfo>(std::move(alter_column_entry));
		// TODO(Dtenwolde) Figure out with nested names;
		set_default_entry->column_name = nested_column_name->ColumnNames()[0];
		return std::move(set_default_entry);
	} else if (alter_column_entry->alter_table_type == AlterTableType::DROP_NOT_NULL) {
		auto drop_not_null = unique_ptr_cast<AlterTableInfo, DropNotNullInfo>(std::move(alter_column_entry));
		drop_not_null->column_name = nested_column_name->ColumnNames()[0];
		return std::move(drop_not_null);
	} else if (alter_column_entry->alter_table_type == AlterTableType::SET_NOT_NULL) {
		auto set_not_null = unique_ptr_cast<AlterTableInfo, SetNotNullInfo>(std::move(alter_column_entry));
		set_not_null->column_name = nested_column_name->ColumnNames()[0];
		return std::move(set_not_null);
	} else if (alter_column_entry->alter_table_type == AlterTableType::ALTER_COLUMN_TYPE) {
		auto change_column_type = unique_ptr_cast<AlterTableInfo, ChangeColumnTypeInfo>(std::move(alter_column_entry));
		change_column_type->column_name = nested_column_name->ColumnNames()[0];
		if (!change_column_type->expression) {
			change_column_type->expression =
			    make_uniq<CastExpression>(change_column_type->target_type, std::move(nested_column_name));
		}
		return std::move(change_column_type);
	} else {
		throw NotImplementedException("Unrecognized type for alter column encountered");
	}
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformDropDefault(PEGTransformer &transformer) {
	return make_uniq<SetDefaultInfo>(AlterEntryData(), "", nullptr);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformChangeNullability(PEGTransformer &transformer,
                                                                             const string &drop_or_set) {
	if (StringUtil::CIEquals(drop_or_set, "drop")) {
		return make_uniq<DropNotNullInfo>(AlterEntryData(), "");
	} else {
		return make_uniq<SetNotNullInfo>(AlterEntryData(), "");
	}
}

unique_ptr<AlterTableInfo>
PEGTransformerFactory::TransformAlterType(PEGTransformer &transformer, const bool &has_result,
                                          const optional<LogicalType> &type,
                                          optional<unique_ptr<ParsedExpression>> using_expression) {
	if (!type && !using_expression) {
		throw ParserException("Omitting the type is only possible in combination with USING");
	}
	auto alter_type = type ? *type : LogicalType::UNKNOWN;
	unique_ptr<ParsedExpression> expression;
	if (using_expression) {
		expression = std::move(*using_expression);
	}
	return make_uniq<ChangeColumnTypeInfo>(AlterEntryData(), "", alter_type, std::move(expression));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformUsingExpression(PEGTransformer &transformer,
                                                                             unique_ptr<ParsedExpression> expression) {
	return expression;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAddDefault(PEGTransformer &transformer,
                                                                      unique_ptr<ParsedExpression> expression) {
	return make_uniq<SetDefaultInfo>(AlterEntryData(), "", std::move(expression));
}

unique_ptr<AlterTableInfo>
PEGTransformerFactory::TransformRenameColumn(PEGTransformer &transformer, const bool &has_result,
                                             unique_ptr<ColumnRefExpression> nested_column_name,
                                             const Identifier &identifier) {
	if (nested_column_name->ColumnNames().size() == 1) {
		auto result = make_uniq<RenameColumnInfo>(AlterEntryData(), nested_column_name->ColumnNames()[0], identifier);
		return std::move(result);
	}
	auto result = make_uniq<RenameFieldInfo>(AlterEntryData(), nested_column_name->ColumnNames(), identifier);
	return std::move(result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformRenameAlter(PEGTransformer &transformer,
                                                                       const Identifier &identifier) {
	return make_uniq<RenameTableInfo>(AlterEntryData(), identifier);
}

unique_ptr<AlterTableInfo>
PEGTransformerFactory::TransformSetPartitionedBy(PEGTransformer &transformer,
                                                 vector<unique_ptr<ParsedExpression>> expression) {
	return make_uniq<SetPartitionedByInfo>(AlterEntryData(), std::move(expression));
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformResetPartitionedBy(PEGTransformer &transformer) {
	vector<unique_ptr<ParsedExpression>> partition_keys;
	return make_uniq<SetPartitionedByInfo>(AlterEntryData(), std::move(partition_keys));
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAddConstraint(PEGTransformer &transformer,
                                                                         unique_ptr<Constraint> top_level_constraint) {
	return make_uniq<AddConstraintInfo>(AlterEntryData(), std::move(top_level_constraint));
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformSetSortedBy(PEGTransformer &transformer,
                                                                       vector<OrderByNode> order_by_expressions) {
	auto result = make_uniq<SetSortedByInfo>(AlterEntryData(), std::move(order_by_expressions));
	return std::move(result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformResetSortedBy(PEGTransformer &transformer) {
	vector<OrderByNode> order_by_exprs;
	auto result = make_uniq<SetSortedByInfo>(AlterEntryData(), std::move(order_by_exprs));
	return std::move(result);
}

unique_ptr<AlterTableInfo>
PEGTransformerFactory::TransformSetOptions(PEGTransformer &transformer,
                                           case_insensitive_map_t<unique_ptr<ParsedExpression>> rel_option_list) {
	return make_uniq<SetTableOptionsInfo>(AlterEntryData(), std::move(rel_option_list));
}

unique_ptr<AlterTableInfo>
PEGTransformerFactory::TransformResetOptions(PEGTransformer &transformer,
                                             case_insensitive_map_t<unique_ptr<ParsedExpression>> rel_option_list) {
	identifier_set_t option_names;
	for (auto &opt : rel_option_list) {
		if (!opt.second) {
			option_names.insert(Identifier(opt.first));
			continue;
		}
		if (opt.second->GetExpressionClass() != ExpressionClass::CONSTANT) {
			throw ParserException("Reset option \"%s\" cannot set any value. Did you mean to use SET?", opt.first);
		}
		auto &const_expr = opt.second->Cast<ConstantExpression>();
		if (!const_expr.GetValue().IsNull()) {
			throw ParserException("Reset option \"%s\" cannot set any value. Did you mean to use SET?", opt.first);
		}
		option_names.insert(Identifier(opt.first));
	}
	return make_uniq<ResetTableOptionsInfo>(AlterEntryData(), std::move(option_names));
}

unique_ptr<ColumnRefExpression> PEGTransformerFactory::TransformNestedColumnName(
    PEGTransformer &transformer, const optional<vector<Identifier>> &identifier_dot, const Identifier &column_name) {
	vector<Identifier> column_names;
	if (identifier_dot) {
		column_names = *identifier_dot;
	}
	column_names.push_back(column_name);
	return make_uniq<ColumnRefExpression>(column_names);
}

Identifier PEGTransformerFactory::TransformIdentifierDot(PEGTransformer &transformer, const Identifier &identifier) {
	return identifier;
}

string PEGTransformerFactory::TransformDropNullability(PEGTransformer &transformer) {
	return "drop";
}

string PEGTransformerFactory::TransformSetNullability(PEGTransformer &transformer) {
	return "set";
}

} // namespace duckdb
