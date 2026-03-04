#include "ast/add_column_entry.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/parsed_data/alter_database_info.hpp"
#include "duckdb/parser/statement/multi_statement.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformAlterStatement(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<AlterStatement>();
	result->info = transformer.Transform<unique_ptr<AlterInfo>>(list_pr.Child<ListParseResult>(1));
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
	return unique_ptr<SQLStatement>(std::move(TransformAndMaterializeAlter(
	    alter_entry_data,
	    make_uniq<AddColumnInfo>(add_column.GetAlterEntryData(), std::move(null_column),
	                             result->info->if_not_found == OnEntryNotFound::RETURN_NULL),
	    column_entry.GetName(), column_entry.DefaultValue().Copy())));
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterOptions(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<AlterInfo>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterTableStmt(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(2));

	auto result = transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ListParseResult>(3));
	result->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	result->catalog = table->catalog_name;
	result->schema = table->schema_name;
	result->name = table->table_name;

	return std::move(result);
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterDatabaseStmt(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	OnEntryNotFound not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;

	auto catalog_name = list_pr.Child<IdentifierParseResult>(2).identifier;
	auto new_name = list_pr.Child<IdentifierParseResult>(6).identifier;
	auto result = make_uniq<RenameDatabaseInfo>(catalog_name, new_name, not_found);
	return std::move(result);
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterViewStmt(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto base_table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(2));
	auto alter_table_info = transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ListParseResult>(3));
	auto rename_table = unique_ptr_cast<AlterTableInfo, RenameTableInfo>(std::move(alter_table_info));
	auto result = make_uniq<RenameViewInfo>(AlterEntryData(), rename_table->new_table_name);
	result->catalog = base_table->catalog_name;
	result->schema = base_table->schema_name;
	result->name = base_table->table_name;
	result->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	return std::move(result);
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterSequenceStmt(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto sequence_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(2));
	auto alter_info = transformer.Transform<unique_ptr<AlterInfo>>(list_pr.Child<ListParseResult>(3));
	if (sequence_name.schema.empty()) {
		alter_info->schema = sequence_name.catalog;
	} else {
		alter_info->catalog = sequence_name.catalog;
		alter_info->schema = sequence_name.schema;
	}
	alter_info->name = sequence_name.name;
	alter_info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	return alter_info;
}

QualifiedName PEGTransformerFactory::TransformQualifiedSequenceName(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = INVALID_SCHEMA;
	transformer.TransformOptional(list_pr, 0, result.catalog);
	transformer.TransformOptional(list_pr, 1, result.schema);
	result.name = list_pr.Child<IdentifierParseResult>(2).identifier;
	return result;
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterSequenceOptions(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	if (list_pr.Child<ChoiceParseResult>(0).result->name == "RenameAlter") {
		return transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ChoiceParseResult>(0).result);
	}
	return transformer.Transform<unique_ptr<AlterInfo>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformSetSequenceOption(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto option_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	for (auto option : option_list) {
		auto seq_option = transformer.Transform<pair<string, unique_ptr<SequenceOption>>>(option);
		if (seq_option.first == "owned") {
			auto owned_by = unique_ptr_cast<SequenceOption, QualifiedSequenceOption>(std::move(seq_option.second));
			auto schema = owned_by->qualified_name.schema.empty() ? DEFAULT_SCHEMA : owned_by->qualified_name.schema;
			return make_uniq<ChangeOwnershipInfo>(CatalogType::SEQUENCE_ENTRY, "", "", "", schema,
			                                      owned_by->qualified_name.name, OnEntryNotFound::THROW_EXCEPTION);
		}
	}
	throw NotImplementedException("ALTER SEQUENCE option not yet supported");
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAlterTableOptions(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ChoiceParseResult>(0).result);
}

void PEGTransformerFactory::AddToMultiStatement(const unique_ptr<MultiStatement> &multi_statement,
                                                unique_ptr<AlterInfo> alter_info) {
	auto alter_statement = make_uniq<AlterStatement>();
	alter_statement->info = std::move(alter_info);
	multi_statement->statements.push_back(std::move(alter_statement));
}

void PEGTransformerFactory::AddUpdateToMultiStatement(const unique_ptr<MultiStatement> &multi_statement,
                                                      const string &column_name, const AlterEntryData &table_data,
                                                      const unique_ptr<ParsedExpression> &original_expression) {
	auto update_statement = make_uniq<UpdateStatement>();
	update_statement->prioritize_table_when_binding = true;

	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->catalog_name = table_data.catalog;
	table_ref->schema_name = table_data.schema;
	table_ref->table_name = table_data.name;
	update_statement->table = std::move(table_ref);

	auto set_info = make_uniq<UpdateSetInfo>();
	set_info->columns.push_back(column_name);
	set_info->expressions.push_back(original_expression->Copy());
	update_statement->set_info = std::move(set_info);

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
	AddToMultiStatement(multi_statement, make_uniq<SetDefaultInfo>(data, column_name, std::move(expression)));

	return multi_statement;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAddColumn(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	AddColumnEntry new_column = transformer.Transform<AddColumnEntry>(list_pr.Child<ListParseResult>(3));
	bool if_not_exists = list_pr.Child<OptionalParseResult>(2).HasResult();
	auto column_definition = ColumnDefinition(new_column.column_path.back(), new_column.type);
	if (new_column.default_value) {
		column_definition.SetDefaultValue(std::move(new_column.default_value));
	}

	unique_ptr<AlterTableInfo> result;

	if (new_column.column_path.size() == 1) {
		result = make_uniq<AddColumnInfo>(AlterEntryData(), std::move(column_definition), if_not_exists);
	} else {
		const auto parent_path = vector<string>(new_column.column_path.begin(), new_column.column_path.end() - 1);
		result = make_uniq<AddFieldInfo>(AlterEntryData(), parent_path, std::move(column_definition), if_not_exists);
	}
	return result;
}

AddColumnEntry PEGTransformerFactory::TransformAddColumnEntry(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	AddColumnEntry new_column;
	new_column.column_path = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<LogicalType>(list_pr, 1, new_column.type);
	if (list_pr.Child<OptionalParseResult>(2).HasResult()) {
		throw ParserException("Adding generated columns after table creation is not supported yet");
	}
	auto constraints_opt = list_pr.Child<OptionalParseResult>(3);
	if (!constraints_opt.HasResult()) {
		return new_column;
	}
	auto constraints_repeat = constraints_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &constraint_entry : constraints_repeat.children) {
		auto constraint_list = constraint_entry->Cast<ListParseResult>();
		auto constraint = constraint_list.Child<ChoiceParseResult>(0).result;
		if (constraint->name == "DefaultValue") {
			if (new_column.default_value) {
				throw ParserException("Cannot define a default value twice");
			}
			new_column.default_value = transformer.Transform<unique_ptr<ParsedExpression>>(constraint);
		} else {
			throw ParserException("Adding columns with constraints not yet supported");
		}
	}
	return new_column;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformDropColumn(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool cascade = false;
	transformer.TransformOptional<bool>(list_pr, 4, cascade);
	bool if_exists = list_pr.Child<OptionalParseResult>(2).HasResult();
	auto nested_column = transformer.Transform<unique_ptr<ColumnRefExpression>>(list_pr.Child<ListParseResult>(3));
	if (nested_column->column_names.size() == 1) {
		auto result = make_uniq<RemoveColumnInfo>(AlterEntryData(), nested_column->column_names[0], if_exists, cascade);
		return std::move(result);
	}
	auto result = make_uniq<RemoveFieldInfo>(AlterEntryData(), nested_column->column_names, if_exists, cascade);
	return std::move(result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAlterColumn(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto nested_column_name = transformer.Transform<unique_ptr<ColumnRefExpression>>(list_pr.Child<ListParseResult>(2));
	auto alter_column_entry = transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ListParseResult>(3));
	if (alter_column_entry->alter_table_type == AlterTableType::SET_DEFAULT) {
		auto set_default_entry = unique_ptr_cast<AlterTableInfo, SetDefaultInfo>(std::move(alter_column_entry));
		// TODO(Dtenwolde) Figure out with nested names;
		set_default_entry->column_name = nested_column_name->column_names[0];
		return std::move(set_default_entry);
	} else if (alter_column_entry->alter_table_type == AlterTableType::DROP_NOT_NULL) {
		auto drop_not_null = unique_ptr_cast<AlterTableInfo, DropNotNullInfo>(std::move(alter_column_entry));
		drop_not_null->column_name = nested_column_name->column_names[0];
		return std::move(drop_not_null);
	} else if (alter_column_entry->alter_table_type == AlterTableType::SET_NOT_NULL) {
		auto set_not_null = unique_ptr_cast<AlterTableInfo, SetNotNullInfo>(std::move(alter_column_entry));
		set_not_null->column_name = nested_column_name->column_names[0];
		return std::move(set_not_null);
	} else if (alter_column_entry->alter_table_type == AlterTableType::ALTER_COLUMN_TYPE) {
		auto change_column_type = unique_ptr_cast<AlterTableInfo, ChangeColumnTypeInfo>(std::move(alter_column_entry));
		change_column_type->column_name = nested_column_name->column_names[0];
		if (!change_column_type->expression) {
			change_column_type->expression =
			    make_uniq<CastExpression>(change_column_type->target_type, std::move(nested_column_name));
		}
		return std::move(change_column_type);
	} else {
		throw NotImplementedException("Unrecognized type for alter column encountered");
	}
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAlterColumnEntry(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformDropDefault(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	return make_uniq<SetDefaultInfo>(AlterEntryData(), "", nullptr);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformChangeNullability(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	string drop_or_set = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	if (StringUtil::CIEquals(drop_or_set, "drop")) {
		return make_uniq<DropNotNullInfo>(AlterEntryData(), "");
	} else {
		return make_uniq<SetNotNullInfo>(AlterEntryData(), "");
	}
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAlterType(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	LogicalType target_type = LogicalType::UNKNOWN;
	transformer.TransformOptional<LogicalType>(list_pr, 2, target_type);
	auto using_expr_opt = list_pr.Child<OptionalParseResult>(3);
	if (target_type == LogicalType::UNKNOWN && !using_expr_opt.HasResult()) {
		throw ParserException("Omitting the type is only possible in combination with USING");
	}
	unique_ptr<ParsedExpression> expr;
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 3, expr);
	return make_uniq<ChangeColumnTypeInfo>(AlterEntryData(), "", target_type, std::move(expr));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformUsingExpression(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

string PEGTransformerFactory::TransformDropOrSet(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice = list_pr.Child<ChoiceParseResult>(0);
	return choice.result->Cast<KeywordParseResult>().keyword;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAddOrDropDefault(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAddDefault(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	return make_uniq<SetDefaultInfo>(AlterEntryData(), "", std::move(expr));
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformRenameColumn(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto nested_column = transformer.Transform<unique_ptr<ColumnRefExpression>>(list_pr.Child<ListParseResult>(2));
	auto new_column_name = list_pr.Child<IdentifierParseResult>(4).identifier;
	if (nested_column->column_names.size() == 1) {
		auto result = make_uniq<RenameColumnInfo>(AlterEntryData(), nested_column->column_names[0], new_column_name);
		return std::move(result);
	}
	auto result = make_uniq<RenameFieldInfo>(AlterEntryData(), nested_column->column_names, new_column_name);
	return std::move(result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformRenameAlter(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto new_table_name = list_pr.Child<IdentifierParseResult>(2).identifier;
	auto result = make_uniq<RenameTableInfo>(AlterEntryData(), new_table_name);
	return std::move(result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformSetPartitionedBy(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(3));
	auto expr_list = ExtractParseResultsFromList(extract_parens);
	vector<unique_ptr<ParsedExpression>> partition_keys;
	for (auto expr : expr_list) {
		partition_keys.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	return make_uniq<SetPartitionedByInfo>(AlterEntryData(), std::move(partition_keys));
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformResetPartitionedBy(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	vector<unique_ptr<ParsedExpression>> partition_keys;
	return make_uniq<SetPartitionedByInfo>(AlterEntryData(), std::move(partition_keys));
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAddConstraint(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto constraint = transformer.Transform<unique_ptr<Constraint>>(list_pr.Child<ListParseResult>(1));
	return make_uniq<AddConstraintInfo>(AlterEntryData(), std::move(constraint));
}

string PEGTransformerFactory::TransformSequenceName(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformSetSortedBy(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(3));
	auto order_by_exprs = transformer.Transform<vector<OrderByNode>>(extract_parens);
	auto result = make_uniq<SetSortedByInfo>(AlterEntryData(), std::move(order_by_exprs));
	return std::move(result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformResetSortedBy(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	vector<OrderByNode> order_by_exprs;
	auto result = make_uniq<SetSortedByInfo>(AlterEntryData(), std::move(order_by_exprs));
	return std::move(result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformSetOptions(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	// SetOptions <- 'SET' RelOptionList
	// child 0: 'SET' keyword, child 1: RelOptionList
	auto options =
	    transformer.Transform<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(1));
	return make_uniq<SetTableOptionsInfo>(AlterEntryData(), std::move(options));
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformResetOptions(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	// ResetOptions <- 'RESET' RelOptionList
	// child 0: 'RESET' keyword, child 1: RelOptionList
	auto options_map =
	    transformer.Transform<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(1));
	case_insensitive_set_t option_names;
	for (auto &opt : options_map) {
		if (!opt.second) {
			option_names.insert(opt.first);
		}
		if (opt.second->GetExpressionClass() != ExpressionClass::CONSTANT) {
			throw ParserException("Reset option \"%s\" cannot set any value. Did you mean to use SET?", opt.first);
		}
		auto &const_expr = opt.second->Cast<ConstantExpression>();
		if (!const_expr.value.IsNull()) {
			throw ParserException("Reset option \"%s\" cannot set any value. Did you mean to use SET?", opt.first);
		}
		option_names.insert(opt.first);
	}
	return make_uniq<ResetTableOptionsInfo>(AlterEntryData(), std::move(option_names));
}

} // namespace duckdb
