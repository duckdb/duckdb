#include "ast/add_column_entry.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/parsed_data/alter_database_info.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformAlterStatement(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<AlterStatement>();
	result->info = transformer.Transform<unique_ptr<AlterInfo>>(list_pr.Child<ListParseResult>(1));
	return std::move(result);
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
	auto new_name = list_pr.Child<IdentifierParseResult>(5).identifier;
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
	new_column.type = transformer.Transform<LogicalType>(list_pr.Child<ListParseResult>(1));
	auto constraints_opt = list_pr.Child<OptionalParseResult>(2);
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
} // namespace duckdb
