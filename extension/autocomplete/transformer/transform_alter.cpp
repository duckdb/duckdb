#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformAlterStatement(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<AlterStatement>();
	result->info = transformer.Transform<unique_ptr<AlterInfo>>(list_pr.Child<ListParseResult>(1));
	return result;
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterOptions(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<AlterInfo>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<AlterInfo> PEGTransformerFactory::TransformAlterTableStmt(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(2));

	auto result = transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ListParseResult>(3));
	result->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	result->catalog = table->catalog_name;
	result->schema = table->schema_name;
	result->name = table->table_name;

	return result;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAlterTableOptions(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAddColumn(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	ColumnDefinition new_column = transformer.Transform<ColumnDefinition>(list_pr.Child<ListParseResult>(3));
	bool if_not_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto result = make_uniq<AddColumnInfo>(AlterEntryData(), std::move(new_column), if_not_exists);
	return result;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformDropColumn(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool cascade = false;
	transformer.TransformOptional<bool>(list_pr, 4, cascade);
	bool if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto nested_column = transformer.Transform<unique_ptr<ColumnRefExpression>>(list_pr.Child<ListParseResult>(3));
	if (nested_column->column_names.size() == 1) {
		auto result = make_uniq<RemoveColumnInfo>(AlterEntryData(), nested_column->column_names[0], if_exists, cascade);
		return result;
	} else {
		auto result = make_uniq<RemoveFieldInfo>(AlterEntryData(), nested_column->column_names, if_exists, cascade);
		return result;
	}
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAlterColumn(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto nested_column_name = transformer.Transform<unique_ptr<ColumnRefExpression>>(list_pr.Child<ListParseResult>(2));
	auto alter_column_entry = transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ListParseResult>(3));
	if (alter_column_entry->alter_table_type == AlterTableType::SET_DEFAULT) {
		auto set_default_entry = unique_ptr_cast<AlterTableInfo, SetDefaultInfo>(std::move(alter_column_entry));
		// TODO(Dtenwolde) Figure out with nested names;
		set_default_entry->column_name = nested_column_name->column_names[0];
		return set_default_entry;
	} else if (alter_column_entry->alter_table_type == AlterTableType::DROP_NOT_NULL) {
		auto drop_not_null = unique_ptr_cast<AlterTableInfo, DropNotNullInfo>(std::move(alter_column_entry));
		drop_not_null->column_name = nested_column_name->column_names[0];
		return drop_not_null;
	} else if (alter_column_entry->alter_table_type == AlterTableType::SET_NOT_NULL) {
		auto set_not_null = unique_ptr_cast<AlterTableInfo, SetNotNullInfo>(std::move(alter_column_entry));
		set_not_null->column_name = nested_column_name->column_names[0];
		return set_not_null;
	} else if (alter_column_entry->alter_table_type == AlterTableType::ALTER_COLUMN_TYPE) {
		auto change_column_type = unique_ptr_cast<AlterTableInfo, ChangeColumnTypeInfo>(std::move(alter_column_entry));
		if (!change_column_type->expression) {
			change_column_type->expression = make_uniq<CastExpression>(change_column_type->target_type, std::move(nested_column_name));
		}
		change_column_type->column_name = nested_column_name->column_names[0];
		return change_column_type;
	} else {
		throw NotImplementedException("Unrecognized type for alter column encountered");
	}

}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAlterColumnEntry(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformDropDefault(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	throw NotImplementedException("Rule 'DropDefault' has not been implemented yet");
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformChangeNullability(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	string drop_or_set = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	if (StringUtil::CIEquals(drop_or_set, "drop")) {
		return make_uniq<DropNotNullInfo>(AlterEntryData(), "");
	} else {
		return make_uniq<SetNotNullInfo>(AlterEntryData(), "");
	}
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAlterType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	LogicalType target_type;
	transformer.TransformOptional<LogicalType>(list_pr, 2, target_type);
	unique_ptr<ParsedExpression> expr;
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 3, expr);
	return make_uniq<ChangeColumnTypeInfo>(AlterEntryData(), "", target_type, std::move(expr));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformUsingExpression(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

string PEGTransformerFactory::TransformDropOrSet(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice = list_pr.Child<ChoiceParseResult>(0);
	return choice.result->Cast<KeywordParseResult>().keyword;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAddOrDropDefault(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<AlterTableInfo>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAddDefault(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	return make_uniq<SetDefaultInfo>(AlterEntryData(), "", std::move(expr));
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformRenameColumn(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto nested_column = transformer.Transform<unique_ptr<ColumnRefExpression>>(list_pr.Child<ListParseResult>(2));
	auto new_column_name = list_pr.Child<IdentifierParseResult>(4).identifier;
	if (nested_column->column_names.size() == 1) {
		auto result = make_uniq<RenameColumnInfo>(AlterEntryData(), nested_column->column_names[0], new_column_name);
		return result;
	}
	auto result = make_uniq<RenameFieldInfo>(AlterEntryData(), nested_column->column_names, new_column_name);
	return result;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformRenameAlter(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto new_table_name = list_pr.Child<IdentifierParseResult>(2).identifier;
	auto result = make_uniq<RenameTableInfo>(AlterEntryData(), new_table_name);
	return result;
}

unique_ptr<AlterTableInfo> PEGTransformerFactory::TransformAddConstraint(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto constraint = transformer.Transform<unique_ptr<Constraint>>(list_pr.Child<ListParseResult>(1));
	return make_uniq<AddConstraintInfo>(AlterEntryData(), std::move(constraint));
}

QualifiedName PEGTransformerFactory::TransformQualifiedSequenceName(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	QualifiedName result;
	transformer.TransformOptional<string>(list_pr, 0, result.catalog);
	transformer.TransformOptional<string>(list_pr, 1, result.schema);
	result.name = transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	return result;
}

string PEGTransformerFactory::TransformSequenceName(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}
} // namespace duckdb
