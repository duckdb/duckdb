#include "ast/column_constraints.hpp"
#include "ast/column_elements.hpp"
#include "ast/create_table_as.hpp"
#include "ast/generated_column_definition.hpp"
#include "ast/key_actions.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCreateStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool replace = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto result = transformer.Transform<unique_ptr<CreateStatement>>(list_pr.Child<ListParseResult>(3));
	auto &conflict_policy = result->info->on_conflict;
	if (replace) {
		if (conflict_policy == OnCreateConflict::IGNORE_ON_CONFLICT) {
			throw ParserException("Cannot specify both OR REPLACE and IF NOT EXISTS within single create statement");
		}
		conflict_policy = OnCreateConflict::REPLACE_ON_CONFLICT;
	}
	auto temporary_pr = list_pr.Child<OptionalParseResult>(2);
	auto persistent_type = SecretPersistType::DEFAULT;
	transformer.TransformOptional<SecretPersistType>(list_pr, 2, persistent_type);
	if (result->info->type == CatalogType::SECRET_ENTRY) {
		auto &secret_info = result->info->Cast<CreateSecretInfo>();
		secret_info.persist_type = persistent_type;
	}
	result->info->temporary = persistent_type == SecretPersistType::TEMPORARY;
	return std::move(result);
}

SecretPersistType PEGTransformerFactory::TransformTemporary(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<SecretPersistType>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<CreateStatement>
PEGTransformerFactory::TransformCreateStatementVariation(PEGTransformer &transformer,
                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.Transform<unique_ptr<CreateStatement>>(choice_pr.result);
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateTableStmt(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<CreateStatement>();
	QualifiedName table_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(2));
	if (table_name.name.empty()) {
		throw ParserException("Empty table name not supported");
	}
	// Use appropriate constructor
	auto info = make_uniq<CreateTableInfo>(table_name.catalog, table_name.schema, table_name.name);

	bool if_not_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	auto &table_as_or_column_list = list_pr.Child<ListParseResult>(3).Child<ChoiceParseResult>(0);
	if (table_as_or_column_list.name == "CreateTableAs") {
		auto create_table_as = transformer.Transform<CreateTableAs>(table_as_or_column_list.result);
		info->query = std::move(create_table_as.select_statement);
		info->columns = std::move(create_table_as.column_names);
	} else {
		auto column_list = transformer.Transform<ColumnElements>(table_as_or_column_list.result);
		info->columns = std::move(column_list.columns);
		info->constraints = std::move(column_list.constraints);
	}
	// On COMMIT is unused apart from checking whether it is ON COMMIT DELETE, which is unsupported
	bool on_commit = false;
	transformer.TransformOptional<bool>(list_pr, 4, on_commit);

	result->info = std::move(info);
	return result;
}

CreateTableAs PEGTransformerFactory::TransformCreateTableAs(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	CreateTableAs result;
	transformer.TransformOptional<ColumnList>(list_pr, 0, result.column_names);
	result.select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(2));
	transformer.TransformOptional<bool>(list_pr, 3, result.with_data);
	if (result.with_data) {
		auto limit_modifier = make_uniq<LimitModifier>();
		limit_modifier->limit = make_uniq<ConstantExpression>(0);
		result.select_statement->node->modifiers.push_back(std::move(limit_modifier));
	}
	return result;
}

ColumnList PEGTransformerFactory::TransformIdentifierList(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto identifier_list = ExtractParseResultsFromList(extract_parens);
	ColumnList result;
	for (auto identifier : identifier_list) {
		result.AddColumn(ColumnDefinition(identifier->Cast<IdentifierParseResult>().identifier, LogicalType::UNKNOWN));
	}
	return result;
}

ColumnElements PEGTransformerFactory::TransformCreateColumnList(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto create_table_column_list = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<ColumnElements>(create_table_column_list);
}

ColumnElements PEGTransformerFactory::TransformCreateTableColumnList(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto column_elements = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	ColumnElements result;
	for (idx_t col_idx = 0; col_idx < column_elements.size(); ++col_idx) {
		auto column_element_child =
		    column_elements[col_idx]->Cast<ListParseResult>().Child<ChoiceParseResult>(0).result;
		if (column_element_child->name == "ColumnDefinition") {
			auto column_result = transformer.Transform<ConstraintColumnDefinition>(column_element_child);
			for (auto &constraint : column_result.constraints) {
				result.constraints.push_back(std::move(constraint));
			}
			for (auto constraint_type : column_result.constraint_types) {
				if (constraint_type.second == ConstraintType::NOT_NULL) {
					result.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(col_idx)));
				} else if (constraint_type.second == ConstraintType::UNIQUE) {
					result.constraints.push_back(make_uniq<UniqueConstraint>(
					    LogicalIndex(col_idx), column_result.column_definition.GetName(), constraint_type.first));
				}
			}
			result.columns.AddColumn(std::move(column_result.column_definition));
		} else if (column_element_child->name == "TopLevelConstraint") {
			result.constraints.push_back(transformer.Transform<unique_ptr<Constraint>>(column_element_child));
		} else {
			throw NotImplementedException("Unknown column type encountered: %s", column_element_child->name);
		}
	}
	return result;
}

// IdentifierOrStringLiteral <- Identifier / StringLiteral
QualifiedName PEGTransformerFactory::TransformIdentifierOrStringLiteral(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = INVALID_SCHEMA;
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		result.name = choice_pr.result->Cast<IdentifierParseResult>().identifier;
	}
	if (choice_pr.result->type == ParseResultType::STRING) {
		result.name = choice_pr.result->Cast<StringLiteralParseResult>().result;
	}
	return result;
}

string PEGTransformerFactory::TransformColIdOrString(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.Transform<string>(choice_pr.result);
}

string PEGTransformerFactory::TransformColLabelOrString(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::STRING) {
		return choice_pr.result->Cast<StringLiteralParseResult>().result;
	}
	return transformer.Transform<string>(choice_pr.result);
}

string PEGTransformerFactory::TransformColId(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		return choice_pr.result->Cast<IdentifierParseResult>().identifier;
	}
	return transformer.Transform<string>(choice_pr.result);
}

string PEGTransformerFactory::TransformIdentifier(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

vector<string> PEGTransformerFactory::TransformDottedIdentifier(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> parts;

	parts.push_back(list_pr.Child<IdentifierParseResult>(0).identifier);

	auto &optional_elements = list_pr.Child<OptionalParseResult>(1);
	if (optional_elements.HasResult()) {
		auto repeat_elements = optional_elements.optional_result->Cast<RepeatParseResult>();
		for (auto &child_ref : repeat_elements.children) {
			auto &sub_list = child_ref->Cast<ListParseResult>();
			parts.push_back(sub_list.Child<IdentifierParseResult>(1).identifier);
		}
	}
	return parts;
}

ConstraintColumnDefinition PEGTransformerFactory::TransformColumnDefinition(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto dotted_identifier = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(0));
	auto qualified_name = StringToQualifiedName(dotted_identifier);
	auto type_opt = list_pr.Child<OptionalParseResult>(1);
	auto generated_opt = list_pr.Child<OptionalParseResult>(2);
	LogicalType type = LogicalType::ANY;
	if (!type_opt.HasResult() && !generated_opt.HasResult()) {
		throw ParserException("Column %s must have a type or be defined as a GENERATED column.",
		                      qualified_name.ToString());
	}
	transformer.TransformOptional<LogicalType>(list_pr, 1, type);
	auto constraints_opt = list_pr.Child<OptionalParseResult>(4);
	CompressionType compression_type = CompressionType::COMPRESSION_AUTO;
	ColumnConstraint column_constraint;
	if (constraints_opt.HasResult()) {
		auto constraints_repeat = constraints_opt.optional_result->Cast<RepeatParseResult>();
		for (auto &constraint_entry : constraints_repeat.children) {
			auto constraint_list = constraint_entry->Cast<ListParseResult>();
			auto constraint = constraint_list.Child<ChoiceParseResult>(0).result;
			if (constraint->name == "DefaultValue") {
				if (column_constraint.default_value) {
					throw ParserException("Cannot define a default value twice");
				}
				column_constraint.default_value = transformer.Transform<unique_ptr<ParsedExpression>>(constraint);
			} else if (constraint->name == "NotNullConstraint" || constraint->name == "UniqueConstraint" ||
			           constraint->name == "PrimaryKeyConstraint") {
				column_constraint.constraint_types.push_back(
				    transformer.Transform<pair<bool, ConstraintType>>(constraint));
			} else if (constraint->name == "ColumnCompression") {
				compression_type = transformer.Transform<CompressionType>(constraint);
				if (compression_type == CompressionType::COMPRESSION_AUTO) {
					throw ParserException(
					    "Unrecognized option for column compression, expected none, uncompressed, rle, "
					    "dictionary, pfor, bitpacking, fsst, chimp, patas, zstd, alp, alprd or roaring");
				}
			} else if (constraint->name == "ForeignKeyConstraint") {
				auto fk_constraint = transformer.Transform<unique_ptr<ForeignKeyConstraint>>(constraint);
				fk_constraint->fk_columns.push_back(qualified_name.name);
				column_constraint.constraints.push_back(std::move(fk_constraint));
			} else if (constraint->name == "ColumnCollation") {
				type = transformer.Transform<LogicalType>(constraint);
			} else {
				column_constraint.constraints.push_back(transformer.Transform<unique_ptr<Constraint>>(constraint));
			}
		}
	}
	if (generated_opt.HasResult()) {
		auto generated = transformer.Transform<GeneratedColumnDefinition>(generated_opt.optional_result);
		if (generated.expr->HasSubquery()) {
			throw ParserException("Expression of generated column \"%s\" contains a subquery, which isn't allowed",
			                      qualified_name.name);
		}
		if (type != LogicalType::ANY) {
			generated.expr = make_uniq<CastExpression>(type, std::move(generated.expr));
		}
		if (generated.expr->HasSubquery()) {
			throw ParserException("Expression of generated column \"%s\" contains a subquery, which isn't allowed",
			                      qualified_name.name);
		}

		ColumnDefinition col(qualified_name.name, type, std::move(generated.expr), TableColumnType::GENERATED);
		col.SetCompressionType(compression_type);
		if (column_constraint.default_value) {
			throw ParserException("Not allowed to set default on a generated column");
		}
		ConstraintColumnDefinition result = {std::move(col), column_constraint.constraint_types,
		                                     std::move(column_constraint.constraints)};
		return result;
	}

	ColumnDefinition col(qualified_name.name, type);

	if (column_constraint.default_value) {
		col.SetDefaultValue(std::move(column_constraint.default_value));
	}
	col.SetCompressionType(compression_type);
	ConstraintColumnDefinition result = {std::move(col), column_constraint.constraint_types,
	                                     std::move(column_constraint.constraints)};
	return result;
}

GeneratedColumnDefinition PEGTransformerFactory::TransformGeneratedColumn(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	GeneratedColumnDefinition generated;
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(2));
	generated.expr = transformer.Transform<unique_ptr<ParsedExpression>>(extract_parens);
	VerifyColumnRefs(*generated.expr);
	auto generated_column_type = list_pr.Child<OptionalParseResult>(3);
	if (generated_column_type.HasResult()) {
		transformer.Transform<bool>(generated_column_type.optional_result);
	}
	return generated;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformDefaultValue(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopLevelConstraint(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	// TODO(dtenwolde) figure out what to do with constraint name.
	auto opt_constraint_name = list_pr.Child<OptionalParseResult>(0);
	auto result = transformer.Transform<unique_ptr<Constraint>>(list_pr.Child<ListParseResult>(1));
	return result;
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopLevelConstraintList(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<Constraint>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopPrimaryKeyConstraint(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto column_list = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(2));
	auto result = make_uniq<UniqueConstraint>(column_list, true);
	return std::move(result);
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopUniqueConstraint(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto column_list = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(1));
	auto result = make_uniq<UniqueConstraint>(column_list, false);
	return std::move(result);
}

unique_ptr<Constraint> PEGTransformerFactory::TransformCheckConstraint(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto check_expr = transformer.Transform<unique_ptr<ParsedExpression>>(extract_parens);
	auto result = make_uniq<CheckConstraint>(std::move(check_expr));
	return std::move(result);
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopForeignKeyConstraint(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto pk_list = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(2));

	auto fk_constraint = transformer.Transform<unique_ptr<ForeignKeyConstraint>>(list_pr.Child<ListParseResult>(3));
	fk_constraint->fk_columns = pk_list;
	return std::move(fk_constraint);
}

vector<string> PEGTransformerFactory::TransformColumnIdList(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> result;
	auto colid_list = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto colids = ExtractParseResultsFromList(colid_list);
	for (auto colid : colids) {
		result.push_back(transformer.Transform<string>(colid));
	}
	return result;
}

string PEGTransformerFactory::TransformTypeFuncName(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	if (choice_pr->type == ParseResultType::IDENTIFIER) {
		return choice_pr->Cast<IdentifierParseResult>().identifier;
	}
	return transformer.Transform<string>(choice_pr);
}

CompressionType PEGTransformerFactory::TransformColumnCompression(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto compression_string = transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	return EnumUtil::FromString<CompressionType>(StringUtil::Lower(compression_string));
}

unique_ptr<ForeignKeyConstraint>
PEGTransformerFactory::TransformForeignKeyConstraint(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	ForeignKeyInfo fk_info;
	auto base_table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(1));
	fk_info.schema = base_table->schema_name;
	fk_info.table = base_table->table_name;
	fk_info.type = ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
	vector<string> pk_list;
	auto col_list_opt = list_pr.Child<OptionalParseResult>(2);
	if (col_list_opt.HasResult()) {
		auto extract_parens = ExtractResultFromParens(col_list_opt.optional_result);
		pk_list = transformer.Transform<vector<string>>(extract_parens);
	}
	auto key_actions = transformer.Transform<KeyActions>(list_pr.Child<ListParseResult>(3));

	return make_uniq<ForeignKeyConstraint>(pk_list, vector<string>(), fk_info);
}

KeyActions PEGTransformerFactory::TransformKeyActions(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	KeyActions results;
	transformer.TransformOptional<string>(list_pr, 0, results.update_action);
	transformer.TransformOptional<string>(list_pr, 1, results.delete_action);
	return results;
}

string PEGTransformerFactory::TransformUpdateAction(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
}

string PEGTransformerFactory::TransformDeleteAction(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
}

string PEGTransformerFactory::TransformKeyAction(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<string>(list_pr.Child<ChoiceParseResult>(0).result);
}

string PEGTransformerFactory::TransformNoKeyAction(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	return "NoKeyAction";
}

string PEGTransformerFactory::TransformRestrictKeyAction(PEGTransformer &transformer,
                                                         optional_ptr<ParseResult> parse_result) {
	return "Restrict";
}

string PEGTransformerFactory::TransformCascadeKeyAction(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	throw ParserException("FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT");
}

string PEGTransformerFactory::TransformSetNullKeyAction(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	throw ParserException("FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT");
}

string PEGTransformerFactory::TransformSetDefaultKeyAction(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	throw ParserException("FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT");
}

pair<bool, ConstraintType>
PEGTransformerFactory::TransformPrimaryKeyConstraint(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	return make_pair(true, ConstraintType::UNIQUE);
}

pair<bool, ConstraintType> PEGTransformerFactory::TransformUniqueConstraint(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	return make_pair(false, ConstraintType::UNIQUE);
}

pair<bool, ConstraintType> PEGTransformerFactory::TransformNotNullConstraint(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto not_null = list_pr.Child<OptionalParseResult>(0).HasResult();
	if (not_null) {
		return make_pair(false, ConstraintType::NOT_NULL);
	}
	return make_pair(false, ConstraintType::INVALID);
}

LogicalType PEGTransformerFactory::TransformColumnCollation(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto dotted_identifier = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(1));
	string collation = StringUtil::Join(dotted_identifier, ".");
	return LogicalType::VARCHAR_COLLATION(collation);
}

bool PEGTransformerFactory::TransformWithData(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	// 'WITH' 'NO'? 'DATA'
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto no_data = list_pr.Child<OptionalParseResult>(1).HasResult();
	return no_data;
}

bool PEGTransformerFactory::TransformCommitAction(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<bool>(list_pr.Child<ListParseResult>(2));
}

bool PEGTransformerFactory::TransformPreserveOrDelete(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	auto preserve_or_delete = choice_pr->Cast<KeywordParseResult>().keyword;
	if (StringUtil::CIEquals(preserve_or_delete, "delete")) {
		throw NotImplementedException("Only ON COMMIT PRESERVE ROWS is supported");
	}
	return true;
}

bool PEGTransformerFactory::TransformGeneratedColumnType(PEGTransformer &transformer,
                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	auto keyword = choice_pr->Cast<KeywordParseResult>().keyword;
	if (StringUtil::CIEquals(keyword, "stored")) {
		throw InvalidInputException("Can not create a STORED generated column!");
	}
	return true;
}

void PEGTransformerFactory::VerifyColumnRefs(const ParsedExpression &expr) {
	ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(expr, [&](const ColumnRefExpression &column_ref) {
		if (column_ref.IsQualified()) {
			throw ParserException(
			    "Qualified (tbl.name) column references are not allowed inside of generated column expressions");
		}
	});
}

} // namespace duckdb
