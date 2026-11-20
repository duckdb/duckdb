#include "duckdb/function/function_language.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

ScalarFunctionCallReplaceInput::ScalarFunctionCallReplaceInput(
    ClientContext &context, optional_ptr<ScalarFunctionInfo> function_info, const QualifiedName &function_name,
    idx_t member_index, const string &language, const string &source, const vector<Identifier> &parameter_names,
    const vector<LogicalType> &parameter_types, const LogicalType &return_type, idx_t argument_count,
    std::function<const LanguageCallArgument &(idx_t)> argument_reader, const vector<CatalogSearchEntry> &search_path)
    : context(context), function_info(function_info), function_name(function_name), member_index(member_index),
      language(language), source(source), parameter_names(parameter_names), parameter_types(parameter_types),
      return_type(return_type), search_path(search_path), argument_count(argument_count),
      argument_reader(std::move(argument_reader)) {
}

idx_t ScalarFunctionCallReplaceInput::ArgumentCount() const {
	return argument_count;
}

const LanguageCallArgument &ScalarFunctionCallReplaceInput::GetArgument(idx_t argument_idx) {
	if (argument_idx >= argument_count) {
		throw InvalidInputException("GetArgument: argument index %llu is out of range", argument_idx);
	}
	return argument_reader(argument_idx);
}

void ScalarFunctionCallReplaceInput::SetReplacement(string sql) {
	replacement_expression.reset();
	replacement_sql = std::move(sql);
}

void ScalarFunctionCallReplaceInput::SetReplacement(unique_ptr<ParsedExpression> expression) {
	replacement_sql.reset();
	replacement_expression = std::move(expression);
}

void ScalarFunctionCallReplaceInput::SetParameterNames(vector<Identifier> names) {
	if (names.size() != parameter_names.size()) {
		throw InvalidInputException("SetParameterNames expects %llu names, got %llu", parameter_names.size(),
		                            names.size());
	}
	identifier_set_t seen;
	for (auto &name : names) {
		if (name.empty()) {
			throw InvalidInputException("SetParameterNames: a parameter name is empty");
		}
		if (!seen.insert(name).second) {
			throw InvalidInputException("SetParameterNames: duplicate parameter name %s", name);
		}
	}
	replacement_parameter_names = std::move(names);
}

void ScalarFunctionCallReplaceInput::SetErrorNote(string note) {
	error_note = std::move(note);
}

bool ScalarFunctionCallReplaceInput::HasReplacement() const {
	return replacement_sql.has_value() || replacement_expression;
}

unique_ptr<ParsedExpression> ScalarFunctionCallReplaceInput::TakeReplacement() {
	if (replacement_expression) {
		return std::move(replacement_expression);
	}
	D_ASSERT(replacement_sql.has_value());
	// generated code must not depend on the session's parser settings
	Parser parser(ParserOptions::Builtin());
	vector<unique_ptr<ParsedExpression>> expressions;
	try {
		expressions = parser.ParseExpressionList(*replacement_sql);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw BinderException("The replacement for %s returned by language \"%s\" does not parse: %s",
		                      function_name.Name(), language, error.RawMessage());
	}
	if (expressions.size() != 1) {
		throw BinderException("The replacement for %s returned by language \"%s\" must be exactly one expression",
		                      function_name.Name(), language);
	}
	return std::move(expressions[0]);
}

const vector<Identifier> &ScalarFunctionCallReplaceInput::GetReplacementParameterNames() const {
	return replacement_parameter_names.empty() ? parameter_names : replacement_parameter_names;
}

const string &ScalarFunctionCallReplaceInput::GetErrorNote() const {
	return error_note;
}

LanguageTypeView::LanguageTypeView(LogicalType type_p) : type(std::move(type_p)) {
}

string LanguageTypeView::ToString() const {
	return type.ToString();
}

LogicalTypeId LanguageTypeView::GetId() const {
	return type.id();
}

string LanguageTypeView::GetAlias() const {
	return type.HasAlias() ? type.GetAlias() : string();
}

idx_t LanguageTypeView::GetChildCount() const {
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UNION:
		return StructType::GetChildCount(type) - (type.id() == LogicalTypeId::UNION ? 1 : 0);
	case LogicalTypeId::MAP:
		return 2;
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
		return 1;
	default:
		return 0;
	}
}

string LanguageTypeView::GetChildName(idx_t child_idx) const {
	if (child_idx >= GetChildCount()) {
		throw InvalidInputException("GetChildName: child index %llu is out of range", child_idx);
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		return StructType::GetChildName(type, child_idx).GetIdentifierName();
	case LogicalTypeId::UNION:
		return UnionType::GetMemberName(type, child_idx).GetIdentifierName();
	case LogicalTypeId::MAP:
		return child_idx == 0 ? "key" : "value";
	default:
		return string();
	}
}

LanguageTypeView LanguageTypeView::GetChild(idx_t child_idx) const {
	if (child_idx >= GetChildCount()) {
		throw InvalidInputException("GetChild: child index %llu is out of range", child_idx);
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		return LanguageTypeView(StructType::GetChildType(type, child_idx));
	case LogicalTypeId::UNION:
		return LanguageTypeView(UnionType::GetMemberType(type, child_idx));
	case LogicalTypeId::MAP:
		return LanguageTypeView(child_idx == 0 ? MapType::KeyType(type) : MapType::ValueType(type));
	case LogicalTypeId::LIST:
		return LanguageTypeView(ListType::GetChildType(type));
	default:
		return LanguageTypeView(ArrayType::GetChildType(type));
	}
}

ScalarFunctionCreateValidateInput::ScalarFunctionCreateValidateInput(ClientContext &context,
                                                                     const QualifiedName &function_name,
                                                                     const string &statement_text,
                                                                     vector<LanguageMemberView> members_p,
                                                                     vector<vector<LogicalType>> description_types_p)
    : context(context), function_name(function_name), statement_text(statement_text), members(std::move(members_p)),
      member_sources(members.size()), description_types(std::move(description_types_p)) {
	D_ASSERT(description_types.size() == members.size());
}

void ScalarFunctionCreateValidateInput::SetMemberSource(idx_t member_idx, string source) {
	if (member_idx >= members.size()) {
		throw InvalidInputException("SetMemberSource: member index %llu is out of range", member_idx);
	}
	member_sources[member_idx] = std::move(source);
}

void ScalarFunctionCreateValidateInput::AddDescription(idx_t member_idx, FunctionDescription description) {
	if (member_idx >= members.size()) {
		throw InvalidInputException("AddDescription: member index %llu is out of range", member_idx);
	}
	auto &member = members[member_idx];
	description.parameter_names.clear();
	for (auto &name : member.parameter_names) {
		description.parameter_names.push_back(name.GetIdentifierName());
	}
	description.parameter_types = description_types[member_idx];
	descriptions.push_back(std::move(description));
}

optional_ptr<const string> ScalarFunctionCreateValidateInput::GetMemberSource(idx_t member_idx) const {
	if (!member_sources[member_idx].has_value()) {
		return nullptr;
	}
	return &member_sources[member_idx].value();
}

vector<FunctionDescription> ScalarFunctionCreateValidateInput::TakeDescriptions() {
	return std::move(descriptions);
}

optional_ptr<ScalarFunctionCatalogEntry> TryGetLanguageFunction(ClientContext &context, const string &language) {
	// only the system catalog: a user object named like the language cannot capture member calls
	QualifiedName name(Identifier::SystemCatalog(), Identifier::DefaultSchema(), Identifier(language));
	EntryLookupInfo lookup(CatalogType::SCALAR_FUNCTION_ENTRY, name);
	auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		return nullptr;
	}
	if (entry->type != CatalogType::SCALAR_FUNCTION_ENTRY ||
	    !entry->Cast<ScalarFunctionCatalogEntry>().functions.IsLanguage()) {
		throw BinderException("Function %s is not a function language", Identifier(language));
	}
	return &entry->Cast<ScalarFunctionCatalogEntry>();
}

} // namespace duckdb
