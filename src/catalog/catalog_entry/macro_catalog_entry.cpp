#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/language_macro_function.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

MacroCatalogEntry::MacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info)
    : FunctionEntry(
          (info.macros[0]->type == MacroType::TABLE_MACRO ? CatalogType::TABLE_MACRO_ENTRY : CatalogType::MACRO_ENTRY),
          catalog, schema, info),
      macros(std::move(info.macros)) {
	this->temporary = info.temporary;
	this->internal = info.internal;
	this->extension_name = info.extension_name;
	this->dependencies = info.dependencies;
	this->comment = info.comment;
	this->tags = info.tags;
}

unique_ptr<MacroCatalogEntry> MacroCatalogEntry::Create(Catalog &catalog, SchemaCatalogEntry &schema,
                                                        CreateMacroInfo &info) {
	// every producer funnels through here: binder dispatch and member selection rely on an entry
	// holding either plain members or language members of one language, so enforce it at the boundary
	for (idx_t member_idx = 1; member_idx < info.macros.size(); member_idx++) {
		auto &first = *info.macros[0];
		auto &member = *info.macros[member_idx];
		if ((first.type == MacroType::LANGUAGE_MACRO) != (member.type == MacroType::LANGUAGE_MACRO)) {
			throw CatalogException("Macro %s cannot mix language function members and plain macro members",
			                       info.GetFunctionName());
		}
		if (first.type == MacroType::LANGUAGE_MACRO &&
		    !StringUtil::CIEquals(first.Cast<LanguageMacroFunction>().language,
		                          member.Cast<LanguageMacroFunction>().language)) {
			throw CatalogException("Macro %s cannot mix members of different languages", info.GetFunctionName());
		}
	}
	for (auto &member : info.macros) {
		if (member->type != MacroType::LANGUAGE_MACRO) {
			continue;
		}
		auto &return_type = member->return_type;
		if (return_type.id() == LogicalTypeId::UNKNOWN && (return_type.HasAlias() || return_type.HasExtensionInfo())) {
			// an undeclared RETURNS type serializes as its bare default, so attached metadata
			// would silently vanish on reopen
			throw CatalogException("Macro %s attaches type metadata to an undeclared RETURNS type",
			                       info.GetFunctionName());
		}
	}
	if (info.type == CatalogType::TABLE_MACRO_ENTRY) {
		return make_uniq<TableMacroCatalogEntry>(catalog, schema, info);
	}
	return make_uniq<ScalarMacroCatalogEntry>(catalog, schema, info);
}

ScalarMacroCatalogEntry::ScalarMacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info)
    : MacroCatalogEntry(catalog, schema, info) {
}

unique_ptr<CatalogEntry> ScalarMacroCatalogEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateMacroInfo>();
	auto result = make_uniq<ScalarMacroCatalogEntry>(catalog, schema, cast_info);
	return std::move(result);
}

TableMacroCatalogEntry::TableMacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info)
    : MacroCatalogEntry(catalog, schema, info) {
}

unique_ptr<CatalogEntry> TableMacroCatalogEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateMacroInfo>();
	auto result = make_uniq<TableMacroCatalogEntry>(catalog, schema, cast_info);
	return std::move(result);
}

unique_ptr<CreateInfo> MacroCatalogEntry::GetInfo() const {
	auto info = make_uniq<CreateMacroInfo>(type);
	info->SetQualifiedName(schema.GetQualifiedName(name));
	for (auto &function : macros) {
		info->macros.push_back(function->Copy());
	}
	info->extension_name = extension_name;
	info->dependencies = dependencies;
	info->descriptions = descriptions;
	info->comment = comment;
	info->tags = tags;
	return std::move(info);
}

optional_idx MacroCatalogEntry::FindMember(const vector<LogicalType> &signature) const {
	// the signature names a declared overload exactly: a type matches an equal declared type and
	// ANY matches an untyped slot, which the create-time conflict rules keep unique
	for (idx_t i = 0; i < macros.size(); i++) {
		if (macros[i]->MatchesSignature(signature)) {
			return i;
		}
	}
	return optional_idx();
}

string MacroCatalogEntry::ToSQL() const {
	auto create_info = GetInfo();
	create_info->StripCatalogQualification();
	return create_info->ToString();
}

} // namespace duckdb
