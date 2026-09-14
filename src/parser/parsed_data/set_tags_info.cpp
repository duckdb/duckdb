#include "duckdb/parser/parsed_data/set_tags_info.hpp"

#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

SetTagsInfo::SetTagsInfo()
    : AlterInfo(AlterType::SET_TAGS), entry_catalog_type(CatalogType::INVALID), action(TagAction::SET) {
}

SetTagsInfo::SetTagsInfo(CatalogType entry_catalog_type_p, QualifiedName name, Identifier column_name_p,
                         TagActionInfo action_info, OnEntryNotFound if_not_found)
    : AlterInfo(AlterType::SET_TAGS, std::move(name), if_not_found), entry_catalog_type(entry_catalog_type_p),
      column_name(std::move(column_name_p)), action(action_info.action), tags(std::move(action_info.tags)),
      tag_names(std::move(action_info.tag_names)) {
}

bool SetTagsInfo::IsColumn() const {
	return !column_name.empty();
}

optional_ptr<CatalogEntry> SetTagsInfo::TryResolveCatalogEntry(CatalogEntryRetriever &retriever) {
	D_ASSERT(IsColumn());
	EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, GetQualifiedName());
	auto entry = retriever.GetEntry(lookup_info, if_not_found);
	if (entry) {
		entry_catalog_type = entry->type;
	}
	return entry;
}

void SetTagsInfo::Apply(InsertionOrderPreservingMap<string> &target) const {
	switch (action) {
	case TagAction::SET:
		for (const auto &tag : tags) {
			target[tag.first] = tag.second;
		}
		break;
	case TagAction::UNSET:
		for (const auto &tag_name : tag_names) {
			auto entry = target.find(tag_name);
			if (entry != target.end()) {
				target.erase(entry);
			}
		}
		break;
	default:
		throw InternalException("Unsupported tag action");
	}
}

unique_ptr<AlterInfo> SetTagsInfo::Copy() const {
	TagActionInfo action_info {action, tags, tag_names};
	return make_uniq<SetTagsInfo>(entry_catalog_type, GetQualifiedName(), column_name, std::move(action_info),
	                              if_not_found);
}

CatalogType SetTagsInfo::GetCatalogType() const {
	if (entry_catalog_type == CatalogType::INVALID) {
		throw InternalException("Attempted to access unresolved catalog type for TAG statement");
	}
	return entry_catalog_type;
}

string SetTagsInfo::ToString() const {
	string result = "TAG ON ";
	if (IsColumn()) {
		result += "COLUMN ";
	} else {
		result += ParseInfo::TypeToString(entry_catalog_type) + " ";
	}
	result += GetQualifiedName().ToString(QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA);
	if (IsColumn()) {
		result += "." + SQLIdentifier(column_name);
	}
	if (action == TagAction::SET) {
		result += " SET (";
		idx_t index = 0;
		for (const auto &tag : tags) {
			if (index++ > 0) {
				result += ", ";
			}
			result += SQLString(tag.first) + " = " + SQLString(tag.second);
		}
	} else {
		result += " UNSET (";
		result += StringUtil::Join(tag_names, tag_names.size(), ", ",
		                           [](const string &tag_name) { return SQLString(tag_name); });
	}
	result += ");";
	return result;
}

} // namespace duckdb
