#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

uint64_t LogicalDependencyHashFunction::operator()(const LogicalDependency &a) const {
	auto &name = a.entry.name;
	auto &schema = a.entry.schema;
	auto &type = a.entry.type;
	auto &catalog = a.catalog;

	hash_t hash = duckdb::Hash(name.c_str());
	hash = CombineHash(hash, duckdb::Hash(schema.c_str()));
	hash = CombineHash(hash, duckdb::Hash(catalog.c_str()));
	hash = CombineHash(hash, duckdb::Hash<uint8_t>(static_cast<uint8_t>(type)));
	return hash;
}

bool LogicalDependencyEquality::operator()(const LogicalDependency &a, const LogicalDependency &b) const {
	if (a.entry.type != b.entry.type) {
		return false;
	}
	if (a.entry.name != b.entry.name) {
		return false;
	}
	if (a.entry.schema != b.entry.schema) {
		return false;
	}
	if (a.catalog != b.catalog) {
		return false;
	}
	return true;
}

LogicalDependency::LogicalDependency() : entry(), catalog() {
}

static string GetSchema(CatalogEntry &entry) {
	if (entry.type == CatalogType::SCHEMA_ENTRY) {
		return entry.name;
	}
	return entry.ParentSchema().name;
}

LogicalDependency::LogicalDependency(CatalogEntry &entry) {
	catalog = INVALID_CATALOG;
	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyEntry>();

		this->entry = dependency_entry.EntryInfo();
	} else {
		this->entry.schema = GetSchema(entry);
		this->entry.name = entry.name;
		this->entry.type = entry.type;
		catalog = entry.ParentCatalog().GetName();
	}
}

LogicalDependency::LogicalDependency(optional_ptr<Catalog> catalog_p, CatalogEntryInfo entry_p, string catalog_str)
    : entry(std::move(entry_p)), catalog(std::move(catalog_str)) {
	if (catalog_p) {
		catalog = catalog_p->GetName();
	}
}

bool LogicalDependency::operator==(const LogicalDependency &other) const {
	return other.entry.name == entry.name && other.entry.schema == entry.schema && other.entry.type == entry.type;
}

void LogicalDependencyList::AddDependency(CatalogEntry &entry) {
	LogicalDependency dependency(entry);
	set.insert(dependency);
}

void LogicalDependencyList::AddDependency(const LogicalDependency &entry) {
	set.insert(entry);
}

bool LogicalDependencyList::Contains(CatalogEntry &entry_p) {
	LogicalDependency logical_entry(entry_p);
	return set.count(logical_entry);
}

void LogicalDependencyList::VerifyDependencies(Catalog &catalog, const string &name) {
	for (auto &dep : set) {
		if (dep.catalog != catalog.GetName()) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    name, dep.entry.name, dep.catalog, catalog.GetName());
		}
	}
}

const LogicalDependencyList::create_info_set_t &LogicalDependencyList::Set() const {
	return set;
}

bool LogicalDependencyList::operator==(const LogicalDependencyList &other) const {
	if (set.size() != other.set.size()) {
		return false;
	}

	for (auto &entry : set) {
		if (!other.set.count(entry)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
