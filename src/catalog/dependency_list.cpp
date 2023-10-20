#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

void PhysicalDependencyList::AddDependency(CatalogEntry &entry) {
	if (entry.internal) {
		return;
	}
	set.insert(entry);
}

void PhysicalDependencyList::VerifyDependencies(Catalog &catalog, const string &name) {
	for (auto &dep_entry : set) {
		auto &dep = dep_entry.get();
		if (&dep.ParentCatalog() != &catalog) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    name, dep.name, dep.ParentCatalog().GetName(), catalog.GetName());
		}
	}
}

LogicalDependencyList PhysicalDependencyList::GetLogical() const {
	LogicalDependencyList result;
	for (auto &entry : set) {
		result.AddDependency(entry);
	}
	return result;
}

bool PhysicalDependencyList::Contains(CatalogEntry &entry) {
	return set.count(entry);
}

uint64_t LogicalDependencyHashFunction::operator()(const LogicalDependency &a) const {
	hash_t hash = duckdb::Hash(a.name.c_str());
	hash = CombineHash(hash, duckdb::Hash(a.schema.c_str()));
	hash = CombineHash(hash, duckdb::Hash(a.catalog.c_str()));
	hash = CombineHash(hash, duckdb::Hash<uint8_t>(static_cast<uint8_t>(a.type)));
	return hash;
}

bool LogicalDependencyEquality::operator()(const LogicalDependency &a, const LogicalDependency &b) const {
	if (a.type != b.type) {
		return false;
	}
	if (a.name != b.name) {
		return false;
	}
	if (a.schema != b.schema) {
		return false;
	}
	if (a.catalog != b.catalog) {
		return false;
	}
	return true;
}

LogicalDependency::LogicalDependency() : name(), schema(), catalog(), type(CatalogType::INVALID) {
}

LogicalDependency::LogicalDependency(CatalogEntry &entry) {
	this->name = entry.name;
	this->schema = INVALID_SCHEMA;
	if (entry.type != CatalogType::SCHEMA_ENTRY) {
		this->schema = entry.ParentSchema().name;
	}
	this->catalog = entry.ParentCatalog().GetName();
	this->type = entry.type;
}

void LogicalDependency::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(0, "name", name);
	serializer.WriteProperty(1, "schema", schema);
	serializer.WriteProperty(2, "catalog", catalog);
	serializer.WriteProperty(3, "type", type);
}

LogicalDependency LogicalDependency::Deserialize(Deserializer &deserializer) {
	LogicalDependency dependency;
	dependency.name = deserializer.ReadProperty<string>(0, "name");
	dependency.schema = deserializer.ReadProperty<string>(1, "schema");
	dependency.catalog = deserializer.ReadProperty<string>(2, "catalog");
	dependency.type = deserializer.ReadProperty<CatalogType>(3, "type");
	return dependency;
}

void LogicalDependencyList::AddDependency(CatalogEntry &entry) {
	LogicalDependency dependency(entry);
	set.insert(dependency);
}

bool LogicalDependencyList::Contains(CatalogEntry &entry_p) {
	LogicalDependency logical_entry(entry_p);
	return set.count(logical_entry);
}

PhysicalDependencyList LogicalDependencyList::GetPhysical(ClientContext &context, Catalog &catalog) const {
	PhysicalDependencyList dependencies;

	for (auto &entry : set) {
		auto &name = entry.name;
		// Don't use the serialized catalog name, could be attached with a different name
		auto &schema = entry.schema;
		auto &type = entry.type;

		CatalogEntryLookup lookup;
		if (type == CatalogType::SCHEMA_ENTRY) {
			auto lookup = catalog.GetSchema(context, name, OnEntryNotFound::THROW_EXCEPTION);
			D_ASSERT(lookup);
			dependencies.AddDependency(*lookup);
		} else {
			auto lookup = catalog.LookupEntry(context, type, schema, name, OnEntryNotFound::THROW_EXCEPTION);
			D_ASSERT(lookup.Found());
			auto catalog_entry = lookup.entry;
			dependencies.AddDependency(*catalog_entry);
		}
	}
	return dependencies;
}

void LogicalDependencyList::VerifyDependencies(Catalog &catalog, const string &name) {
	for (auto &dep : set) {
		if (dep.catalog != catalog.GetName()) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    name, dep.name, dep.catalog, catalog.GetName());
		}
	}
}

void LogicalDependencyList::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(0, "logical_dependencies", set);
}

const LogicalDependencyList::create_info_set_t &LogicalDependencyList::Set() const {
	return set;
}

LogicalDependencyList LogicalDependencyList::Deserialize(Deserializer &deserializer) {
	LogicalDependencyList dependency;
	dependency.set = deserializer.ReadProperty<create_info_set_t>(0, "logical_dependencies");
	return dependency;
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
