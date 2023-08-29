#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

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

uint64_t CreateInfoHashFunction::operator()(const LogicalDependency &a) const {
	hash_t hash = duckdb::Hash(a.name.c_str());
	hash = CombineHash(hash, duckdb::Hash(a.schema.c_str()));
	hash = CombineHash(hash, duckdb::Hash(a.catalog.c_str()));
	hash = CombineHash(hash, duckdb::Hash<uint8_t>(static_cast<uint8_t>(a.type)));
	return hash;
}

bool CreateInfoEquality::operator()(const LogicalDependency &a, const LogicalDependency &b) const {
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

void LogicalDependency::FormatSerialize(FormatSerializer &serializer) const {
	serializer.WriteProperty(0, "name", name);
	serializer.WriteProperty(1, "schema", schema);
	serializer.WriteProperty(2, "catalog", catalog);
	serializer.WriteProperty(3, "type", type);
}

LogicalDependency LogicalDependency::FormatDeserialize(FormatDeserializer &deserializer) {
	LogicalDependency dependency;
	dependency.name = deserializer.ReadProperty<string>(0, "name");
	dependency.schema = deserializer.ReadProperty<string>(1, "schema");
	dependency.catalog = deserializer.ReadProperty<string>(2, "catalog");
	dependency.type = deserializer.ReadProperty<CatalogType>(3, "type");
	return dependency;
}

void LogicalDependencyList::AddDependency(LogicalDependency entry) {
	set.insert(entry);
}

void LogicalDependencyList::AddDependency(CatalogEntry &entry) {
	LogicalDependency dependency(entry);
	set.insert(dependency);
}

bool LogicalDependencyList::Contains(LogicalDependency &entry) {
	return set.count(entry);
}

bool LogicalDependencyList::Contains(CatalogEntry &entry) {
	LogicalDependency dependency(entry);
	return set.count(dependency);
}

PhysicalDependencyList LogicalDependencyList::GetPhysical(ClientContext &context) const {
	PhysicalDependencyList dependencies;
	for (auto &entry : set) {
		auto &name = entry.name;
		auto &catalog = entry.catalog;
		auto &schema = entry.schema;
		auto &type = entry.type;

		auto catalog_entry = Catalog::GetEntry(context, type, catalog, schema, name, OnEntryNotFound::THROW_EXCEPTION);
		dependencies.AddDependency(*catalog_entry);
	}
	return dependencies;
}

PhysicalDependencyList LogicalDependencyList::GetPhysical(optional_ptr<ClientContext> context_p) const {
	if (set.empty()) {
		return PhysicalDependencyList();
	}
	if (!context_p) {
		throw InternalException("ClientContext is required to convert logical to physical dependency!");
	}
	auto &context = *context_p;
	return GetPhysical(context);
}

void LogicalDependencyList::Serialize(Serializer &writer) const {
	writer.Write<idx_t>(set.size());
	for (auto &entry : set) {
		writer.WriteString(entry.name);
		writer.WriteString(entry.schema);
		writer.WriteString(entry.catalog);
		writer.Write(entry.type);
	}
}

LogicalDependencyList LogicalDependencyList::Deserialize(Deserializer &source) {
	LogicalDependencyList result;
	auto size = source.Read<idx_t>();
	for (idx_t i = 0; i < size; i++) {
		LogicalDependency dependency;
		dependency.name = source.Read<string>();
		dependency.schema = source.Read<string>();
		dependency.catalog = source.Read<string>();
		dependency.type = source.Read<CatalogType>();
		result.set.insert(dependency);
	}
	return result;
}

void LogicalDependencyList::FormatSerialize(FormatSerializer &serializer) const {
	serializer.WriteProperty(0, "logical_dependencies", set);
}

LogicalDependencyList LogicalDependencyList::FormatDeserialize(FormatDeserializer &deserializer) {
	LogicalDependencyList dependency;
	dependency.set = deserializer.ReadProperty<create_info_set_t>(0, "logical_dependencies");
	return dependency;
}

} // namespace duckdb
