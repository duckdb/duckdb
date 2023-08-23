#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"

namespace duckdb {

void DependencyList::AddDependency(CatalogEntry &entry) {
	if (entry.internal) {
		return;
	}
	set.insert(entry);
}

void DependencyList::VerifyDependencies(Catalog &catalog, const string &name) {
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

bool DependencyList::Contains(CatalogEntry &entry) {
	return set.count(entry);
}

void DependencyList::FormatSerialize(FormatSerializer &serializer) const {
	serializer.WriteProperty(100, "set", set);
}

DependencyList DependencyList::FormatDeserialize(FormatDeserializer &serializer) {
	DependencyList dependencies;
	dependencies.set = serializer.ReadProperty<catalog_entry_set_t>(100, "set");
	return dependencies;
}

void DependencyList::Serialize(Serializer &serializer) const {
	idx_t size = set.size();
	serializer.Write(size);
	for (auto &entry_p : set) {
		auto &entry = entry_p.get();
		auto type = entry.type;
		auto catalog = entry.ParentCatalog().GetName();
		auto schema = entry.ParentSchema().name;
		auto name = entry.name;

		serializer.Write(type);
		serializer.WriteString(catalog);
		serializer.WriteString(schema);
		serializer.WriteString(name);
	}
}

DependencyList DependencyList::Deserialize(Deserializer &deserializer) {
	DependencyList dependencies;

	auto count = deserializer.Read<idx_t>();
	auto &context = deserializer.GetContext();
	for (idx_t i = 0; i < count; i++) {
		auto type = deserializer.Read<CatalogType>();
		auto catalog = deserializer.Read<string>();
		auto schema = deserializer.Read<string>();
		auto name = deserializer.Read<string>();
		// FIXME: this can get called before the dependencies are added to the catalog
		auto entry = Catalog::GetEntry(context, type, catalog, schema, name, OnEntryNotFound::THROW_EXCEPTION);
		dependencies.AddDependency(*entry);
	}
	return dependencies;
}

} // namespace duckdb
