//#include "duckdb/catalog/dependency/dependency_list.hpp"
//#include "duckdb/catalog/catalog_entry.hpp"
//#include "duckdb/catalog/catalog.hpp"

// namespace duckdb {

// uint64_t LogicalDependencyHashFunction::operator()(const LogicalDependency &a) const {
//	hash_t hash = duckdb::Hash(a.name.c_str());
//	hash = CombineHash(hash, duckdb::Hash(a.schema.c_str()));
//	hash = CombineHash(hash, duckdb::Hash(a.catalog.c_str()));
//	hash = CombineHash(hash, duckdb::Hash<uint8_t>(static_cast<uint8_t>(a.type)));
//	return hash;
//}

// bool LogicalDependencyEquality::operator()(const LogicalDependency &a, const LogicalDependency &b) const {
//	if (a.type != b.type) {
//		return false;
//	}
//	if (a.name != b.name) {
//		return false;
//	}
//	if (a.schema != b.schema) {
//		return false;
//	}
//	if (a.catalog != b.catalog) {
//		return false;
//	}
//	return true;
//}

// LogicalDependency::LogicalDependency() : name(), schema(), catalog(), type(CatalogType::INVALID) {
//}

// static string GetSchema(CatalogEntry &entry) {
//	if (entry.type == CatalogType::SCHEMA_ENTRY) {
//		return entry.name;
//	}
//	return entry.ParentSchema().name;
//}

// LogicalDependency::LogicalDependency(CatalogEntry &entry) {
//	catalog = INVALID_CATALOG;
//	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
//		auto &dependency_entry = entry.Cast<DependencyCatalogEntry>();

//		schema = dependency_entry.EntrySchema();
//		name = dependency_entry.EntryName();
//		type = dependency_entry.EntryType();
//		// FIXME: do we also want to set 'catalog' here?
//	} else if (entry.type == CatalogType::DEPENDENCY_SET) {
//		auto &dependency_set = entry.Cast<DependencySet>();

//		schema = dependency_set.EntrySchema();
//		name = dependency_set.EntryName();
//		type = dependency_set.EntryType();
//		// FIXME: do we also want to set 'catalog' here?
//	} else {
//		schema = GetSchema(entry);
//		name = entry.name;
//		type = entry.type;
//		catalog = entry.ParentCatalog().GetName();
//	}
//}

// void LogicalDependency::Serialize(Serializer &serializer) const {
//	serializer.WriteProperty(0, "name", name);
//	serializer.WriteProperty(1, "schema", schema);
//	serializer.WriteProperty(2, "catalog", catalog);
//	serializer.WriteProperty(3, "type", type);
//}

// LogicalDependency LogicalDependency::Deserialize(Deserializer &deserializer) {
//	LogicalDependency dependency;
//	dependency.name = deserializer.ReadProperty<string>(0, "name");
//	dependency.schema = deserializer.ReadProperty<string>(1, "schema");
//	dependency.catalog = deserializer.ReadProperty<string>(2, "catalog");
//	dependency.type = deserializer.ReadProperty<CatalogType>(3, "type");
//	return dependency;
//}

// bool LogicalDependency::operator==(const LogicalDependency &other) const {
//	return other.name == name && other.schema == schema && other.type == type;
//}

//} // namespace duckdb
