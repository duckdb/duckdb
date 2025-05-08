#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/catalog/default/default_views.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb {

class OpenFileDefaultGenerator : public DefaultGenerator {
public:
	OpenFileDefaultGenerator(Catalog &catalog, SchemaCatalogEntry &schema, const case_insensitive_set_t &view_names_p,
	                         string file_p)
	    : DefaultGenerator(catalog), schema(schema), file(std::move(file_p)) {
		for (auto &view_name : view_names_p) {
			view_names.push_back(view_name);
		}
	}

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override {
		for (auto &entry : view_names) {
			if (StringUtil::CIEquals(entry_name, entry)) {
				auto result = make_uniq<CreateViewInfo>();
				result->schema = DEFAULT_SCHEMA;
				result->view_name = entry;
				result->sql = StringUtil::Format("SELECT * FROM %s", SQLString(file));
				auto view_info = CreateViewInfo::FromSelect(context, std::move(result));
				return make_uniq_base<CatalogEntry, ViewCatalogEntry>(catalog, schema, *view_info);
			}
		}
		return nullptr;
	}

	vector<string> GetDefaultEntries() override {
		return view_names;
	}

private:
	SchemaCatalogEntry &schema;
	vector<string> view_names;
	string file;
};

unique_ptr<Catalog> OpenFileStorageAttach(StorageExtensionInfo *storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AccessMode access_mode) {
	auto file = info.path;
	// open an in-memory database
	info.path = ":memory:";
	auto catalog = make_uniq<DuckCatalog>(db);
	catalog->Initialize(false);

	case_insensitive_set_t view_names;
	view_names.insert("file");
	view_names.insert(name);

	// set up the default view generator for "file" and the derived name of the file
	auto system_transaction = CatalogTransaction::GetSystemTransaction(db.GetDatabase());
	auto &schema = catalog->GetSchema(system_transaction, DEFAULT_SCHEMA);
	auto &duck_schema = schema.Cast<DuckSchemaEntry>();
	auto &catalog_set = duck_schema.GetCatalogSet(CatalogType::VIEW_ENTRY);
	auto default_generator = make_uniq<OpenFileDefaultGenerator>(*catalog, schema, view_names, std::move(file));
	catalog_set.SetDefaultGenerator(std::move(default_generator));

	return std::move(catalog);
}

unique_ptr<TransactionManager> OpenFileStorageTransactionManager(StorageExtensionInfo *storage_info,
                                                                 AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<DuckTransactionManager>(db);
}

unique_ptr<StorageExtension> OpenFileStorageExtension::Create() {
	auto result = make_uniq<StorageExtension>();
	result->attach = OpenFileStorageAttach;
	result->create_transaction_manager = OpenFileStorageTransactionManager;
	return result;
}

} // namespace duckdb
