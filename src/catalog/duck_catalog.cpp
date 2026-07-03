#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/function/function_list.hpp"
#include "duckdb/common/encryption_state.hpp"

namespace duckdb {

DuckCatalog::DuckCatalog(AttachedDatabase &db)
    : Catalog(db), dependency_manager(make_uniq<DependencyManager>(*this)),
      schemas(make_uniq<CatalogSet>(*this, IsSystemCatalog() ? make_uniq<DefaultSchemaGenerator>(*this) : nullptr)) {
}

DuckCatalog::~DuckCatalog() {
}

void DuckCatalog::Initialize(bool load_builtin) {
	// first initialize the base system catalogs
	// these are never written to the WAL
	// we start these at 1 because deleted entries default to 0
	auto data = CatalogTransaction::GetSystemTransaction(GetDatabase());

	// create the default schema
	CreateSchemaInfo info;
	info.SetQualifiedName(QualifiedName({Identifier::DefaultSchema()}, Identifier()));
	info.internal = true;
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	CreateSchema(data, info);

	if (load_builtin) {
		BuiltinFunctions builtin(data, *this);
		builtin.Initialize();

		// initialize default functions
		FunctionList::RegisterFunctions(*this, data);
	}

	Verify();
}

bool DuckCatalog::IsDuckCatalog() {
	return true;
}

bool DuckCatalog::SupportsMultipleDMLCTEs() const {
	return true;
}

optional_ptr<DependencyManager> DuckCatalog::GetDependencyManager() {
	return dependency_manager.get();
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> DuckCatalog::CreateSchemaInternal(CatalogTransaction transaction, CreateSchemaInfo &info) {
	LogicalDependencyList dependencies;

	auto parents = info.ParentSchemas();
	if (parents.empty()) {
		// top-level schema
		if (!info.internal && DefaultSchemaGenerator::IsDefaultSchema(info.SchemaName())) {
			return nullptr;
		}
		auto entry = make_uniq<DuckSchemaEntry>(*this, info);
		auto result = entry.get();
		if (!schemas->CreateEntry(transaction, info.SchemaName(), std::move(entry), dependencies)) {
			return nullptr;
		}
		return result;
	}
	// nested schema: navigate to the deepest parent schema, then create the schema inside it
	optional_ptr<CatalogEntry> parent_entry = schemas->GetEntry(transaction, parents[0]);
	if (!parent_entry) {
		// the root component was not a catalog (otherwise it would have been resolved as one) nor an existing schema
		throw CatalogException("\"%s\" is not a catalog or schema", parents[0].GetIdentifierName());
	}
	for (idx_t i = 1; i < parents.size(); i++) {
		auto &duck_parent = parent_entry->Cast<DuckSchemaEntry>();
		parent_entry = duck_parent.GetCatalogSet(CatalogType::SCHEMA_ENTRY).GetEntry(transaction, parents[i]);
		if (!parent_entry) {
			throw CatalogException("Cannot create nested schema \"%s\": parent schema \"%s\" does not exist",
			                       info.SchemaName().GetIdentifierName(), parents[i].GetIdentifierName());
		}
	}
	return parent_entry->Cast<DuckSchemaEntry>().CreateSchema(transaction, info);
}

optional_ptr<CatalogEntry> DuckCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	D_ASSERT(!info.SchemaName().empty());
	auto result = CreateSchemaInternal(transaction, info);
	if (!result) {
		switch (info.on_conflict) {
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw CatalogException::EntryAlreadyExists(CatalogType::SCHEMA_ENTRY, info.SchemaName());
		case OnCreateConflict::REPLACE_ON_CONFLICT: {
			DropInfo drop_info;
			drop_info.type = CatalogType::SCHEMA_ENTRY;
			// build the path [catalog, parent schemas..., schema] so a nested schema can be navigated on drop
			vector<Identifier> drop_path;
			drop_path.push_back(info.SchemaCatalog());
			for (auto &parent : info.ParentSchemas()) {
				drop_path.push_back(parent);
			}
			drop_info.SetQualifiedName(QualifiedName(std::move(drop_path), info.SchemaName()));
			DropSchema(transaction, drop_info);
			result = CreateSchemaInternal(transaction, info);
			if (!result) {
				throw InternalException("Failed to create schema entry in CREATE_OR_REPLACE");
			}
			break;
		}
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			break;
		default:
			throw InternalException("Unsupported OnCreateConflict for CreateSchema");
		}
		return nullptr;
	}
	return result;
}

void DuckCatalog::DropSchema(CatalogTransaction transaction, DropInfo &info) {
	auto &path = info.GetQualifiedName().Path();
	auto &schema_name = info.GetQualifiedName().Name();
	D_ASSERT(!schema_name.empty());
	// navigate to the catalog set that holds the schema to drop: the root set for a top-level schema, or the nested
	// schemas set of the deepest parent. The path is [catalog, parent schemas..., schema] after binding; for internal
	// callers it can be just [schema].
	reference<CatalogSet> target_set = *schemas;
	for (idx_t i = 1; i + 1 < path.size(); i++) {
		auto parent_entry = target_set.get().GetEntry(transaction, path[i]);
		if (!parent_entry) {
			if (info.if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
				throw CatalogException("Cannot drop schema \"%s\": parent schema \"%s\" does not exist",
				                       schema_name.GetIdentifierName(), path[i].GetIdentifierName());
			}
			return;
		}
		target_set = parent_entry->Cast<DuckSchemaEntry>().GetCatalogSet(CatalogType::SCHEMA_ENTRY);
	}
	// drop exactly this schema - the dependency manager blocks the drop (RESTRICT) or cascades to the schema's
	// contents, including any nested schemas (which depend on it), when CASCADE is given
	if (!target_set.get().DropEntry(transaction, schema_name, info.cascade, info.allow_drop_internal)) {
		if (info.if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw CatalogException::MissingEntry(CatalogType::SCHEMA_ENTRY, schema_name, string());
		}
	}
}

void DuckCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	DropSchema(GetCatalogTransaction(context), info);
}

static void ScanNestedSchemas(CatalogTransaction transaction, SchemaCatalogEntry &schema,
                              const std::function<void(SchemaCatalogEntry &)> &callback) {
	// scan the nested schemas using the already-obtained transaction - re-deriving it from the context here would
	// acquire the meta-transaction lock while a catalog set lock is held, inverting the lock order
	schema.Cast<DuckSchemaEntry>().GetCatalogSet(CatalogType::SCHEMA_ENTRY).Scan(transaction, [&](CatalogEntry &entry) {
		auto &nested = entry.Cast<SchemaCatalogEntry>();
		callback(nested);
		ScanNestedSchemas(transaction, nested, callback);
	});
}

static void ScanNestedSchemas(SchemaCatalogEntry &schema, const std::function<void(SchemaCatalogEntry &)> &callback) {
	schema.Scan(CatalogType::SCHEMA_ENTRY, [&](CatalogEntry &entry) {
		auto &nested = entry.Cast<SchemaCatalogEntry>();
		callback(nested);
		ScanNestedSchemas(nested, callback);
	});
}

void DuckCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	// obtain the transaction once (up front) so the nested scan does not re-acquire the meta-transaction lock while
	// holding a catalog set lock
	auto transaction = GetCatalogTransaction(context);
	schemas->Scan(transaction, [&](CatalogEntry &entry) {
		auto &schema = entry.Cast<SchemaCatalogEntry>();
		callback(schema);
		ScanNestedSchemas(transaction, schema, callback);
	});
}

void DuckCatalog::ScanSchemas(std::function<void(SchemaCatalogEntry &)> callback) {
	schemas->Scan([&](CatalogEntry &entry) {
		auto &schema = entry.Cast<SchemaCatalogEntry>();
		callback(schema);
		ScanNestedSchemas(schema, callback);
	});
}

CatalogSet &DuckCatalog::GetSchemaCatalogSet() {
	return *schemas;
}

optional_ptr<SchemaCatalogEntry> DuckCatalog::LookupSchema(CatalogTransaction transaction,
                                                           const EntryLookupInfo &schema_lookup,
                                                           OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	D_ASSERT(!schema_name.empty());
	auto entry = schemas->GetEntry(transaction, Identifier(schema_name));
	if (!entry) {
		if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw CatalogException(schema_lookup.GetErrorContext(), "Schema with name %s does not exist!", schema_name);
		}
		return nullptr;
	}
	return &entry->Cast<SchemaCatalogEntry>();
}

DatabaseSize DuckCatalog::GetDatabaseSize(ClientContext &context) {
	auto &transaction = DuckTransactionManager::Get(db);
	auto lock = transaction.SharedCheckpointLock();
	return db.GetStorageManager().GetDatabaseSize();
}

vector<MetadataBlockInfo> DuckCatalog::GetMetadataInfo(ClientContext &context) {
	auto &transaction = DuckTransactionManager::Get(db);
	auto lock = transaction.SharedCheckpointLock();
	return db.GetStorageManager().GetMetadataInfo();
}

bool DuckCatalog::InMemory() {
	return db.GetStorageManager().InMemory();
}

string DuckCatalog::GetDBPath() {
	return db.GetStorageManager().GetDBPath();
}

bool DuckCatalog::IsEncrypted() const {
	return IsSystemCatalog() ? false : db.GetStorageManager().IsEncrypted();
}

string DuckCatalog::GetEncryptionCipher() const {
	return IsSystemCatalog() ? string() : EncryptionTypes::CipherToString(db.GetStorageManager().GetCipher());
}

void DuckCatalog::Verify() {
#ifdef DEBUG
	Catalog::Verify();
	schemas->Verify(*this);
#endif
}

optional_idx DuckCatalog::GetCatalogVersion(ClientContext &context) {
	auto &transaction_manager = DuckTransactionManager::Get(db);
	auto transaction = GetCatalogTransaction(context);
	D_ASSERT(transaction.transaction);
	return transaction_manager.GetCatalogVersion(*transaction.transaction);
}

//===--------------------------------------------------------------------===//
// Encryption
//===--------------------------------------------------------------------===//
void DuckCatalog::SetEncryptionKeyId(const string &key_id) {
	encryption_key_id = key_id;
}

string &DuckCatalog::GetEncryptionKeyId() {
	return encryption_key_id;
}

void DuckCatalog::SetIsEncrypted() {
	is_encrypted = true;
}

bool DuckCatalog::GetIsEncrypted() {
	return is_encrypted;
}

} // namespace duckdb
