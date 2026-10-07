#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/passthrough_catalog.hpp"

namespace duckdb::capiv2 {

// A passthrough catalog type being described: registered into the database's storage extensions on Register.
class CV2RemoteCatalogType {
public:
	explicit CV2RemoteCatalogType(DatabaseInstance &db) : db(db) {
	}

	void Register() {
		if (name.empty()) {
			throw InvalidInputException("A name must be set for the remote catalog type.");
		}
		if (!has_query_function) {
			throw InvalidInputException("A query function must be set for the remote catalog type.");
		}
		if (registered) {
			throw InvalidInputException("The remote catalog type is already registered.");
		}
		auto &config = DBConfig::GetConfig(db);
		if (StorageExtension::Find(config, name)) {
			throw InvalidInputException("A storage type named \"%s\" is already registered.", name);
		}
		StorageExtension::Register(config, name, make_shared_ptr<PassthroughStorageExtension>(name, query_function));
		registered = true;
	}

public:
	DatabaseInstance &db;
	string name;
	QualifiedName query_function;
	bool has_query_function = false;
	bool registered = false;
};

static auto Convert(duckdb_v2_remote_catalog_type_handle type) -> CV2RemoteCatalogType * {
	return reinterpret_cast<CV2RemoteCatalogType *>(type);
}
static auto Convert(CV2RemoteCatalogType *type) -> duckdb_v2_remote_catalog_type_handle {
	return reinterpret_cast<duckdb_v2_remote_catalog_type_handle>(type);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_remote_catalog_type_create_with_extension(duckdb_v2_extension_handle extension,
                                                                    duckdb_v2_remote_catalog_type_handle *out_type,
                                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(out_type);
	*out_type = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &db = GetExtensionLoader(extension).GetDatabaseInstance();
		auto type = duckdb::make_uniq<CV2RemoteCatalogType>(db);
		*out_type = Convert(type.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_remote_catalog_type_set_name(duckdb_v2_remote_catalog_type_handle type,
                                                       const duckdb_v2_identifier_t *name,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(type);
	DUCKDB_CHECK_ARG(name);
	return WithErrorHandler(
	    err, [&]() { Convert(type)->name = duckdb::StringUtil::Lower(duckdb::string(ConvertIdentifierName(*name))); });
}

DUCKDB_V2_ERROR duckdb_v2_remote_catalog_type_set_query_function(duckdb_v2_remote_catalog_type_handle type,
                                                                 duckdb_v2_qname_handle name,
                                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(type);
	DUCKDB_CHECK_ARG(name);
	return WithErrorHandler(err, [&]() {
		Convert(type)->query_function = *Convert(name);
		Convert(type)->has_query_function = true;
	});
}

DUCKDB_V2_ERROR duckdb_v2_remote_catalog_type_register(duckdb_v2_remote_catalog_type_handle type,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(type);
	return WithErrorHandler(err, [&]() { Convert(type)->Register(); });
}

DUCKDB_V2_ERROR duckdb_v2_remote_catalog_type_destroy(duckdb_v2_remote_catalog_type_handle *type) {
	return WithErrorHandler(nullptr, [&]() {
		if (!type) {
			return;
		}
		if (*type) {
			delete Convert(*type);
			*type = nullptr;
		}
	});
}
