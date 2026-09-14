#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb::capiv2 {

class CV2CustomType {
public:
	// Validates the configuration and returns the type to install: the base type carrying the custom type's name as
	// its alias, which is what makes it logically distinct from the base type.
	LogicalType Build() {
		if (name.empty()) {
			throw InvalidInputException("Type name cannot be empty.");
		}
		if (base_type.id() == LogicalTypeId::INVALID) {
			throw InvalidInputException("Base type must be set for the type.");
		}
		if (!base_type.IsComplete()) {
			throw InvalidInputException("Base type must be a fully defined concrete type");
		}
		return base_type.WithAlias(name.GetIdentifierName());
	}

	void Register() {
		RegisterToCatalog(Build());
	}

	virtual ~CV2CustomType() = default;
	virtual void RegisterToCatalog(LogicalType type) = 0;

public:
	Identifier name;
	LogicalType base_type;
};

class CV2ConnectionCustomType : public CV2CustomType {
public:
	explicit CV2ConnectionCustomType(Connection &connection) : connection(connection) {
	}

	void RegisterToCatalog(LogicalType type) override {
		auto &context = *connection.context;

		context.RunFunctionInTransaction([&]() {
			auto &catalog = Catalog::GetSystemCatalog(context);
			// Read the name before the type is moved out: sibling arguments have no evaluation order.
			auto name = type.GetAlias();
			CreateTypeInfo type_info(std::move(name), std::move(type));
			type_info.temporary = true;
			type_info.internal = true;
			type_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
			catalog.CreateType(context, type_info);
		});
	}

private:
	Connection &connection;
};

class CV2ExtensionCustomType : public CV2CustomType {
public:
	explicit CV2ExtensionCustomType(ExtensionLoader &loader) : loader(loader) {
	}

	void RegisterToCatalog(LogicalType type) override {
		auto name = type.GetAlias();
		loader.RegisterType(std::move(name), std::move(type));
	}

private:
	ExtensionLoader &loader;
};

static auto Convert(duckdb_v2_custom_type_handle type) -> CV2CustomType * {
	return reinterpret_cast<CV2CustomType *>(type);
}
static auto Convert(CV2CustomType *type) -> duckdb_v2_custom_type_handle {
	return reinterpret_cast<duckdb_v2_custom_type_handle>(type);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_custom_type_create_with_connection(duckdb_v2_connection_handle connection,
                                                             duckdb_v2_custom_type_handle *out_type,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(connection);
	DUCKDB_CHECK_ARG(out_type);
	*out_type = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &conn = *Convert(connection);
		auto type = duckdb::make_uniq<CV2ConnectionCustomType>(conn);
		*out_type = Convert(type.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_custom_type_create_with_extension(duckdb_v2_extension_handle extension,
                                                            duckdb_v2_custom_type_handle *out_type,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(out_type);
	*out_type = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &loader = GetExtensionLoader(extension);
		auto type = duckdb::make_uniq<CV2ExtensionCustomType>(loader);
		*out_type = Convert(type.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_custom_type_set_name(duckdb_v2_custom_type_handle type, duckdb_v2_identifier_t name,
                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(type);
	DUCKDB_CHECK_ARG(name);
	return WithErrorHandler(err, [&]() { Convert(type)->name = duckdb::Identifier(Convert(name)); });
}

DUCKDB_V2_ERROR duckdb_v2_custom_type_set_base_type(duckdb_v2_custom_type_handle type,
                                                    duckdb_v2_logical_type_handle base_type,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(type);
	DUCKDB_CHECK_ARG(base_type);
	return WithErrorHandler(err, [&]() { Convert(type)->base_type = *Convert(base_type); });
}

DUCKDB_V2_ERROR duckdb_v2_custom_type_register(duckdb_v2_custom_type_handle type, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(type);
	return WithErrorHandler(err, [&]() { Convert(type)->Register(); });
}

DUCKDB_V2_ERROR duckdb_v2_custom_type_destroy(duckdb_v2_custom_type_handle *type) {
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
