#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

PreparedStatementData::PreparedStatementData(StatementType type) : statement_type(type) {
}

PreparedStatementData::~PreparedStatementData() {
}

void PreparedStatementData::CheckParameterCount(idx_t parameter_count) {
	const auto required = properties.parameter_count;
	if (parameter_count != required) {
		throw BinderException("Parameter/argument count mismatch for prepared statement. Expected %llu, got %llu",
		                      required, parameter_count);
	}
}

bool CheckCatalogIdentity(ClientContext &context, const string &catalog_name,
                          const StatementProperties::CatalogIdentity catalog_identity) {
	// some catalogs don't support catalog version, we can't check identity in that case
	if (!catalog_identity.catalog_version.IsValid()) {
		return false;
	}
	auto database = DatabaseManager::Get(context).GetDatabase(context, catalog_name);
	if (!database) {
		throw BinderException("Prepared statement requires database %s but it was not attached", catalog_name);
	}
	Transaction::Get(context, *database);
	auto current_catalog_oid = database->GetCatalog().GetOid();
	auto current_catalog_version = database->GetCatalog().GetCatalogVersion(context);

	return StatementProperties::CatalogIdentity {current_catalog_oid, current_catalog_version} == catalog_identity;
}

bool PreparedStatementData::RequireRebind(ClientContext &context,
                                          optional_ptr<case_insensitive_map_t<BoundParameterData>> values) {
	idx_t count = values ? values->size() : 0;
	CheckParameterCount(count);
	if (!unbound_statement) {
		throw InternalException("Prepared statement without unbound statement");
	}
	if (properties.always_require_rebind) {
		// this statement must always be re-bound
		return true;
	}
	if (!properties.bound_all_parameters) {
		// parameters not yet bound: query always requires a rebind
		return true;
	}
	for (auto &it : value_map) {
		auto &identifier = it.first;
		auto lookup = values->find(identifier);
		if (lookup == values->end()) {
			break;
		}
		if (lookup->second.GetValue().type() != it.second->return_type) {
			return true;
		}
	}
	// Check the catalog versions to ensure all catalog entries we rely on are current
	for (auto &it : properties.read_databases) {
		if (!CheckCatalogIdentity(context, it.first, it.second)) {
			return true;
		}
	}
	for (auto &it : properties.modified_databases) {
		if (!CheckCatalogIdentity(context, it.first, it.second)) {
			return true;
		}
	}
	return false;
}

void PreparedStatementData::Bind(case_insensitive_map_t<BoundParameterData> values) {
	// set parameters
	D_ASSERT(!unbound_statement || unbound_statement->named_param_map.size() == properties.parameter_count);
	CheckParameterCount(values.size());

	// bind the required values
	for (auto &it : value_map) {
		const string &identifier = it.first;
		auto lookup = values.find(identifier);
		if (lookup == values.end()) {
			throw BinderException("Could not find parameter with identifier %s", identifier);
		}
		D_ASSERT(it.second);
		auto value = lookup->second.GetValue();
		if (!value.DefaultTryCastAs(it.second->return_type)) {
			throw BinderException(
			    "Type mismatch for binding parameter with identifier %s, expected type %s but got type %s", identifier,
			    it.second->return_type.ToString().c_str(), value.type().ToString().c_str());
		}
		it.second->SetValue(std::move(value));
	}
}

bool PreparedStatementData::TryGetType(const string &identifier, LogicalType &result) {
	auto it = value_map.find(identifier);
	if (it == value_map.end()) {
		return false;
	}
	if (it->second->return_type.id() != LogicalTypeId::INVALID) {
		result = it->second->return_type;
	} else {
		result = it->second->GetValue().type();
	}
	return true;
}

LogicalType PreparedStatementData::GetType(const string &identifier) {
	LogicalType result;
	if (!TryGetType(identifier, result)) {
		throw BinderException("Could not find parameter identified with: %s", identifier);
	}
	return result;
}

} // namespace duckdb
