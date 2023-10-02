#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/sql_statement.hpp"

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

bool PreparedStatementData::RequireRebind(ClientContext &context, optional_ptr<case_insensitive_map_t<Value>> values) {
	idx_t count = values ? values->size() : 0;
	CheckParameterCount(count);
	if (!unbound_statement) {
		// no unbound statement!? cannot rebind?
		return false;
	}
	if (!properties.bound_all_parameters) {
		// parameters not yet bound: query always requires a rebind
		return true;
	}
	if (Catalog::GetSystemCatalog(context).GetCatalogVersion() != catalog_version) {
		//! context is out of bounds
		return true;
	}
	for (auto &it : value_map) {
		auto &identifier = it.first;
		auto lookup = values->find(identifier);
		D_ASSERT(lookup != values->end());
		if (lookup->second.type() != it.second->return_type) {
			return true;
		}
	}
	return false;
}

void PreparedStatementData::Bind(case_insensitive_map_t<Value> values) {
	// set parameters
	D_ASSERT(!unbound_statement || unbound_statement->n_param == properties.parameter_count);
	CheckParameterCount(values.size());

	// bind the required values
	for (auto &it : value_map) {
		const string &identifier = it.first;
		auto lookup = values.find(identifier);
		if (lookup == values.end()) {
			throw BinderException("Could not find parameter with identifier %s", identifier);
		}
		D_ASSERT(it.second);
		auto &value = lookup->second;
		if (!value.DefaultTryCastAs(it.second->return_type)) {
			throw BinderException(
			    "Type mismatch for binding parameter with identifier %s, expected type %s but got type %s", identifier,
			    it.second->return_type.ToString().c_str(), value.type().ToString().c_str());
		}
		it.second->SetValue(value);
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
