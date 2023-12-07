#include "duckdb/parser/statement/create_secret_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

void Transformer::TransformCreateSecretOptions(CreateSecretInfo &info,
                                               optional_ptr<duckdb_libpgquery::PGList> options) {
	if (!options) {
		return;
	}

	duckdb_libpgquery::PGListCell *cell;
	// iterate over each option
	for_each_cell(cell, options->head) {
		auto def_elem = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);
		auto lower_name = StringUtil::Lower(def_elem->defname);
		if (lower_name == "scope") {
			// format specifier: interpret this option
			auto scope_val = PGPointerCast<duckdb_libpgquery::PGValue>(def_elem->arg);
			if (!scope_val) {
				throw ParserException("Unsupported parameter type for SCOPE");
			} else if (scope_val->type == duckdb_libpgquery::T_PGString) {
				info.scope.push_back(scope_val->val.str);
				continue;
			} else if (scope_val->type != duckdb_libpgquery::T_PGList) {
				throw ParserException("%s has to be a string, or a list of strings", lower_name);
			}

			auto list = PGPointerCast<duckdb_libpgquery::PGList>(def_elem->arg);
			for (auto scope_cell = list->head; scope_cell != nullptr; scope_cell = lnext(scope_cell)) {
				auto scope_val_entry = PGPointerCast<duckdb_libpgquery::PGValue>(scope_cell->data.ptr_value);
				info.scope.push_back(scope_val_entry->val.str);
			}
			continue;
		} else if (lower_name == "type") {
			auto type_val = PGPointerCast<duckdb_libpgquery::PGValue>(def_elem->arg);
			if (type_val->type != duckdb_libpgquery::T_PGString) {
				throw ParserException("%s has to be a string", lower_name);
			}
			info.type = StringUtil::Lower(type_val->val.str);
			continue;
		} else if (lower_name == "provider") {
			auto provider_val = PGPointerCast<duckdb_libpgquery::PGValue>(def_elem->arg);
			if (provider_val->type != duckdb_libpgquery::T_PGString) {
				throw ParserException("%s has to be a string", lower_name);
			}
			info.provider = StringUtil::Lower(provider_val->val.str);
			continue;
		}

		// All the other options end up in the generic
		case_insensitive_map_t<vector<Value>> vector_options;
		ParseGenericOptionListEntry(vector_options, lower_name, def_elem->arg);

		for (const auto &entry : vector_options) {
			if (entry.second.size() != 1) {
				throw ParserException("Invalid parameter passed to option '%s'", entry.first);
			}
			info.options[entry.first] = entry.second.at(0);
		}
	}
}

unique_ptr<CreateSecretStatement> Transformer::TransformSecret(duckdb_libpgquery::PGCreateSecretStmt &stmt) {
	auto result = make_uniq<CreateSecretStatement>(
	    TransformOnConflict(stmt.onconflict),
	    EnumUtil::FromString<SecretPersistMode>(StringUtil::Upper(stmt.persist_option)));

	if (stmt.secret_name) {
		result->info->name = StringUtil::Lower(stmt.secret_name);
	}

	if (stmt.options) {
		TransformCreateSecretOptions(*result->info, stmt.options);
	}

	if (result->info->type.empty()) {
		throw ParserException("Failed to create secret - secret must have a type defined");
	}
	if (result->info->name.empty()) {
		result->info->name = "__default_" + result->info->type;
	}

	return result;
}

} // namespace duckdb
