#include "duckdb/common/enum_class_hash.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<AlterStatement> Transformer::TransformAlterSequence(duckdb_libpgquery::PGAlterSeqStmt &stmt) {
	auto result = make_uniq<AlterStatement>();

	auto qname = TransformQualifiedName(*stmt.sequence);
	auto sequence_catalog = qname.catalog;
	auto sequence_schema = qname.schema;
	auto sequence_name = qname.name;

	if (!stmt.options) {
		throw InternalException("Expected an argument for ALTER SEQUENCE.");
	}

	unordered_set<SequenceInfo, EnumClassHash> used;
	duckdb_libpgquery::PGListCell *cell;
	for_each_cell(cell, stmt.options->head) {
		auto def_elem = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);
		string opt_name = string(def_elem->defname);

		if (opt_name == "owned_by") {
			if (used.find(SequenceInfo::SEQ_OWN) != used.end()) {
				throw ParserException("Owned by value should be passed as most once");
			}
			used.insert(SequenceInfo::SEQ_OWN);

			auto val = PGPointerCast<duckdb_libpgquery::PGList>(def_elem->arg);
			if (!val) {
				throw InternalException("Expected an argument for option %s", opt_name);
			}
			D_ASSERT(val);
			if (val->type != duckdb_libpgquery::T_PGList) {
				throw InternalException("Expected a string argument for option %s", opt_name);
			}
			auto opt_values = vector<string>();

			for (auto c = val->head; c != nullptr; c = lnext(c)) {
				auto target = PGPointerCast<duckdb_libpgquery::PGResTarget>(c->data.ptr_value);
				opt_values.emplace_back(target->name);
			}
			D_ASSERT(!opt_values.empty());
			string owner_schema = INVALID_SCHEMA;
			string owner_name;
			if (opt_values.size() == 2) {
				owner_schema = opt_values[0];
				owner_name = opt_values[1];
			} else if (opt_values.size() == 1) {
				owner_schema = DEFAULT_SCHEMA;
				owner_name = opt_values[0];
			} else {
				throw InternalException("Wrong argument for %s. Expected either <schema>.<name> or <name>", opt_name);
			}
			auto info = make_uniq<ChangeOwnershipInfo>(CatalogType::SEQUENCE_ENTRY, sequence_catalog, sequence_schema,
			                                           sequence_name, owner_schema, owner_name,
			                                           TransformOnEntryNotFound(stmt.missing_ok));
			result->info = std::move(info);
		} else {
			throw NotImplementedException("ALTER SEQUENCE option not supported yet!");
		}
	}
	result->info->if_not_found = TransformOnEntryNotFound(stmt.missing_ok);
	return result;
}
} // namespace duckdb
