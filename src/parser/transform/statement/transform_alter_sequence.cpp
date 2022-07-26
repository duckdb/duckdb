#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/common/enum_class_hash.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

unique_ptr<AlterStatement> Transformer::TransformAlterSequence(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGAlterSeqStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<AlterStatement>();

	auto qname = TransformQualifiedName(stmt->sequence);
	auto sequence_schema = qname.schema;
	auto sequence_name = qname.name;

	if (!stmt->options) {
		throw InternalException("Expected an argument for ALTER SEQUENCE.");
	}

	unordered_set<SequenceInfo, EnumClassHash> used;
	duckdb_libpgquery::PGListCell *cell = nullptr;
	for_each_cell(cell, stmt->options->head) {
		auto *def_elem = reinterpret_cast<duckdb_libpgquery::PGDefElem *>(cell->data.ptr_value);
		string opt_name = string(def_elem->defname);

		if (opt_name == "owned_by") {
			if (used.find(SequenceInfo::SEQ_OWN) != used.end()) {
				throw ParserException("Owned by value should be passed as most once");
			}
			used.insert(SequenceInfo::SEQ_OWN);

			auto val = (duckdb_libpgquery::PGValue *)def_elem->arg;
			if (!val) {
				throw InternalException("Expected an argument for option %s", opt_name);
			}
			D_ASSERT(val);
			if (val->type != duckdb_libpgquery::T_PGList) {
				throw InternalException("Expected a string argument for option %s", opt_name);
			}
			auto opt_values = vector<string>();

			auto opt_value_list = (duckdb_libpgquery::PGList *)(val);
			for (auto c = opt_value_list->head; c != nullptr; c = lnext(c)) {
				auto target = (duckdb_libpgquery::PGResTarget *)(c->data.ptr_value);
				opt_values.emplace_back(target->name);
			}
			D_ASSERT(!opt_values.empty());
			string owner_schema = "";
			string owner_name = "";
			if (opt_values.size() == 2) {
				owner_schema = opt_values[0];
				owner_name = opt_values[1];
			} else if (opt_values.size() == 1) {
				owner_schema = DEFAULT_SCHEMA;
				owner_name = opt_values[0];
			} else {
				throw InternalException("Wrong argument for %s. Expected either <schema>.<name> or <name>", opt_name);
			}
			auto info = make_unique<ChangeOwnershipInfo>(CatalogType::SEQUENCE_ENTRY, sequence_schema, sequence_name,
			                                             owner_schema, owner_name);
			result->info = move(info);
		} else {
			throw NotImplementedException("ALTER SEQUENCE option not supported yet!");
		}
	}
	result->info->if_exists = stmt->missing_ok;
	return result;
}
} // namespace duckdb
