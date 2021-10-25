#include "duckdb/parser/parsed_data/alter_sequence_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<AlterStatement> Transformer::TransformAlterSequence(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGAlterSeqStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<AlterStatement>();

	auto qname = TransformQualifiedName(stmt->sequence);
	auto sequence_schema = qname.schema;
	auto sequence_name = qname.name;

	if (stmt->options) {
		duckdb_libpgquery::PGListCell *cell = nullptr;
		for_each_cell(cell, stmt->options->head) {
			auto *def_elem = reinterpret_cast<duckdb_libpgquery::PGDefElem *>(cell->data.ptr_value);
			string opt_name = string(def_elem->defname);

			if (opt_name == "owned_by") {
				auto val = (duckdb_libpgquery::PGValue *)def_elem->arg;
				if (!val) {
					throw ParserException("Expected an argument for option %s", opt_name);
				}
				D_ASSERT(val);
				if (val->type != duckdb_libpgquery::T_PGList) {
					throw ParserException("Expected a string argument for option %s", opt_name);
				}
				auto opt_values = vector<string>();

				auto opt_value_list = (duckdb_libpgquery::PGList *)(val);
				for (auto c = opt_value_list->head; c != nullptr; c = lnext(c)) {
					auto target = (duckdb_libpgquery::PGResTarget *)(c->data.ptr_value);
					opt_values.push_back(target->name);
				}
				D_ASSERT(opt_values.size() > 0);
				string table_schema = "";
				string table_name = "";
				if (opt_values.size() == 2) {
					table_schema = opt_values[0];
					table_name = opt_values[1];
				} else if (opt_values.size() == 1) {
					table_schema = "main";
					table_name = opt_values[0];
				} else {
					throw ParserException("Expected an argument for option %s", opt_name);
				}
				auto info = make_unique<ChangeOwnershipInfo>(sequence_schema, sequence_name, table_schema, table_name);
				result->info = move(info);
			}
		}
	}
	return result;
}
} // namespace duckdb