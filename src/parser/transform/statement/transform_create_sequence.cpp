#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<CreateStatement> Transformer::TransformCreateSequence(PGNode *node) {
	auto stmt = reinterpret_cast<PGCreateSeqStmt *>(node);

	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateSequenceInfo>();

	auto sequence_name = TransformRangeVar(stmt->sequence);
	auto &sequence_ref = (BaseTableRef &)*sequence_name;
	info->schema = sequence_ref.schema_name;
	info->name = sequence_ref.table_name;

	if (stmt->options) {
		PGListCell *cell = nullptr;
		for_each_cell(cell, stmt->options->head) {
			auto *def_elem = reinterpret_cast<PGDefElem *>(cell->data.ptr_value);
			string opt_name = string(def_elem->defname);

			auto val = (PGValue *)def_elem->arg;
			if (def_elem->defaction == PG_DEFELEM_UNSPEC && !val) { // e.g. NO MINVALUE
				continue;
			}
			assert(val);

			if (opt_name == "increment") {
				assert(val->type == T_PGInteger);
				info->increment = val->val.ival;
				if (info->increment == 0) {
					throw ParserException("Increment must not be zero");
				}
				if (info->increment < 0) {
					info->start_value = info->max_value = -1;
					info->min_value = numeric_limits<int64_t>::min();
				} else {
					info->start_value = info->min_value = 1;
					info->max_value = numeric_limits<int64_t>::max();
				}
			} else if (opt_name == "minvalue") {
				assert(val->type == T_PGInteger);
				info->min_value = val->val.ival;
				if (info->increment > 0) {
					info->start_value = info->min_value;
				}
			} else if (opt_name == "maxvalue") {
				assert(val->type == T_PGInteger);
				info->max_value = val->val.ival;
				if (info->increment < 0) {
					info->start_value = info->max_value;
				}
			} else if (opt_name == "start") {
				assert(val->type == T_PGInteger);
				info->start_value = val->val.ival;
			} else if (opt_name == "cycle") {
				assert(val->type == T_PGInteger);
				info->cycle = val->val.ival > 0;
			} else {
				throw ParserException("Unrecognized option \"%s\" for CREATE SEQUENCE", opt_name.c_str());
			}
		}
	}
	info->temporary = !stmt->sequence->relpersistence;
	info->on_conflict = stmt->if_not_exists ? OnCreateConflict::IGNORE : OnCreateConflict::ERROR;
	if (info->max_value <= info->min_value) {
		throw ParserException("MINVALUE (%lld) must be less than MAXVALUE (%lld)", info->min_value, info->max_value);
	}
	if (info->start_value < info->min_value) {
		throw ParserException("START value (%lld) cannot be less than MINVALUE (%lld)", info->start_value,
		                      info->min_value);
	}
	if (info->start_value > info->max_value) {
		throw ParserException("START value (%lld) cannot be greater than MAXVALUE (%lld)", info->start_value,
		                      info->max_value);
	}
	result->info = move(info);
	return result;
}
