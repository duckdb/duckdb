#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/enum_class_hash.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateSequence(duckdb_libpgquery::PGCreateSeqStmt &stmt) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateSequenceInfo>();

	auto qname = TransformQualifiedName(*stmt.sequence);
	info->catalog = qname.catalog;
	info->schema = qname.schema;
	info->name = qname.name;

	if (stmt.options) {
		int64_t default_start_value = info->start_value;
		bool has_start_value = false;
		unordered_set<SequenceInfo, EnumClassHash> used;
		duckdb_libpgquery::PGListCell *cell = nullptr;
		for_each_cell(cell, stmt.options->head) {
			auto def_elem = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);
			string opt_name = string(def_elem->defname);
			auto val = PGPointerCast<duckdb_libpgquery::PGValue>(def_elem->arg);
			bool nodef = def_elem->defaction == duckdb_libpgquery::PG_DEFELEM_UNSPEC && !val; // e.g. NO MINVALUE
			int64_t opt_value = 0;

			if (val) {
				if (val->type == duckdb_libpgquery::T_PGInteger) {
					opt_value = val->val.ival;
				} else if (val->type == duckdb_libpgquery::T_PGFloat) {
					if (!TryCast::Operation<string_t, int64_t>(string_t(val->val.str), opt_value, true)) {
						throw ParserException("Expected an integer argument for option %s", opt_name);
					}
				} else {
					throw ParserException("Expected an integer argument for option %s", opt_name);
				}
			}
			if (opt_name == "increment") {
				if (used.find(SequenceInfo::SEQ_INC) != used.end()) {
					throw ParserException("Increment value should be passed as most once");
				}
				used.insert(SequenceInfo::SEQ_INC);
				if (nodef) {
					continue;
				}

				info->increment = opt_value;
				if (info->increment == 0) {
					throw ParserException("Increment must not be zero");
				}
				if (info->increment < 0) {
					default_start_value = info->max_value = -1;
					info->min_value = NumericLimits<int64_t>::Minimum();
				} else {
					default_start_value = info->min_value = 1;
					info->max_value = NumericLimits<int64_t>::Maximum();
				}
			} else if (opt_name == "minvalue") {
				if (used.find(SequenceInfo::SEQ_MIN) != used.end()) {
					throw ParserException("Minvalue should be passed as most once");
				}
				used.insert(SequenceInfo::SEQ_MIN);
				if (nodef) {
					continue;
				}

				info->min_value = opt_value;
				if (info->increment > 0) {
					default_start_value = info->min_value;
				}
			} else if (opt_name == "maxvalue") {
				if (used.find(SequenceInfo::SEQ_MAX) != used.end()) {
					throw ParserException("Maxvalue should be passed as most once");
				}
				used.insert(SequenceInfo::SEQ_MAX);
				if (nodef) {
					continue;
				}

				info->max_value = opt_value;
				if (info->increment < 0) {
					default_start_value = info->max_value;
				}
			} else if (opt_name == "start") {
				if (used.find(SequenceInfo::SEQ_START) != used.end()) {
					throw ParserException("Start value should be passed as most once");
				}
				used.insert(SequenceInfo::SEQ_START);
				if (nodef) {
					continue;
				}
				has_start_value = true;
				info->start_value = opt_value;
			} else if (opt_name == "cycle") {
				if (used.find(SequenceInfo::SEQ_CYCLE) != used.end()) {
					throw ParserException("Cycle value should be passed as most once");
				}
				used.insert(SequenceInfo::SEQ_CYCLE);
				if (nodef) {
					continue;
				}

				info->cycle = opt_value > 0;
			} else {
				throw ParserException("Unrecognized option \"%s\" for CREATE SEQUENCE", opt_name);
			}
		}
		if (!has_start_value) {
			info->start_value = default_start_value;
		}
	}
	info->temporary = !stmt.sequence->relpersistence;
	info->on_conflict = TransformOnConflict(stmt.onconflict);
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
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
