#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/core_functions/scalar/struct_functions.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"

#include <cstring>

namespace duckdb {

void Transformer::ParseGenericOptionListEntry(case_insensitive_map_t<vector<Value>> &result_options, string &name,
                                              duckdb_libpgquery::PGNode *arg) {
	// otherwise
	if (result_options.find(name) != result_options.end()) {
		throw ParserException("Unexpected duplicate option \"%s\"", name);
	}
	if (!arg) {
		result_options[name] = vector<Value>();
		return;
	}
	switch (arg->type) {
	case duckdb_libpgquery::T_PGList: {
		auto column_list = PGPointerCast<duckdb_libpgquery::PGList>(arg);
		for (auto c = column_list->head; c != nullptr; c = lnext(c)) {
			auto target = PGPointerCast<duckdb_libpgquery::PGResTarget>(c->data.ptr_value);
			result_options[name].push_back(Value(target->name));
		}
		break;
	}
	case duckdb_libpgquery::T_PGAStar:
		result_options[name].push_back(Value("*"));
		break;
	case duckdb_libpgquery::T_PGFuncCall: {
		auto func_call = PGPointerCast<duckdb_libpgquery::PGFuncCall>(arg);
		auto func_expr = TransformFuncCall(*func_call);

		Value value;
		if (!Transformer::ConstructConstantFromExpression(*func_expr, value)) {
			throw ParserException("Unsupported expression in option list: %s", func_expr->ToString());
		}
		result_options[name].push_back(std::move(value));
		break;
	}
	default: {
		auto val = PGPointerCast<duckdb_libpgquery::PGValue>(arg);
		result_options[name].push_back(TransformValue(*val)->value);
		break;
	}
	}
}

void Transformer::TransformCopyOptions(CopyInfo &info, optional_ptr<duckdb_libpgquery::PGList> options) {
	if (!options) {
		return;
	}

	duckdb_libpgquery::PGListCell *cell;
	// iterate over each option
	for_each_cell(cell, options->head) {
		auto def_elem = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);
		if (StringUtil::Lower(def_elem->defname) == "format") {
			// format specifier: interpret this option
			auto format_val = PGPointerCast<duckdb_libpgquery::PGValue>(def_elem->arg);
			if (!format_val || format_val->type != duckdb_libpgquery::T_PGString) {
				throw ParserException("Unsupported parameter type for FORMAT: expected e.g. FORMAT 'csv', 'parquet'");
			}
			info.format = StringUtil::Lower(format_val->val.str);
			continue;
		}

		// The rest ends up in the options
		string name = def_elem->defname;
		ParseGenericOptionListEntry(info.options, name, def_elem->arg);
	}
}

unique_ptr<CopyStatement> Transformer::TransformCopy(duckdb_libpgquery::PGCopyStmt &stmt) {
	auto result = make_uniq<CopyStatement>();
	auto &info = *result->info;

	// get file_path and is_from
	info.is_from = stmt.is_from;
	if (!stmt.filename) {
		// stdin/stdout
		info.file_path = info.is_from ? "/dev/stdin" : "/dev/stdout";
	} else {
		// copy to a file
		info.file_path = stmt.filename;
	}

	if (ReplacementScan::CanReplace(info.file_path, {"parquet"})) {
		info.format = "parquet";
	} else if (ReplacementScan::CanReplace(info.file_path, {"json", "jsonl", "ndjson"})) {
		info.format = "json";
	} else {
		info.format = "csv";
	}

	// get select_list
	if (stmt.attlist) {
		for (auto n = stmt.attlist->head; n != nullptr; n = n->next) {
			auto target = PGPointerCast<duckdb_libpgquery::PGResTarget>(n->data.ptr_value);
			if (target->name) {
				info.select_list.emplace_back(target->name);
			}
		}
	}

	if (stmt.relation) {
		auto ref = TransformRangeVar(*stmt.relation);
		auto &table = ref->Cast<BaseTableRef>();
		info.table = table.table_name;
		info.schema = table.schema_name;
		info.catalog = table.catalog_name;
	} else {
		info.select_statement = TransformSelectNode(*PGPointerCast<duckdb_libpgquery::PGSelectStmt>(stmt.query));
	}

	// handle the different options of the COPY statement
	TransformCopyOptions(info, stmt.options);

	return result;
}

} // namespace duckdb
