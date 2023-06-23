#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/core_functions/scalar/struct_functions.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"

#include <cstring>

namespace duckdb {

static Value ConstructValueFromFunction(const ParsedExpression &expr) {
	// We have to construct it like this because we don't have the ClientContext for binding/executing the expr here
	switch (expr.type) {
	case ExpressionType::FUNCTION: {
		auto &function = expr.Cast<FunctionExpression>();
		if (function.function_name == "struct_pack") {
			unordered_set<string> unique_names;
			child_list_t<Value> values;
			values.reserve(function.children.size());
			for (const auto &child : function.children) {
				if (!unique_names.insert(child->alias).second) {
					throw BinderException("Duplicate struct entry name \"%s\"", child->alias);
				}
				values.emplace_back(child->alias, ConstructValueFromFunction(*child));
			}
			return Value::STRUCT(std::move(values));
		} else {
			throw ParserException("Unsupported function in COPY options: %s", function.function_name);
		}
	}
	case ExpressionType::VALUE_CONSTANT: {
		auto &constant = expr.Cast<ConstantExpression>();
		return constant.value;
	}
	default:
		throw ParserException("Unsupported expression in COPY options: %s", expr.ToString());
	}
}

void Transformer::TransformCopyOptions(CopyInfo &info, optional_ptr<duckdb_libpgquery::PGList> options) {
	if (!options) {
		return;
	}

	// iterate over each option
	duckdb_libpgquery::PGListCell *cell;
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
		// otherwise
		if (info.options.find(def_elem->defname) != info.options.end()) {
			throw ParserException("Unexpected duplicate option \"%s\"", def_elem->defname);
		}
		if (!def_elem->arg) {
			info.options[def_elem->defname] = vector<Value>();
			continue;
		}
		switch (def_elem->arg->type) {
		case duckdb_libpgquery::T_PGList: {
			auto column_list = PGPointerCast<duckdb_libpgquery::PGList>(def_elem->arg);
			for (auto c = column_list->head; c != nullptr; c = lnext(c)) {
				auto target = PGPointerCast<duckdb_libpgquery::PGResTarget>(c->data.ptr_value);
				info.options[def_elem->defname].push_back(Value(target->name));
			}
			break;
		}
		case duckdb_libpgquery::T_PGAStar:
			info.options[def_elem->defname].push_back(Value("*"));
			break;
		case duckdb_libpgquery::T_PGFuncCall: {
			auto func_call = PGPointerCast<duckdb_libpgquery::PGFuncCall>(def_elem->arg);
			auto func_expr = TransformFuncCall(*func_call);
			info.options[def_elem->defname].push_back(ConstructValueFromFunction(*func_expr));
			break;
		}
		default: {
			auto val = PGPointerCast<duckdb_libpgquery::PGValue>(def_elem->arg);
			info.options[def_elem->defname].push_back(TransformValue(*val)->value);
			break;
		}
		}
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
	if (StringUtil::EndsWith(info.file_path, ".parquet")) {
		info.format = "parquet";
	} else if (StringUtil::EndsWith(info.file_path, ".json") || StringUtil::EndsWith(info.file_path, ".ndjson")) {
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
		result->select_statement = TransformSelectNode(*PGPointerCast<duckdb_libpgquery::PGSelectStmt>(stmt.query));
	}

	// handle the different options of the COPY statement
	TransformCopyOptions(info, stmt.options);

	return result;
}

} // namespace duckdb
