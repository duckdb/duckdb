#include "duckdb/parser/statement/vacuum_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

VacuumOptions ParseOptions(const int32_t options) {
	VacuumOptions result;
	if (options & duckdb_libpgquery::PGVacuumOption::PG_VACOPT_VACUUM) {
		result.vacuum = true;
	}
	if (options & duckdb_libpgquery::PGVacuumOption::PG_VACOPT_ANALYZE) {
		result.analyze = true;
	}
	if (options & duckdb_libpgquery::PGVacuumOption::PG_VACOPT_VERBOSE) {
		throw NotImplementedException("Verbose vacuum option");
	}
	if (options & duckdb_libpgquery::PGVacuumOption::PG_VACOPT_FREEZE) {
		throw NotImplementedException("Freeze vacuum option");
	}
	if (options & duckdb_libpgquery::PGVacuumOption::PG_VACOPT_FULL) {
		throw NotImplementedException("Full vacuum option");
	}
	if (options & duckdb_libpgquery::PGVacuumOption::PG_VACOPT_NOWAIT) {
		throw NotImplementedException("No Wait vacuum option");
	}
	if (options & duckdb_libpgquery::PGVacuumOption::PG_VACOPT_SKIPTOAST) {
		throw NotImplementedException("Skip Toast vacuum option");
	}
	if (options & duckdb_libpgquery::PGVacuumOption::PG_VACOPT_DISABLE_PAGE_SKIPPING) {
		throw NotImplementedException("Disable Page Skipping vacuum option");
	}
	return result;
}

unique_ptr<SQLStatement> Transformer::TransformVacuum(duckdb_libpgquery::PGVacuumStmt &stmt) {
	auto result = make_uniq<VacuumStatement>(ParseOptions(stmt.options));

	if (stmt.relation) {
		result->info->ref = TransformRangeVar(*stmt.relation);
		result->info->has_table = true;
	}

	if (stmt.va_cols) {
		D_ASSERT(result->info->has_table);
		for (auto col_node = stmt.va_cols->head; col_node != nullptr; col_node = col_node->next) {
			auto value = PGPointerCast<duckdb_libpgquery::PGValue>(col_node->data.ptr_value);
			result->info->columns.emplace_back(value->val.str);
		}
	}
	return std::move(result);
}

} // namespace duckdb
