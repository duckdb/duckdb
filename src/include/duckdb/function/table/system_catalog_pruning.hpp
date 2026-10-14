//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/system_catalog_pruning.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! Bind data for the system table functions that emit the catalog/schema identity columns
//! (database_name, database_oid, schema_name, schema_oid). Holds the filter predicates that
//! SystemCatalogPruning::PushdownComplexFilter retained; SystemCatalogPruning::GetSchemas uses
//! them in Init to skip catalogs and schemas that cannot contribute rows.
struct SystemCatalogPruningData : public FunctionData {
	//! Position of each identity column in the function's output; invalid when the function lacks it.
	optional_idx database_name_idx;
	optional_idx database_oid_idx;
	optional_idx schema_name_idx;
	optional_idx schema_oid_idx;
	//! Retained predicates. Every BoundColumnRefExpression is rebound to
	//! ColumnBinding(<table index>, <primary column index>) so GetSchemas can match on primary index.
	vector<unique_ptr<Expression>> predicates;

	//! Whether the given primary column index is one of the four identity columns.
	bool IsIdentityColumn(idx_t primary_index) const;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! Counters reported through get_metrics for EXPLAIN ANALYZE.
struct SystemCatalogPruningStats {
	idx_t catalogs_total = 0;
	idx_t catalogs_scanned = 0;
	idx_t schemas_total = 0;
	idx_t schemas_scanned = 0;
	bool used_schema_lookup = false;
};

class SystemCatalogPruning {
public:
	//! Bind helper: locate the identity columns by name in `names`. Always returns non-null bind data.
	static unique_ptr<FunctionData> CreateBindData(const vector<Identifier> &names);
	//! pushdown_complex_filter callback. Copies eligible predicates into the bind data; `filters` is left untouched.
	static void PushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
	                                  vector<unique_ptr<Expression>> &filters);
	//! Init helper: the schemas that survive the predicates, in Catalog::GetAllSchemas order.
	//! With no bind data or no predicates this is exactly Catalog::GetAllSchemas(context).
	static vector<reference<SchemaCatalogEntry>>
	GetSchemas(ClientContext &context, optional_ptr<const FunctionData> bind_data, SystemCatalogPruningStats &stats);
	//! to_string helper: a "Catalog Filters" entry listing the retained predicates, if any.
	static InsertionOrderPreservingMap<string> ToString(TableFunctionToStringInput &input);
	//! get_metrics helper: writes the counters into input.operator_metrics via AddExtraInfo.
	static void ReportMetrics(TableFunctionGetMetricsInput &input, const SystemCatalogPruningStats &stats);
	//! Sets pushdown_complex_filter and to_string on a TableFunction. get_metrics is per function because
	//! it needs the function's own GlobalTableFunctionState type to reach the stats.
	static void RegisterPushdown(TableFunction &function);
};

} // namespace duckdb
