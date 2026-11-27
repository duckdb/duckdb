//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_order_source_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics.hpp"

namespace duckdb {

class ClientContext;
class LogicalGet;
class LogicalOperator;

struct JoinColumnSource {
	ColumnBinding binding;
	//! The provider's integer row count, before estimated scan/filter selectivity.
	idx_t source_cardinality;
	CardinalityValue scan_cardinality;
	DistinctCount domain;
};

struct JoinColumnSources {
public:
	explicit JoinColumnSources(JoinColumnSource direct) : direct(direct) {
	}

public:
	JoinColumnSource direct;
	vector<JoinColumnSource> equivalents;
};

using join_column_sources_t = column_binding_map_t<JoinColumnSources>;

//! Source selection shared by predicate fallbacks and factor-local equality costing.
//! Updates are raw observations; resolving a binding never changes its original population.
class JoinDomainEstimate {
public:
	void Update(ColumnBinding binding, const DistinctCount &domain, optional_ptr<const JoinColumnSources> sources);
	DistinctCount Resolve(const vector<ColumnBinding> &bindings) const;
	//! The selected raw fallback, before a binding's local population restriction.
	optional<CardinalityValue> UnknownScanBasis() const;

private:
	void AddSource(const JoinColumnSource &source);

private:
	optional<DistinctCount> estimate;
	column_binding_map_t<DistinctCount> local_fallbacks;
	column_binding_set_t sources;
	idx_t reliable_source_count = 0;
	idx_t reliable_source_cardinality = NumericLimits<idx_t>::Maximum();
	vector<CardinalityValue> min_max_candidates;
};

//! Immutable source evidence captured before any join-order subtree is optimized.
class JoinOrderSourceStats {
public:
	JoinOrderSourceStats(ClientContext &context, LogicalOperator &root);

public:
	//! Capture additional read-only roots, such as externally supplied CTE definitions.
	void AddPlan(ClientContext &context, LogicalOperator &root);
	optional_ptr<const RelationStats> FindScanStats(const LogicalGet &get) const;
	//! Resolve the visible columns of one relation, respecting statistics boundaries.
	join_column_sources_t Resolve(LogicalOperator &root) const;

private:
	struct ScanStats {
		idx_t cardinality;
		RelationStats stats;
		column_binding_map_t<DistinctCount> source_domains;
	};

private:
	unordered_map<TableIndex, ScanStats> scans;
	unordered_map<TableIndex, join_column_sources_t> projections;
};

} // namespace duckdb
