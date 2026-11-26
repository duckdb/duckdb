//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/relation_statistics/finite_filter_selection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"

namespace duckdb {

class EqualityCardinalityModel;

//! Compose current-value predicates before selecting from the already conditioned population.
class FiniteFilterSelection {
public:
	explicit FiniteFilterSelection(const RelationStats &stats);
	~FiniteFilterSelection();

public:
	void AddOptionalSourceFacts(const Expression &expression, const vector<ColumnBinding> &bindings,
	                            optional_ptr<const BaseStatistics> statistics);
	void Add(const Expression &expression, const vector<ColumnBinding> &bindings, bool proven_true = false,
	         optional_ptr<const BaseStatistics> statistics = nullptr);

	//! Apply once after collecting predicates. Only IsApplied and PublishNullRejection may follow.
	FilterEstimate Apply(RelationStats &stats);
	bool IsApplied(const Expression &expression) const;
	void PublishNullRejection(RelationStats &stats) const;

private:
	struct ParsedRestriction {
		idx_t column;
		shared_ptr<const ColumnValueRestriction> restriction;
	};
	struct ColumnSelection {
		vector<shared_ptr<const ColumnValueRestriction>> predicates;
		vector<shared_ptr<const ColumnValueRestriction>> proven;
		vector<reference<const Expression>> expressions;
	};

private:
	static shared_ptr<const ColumnValueRestriction>
	SourceImpossibleValues(const shared_ptr<const ColumnValueRestriction> &restriction,
	                       const BaseStatistics &statistics);
	static bool IsStrongerSelection(const ColumnValueSelection &candidate, const ColumnValueSelection &existing,
	                                CardinalityValue total);

	optional_idx ResolveValueColumn(const Expression &expression, const vector<ColumnBinding> &bindings);
	optional<ParsedRestriction> Parse(const Expression &expression, const vector<ColumnBinding> &bindings,
	                                  bool negated = false);
	optional<ParsedRestriction> ParseConjunction(const Expression &expression, const vector<ColumnBinding> &bindings,
	                                             bool negated);
	optional<ParsedRestriction> ParseAtom(const Expression &expression, const vector<ColumnBinding> &bindings,
	                                      bool negated);

private:
	const RelationStats &source;
	//! The input snapshot outlives the model borrowing it; Apply moves its result to the caller.
	vector<RelationStats> inputs;
	unique_ptr<EqualityCardinalityModel> model;
	vector<idx_t> value_aliases;

	map<idx_t, ColumnSelection> column_selections;
	unordered_set<const Expression *> selected;
};

} // namespace duckdb
