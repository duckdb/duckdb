#include "duckdb/optimizer/relation_statistics/finite_filter_selection.hpp"
#include "duckdb/optimizer/relation_statistics/equality_cardinality.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

FiniteFilterSelection::FiniteFilterSelection(const RelationStats &stats) : source(stats) {
}

FiniteFilterSelection::~FiniteFilterSelection() = default;

void FiniteFilterSelection::AddOptionalSourceFacts(const Expression &expression, const vector<ColumnBinding> &bindings,
                                                   optional_ptr<const BaseStatistics> statistics) {
	// An optional predicate does not restrict rows, but its literals can expose facts about the source.
	auto child = ExpressionFilter::GetOptionalFilterChild(expression);
	if (!child || !statistics || statistics->CanHaveNull() || !statistics->CanHaveNoNull()) {
		return;
	}
	auto parsed = Parse(*child, bindings);
	if (parsed) {
		auto excluded = SourceImpossibleValues(parsed->restriction, *statistics);
		if (excluded) {
			column_selections[parsed->column].proven.push_back(std::move(excluded));
		}
	}
}

void FiniteFilterSelection::Add(const Expression &expression, const vector<ColumnBinding> &bindings, bool proven_true,
                                optional_ptr<const BaseStatistics> statistics) {
	if (ExpressionFilter::IsRootOptionalExpression(expression)) {
		AddOptionalSourceFacts(expression, bindings, statistics);
		return;
	}
	auto parsed = Parse(expression, bindings);
	if (!parsed) {
		return;
	}
	auto &entry = column_selections[parsed->column];
	entry.expressions.push_back(expression);
	if (statistics && !statistics->CanHaveNull() && statistics->CanHaveNoNull()) {
		auto excluded = SourceImpossibleValues(parsed->restriction, *statistics);
		if (!parsed->restriction || excluded == parsed->restriction) {
			proven_true = true;
		} else if (excluded) {
			entry.proven.push_back(std::move(excluded));
		}
	}
	if (proven_true) {
		entry.proven.push_back(std::move(parsed->restriction));
		selected.insert(&expression);
	} else {
		entry.predicates.push_back(std::move(parsed->restriction));
	}
}

bool FiniteFilterSelection::IsApplied(const Expression &expression) const {
	return selected.count(&expression) != 0;
}

void FiniteFilterSelection::PublishNullRejection(RelationStats &stats) const {
	// Estimate from the input's NULL possibility first. Every recognized mandatory predicate rejects NULL.
	for (idx_t c = 0; c < value_aliases.size(); c++) {
		if (column_selections.count(value_aliases[c])) {
			stats.columns[c].can_have_null = false;
		}
	}
}

bool FiniteFilterSelection::IsStrongerSelection(const ColumnValueSelection &candidate,
                                                const ColumnValueSelection &existing, CardinalityValue total) {
	auto retained = [&](const ColumnValueSelection &selection) {
		auto count = CardinalityValue(selection.count);
		return selection.excluded ? total - MinValue(total, count) : MinValue(total, count);
	};
	auto candidate_retained = retained(candidate);
	auto existing_retained = retained(existing);
	if (candidate_retained != existing_retained) {
		return candidate_retained < existing_retained;
	}
	return candidate.excluded != existing.excluded ? !candidate.excluded
	       : candidate.excluded                    ? candidate.count > existing.count
	                                               : candidate.count < existing.count;
}

FilterEstimate FiniteFilterSelection::Apply(RelationStats &stats) {
	if (column_selections.empty()) {
		return {stats.cardinality, true};
	}
	map<idx_t, ColumnValueSelection> numeric_selections;
	double fallback_fraction = 1;
	bool proven_empty = false;
	map<idx_t, shared_ptr<const ColumnValueRestriction>> restrictions;
	map<idx_t, vector<shared_ptr<const ColumnValueRestriction>>> inherited;
	for (idx_t c = 0; c < inputs[0].columns.size(); c++) {
		inherited[value_aliases[c]].push_back(inputs[0].columns[c].value_restriction);
	}
	for (auto &entry : column_selections) {
		auto &selection = entry.second;
		auto &column = inputs[0].columns[entry.first];
		selection.proven.push_back(
		    ColumnValueRestriction::Combine(inherited.at(entry.first), RestrictionCombination::CONJUNCTION));
		auto prior = ColumnValueRestriction::Combine(selection.proven, RestrictionCombination::CONJUNCTION);
		auto predicate = ColumnValueRestriction::Combine(selection.predicates, RestrictionCombination::CONJUNCTION);
		auto combined = ColumnValueRestriction::Combine({prior, predicate}, RestrictionCombination::CONJUNCTION);
		restrictions[entry.first] = combined;
		bool consume = true;
		if (combined && !combined->excluded && combined->values.empty()) {
			// The actual null-rejecting predicate has no TRUE values, independently of estimated NDV.
			proven_empty = true;
		} else if (selection.predicates.empty() || (!column.can_have_null && combined == prior)) {
			// A proven/entailed predicate contributes its value constraint without selecting again.
		} else if (column.total_domain.has_usable_domain && !column.can_have_null) {
			D_ASSERT(combined);
			auto count = combined->values.size();
			if (combined->excluded && prior) {
				D_ASSERT(prior->excluded && count >= prior->values.size());
				count -= prior->values.size();
			}
			auto numeric =
			    model->Normalize(ColumnBinding(TableIndex(0), ProjectionIndex(entry.first))).column_index.GetIndex();
			ColumnValueSelection candidate {numeric, count, combined->excluded};
			auto existing = numeric_selections.find(numeric);
			if (existing == numeric_selections.end()) {
				numeric_selections.emplace(numeric, candidate);
			} else if (IsStrongerSelection(candidate, existing->second,
			                               inputs[0].columns[numeric].total_domain.distinct_count)) {
				// Distinct value coordinates in one statistical class contribute only the strongest conjunct.
				existing->second = candidate;
			}
		} else if (prior && column.can_have_null) {
			// A non-NULL value implication cannot count the NULL rows introduced at a later boundary.
			auto finite = predicate && !predicate->excluded;
			auto fraction = finite ? RelationStatisticsHelper::DEFAULT_SELECTIVITY
			                       : 1 - RelationStatisticsHelper::DEFAULT_SELECTIVITY;
			fallback_fraction = MinValue(fallback_fraction, fraction);
		} else {
			// Capture mandatory value implications even when the ordinary numerical path is unsupported.
			consume = false;
		}
		if (consume) {
			for (auto &expression : selection.expressions) {
				selected.insert(&expression.get());
			}
		}
	}
	for (idx_t c = 0; c < inputs[0].columns.size(); c++) {
		auto restriction = restrictions.find(value_aliases[c]);
		if (restriction != restrictions.end()) {
			inputs[0].columns[c].value_restriction = restriction->second;
		}
	}
	stats = std::move(inputs[0]);
	if (proven_empty) {
		stats.max_cardinality = optional_idx(0);
		RelationStatisticsHelper::ApplyCardinality(stats, 0);
	} else if (!numeric_selections.empty()) {
		vector<ColumnValueSelection> coordinates;
		coordinates.reserve(numeric_selections.size());
		for (auto &entry : numeric_selections) {
			coordinates.push_back(entry.second);
		}
		RelationStatisticsHelper::ApplyFiniteSelection(stats, coordinates);
	}
	return {RelationStatisticsHelper::FilterFraction(stats.cardinality, fallback_fraction), true, proven_empty};
}

shared_ptr<const ColumnValueRestriction>
FiniteFilterSelection::SourceImpossibleValues(const shared_ptr<const ColumnValueRestriction> &restriction,
                                              const BaseStatistics &statistics) {
	if (!restriction || statistics.GetStatsType() != StatisticsType::NUMERIC_STATS) {
		return nullptr;
	}
	value_set_t excluded;
	for (auto &value : restriction->values) {
		if (!statistics.GetType().EqualsIncludingCollation(value.type())) {
			return nullptr;
		}
		auto pruning =
		    NumericStats::CheckZonemap(statistics, ExpressionType::COMPARE_EQUAL, array_ptr<const Value>(&value, 1));
		if (pruning == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
		    pruning == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
			excluded.insert(value);
		}
	}
	// A source-impossible value is absent before either an inclusion or an exclusion is applied.
	return restriction->excluded && excluded.size() == restriction->values.size()
	           ? restriction
	           : ColumnValueRestriction::Create(std::move(excluded), true);
}

optional_idx FiniteFilterSelection::ResolveValueColumn(const Expression &expression,
                                                       const vector<ColumnBinding> &bindings) {
	auto &stats = model ? inputs[0] : source;
	RelationColumnStatsLookup columns(stats);
	auto column = RelationStatisticsHelper::FindFilterColumn(expression, columns, bindings);
	if (!column) {
		return {};
	}
	auto index = stats.FindColumn(column->binding);
	if (!model) {
		inputs.push_back(source);
		model = make_uniq<EqualityCardinalityModel>(inputs);
		value_aliases = inputs[0].GetValueAliases();
	}
	return value_aliases[index.GetIndex()];
}

optional<FiniteFilterSelection::ParsedRestriction>
FiniteFilterSelection::Parse(const Expression &expression, const vector<ColumnBinding> &bindings, bool negated) {
	if (ExpressionFilter::IsRootOptionalExpression(expression)) {
		return {};
	}
	auto type = expression.GetExpressionType();
	if (type == ExpressionType::OPERATOR_NOT && expression.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &children = expression.Cast<BoundOperatorExpression>().GetChildren();
		return children.size() == 1 ? Parse(*children[0], bindings, !negated) : optional<ParsedRestriction>();
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		return ParseConjunction(expression, bindings, negated);
	}
	return ParseAtom(expression, bindings, negated);
}

optional<FiniteFilterSelection::ParsedRestriction>
FiniteFilterSelection::ParseConjunction(const Expression &expression, const vector<ColumnBinding> &bindings,
                                        bool negated) {
	auto type = expression.GetExpressionType();
	optional<ParsedRestriction> result;
	vector<shared_ptr<const ColumnValueRestriction>> predicates;
	for (auto &child : expression.Cast<BoundConjunctionExpression>().GetChildren()) {
		auto next = Parse(*child, bindings, negated);
		if (!next || (result && result->column != next->column)) {
			return {};
		}
		predicates.push_back(next->restriction);
		result = std::move(next);
	}
	if (result) {
		result->restriction = ColumnValueRestriction::Combine(
		    predicates, (type == ExpressionType::CONJUNCTION_AND) != negated ? RestrictionCombination::CONJUNCTION
		                                                                     : RestrictionCombination::DISJUNCTION);
	}
	return result;
}

optional<FiniteFilterSelection::ParsedRestriction>
FiniteFilterSelection::ParseAtom(const Expression &expression, const vector<ColumnBinding> &bindings, bool negated) {
	auto type = expression.GetExpressionType();
	optional_ptr<const Expression> coordinate;
	vector<reference<const Expression>> constants;
	auto excluded = type == ExpressionType::COMPARE_NOTEQUAL || type == ExpressionType::COMPARE_NOT_IN;
	if (type == ExpressionType::COMPARE_EQUAL || type == ExpressionType::COMPARE_NOTEQUAL) {
		auto &comparison = expression.Cast<BoundFunctionExpression>();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			coordinate = right;
			constants.push_back(left);
		} else {
			coordinate = left;
			constants.push_back(right);
		}
	} else if ((type == ExpressionType::COMPARE_IN || type == ExpressionType::COMPARE_NOT_IN) &&
	           expression.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &children = expression.Cast<BoundOperatorExpression>().GetChildren();
		if (children.empty()) {
			return {};
		}
		coordinate = *children[0];
		for (idx_t i = 1; i < children.size(); i++) {
			constants.push_back(*children[i]);
		}
	} else {
		return {};
	}
	// Negate supported atoms before interpreting NULLs, rather than complementing their TRUE sets.
	excluded = excluded != negated;
	// Top-level UNION equality can be UNKNOWN for an active NULL despite a non-NULL outer Value.
	if (coordinate->GetReturnType().id() == LogicalTypeId::UNION) {
		return {};
	}
	auto column = ResolveValueColumn(*coordinate, bindings);
	if (!column.IsValid()) {
		return {};
	}
	value_set_t values;
	bool has_null = false;
	for (auto &constant : constants) {
		if (constant.get().GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return {};
		}
		auto &value = constant.get().Cast<BoundConstantExpression>().GetValue();
		if (!coordinate->GetReturnType().EqualsIncludingCollation(value.type())) {
			return {};
		}
		if (value.IsNull()) {
			has_null = true;
		} else {
			values.insert(value);
		}
	}
	if (excluded && has_null) {
		// Neither <> NULL nor NOT IN (..., NULL, ...) can be TRUE, even for a NULL probe.
		values.clear();
		excluded = false;
	}
	return ParsedRestriction {column.GetIndex(), ColumnValueRestriction::Create(std::move(values), excluded)};
}

} // namespace duckdb
