#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"
#include "duckdb/optimizer/relation_statistics/equality_cardinality.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

#include "duckdb/common/map.hpp"

namespace duckdb {

static idx_t CapMinMaxDistinctCount(uint64_t distinct_count, idx_t base_table_cardinality) {
	if (base_table_cardinality == 0 || distinct_count == 0) {
		return 0;
	}
	auto capped_distinct_count = MinValue<idx_t>(distinct_count, base_table_cardinality);
	return capped_distinct_count == NumericLimits<idx_t>::Maximum() ? 0 : capped_distinct_count;
}

static idx_t GetMinMaxSpanDistinctCount(uint64_t span, idx_t base_table_cardinality) {
	uint64_t distinct_count;
	if (!TryAddOperator::Operation<uint64_t, uint64_t, uint64_t>(span, 1, distinct_count)) {
		return 0;
	}
	return CapMinMaxDistinctCount(distinct_count, base_table_cardinality);
}

template <class T>
static idx_t GetSignedMinMaxDistinctCount(const BaseStatistics &base_stats, idx_t base_table_cardinality) {
	auto min_value = NumericStats::Min(base_stats).GetValueUnsafe<T>();
	auto max_value = NumericStats::Max(base_stats).GetValueUnsafe<T>();
	if (max_value < min_value) {
		return 0;
	}
	hugeint_t span;
	if (!TrySubtractOperator::Operation(hugeint_t(static_cast<int64_t>(max_value)),
	                                    hugeint_t(static_cast<int64_t>(min_value)), span)) {
		return 0;
	}
	uint64_t unsigned_span;
	if (!Hugeint::TryCast(span, unsigned_span)) {
		return 0;
	}
	return GetMinMaxSpanDistinctCount(unsigned_span, base_table_cardinality);
}

template <class T>
static idx_t GetUnsignedMinMaxDistinctCount(const BaseStatistics &base_stats, idx_t base_table_cardinality) {
	auto min_value = NumericStats::Min(base_stats).GetValueUnsafe<T>();
	auto max_value = NumericStats::Max(base_stats).GetValueUnsafe<T>();
	T span;
	if (!TrySubtractOperator::Operation(max_value, min_value, span)) {
		return 0;
	}
	return GetMinMaxSpanDistinctCount(static_cast<uint64_t>(span), base_table_cardinality);
}

static idx_t GetBooleanMinMaxDistinctCount(const BaseStatistics &base_stats, idx_t base_table_cardinality) {
	auto min_value = NumericStats::Min(base_stats).GetValueUnsafe<bool>();
	auto max_value = NumericStats::Max(base_stats).GetValueUnsafe<bool>();
	return CapMinMaxDistinctCount(min_value == max_value ? idx_t(1) : idx_t(2), base_table_cardinality);
}

static idx_t GetMinMaxDistinctCount(const BaseStatistics &base_stats, idx_t base_table_cardinality) {
	if (base_table_cardinality == 0 || base_stats.GetStatsType() != StatisticsType::NUMERIC_STATS ||
	    !NumericStats::HasMinMax(base_stats)) {
		return 0;
	}

	switch (base_stats.GetType().InternalType()) {
	case PhysicalType::BOOL:
		return GetBooleanMinMaxDistinctCount(base_stats, base_table_cardinality);
	case PhysicalType::INT8:
		return GetSignedMinMaxDistinctCount<int8_t>(base_stats, base_table_cardinality);
	case PhysicalType::INT16:
		return GetSignedMinMaxDistinctCount<int16_t>(base_stats, base_table_cardinality);
	case PhysicalType::INT32:
		return GetSignedMinMaxDistinctCount<int32_t>(base_stats, base_table_cardinality);
	case PhysicalType::INT64:
		return GetSignedMinMaxDistinctCount<int64_t>(base_stats, base_table_cardinality);
	case PhysicalType::UINT8:
		return GetUnsignedMinMaxDistinctCount<uint8_t>(base_stats, base_table_cardinality);
	case PhysicalType::UINT16:
		return GetUnsignedMinMaxDistinctCount<uint16_t>(base_stats, base_table_cardinality);
	case PhysicalType::UINT32:
		return GetUnsignedMinMaxDistinctCount<uint32_t>(base_stats, base_table_cardinality);
	case PhysicalType::UINT64:
		return GetUnsignedMinMaxDistinctCount<uint64_t>(base_stats, base_table_cardinality);
	default:
		return 0;
	}
}

static DistinctCount GetDistinctCountFromStats(BaseStatistics &base_stats, idx_t base_table_cardinality) {
	auto distinct_count = base_stats.GetDistinctCount();
	if (distinct_count > 0) {
		return DistinctCount(distinct_count, DistinctCountSource::HLL);
	}
	distinct_count = GetMinMaxDistinctCount(base_stats, base_table_cardinality);
	if (distinct_count > 0) {
		return DistinctCount(distinct_count, DistinctCountSource::MIN_MAX);
	}
	return DistinctCount(0, DistinctCountSource::CARDINALITY);
}

struct FilterEstimate {
	CardinalityValue cardinality;
	bool supported;
	bool proven_empty = false;

	void Combine(const FilterEstimate &other) {
		cardinality = MinValue(cardinality, other.cardinality);
		supported &= other.supported;
		proven_empty |= other.proven_empty;
	}
};

static CardinalityValue FilterFraction(CardinalityValue cardinality, double fraction) {
	return cardinality * CardinalityValue(MaxValue(0.0, MinValue(fraction, 1.0)));
}

class IntegerRangeEstimate {
public:
	explicit IntegerRangeEstimate(const BaseStatistics &statistics) : statistics(statistics) {
	}

	bool AddBound(ExpressionType comparison, const Value &constant) {
		if (constant.IsNull() || !constant.type().EqualsIncludingCollation(statistics.GetType()) ||
		    statistics.GetStatsType() != StatisticsType::NUMERIC_STATS || !NumericStats::HasMinMax(statistics)) {
			return false;
		}
		switch (constant.type().id()) {
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
			break;
		default:
			return false;
		}
		if (!has_bounds) {
			lower = NumericStats::Min(statistics).GetValue<hugeint_t>();
			upper = NumericStats::Max(statistics).GetValue<hugeint_t>();
			if (lower > upper) {
				return false;
			}
			// Widen before endpoint arithmetic: a complete 64-bit interval has 2^64 values.
			count = upper - lower + 1;
			has_bounds = true;
		}
		auto value = constant.GetValue<hugeint_t>();
		switch (comparison) {
		case ExpressionType::COMPARE_LESSTHAN:
			upper = MinValue(upper, value - 1);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			upper = MinValue(upper, value);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			lower = MaxValue(lower, value + 1);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			lower = MaxValue(lower, value);
			break;
		default:
			throw InternalException("Expected a range comparison");
		}
		return true;
	}

	optional<double> Fraction() const {
		if (!has_bounds) {
			return {};
		}
		auto matching = MaxValue(hugeint_t(0), upper - lower + 1);
		return Hugeint::Cast<double>(matching) / Hugeint::Cast<double>(count);
	}

private:
	const BaseStatistics &statistics;
	hugeint_t lower;
	hugeint_t upper;
	hugeint_t count;
	bool has_bounds = false;
};

static optional_ptr<const RelationColumnStats> FilterColumn(const Expression &expr,
                                                            const RelationColumnStatsLookup &columns,
                                                            const vector<ColumnBinding> &positional_bindings) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &ref = expr.Cast<BoundColumnRefExpression>();
		return ref.Depth() == 0 ? columns.Find(ref.Binding()) : nullptr;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto index = expr.Cast<BoundReferenceExpression>().Index();
		if (index < positional_bindings.size()) {
			return columns.Find(positional_bindings[index]);
		}
	}
	return nullptr;
}

template <class CALLBACK>
static void VisitFilterConjuncts(const Expression &expression, CALLBACK &&callback) {
	if (expression.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		for (auto &child : expression.Cast<BoundConjunctionExpression>().GetChildren()) {
			VisitFilterConjuncts(*child, callback);
		}
	} else {
		callback(expression);
	}
}

//! Compose current-value predicates before selecting from the already conditioned population.
class FiniteFilterSelection {
public:
	explicit FiniteFilterSelection(const RelationStats &stats) : source(stats) {
	}

	void AddOptionalSourceFacts(const Expression &expression, const vector<ColumnBinding> &bindings,
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
				alternatives[parsed->column].proven.push_back(std::move(excluded));
			}
		}
	}

	void Add(const Expression &expression, const vector<ColumnBinding> &bindings, bool proven_true = false,
	         optional_ptr<const BaseStatistics> statistics = nullptr) {
		if (ExpressionFilter::IsRootOptionalExpression(expression)) {
			AddOptionalSourceFacts(expression, bindings, statistics);
			return;
		}
		auto parsed = Parse(expression, bindings);
		if (!parsed) {
			return;
		}
		auto &entry = alternatives[parsed->column];
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

	bool Contains(const Expression &expression) const {
		return selected.count(&expression) != 0;
	}

	void PublishNullRejection(RelationStats &stats) const {
		// Estimate from the input's NULL possibility first. Every recognized mandatory predicate rejects NULL.
		for (idx_t c = 0; c < value_aliases.size(); c++) {
			if (alternatives.count(value_aliases[c])) {
				stats.columns[c].can_have_null = false;
			}
		}
	}

	FilterEstimate Apply(RelationStats &stats) {
		if (alternatives.empty()) {
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
		for (auto &entry : alternatives) {
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
				auto numeric = model->Normalize(ColumnBinding(TableIndex(0), ProjectionIndex(entry.first)))
				                   .column_index.GetIndex();
				ColumnValueSelection candidate {numeric, count, combined->excluded};
				auto retained = [&](const ColumnValueSelection &selection) {
					auto total = inputs[0].columns[numeric].total_domain.distinct_count;
					auto count = CardinalityValue(selection.count);
					return selection.excluded ? total - MinValue(total, count) : MinValue(total, count);
				};
				auto existing = numeric_selections.find(numeric);
				if (existing == numeric_selections.end()) {
					numeric_selections.emplace(numeric, candidate);
				} else {
					auto &old = existing->second;
					auto stronger = retained(candidate) < retained(old);
					if (retained(candidate) == retained(old)) {
						stronger = candidate.excluded != old.excluded ? !candidate.excluded
						           : candidate.excluded               ? candidate.count > old.count
						                                              : candidate.count < old.count;
					}
					if (stronger) {
						// Distinct value coordinates in one statistical class contribute only the strongest conjunct.
						old = candidate;
					}
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
			for (auto &entry : numeric_selections) {
				coordinates.push_back(entry.second);
			}
			RelationStatisticsHelper::ApplyFiniteSelection(stats, coordinates);
		}
		return {FilterFraction(stats.cardinality, fallback_fraction), true, proven_empty};
	}

private:
	static shared_ptr<const ColumnValueRestriction>
	SourceImpossibleValues(const shared_ptr<const ColumnValueRestriction> &restriction,
	                       const BaseStatistics &statistics) {
		if (!restriction || statistics.GetStatsType() != StatisticsType::NUMERIC_STATS) {
			return nullptr;
		}
		value_set_t excluded;
		for (auto &value : restriction->values) {
			if (!statistics.GetType().EqualsIncludingCollation(value.type())) {
				return nullptr;
			}
			auto pruning = NumericStats::CheckZonemap(statistics, ExpressionType::COMPARE_EQUAL,
			                                          array_ptr<const Value>(&value, 1));
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

	struct Alternatives {
		idx_t column;
		shared_ptr<const ColumnValueRestriction> restriction;
	};

	struct Selection {
		vector<shared_ptr<const ColumnValueRestriction>> predicates;
		vector<shared_ptr<const ColumnValueRestriction>> proven;
		vector<reference<const Expression>> expressions;
	};

	optional_idx Column(const Expression &expression, const vector<ColumnBinding> &bindings) {
		auto &stats = model ? inputs[0] : source;
		RelationColumnStatsLookup columns(stats);
		auto column = FilterColumn(expression, columns, bindings);
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

	optional<Alternatives> Parse(const Expression &expression, const vector<ColumnBinding> &bindings,
	                             bool negated = false) {
		if (ExpressionFilter::IsRootOptionalExpression(expression)) {
			return {};
		}
		auto type = expression.GetExpressionType();
		if (type == ExpressionType::OPERATOR_NOT &&
		    expression.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
			auto &children = expression.Cast<BoundOperatorExpression>().GetChildren();
			return children.size() == 1 ? Parse(*children[0], bindings, !negated) : optional<Alternatives>();
		}
		if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
			optional<Alternatives> result;
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
				result->restriction =
				    ColumnValueRestriction::Combine(predicates, (type == ExpressionType::CONJUNCTION_AND) != negated
				                                                    ? RestrictionCombination::CONJUNCTION
				                                                    : RestrictionCombination::DISJUNCTION);
			}
			return result;
		}
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
		auto column = Column(*coordinate, bindings);
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
		return Alternatives {column.GetIndex(), ColumnValueRestriction::Create(std::move(values), excluded)};
	}

	const RelationStats &source;
	vector<RelationStats> inputs;
	unique_ptr<EqualityCardinalityModel> model;
	vector<idx_t> value_aliases;
	map<idx_t, Selection> alternatives;
	unordered_set<const Expression *> selected;
};

static FilterEstimate EstimateNullCheckRows(CardinalityValue cardinality, const BoundOperatorExpression &expr,
                                            const RelationColumnStatsLookup &columns,
                                            const vector<ColumnBinding> &positional_bindings) {
	auto &children = expr.GetChildren();
	if (children.size() != 1 || !FilterColumn(*children[0], columns, positional_bindings)) {
		return {cardinality, false};
	}
	const auto selectivity = RelationStatisticsHelper::DEFAULT_SELECTIVITY;
	return {FilterFraction(cardinality, expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ? selectivity
	                                                                                                 : 1 - selectivity),
	        true};
}

static FilterEstimate EstimateEqualityRows(CardinalityValue cardinality, ExpressionType type,
                                           const RelationColumnStats &column) {
	const auto selectivity = RelationStatisticsHelper::DEFAULT_SELECTIVITY;
	bool equal = type == ExpressionType::COMPARE_EQUAL || type == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	if (cardinality == 0) {
		return {0, true};
	}
	if (!column.total_domain.has_usable_domain) {
		return {FilterFraction(cardinality, equal ? selectivity : 1 - selectivity), true};
	}
	auto domain = MinValue(cardinality, column.current_domain.distinct_count);
	if (domain == 0) {
		return {cardinality, false};
	}
	auto fraction = MinValue(CardinalityValue(1), CardinalityValue(1) / domain);
	return {cardinality * (equal ? fraction : CardinalityValue(1) - fraction), true};
}

static FilterEstimate EstimateComparisonRows(CardinalityValue cardinality, const BoundFunctionExpression &comparison,
                                             const RelationColumnStatsLookup &columns,
                                             const vector<ColumnBinding> &positional_bindings,
                                             FilterPropagateResult pruning, optional_ptr<IntegerRangeEstimate> range) {
	const auto selectivity = RelationStatisticsHelper::DEFAULT_SELECTIVITY;
	auto &children = comparison.GetChildren();
	auto type = comparison.GetExpressionType();
	auto column = FilterColumn(*children[0], columns, positional_bindings);
	optional_ptr<const Expression> value = *children[1];
	if (!column) {
		column = FilterColumn(*children[1], columns, positional_bindings);
		value = *children[0];
		type = FlipComparisonExpression(type);
	}
	if (!column || (value->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT &&
	                value->GetExpressionClass() != ExpressionClass::BOUND_PARAMETER)) {
		return {cardinality, false};
	}
	if (value->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
	    value->Cast<BoundConstantExpression>().GetValue().IsNull()) {
		if (type == ExpressionType::COMPARE_NOT_DISTINCT_FROM || type == ExpressionType::COMPARE_DISTINCT_FROM) {
			return {FilterFraction(cardinality,
			                       type == ExpressionType::COMPARE_NOT_DISTINCT_FROM ? selectivity : 1 - selectivity),
			        true};
		}
		return {0, true, true};
	}
	if (pruning == FilterPropagateResult::FILTER_TRUE_OR_NULL && type != ExpressionType::COMPARE_DISTINCT_FROM &&
	    type != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		return {FilterFraction(cardinality, 1 - selectivity), true};
	}
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_DISTINCT_FROM:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return EstimateEqualityRows(cardinality, type, *column);
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		if (range && value->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		    range->AddBound(type, value->Cast<BoundConstantExpression>().GetValue())) {
			return {cardinality, true};
		}
		return {FilterFraction(cardinality, selectivity), true};
	default:
		return {cardinality, false};
	}
}

static FilterEstimate EstimateFilterRows(CardinalityValue cardinality, const Expression &expr,
                                         const RelationColumnStatsLookup &columns,
                                         const vector<ColumnBinding> &positional_bindings,
                                         optional_ptr<ClientContext> context = nullptr,
                                         optional_ptr<const BaseStatistics> scan_statistics = nullptr,
                                         optional_ptr<IntegerRangeEstimate> range = nullptr) {
	if (ExpressionFilter::IsRootOptionalExpression(expr)) {
		return {cardinality, true};
	}
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		FilterEstimate result {cardinality, true};
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			result.Combine(
			    EstimateFilterRows(cardinality, *child, columns, positional_bindings, context, scan_statistics, range));
		}
		return result;
	}
	auto pruning = FilterPropagateResult::NO_PRUNING_POSSIBLE;
	if (scan_statistics && !expr.HasParameter()) {
		pruning = ExpressionFilter::CheckExpressionStatistics(context, expr, *scan_statistics);
		if (pruning == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
			return {cardinality, true};
		}
		if (pruning == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
		    pruning == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
			return {0, true, true};
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT && expr.GetReturnType() == LogicalType::BOOLEAN) {
		auto &value = expr.Cast<BoundConstantExpression>().GetValue();
		auto empty = value.IsNull() || !BooleanValue::Get(value);
		return {empty ? CardinalityValue(0) : cardinality, true, empty};
	}
	// Preserve the existing scan-specific non-null NOT LIKE estimate.
	if (scan_statistics && expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	    expr.Cast<BoundFunctionExpression>().Function().GetName() == "!~~" && !scan_statistics->CanHaveNull()) {
		return {FilterFraction(cardinality, 1 - RelationStatisticsHelper::DEFAULT_SELECTIVITY), true};
	}
	if (expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ||
	    expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
		return EstimateNullCheckRows(cardinality, expr.Cast<BoundOperatorExpression>(), columns, positional_bindings);
	}
	if (BoundComparisonExpression::IsComparison(expr)) {
		return EstimateComparisonRows(cardinality, expr.Cast<BoundFunctionExpression>(), columns, positional_bindings,
		                              pruning, range);
	}
	return {cardinality, false};
}

static void ApplyFilterEstimate(RelationStats &stats, FilterEstimate estimate) {
	if (estimate.proven_empty) {
		stats.max_cardinality = optional_idx(0);
	}
	if (!estimate.supported) {
		// An unsupported conjunct retains its fallback even when another conjunct has a weaker estimate.
		estimate.cardinality = MinValue(
		    estimate.cardinality, FilterFraction(stats.cardinality, RelationStatisticsHelper::DEFAULT_SELECTIVITY));
	}
	RelationStatisticsHelper::ApplyCardinality(stats, estimate.cardinality);
}

static void ApplyFilterDomainCap(RelationColumnStats &column, optional_idx cap) {
	if (!cap.IsValid()) {
		return;
	}
	column.current_domain =
	    DistinctCount(MinValue(column.current_domain.distinct_count, CardinalityValue(cap.GetIndex())),
	                  DistinctCountSource::CARDINALITY);
	column.semi_anti_join_domain_evidence.TightenDomainBound(cap.GetIndex());
}

static void ApplyDirectFilterDomain(RelationColumnStats &column, const Expression &expression,
                                    optional<ColumnBinding> binding) {
	auto bound = column.semi_anti_join_domain_evidence.GetDomainBound();
	ApplyFilterDomainCap(
	    column, RelationStatisticsHelper::EstimateDirectFilterDomain(
	                expression, binding, bound.IsValid() ? bound.GetIndex() : NumericLimits<idx_t>::Maximum()));
}

//===--------------------------------------------------------------------===//
// Zonemap bound
//===--------------------------------------------------------------------===//
struct ZonemapFilter {
	ZonemapFilter(const Expression &expr, const LogicalType &column_type, StorageIndex storage_index_p)
	    : expr(expr), column_type(column_type), storage_index(std::move(storage_index_p)) {
	}

	const Expression &expr;
	const LogicalType &column_type;
	StorageIndex storage_index;
};

//! An upper bound on the rows that a scan of a DuckDB table returns: the rows of the row groups whose zonemaps
//! can satisfy every table filter of the scan. A row group that cannot be skipped counts whole.
//! Returns an invalid index if the bound is not below the estimate that the scan already has.
static optional_idx ZonemapFilterBound(LogicalGet &get, ClientContext &context,
                                       const vector<BaseStatistics> &scan_statistics, CardinalityValue estimate) {
	//! The work per plan is limited to the row groups of a table of this many row groups (2 billion rows at the
	//! default row group size). A larger table keeps its estimate: checking only some of its row groups would not
	//! give a bound, as the row groups that are not checked can hold all of the matching rows
	static constexpr idx_t ZONEMAP_MAX_ROW_GROUPS = 16384;

	auto table = get.GetTable();
	if (!table || !table->IsDuckTable() || !get.function.get_partition_stats) {
		return optional_idx();
	}
	vector<ZonemapFilter> filters;
	for (auto &entry : get.table_filters) {
		if (ExpressionFilter::IsOptionalFilter(entry.Filter())) {
			// optional filters do not restrict the result of the scan
			continue;
		}
		auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "ZonemapFilterBound");
		if (filter.expr->HasParameter()) {
			continue;
		}
		const auto column_idx = entry.GetIndex().GetIndex();
		if (column_idx >= get.GetColumnIds().size() || column_idx >= scan_statistics.size()) {
			continue;
		}
		StorageIndex storage_index;
		if (!get.TryGetStorageIndex(get.GetColumnIds()[column_idx], storage_index)) {
			continue;
		}
		filters.emplace_back(*filter.expr, scan_statistics[column_idx].GetType(), std::move(storage_index));
	}
	if (filters.empty()) {
		return optional_idx();
	}
	// the limit is checked before any row group is loaded or collected, and covers the row groups of the table
	// and the ones that the transaction has added
	auto &storage = table->Cast<DuckTableEntry>().GetStorage();
	if (storage.GetRowGroupCountWithoutLoading(context) > ZONEMAP_MAX_ROW_GROUPS) {
		return optional_idx();
	}

	GetPartitionStatsInput input(get.function, get.bind_data.get());
	auto partitions = get.function.get_partition_stats(context, input);
	if (partitions.empty() || partitions.size() > ZONEMAP_MAX_ROW_GROUPS) {
		return optional_idx();
	}
	// the rows of the row groups that no filter skips
	idx_t matching_rows = 0;
	for (auto &partition : partitions) {
		bool skipped = false;
		for (auto &filter : filters) {
			if (!partition.partition_row_group) {
				break;
			}
			auto stats = partition.partition_row_group->GetColumnStatistics(filter.storage_index);
			if (!stats || !stats->GetType().EqualsIncludingCollation(filter.column_type)) {
				// without statistics the row group cannot be skipped for this filter
				continue;
			}
			// the check that the scan uses to skip the row group
			auto pruning = ExpressionFilter::CheckExpressionStatistics(context, filter.expr, *stats);
			if (pruning == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
			    pruning == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
				skipped = true;
				break;
			}
		}
		if (skipped) {
			continue;
		}
		if (!TryAddOperator::Operation(matching_rows, partition.count, matching_rows)) {
			return {};
		}
		if (matching_rows >= estimate) {
			// the bound only grows from here on: it cannot improve on the estimate, so the remaining row groups
			// need not be read
			return optional_idx();
		}
	}
	return optional_idx(matching_rows);
}

unique_ptr<BaseStatistics> RelationStatisticsHelper::GetColumnStatistics(LogicalGet &get, ClientContext &context,
                                                                         const ColumnIndex &column_id) {
	if (!get.bind_data || (!get.function.statistics && !get.function.statistics_extended)) {
		return nullptr;
	}
	if (get.function.statistics_extended) {
		TableFunctionGetStatisticsInput input(get.bind_data.get(), column_id);
		return get.function.statistics_extended(context, input);
	}
	return get.function.statistics(context, get.bind_data.get(), column_id.GetPrimaryIndex());
}

RelationStats RelationStatisticsHelper::ExtractGetStats(LogicalGet &get, ClientContext &context) {
	return ExtractGetStats(get, context, get.EstimateSourceStatistics(context));
}

RelationStats RelationStatisticsHelper::ExtractGetStats(LogicalGet &get, ClientContext &context,
                                                        idx_t base_table_cardinality) {
	return ExtractGetStats(get, context, NodeStatistics(base_table_cardinality));
}

RelationStats
RelationStatisticsHelper::ExtractGetStats(LogicalGet &get, ClientContext &context, const NodeStatistics &source_stats,
                                          optional_ptr<column_binding_map_t<DistinctCount>> source_domains) {
	D_ASSERT(source_stats.has_estimated_cardinality);
	auto base_table_cardinality = source_stats.estimated_cardinality;
	RelationStats result;
	result.cardinality = base_table_cardinality;
	if (source_stats.has_max_cardinality) {
		result.max_cardinality = optional_idx(source_stats.max_cardinality);
	}
	result.row_retention = result.IsEmpty() ? 0 : 1;
	result.table_name = get.GetTable() ? get.GetTable()->name : Identifier(get.GetName());

	// Include hidden filter columns, and acquire each scanned column's provider statistics once.
	vector<BaseStatistics> scan_statistics;
	for (idx_t i = 0; i < MaxValue<idx_t>(1, get.GetColumnIds().size()); i++) {
		ColumnBinding binding(get.table_index, ProjectionIndex(i));
		ColumnIndex fallback_column(get.GetAnyColumn());
		auto column_id = get.GetColumnIds().empty() ? fallback_column : get.GetColumnIndex(binding);
		auto column_statistics = GetColumnStatistics(get, context, column_id);
		auto total_domain = column_statistics ? GetDistinctCountFromStats(*column_statistics, base_table_cardinality)
		                                      : DistinctCount(0, DistinctCountSource::CARDINALITY);
		if (source_stats.has_max_cardinality && source_stats.max_cardinality == 0) {
			total_domain = DistinctCount(0, DistinctCountSource::EXACT);
		} else if (total_domain.distinct_count == 0) {
			total_domain = DistinctCount(base_table_cardinality, DistinctCountSource::CARDINALITY);
		}
		result.columns.emplace_back(binding, total_domain,
		                            Identifier(get.GetName() + "." + get.GetColumnName(column_id)));
		if (source_stats.has_max_cardinality) {
			result.columns.back().TightenPopulationBound(source_stats.max_cardinality);
		}
		if (column_statistics && column_statistics->GetType().EqualsIncludingCollation(get.GetColumnType(column_id))) {
			result.columns.back().can_have_null = column_statistics->CanHaveNull();
			scan_statistics.push_back(std::move(*column_statistics));
		} else {
			scan_statistics.push_back(BaseStatistics::CreateUnknown(get.GetColumnType(column_id)));
		}
	}
	CaptureUniqueKeys(get, result, context, CardinalityValue(base_table_cardinality));
	if (source_domains) {
		for (auto &column : result.columns) {
			source_domains->emplace(column.binding, column.total_domain);
		}
	}
	FiniteFilterSelection finite(result);
	for (auto &entry : get.table_filters) {
		auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "ExtractGetStats");
		vector<ColumnBinding> bindings {ColumnBinding(get.table_index, entry.GetIndex())};
		auto &statistics = scan_statistics[entry.GetIndex().GetIndex()];
		if (ExpressionFilter::IsOptionalFilter(entry.Filter())) {
			finite.AddOptionalSourceFacts(*filter.expr, bindings, statistics);
			continue;
		}
		VisitFilterConjuncts(*filter.expr, [&](const Expression &expression) {
			// An exclusion outside a proven source range does not remove a population value.
			auto proven_true = !expression.HasParameter() &&
			                   ExpressionFilter::CheckExpressionStatistics(context, expression, statistics) ==
			                       FilterPropagateResult::FILTER_ALWAYS_TRUE;
			finite.Add(expression, bindings, proven_true, statistics);
		});
	}
	auto estimate = finite.Apply(result);
	const auto filtered_rows = result.cardinality;
	RelationColumnStatsLookup columns(result);
	for (auto &entry : get.table_filters) {
		auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "ExtractGetStats");
		vector<ColumnBinding> bindings {ColumnBinding(get.table_index, entry.GetIndex())};
		D_ASSERT(filter.column_indexes.size() <= 1);
		auto &statistics = scan_statistics[entry.GetIndex().GetIndex()];
		IntegerRangeEstimate range(statistics);
		VisitFilterConjuncts(*filter.expr, [&](const Expression &expression) {
			// Statistics-based impossibility remains a proof, including a selected literal outside the source range.
			if (finite.Contains(expression)) {
				auto pruning = ExpressionFilter::CheckExpressionStatistics(context, expression, statistics);
				if (pruning == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
				    pruning == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
					estimate.Combine({0, true, true});
				}
			} else {
				estimate.Combine(
				    EstimateFilterRows(filtered_rows, expression, columns, bindings, context, statistics, range));
			}
		});
		auto fraction = range.Fraction();
		if (fraction) {
			// Intersect same-column bounds before applying the default-selectivity floor.
			auto range_cardinality = *fraction == 0
			                             ? CardinalityValue(0)
			                             : FilterFraction(filtered_rows, MaxValue(DEFAULT_SELECTIVITY, *fraction));
			estimate.Combine({range_cardinality, true, *fraction == 0});
		}
	}
	if (get.table_filters.HasFilters()) {
		// the scan skips the row groups whose zonemaps cannot satisfy the filters: their rows bound the estimate
		auto current_estimate = estimate.cardinality;
		if (!estimate.supported) {
			current_estimate = MinValue(current_estimate, FilterFraction(filtered_rows, DEFAULT_SELECTIVITY));
		}
		// an estimate of at most one row cannot be improved
		if (current_estimate > 1) {
			auto zonemap_bound = ZonemapFilterBound(get, context, scan_statistics, current_estimate);
			if (zonemap_bound.IsValid()) {
				estimate.Combine({zonemap_bound.GetIndex(), true, zonemap_bound.GetIndex() == 0});
			}
		}
	}
	ApplyFilterEstimate(result, estimate);
	finite.PublishNullRejection(result);
	for (auto &column : result.columns) {
		for (auto &entry : get.table_filters) {
			if (ExpressionFilter::IsOptionalFilter(entry.Filter()) || entry.GetIndex() != column.binding.column_index) {
				continue;
			}
			auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "ExtractGetStats");
			ApplyDirectFilterDomain(column, *filter.expr, {});
		}
	}
	vector<ColumnBinding> output_bindings;
	for (auto binding : get.GetColumnBindings()) {
		if (binding.table_index == get.table_index) {
			output_bindings.push_back(binding);
		}
	}
	result.stats_initialized = true;
	D_ASSERT(base_table_cardinality == 0 || base_table_cardinality >= result.cardinality);
	return *ProjectOutputStats(result, output_bindings);
}

static void CaptureFilterEqualities(const Expression &expression, RelationStats &stats) {
	if (expression.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		for (auto &child : expression.Cast<BoundConjunctionExpression>().GetChildren()) {
			CaptureFilterEqualities(*child, stats);
		}
		return;
	}
	if (expression.GetExpressionType() != ExpressionType::COMPARE_EQUAL &&
	    expression.GetExpressionType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		return;
	}
	auto &comparison = expression.Cast<BoundFunctionExpression>();
	vector<idx_t> columns;
	for (auto &child : comparison.GetChildren()) {
		if (child->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return;
		}
		auto &ref = child->Cast<BoundColumnRefExpression>();
		auto column = stats.FindColumn(ref.Binding());
		if (ref.Depth() != 0 || !column.IsValid()) {
			return;
		}
		columns.push_back(column.GetIndex());
	}
	if (columns.size() == 2 && columns[0] != columns[1]) {
		auto &children = comparison.GetChildren();
		if (children[0]->GetReturnType().EqualsIncludingCollation(children[1]->GetReturnType())) {
			stats.value_equivalent_columns.push_back(columns);
		}
		stats.equivalent_columns.push_back(std::move(columns));
	}
}

optional<RelationStats> RelationStatisticsHelper::ExtractFilterStats(LogicalFilter &filter,
                                                                     const RelationStats &child_stats) {
	return ExtractFilterStats(filter, child_stats, filter.expressions);
}

optional<RelationStats>
RelationStatisticsHelper::ExtractFilterStats(LogicalFilter &filter, const RelationStats &child_stats,
                                             const vector<unique_ptr<Expression>> &expressions) {
	auto result = ConditionFilterStats(child_stats, expressions, filter.children[0]->GetColumnBindings());
	return ProjectOutputStats(result, filter);
}

RelationStats RelationStatisticsHelper::ConditionFilterStats(const RelationStats &child_stats,
                                                             const vector<unique_ptr<Expression>> &expressions,
                                                             const vector<ColumnBinding> &bindings) {
	auto result = child_stats;
	for (auto &expression : expressions) {
		CaptureFilterEqualities(*expression, result);
	}
	FiniteFilterSelection finite(result);
	for (auto &expression : expressions) {
		VisitFilterConjuncts(*expression, [&](const Expression &child) { finite.Add(child, bindings); });
	}
	auto estimate = finite.Apply(result);
	RelationColumnStatsLookup columns(result);
	for (auto &expression : expressions) {
		VisitFilterConjuncts(*expression, [&](const Expression &child) {
			if (!finite.Contains(child)) {
				estimate.Combine(EstimateFilterRows(result.cardinality, child, columns, bindings));
			}
		});
	}
	ApplyFilterEstimate(result, estimate);
	finite.PublishNullRejection(result);
	for (auto &column : result.columns) {
		for (auto &expression : expressions) {
			ApplyDirectFilterDomain(column, *expression, column.binding);
		}
	}
	return result;
}

RelationStats RelationStatisticsHelper::ExtractDelimGetStats(LogicalDelimGet &delim_get, ClientContext &context) {
	RelationStats result;
	result.table_name = Identifier(delim_get.GetName());
	result.cardinality = delim_get.EstimateCardinality(context);
	result.stats_initialized = true;
	for (auto &binding : delim_get.GetColumnBindings()) {
		result.columns.emplace_back(binding, DistinctCount(1, DistinctCountSource::CARDINALITY),
		                            Identifier(StringUtil::Format("column%llu", binding.column_index.GetIndex())));
	}
	result.CapCurrentDomainsToCardinality();
	result.Verify(delim_get.GetColumnBindings());
	return result;
}

RelationStats RelationStatisticsHelper::ExtractDummyScanStats(LogicalDummyScan &dummy_scan, ClientContext &context) {
	RelationStats result;
	result.cardinality = dummy_scan.EstimateCardinality(context);
	result.max_cardinality = optional_idx(1);
	result.table_name = "dummy scan";
	result.stats_initialized = true;
	for (auto &binding : dummy_scan.GetColumnBindings()) {
		result.columns.emplace_back(binding, DistinctCount(result.cardinality, DistinctCountSource::CARDINALITY),
		                            Identifier("dummy_scan_column"));
	}
	result.Verify(dummy_scan.GetColumnBindings());
	return result;
}

RelationStats RelationStatisticsHelper::ExtractExpressionGetStats(LogicalExpressionGet &expression_get,
                                                                  ClientContext &context) {
	RelationStats result;
	result.cardinality = expression_get.EstimateCardinality(context);
	if (expression_get.children.size() == 1 &&
	    expression_get.children[0]->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		result.max_cardinality = optional_idx(expression_get.expressions.size());
	}
	result.table_name = "expression_get";
	result.stats_initialized = true;
	for (auto &binding : expression_get.GetColumnBindings()) {
		result.columns.emplace_back(binding, DistinctCount(result.cardinality, DistinctCountSource::CARDINALITY),
		                            Identifier("expression_get_column"));
		if (expression_get.children.size() == 1 &&
		    expression_get.children[0]->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
			result.columns.back().semi_anti_join_domain_evidence.TightenDomainBound(expression_get.expressions.size());
			result.columns.back().TightenPopulationBound(expression_get.expressions.size());
		}
	}
	result.Verify(expression_get.GetColumnBindings());
	return result;
}

RelationStats RelationStatisticsHelper::ExtractColumnDataGetStats(LogicalColumnDataGet &column_data_get,
                                                                  ClientContext &) {
	RelationStats result;
	result.cardinality = column_data_get.collection->Count();
	// Borrowed or shared collections can be populated after planning.
	if (column_data_get.collection.get_owned_unique()) {
		result.max_cardinality = optional_idx(column_data_get.collection->Count());
	}
	result.table_name = Identifier(column_data_get.GetName());
	result.stats_initialized = true;
	for (auto &binding : column_data_get.GetColumnBindings()) {
		result.columns.emplace_back(binding, DistinctCount(result.cardinality, DistinctCountSource::CARDINALITY),
		                            Identifier("column_data"));
		if (column_data_get.collection.get_owned_unique()) {
			result.columns.back().semi_anti_join_domain_evidence.TightenDomainBound(
			    column_data_get.collection->Count());
			result.columns.back().TightenPopulationBound(column_data_get.collection->Count());
		}
	}
	result.CapCurrentDomainsToCardinality();
	result.Verify(column_data_get.GetColumnBindings());
	return result;
}

} // namespace duckdb
