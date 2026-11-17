#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

unique_ptr<Expression> RelationStatisticsHelper::CopyMandatoryFilter(const Expression &filter) {
	if (ExpressionFilter::IsRootOptionalExpression(filter)) {
		return make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	}
	if (filter.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		vector<unique_ptr<Expression>> children;
		for (auto &child : filter.Cast<BoundConjunctionExpression>().GetChildren()) {
			children.push_back(CopyMandatoryFilter(*child));
		}
		return BoundConjunctionExpression::Create(filter.GetExpressionType(), std::move(children));
	}
	return filter.Copy();
}

//! Rewrites filters over a scan into filters over the columns of a row sample of that scan
class SampleFilterBinder {
public:
	explicit SampleFilterBinder(LogicalGet &get) : get(get) {
	}

	//! A table filter references its own columns, which index the scan's projection
	unique_ptr<Expression> BindTableFilter(ProjectionIndex filter_column, const ExpressionFilter &filter) {
		if (filter.expr->HasParameter() || filter.expr->IsVolatile()) {
			return nullptr;
		}
		auto result = RelationStatisticsHelper::CopyMandatoryFilter(*filter.expr);
		bool valid = true;
		ExpressionIterator::VisitExpressionMutable<BoundReferenceExpression>(
		    result, [&](BoundReferenceExpression &ref, unique_ptr<Expression> &expr) {
			    auto index = ref.Index();
			    if (filter.column_indexes.empty() ? index != 0 : index >= filter.column_indexes.size()) {
				    valid = false;
				    return;
			    }
			    auto column = filter.column_indexes.empty() ? filter_column : filter.column_indexes[index];
			    auto sample_column = GetSampleColumn(column);
			    valid = valid && sample_column.IsValid();
			    expr = ref.WithIndex(sample_column.IsValid() ? sample_column.GetIndex() : 0);
		    });
		return valid ? std::move(result) : nullptr;
	}

	//! A filter operator on top of the scan references the scan's column bindings
	unique_ptr<Expression> BindFilter(const Expression &filter) {
		if (filter.HasParameter() || filter.IsVolatile()) {
			return nullptr;
		}
		auto result = filter.Copy();
		bool valid = true;
		ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
		    result, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &expr) {
			    auto &binding = colref.Binding();
			    if (colref.Depth() > 0 || binding.table_index != get.table_index) {
				    valid = false;
				    return;
			    }
			    auto sample_column = GetSampleColumn(binding.column_index);
			    if (!sample_column.IsValid()) {
				    valid = false;
				    return;
			    }
			    expr = make_uniq<BoundReferenceExpression>(colref.GetReturnType(), sample_column.GetIndex());
		    });
		return valid ? std::move(result) : nullptr;
	}

	const vector<ColumnIndex> &GetColumns() const {
		return columns;
	}

	//! The sample column of a column of the scan's projection, added to the sample if needed
	optional_idx GetSampleColumn(ProjectionIndex projection) {
		if (projection.GetIndex() >= get.GetColumnIds().size()) {
			return optional_idx();
		}
		auto &column = get.GetColumnIndex(projection);
		for (idx_t i = 0; i < columns.size(); i++) {
			if (columns[i] == column) {
				return i;
			}
		}
		columns.push_back(column);
		return columns.size() - 1;
	}

private:
	LogicalGet &get;
	vector<ColumnIndex> columns;
};

double RelationStatisticsHelper::SampledSelectivity(idx_t passing_count, idx_t evaluated_count,
                                                    double prior_selectivity) {
	D_ASSERT(evaluated_count > 0 && passing_count <= evaluated_count);
	// a filter that no sampled row passes is taken to pass half a row, since the sample may have missed its rows
	auto passing = passing_count == 0 ? 0.5 : static_cast<double>(passing_count);
	auto selectivity = passing / static_cast<double>(evaluated_count);
	// Sparse matches cannot justify raising a more selective estimate.
	return passing_count <= 1 || prior_selectivity == 0 ? MinValue(selectivity, prior_selectivity) : selectivity;
}

FilterSampleCounts
RelationStatisticsHelper::SampleFilterPassCounts(ClientContext &context, LogicalGet &get,
                                                 const vector<vector<reference<const Expression>>> &filter_stages,
                                                 optional_ptr<const Expression> conditioning) {
	auto sample_size = Settings::Get<FilterSelectivitySampleSizeSetting>(context);
	if (sample_size == 0 || !get.bind_data || !get.children.empty()) {
		return {};
	}
	auto table = get.GetTable();
	auto native = table ? table->TryGetDuckTableEntry() : nullptr;
	if (!native) {
		return {};
	}
	SampleFilterBinder binder(get);
	unique_ptr<Expression> condition;
	if (conditioning) {
		condition = binder.BindFilter(*conditioning);
		if (!condition) {
			return {};
		}
	}
	vector<vector<unique_ptr<Expression>>> stages(filter_stages.size() + 1);
	for (auto &entry : get.table_filters) {
		if (ExpressionFilter::IsOptionalFilter(entry.Filter())) {
			continue;
		}
		auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "SampleFilterPassCounts");
		auto expr = binder.BindTableFilter(entry.GetIndex(), filter);
		if (!expr) {
			return {};
		}
		stages[0].push_back(std::move(expr));
	}
	for (idx_t stage_idx = 0; stage_idx < filter_stages.size(); stage_idx++) {
		for (auto &filter : filter_stages[stage_idx]) {
			auto expr = binder.BindFilter(filter.get());
			if (!expr) {
				return {};
			}
			stages[stage_idx + 1].push_back(std::move(expr));
		}
	}
	if (binder.GetColumns().empty()) {
		return {};
	}
	unique_ptr<ColumnDataCollection> sample;
	try {
		sample = native->SampleRows(context, binder.GetColumns(), sample_size);
	} catch (std::exception &ex) {
		if (ErrorData(ex).Type() != ExceptionType::OUT_OF_MEMORY) {
			throw;
		}
		return {};
	}
	if (!sample) {
		return {};
	}

	FilterSampleCounts result;
	try {
		unique_ptr<ExpressionExecutor> condition_executor;
		if (condition) {
			condition_executor = make_uniq<ExpressionExecutor>(context, *condition);
		}
		vector<vector<unique_ptr<ExpressionExecutor>>> executors(stages.size());
		for (idx_t stage_idx = 0; stage_idx < stages.size(); stage_idx++) {
			for (auto &expr : stages[stage_idx]) {
				executors[stage_idx].push_back(make_uniq<ExpressionExecutor>(context, *expr));
			}
		}
		ColumnDataScanState scan_state;
		DataChunk chunk;
		sample->InitializeScan(scan_state);
		sample->InitializeScanChunk(scan_state, chunk);
		while (sample->Scan(scan_state, chunk)) {
			context.InterruptCheck();
			if (condition_executor) {
				SelectionVector sel(STANDARD_VECTOR_SIZE);
				result.condition_count += condition_executor->SelectExpression(chunk, sel);
			}
			for (idx_t stage_idx = 0; stage_idx < executors.size(); stage_idx++) {
				if (stage_idx + 1 == executors.size()) {
					result.evaluated_count += chunk.size();
				}
				for (auto &executor : executors[stage_idx]) {
					if (chunk.size() == 0) {
						break;
					}
					SelectionVector sel(STANDARD_VECTOR_SIZE);
					auto count = executor->SelectExpression(chunk, sel);
					chunk.Slice(sel, count);
				}
			}
			result.passing_count += chunk.size();
			if (condition_executor && chunk.size() > 0) {
				SelectionVector sel(STANDARD_VECTOR_SIZE);
				result.filtered_condition_count += condition_executor->SelectExpression(chunk, sel);
			}
		}
	} catch (std::exception &ex) {
		auto type = ErrorData(ex).Type();
		if (type == ExceptionType::INTERRUPT || type == ExceptionType::DATA_CORRUPTION ||
		    type == ExceptionType::INTERNAL || Exception::InvalidatesDatabase(type)) {
			throw;
		}
		// a filter can fail on a sampled row that the query itself never evaluates it on
		return {};
	}
	return result;
}

static bool IsIntegerRange(const BaseStatistics &stats) {
	if (stats.GetStatsType() != StatisticsType::NUMERIC_STATS || !NumericStats::HasMinMax(stats)) {
		return false;
	}
	switch (stats.GetType().id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return true;
	default:
		return false;
	}
}

optional<double> RelationStatisticsHelper::ConditionalFilterFactor(ClientContext &context, LogicalGet &filtered,
                                                                   ProjectionIndex filtered_key, LogicalGet &population,
                                                                   ProjectionIndex population_key) {
	if (Settings::Get<FilterSelectivitySampleSizeSetting>(context) == 0) {
		return {};
	}
	auto table = filtered.GetTable();
	if (!table || !table->TryGetDuckTableEntry()) {
		return {};
	}
	bool has_filter = false;
	for (auto &entry : filtered.table_filters) {
		has_filter |= !ExpressionFilter::IsOptionalFilter(entry.Filter());
	}
	if (!has_filter || filtered_key.GetIndex() >= filtered.GetColumnIds().size() ||
	    population_key.GetIndex() >= population.GetColumnIds().size()) {
		return {};
	}
	auto filtered_stats = GetColumnStatistics(filtered, context, filtered.GetColumnIndex(filtered_key));
	auto population_stats = GetColumnStatistics(population, context, population.GetColumnIndex(population_key));
	if (!filtered_stats || !population_stats || !IsIntegerRange(*filtered_stats) ||
	    !IsIntegerRange(*population_stats) ||
	    !filtered_stats->GetType().EqualsIncludingCollation(population_stats->GetType())) {
		return {};
	}
	auto lower = NumericStats::Min(*filtered_stats).GetValue<hugeint_t>();
	auto upper = NumericStats::Max(*filtered_stats).GetValue<hugeint_t>();
	auto population_lower = NumericStats::Min(*population_stats).GetValue<hugeint_t>();
	auto population_upper = NumericStats::Max(*population_stats).GetValue<hugeint_t>();
	if (population_lower < lower || population_upper > upper || population_lower > population_upper) {
		return {};
	}
	// Limit extra sampling to substantially smaller key ranges.
	if ((population_upper - population_lower + 1) * 2 > upper - lower + 1) {
		return {};
	}
	auto column = make_uniq<BoundColumnRefExpression>(filtered_stats->GetType(),
	                                                  ColumnBinding(filtered.table_index, filtered_key));
	auto low =
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHANOREQUALTO, column->Copy(),
	                                      make_uniq<BoundConstantExpression>(NumericStats::Min(*population_stats)));
	auto high =
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(column),
	                                      make_uniq<BoundConstantExpression>(NumericStats::Max(*population_stats)));
	auto condition =
	    make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(low), std::move(high));
	auto counts = SampleFilterPassCounts(context, filtered, {}, *condition);
	if (counts.evaluated_count == 0 || counts.filtered_condition_count <= 1 ||
	    counts.condition_count == counts.evaluated_count) {
		return {};
	}
	auto global = double(counts.passing_count) / double(counts.evaluated_count);
	auto conditional = double(counts.filtered_condition_count) / double(counts.condition_count);
	return global / conditional;
}

optional<double> RelationStatisticsHelper::SampleFilterSelectivity(ClientContext &context, LogicalFilter &filter,
                                                                   double prior_selectivity) {
	// the filters between this one and the scan are evaluated first, as they are below it in the plan
	vector<vector<reference<const Expression>>> stages;
	reference<LogicalOperator> op = filter;
	while (op.get().type == LogicalOperatorType::LOGICAL_FILTER) {
		stages.emplace_back();
		for (auto &expr : op.get().expressions) {
			stages.back().push_back(*expr);
		}
		op = *op.get().children[0];
	}
	if (op.get().type != LogicalOperatorType::LOGICAL_GET) {
		return {};
	}
	std::reverse(stages.begin(), stages.end());
	auto counts = SampleFilterPassCounts(context, op.get().Cast<LogicalGet>(), stages);
	if (counts.evaluated_count == 0) {
		return {};
	}
	return SampledSelectivity(counts.passing_count, counts.evaluated_count, prior_selectivity);
}

} // namespace duckdb
