#include "duckdb/optimizer/remove_unnecessary_aggregates.hpp"

#include "duckdb/common/type_visitor.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

static bool AllExpressionsNonVolatile(const vector<unique_ptr<Expression>> &expressions) {
	for (auto &expr : expressions) {
		if (expr->IsVolatile()) {
			return false;
		}
	}
	return true;
}

//! Whether the result of this aggregation depends on duplicate rows in its input
static AggregateDistinctDependent AggregateDistinctDependence(const LogicalAggregate &aggr) {
	// a volatile group expression (e.g. GROUP BY random()) gives every duplicate input row its own group
	if (!AllExpressionsNonVolatile(aggr.groups)) {
		return AggregateDistinctDependent::DISTINCT_DEPENDENT;
	}
	for (auto &expr : aggr.expressions) {
		// a volatile argument (e.g. min(x + random())) draws a new value for every duplicate input row
		if (expr->IsVolatile()) {
			return AggregateDistinctDependent::DISTINCT_DEPENDENT;
		}
		auto &aggregate = expr->Cast<BoundAggregateExpression>();
		if (aggregate.IsDistinct()) {
			continue;
		}
		if (aggregate.Function().GetDistinctDependent() == AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT) {
			continue;
		}
		return AggregateDistinctDependent::DISTINCT_DEPENDENT;
	}
	return AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;
}

static bool GroupingMergesDistinguishableValues(const LogicalType &type) {
	// grouping compares the physical representation with the hardcoded comparators in comparison_operators.hpp,
	// so this is decided purely by the type id: extension types inherit the behavior of the id they alias.
	// values that only merge when their physical representation is identical are never a problem: identical
	// values cannot behave differently downstream. Non-canonical encodings (two representations of the same
	// logical value) merely fail to merge, which is the harmless opposite direction. The problematic types are
	// exactly the ones with a comparator that normalizes before comparing.
	return TypeVisitor::Contains(type, [](const LogicalType &ty) {
		switch (ty.id()) {
		// fixed-width scalars compared by value: they only merge identical values
		// (including TIME_TZ, whose comparison includes the offset, not just the instant)
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::HUGEINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::UHUGEINT:
		case LogicalTypeId::DECIMAL:
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIME_TZ:
		case LogicalTypeId::TIME_NS:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::TIMESTAMP_TZ_NS:
		case LogicalTypeId::UUID:
		case LogicalTypeId::ENUM:
			return false;
		// bytewise string_t equality (no unicode or other normalization): they only merge identical values
		case LogicalTypeId::BLOB:
		case LogicalTypeId::BIT:
		case LogicalTypeId::BIGNUM:
		case LogicalTypeId::GEOMETRY:
		case LogicalTypeId::TYPE:
			return false;
		case LogicalTypeId::VARCHAR:
			// bytewise equality, unless a collation is attached (e.g. NOCASE merges 'a' and 'A')
			return !StringType::GetCollation(ty).empty();
		// the only value is NULL
		case LogicalTypeId::SQLNULL:
			return false;
		// element-wise equality, the child types are visited separately; MAP is key-order-sensitive and
		// UNION is tag-sensitive, so the containers themselves only merge identical values
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::TUPLE:
		case LogicalTypeId::LIST:
		case LogicalTypeId::ARRAY:
		case LogicalTypeId::MAP:
		case LogicalTypeId::UNION:
			return false;
		// normalizing comparator (EqualsFloat): -0.0 and 0.0 merge, as do all NaN bit patterns;
		// a VARCHAR cast or signbit() tells both pairs apart ('0.0' vs '-0.0', 'nan' vs '-nan'),
		// and 1/x additionally tells the zeros apart (inf vs -inf)
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			return true;
		// normalizing comparator (Interval::Equals): 1 month = 30 days, but the values behave differently
		// in date arithmetic and casts to VARCHAR
		case LogicalTypeId::INTERVAL:
			return true;
		// VARIANT holds its payload in a fixed physical layout, so TypeVisitor cannot visit the types actually
		// stored inside it. Its comparator recurses into those values, inheriting their normalization: a VARIANT
		// holding -0.0 compares equal to one holding 0.0, while a cast to VARCHAR tells them apart
		case LogicalTypeId::VARIANT:
			return true;
		// internal and planning-only types that should not appear as the type of a bound group expression;
		// conservatively assume they merge
		case LogicalTypeId::INVALID:
		case LogicalTypeId::UNKNOWN:
		case LogicalTypeId::ANY:
		case LogicalTypeId::UNBOUND:
		case LogicalTypeId::TEMPLATE:
		case LogicalTypeId::CHAR:
		case LogicalTypeId::STRING_LITERAL:
		case LogicalTypeId::INTEGER_LITERAL:
		case LogicalTypeId::POINTER:
		case LogicalTypeId::VALIDITY:
		case LogicalTypeId::TABLE:
		case LogicalTypeId::LAMBDA:
		case LogicalTypeId::LEGACY_AGGREGATE_STATE:
			return true;
		}
		throw InternalException("Unhandled type in GroupingMergesDistinguishableValues");
	});
}

//! Collects every column binding referenced by any expression in the plan
class ColumnBindingGatherer : public LogicalOperatorVisitor {
public:
	explicit ColumnBindingGatherer(column_binding_set_t &column_references) : column_references(column_references) {
	}

	void VisitOperator(LogicalOperator &op) override {
		VisitOperatorExpressions(op);
		VisitOperatorChildren(op);
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		column_references.insert(expr.Binding());
		return nullptr;
	}

private:
	column_binding_set_t &column_references;
};

RemoveUnnecessaryAggregates::RemoveUnnecessaryAggregates(Optimizer &optimizer) : optimizer(optimizer) {
}

void RemoveUnnecessaryAggregates::GatherColumnReferences() {
	column_references.clear();
	// the plan's own output is referenced by whoever consumes the query result
	for (auto &binding : plan_root->GetColumnBindings()) {
		column_references.insert(binding);
	}
	ColumnBindingGatherer gatherer(column_references);
	gatherer.VisitOperator(*plan_root);
}

void RemoveUnnecessaryAggregates::Optimize(unique_ptr<LogicalOperator> &op) {
	plan_root = op.get();
	GatherColumnReferences();

	VisitOperator(op, AggregateDistinctDependent::DISTINCT_DEPENDENT, OperatorPath());
}

void RemoveUnnecessaryAggregates::VisitOperator(unique_ptr<LogicalOperator> &op_ref,
                                                AggregateDistinctDependent parent_distinct_dependent,
                                                OperatorPath path) {
	auto &op = *op_ref;
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op.Cast<LogicalAggregate>();
		if (parent_distinct_dependent == AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT &&
		    CanReplaceAggregateWithProjection(aggr)) {
			ReplaceAggregateWithProjection(op_ref, path);
			// re-dispatch on the projection we just put in place: it propagates the property under the same rules
			// as any other projection, which matters when a group expression is volatile (e.g. GROUP BY
			// random()). Grouping on it evaluates it once per input row, exactly like the projection does, so the
			// removal itself is fine - but eliminating duplicates *below* the projection would change how many
			// values it draws, so the property must stop here
			VisitOperator(op_ref, parent_distinct_dependent, std::move(path));
			return;
		}
		// FIXME: the unreferenced groups of this aggregate need to stay, they decide which rows are duplicates
		// of each other -> but they don't need to be scanned and output. Note that dropping them from the
		// output cannot renumber the groups themselves: grouping sets are defined as positions into the group
		// list, and GROUPING() refers to groups by index as well
		// this aggregate's output is unaffected by how many duplicate rows it receives, so the path below it
		// starts empty again
		VisitOperator(op.children[0], AggregateDistinctDependence(aggr), OperatorPath());
		return;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER: {
		// projections and filters evaluate their expressions per-row, so duplicate rows stay duplicates and are
		// filtered identically - unless an expression is volatile, which draws a new value per duplicate row
		if (parent_distinct_dependent == AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT &&
		    AllExpressionsNonVolatile(op.expressions)) {
			path.push_back(op);
			VisitOperator(op.children[0], parent_distinct_dependent, std::move(path));
			return;
		}
		break;
	}
	default:
		break;
	}
	// every other operator can observe duplicate rows in its input (or does not pass them through unmodified)
	for (auto &child : op.children) {
		VisitOperator(child, AggregateDistinctDependent::DISTINCT_DEPENDENT, OperatorPath());
	}
}

bool RemoveUnnecessaryAggregates::CanReplaceAggregateWithProjection(const LogicalAggregate &aggr) const {
	if (!aggr.expressions.empty()) {
		// the aggregate computes values, not just a distinct set of groups
		return false;
	}
	if (aggr.groups.empty()) {
		// scalar aggregate, produces exactly one row instead of eliminating duplicates
		return false;
	}
	if (aggr.grouping_sets.size() > 1 || !aggr.grouping_functions.empty()) {
		// with multiple grouping sets (ROLLUP/CUBE) the aggregate is not just a duplicate eliminator: it outputs one
		// batch of rows per grouping set (e.g. the subtotal and grand-total rows of a ROLLUP), which a projection of
		// the input rows cannot produce; and GROUPING() reports which set a row belongs to, which has no projection
		// equivalent either
		return false;
	}
	if (!aggr.grouping_sets.empty() && aggr.grouping_sets[0].size() != aggr.groups.size()) {
		// groups that are not part of the (single) grouping set are output as NULL instead of being grouped on,
		// a projection would output their actual values
		return false;
	}
	for (idx_t i = 0; i < aggr.groups.size(); i++) {
		if (column_references.find(ColumnBinding(aggr.group_index, ProjectionIndex(i))) == column_references.end()) {
			// unreferenced groups are not observable, so their values do not matter
			continue;
		}
		if (GroupingMergesDistinguishableValues(aggr.groups[i]->GetReturnType())) {
			// the grouping outputs one representative per group of equal-comparing values, a projection would
			// expose all of them (e.g. both -0.0 and 0.0)
			return false;
		}
	}
	// volatile group expressions (e.g. GROUP BY random()) do not block the removal: grouping evaluates the group
	// key once per input row (not per group), exactly like the replacement projection does
	return true;
}

void RemoveUnnecessaryAggregates::ReplaceAggregateWithProjection(unique_ptr<LogicalOperator> &op_ref,
                                                                 const OperatorPath &path) {
	auto &aggr = op_ref->Cast<LogicalAggregate>();
	// the aggregate is dropped as a whole, so its positional side tables (group_stats, grouping_sets) go with
	// it - the projection reuses the group table index, which keeps the bindings of the groups valid
	auto proj = make_uniq<LogicalProjection>(aggr.group_index, std::move(aggr.groups));
	proj->children.push_back(std::move(aggr.children[0]));

	// the operators between here and the not-distinct-dependent aggregate above were estimated based on this
	// aggregate's deduplicated output, but now process all of its input rows instead. That is an upper bound for
	// filters among them: their selectivity was estimated on the deduplicated stream, which can be arbitrarily
	// different on the duplicated one (a 50% filter on two distinct values can match 99% of the raw rows), so there
	// is nothing better to scale by
	auto &aggr_input = *proj->children[0];
	if (aggr_input.has_estimated_cardinality) {
		proj->SetEstimatedCardinality(aggr_input.estimated_cardinality);
		for (auto &ancestor : path) {
			ancestor.get().SetEstimatedCardinality(aggr_input.estimated_cardinality);
		}
	}
	op_ref = std::move(proj);

	// unreferenced groups are dead expressions of this projection, but they still reference the operators
	// below. Clearing them can drop the last reference to an aggregate expression further down, which leaves
	// that aggregate a pure duplicate eliminator, so this has to happen before we descend and decide about it.
	// VisitSubtree is what also prunes the columns of the projection itself
	RemoveUnusedColumns unused_optimizer(optimizer);
	unused_optimizer.VisitSubtree(op_ref, *plan_root);
	GatherColumnReferences();
}

} // namespace duckdb
