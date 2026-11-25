#include "duckdb/optimizer/scalar_aggregate_merger.hpp"

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

//! The aggregates that are merged: FILTER (WHERE p) feeds exactly the rows where p holds, so the result over the rows
//! where p holds equals the result with FILTER (WHERE p) over any superset of them, for empty input too
//! (0 for the counts, NULL otherwise). min(x, n) and max(x, n) take two arguments and are not matched.
static bool IsMergeableAggregateFunction(const BoundAggregateExpression &aggregate) {
	auto &name = aggregate.Function().GetName().GetIdentifierName();
	const auto argument_count = aggregate.GetChildren().size();
	if (name == "count_star") {
		return argument_count == 0;
	}
	return argument_count == 1 && (name == "count" || name == "sum" || name == "avg" || name == "min" || name == "max");
}

//! An expression that may be evaluated on more rows than before: neither volatile nor able to throw. A foldable
//! subtree that can throw failed to fold, and a CASE or OR above it decides per row whether it is reached.
static bool IsSafeOnOtherRows(const Expression &expr) {
	return !expr.IsVolatile() && !expr.CanThrow();
}

static bool IsMergeableAggregate(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
		return false;
	}
	auto &aggregate = expr.Cast<BoundAggregateExpression>();
	// DISTINCT is fine: the FILTER applies before the deduplication. ORDER BY is not matched.
	if (aggregate.GetOrderBys() || !IsMergeableAggregateFunction(aggregate)) {
		return false;
	}
	for (auto &child : aggregate.GetChildren()) {
		if (!IsSafeOnOtherRows(*child)) {
			return false;
		}
	}
	return !aggregate.GetFilter() || IsSafeOnOtherRows(*aggregate.GetFilter());
}

static void SplitConjunction(const Expression &expr, vector<reference<const Expression>> &conjuncts) {
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			SplitConjunction(*child, conjuncts);
		}
		return;
	}
	conjuncts.push_back(expr);
}

//! A scan as bound: no filters, time travel, ordinality, projection list, sample, input table or process-local input,
//! and only plain columns. A scan that reads no column gets the empty placeholder once its types are resolved.
static bool IsPlainScan(const LogicalGet &get) {
	if (!get.children.empty() || get.bind_info || get.table_filters.HasFilters() || get.at_clause ||
	    get.source_ordinality != OrdinalityType::WITHOUT_ORDINALITY || !get.projection_ids.empty() ||
	    get.has_pushed_projection || get.extra_info.sample_options || get.dynamic_filters ||
	    !get.input_table_types.empty() || !get.projected_input.empty()) {
		return false;
	}
	for (auto &column_id : get.GetColumnIds()) {
		if (column_id.IsEmptyColumn()) {
			continue;
		}
		if (column_id.IsVirtualColumn() || column_id.HasChildren()) {
			return false;
		}
	}
	return true;
}

static bool SameValues(const vector<Value> &a, const vector<Value> &b) {
	if (a.size() != b.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.size(); i++) {
		if (a[i].type() != b[i].type() || !Value::NotDistinctFrom(a[i], b[i])) {
			return false;
		}
	}
	return true;
}

static bool SameNamedParameters(const named_argument_map_t &a, const named_argument_map_t &b) {
	if (a.size() != b.size()) {
		return false;
	}
	for (auto &entry : a) {
		auto other = b.find(entry.first);
		if (other == b.end() || entry.second.type() != other->second.type() ||
		    !Value::NotDistinctFrom(entry.second, other->second)) {
			return false;
		}
	}
	return true;
}

//! Two plain scans that read the same rows: the same catalog table, or the same table function (read_parquet,
//! read_csv, ...) called with the same arguments. What a call without arguments reads is known to its bind data alone.
//! HashRelation must stay consistent with changes to this comparison.
static bool IsSameRelation(const LogicalGet &a, const LogicalGet &b) {
	if (a.function != b.function || !SameValues(a.parameters, b.parameters) ||
	    !SameNamedParameters(a.named_parameters, b.named_parameters)) {
		return false;
	}
	auto table = a.GetTable();
	if (table || b.GetTable()) {
		return table.get() == b.GetTable().get();
	}
	return !a.parameters.empty() || FunctionData::Equals(a.bind_data.get(), b.bind_data.get());
}

//! The columns a scan reads, without the empty placeholder
static set<column_t> ReadColumns(const LogicalGet &get) {
	set<column_t> columns;
	for (auto &column_id : get.GetColumnIds()) {
		if (!column_id.IsEmptyColumn()) {
			columns.insert(column_id.GetPrimaryIndex());
		}
	}
	return columns;
}

//! A named argument that is set: given, not NULL and not false
static bool HasOption(const LogicalGet &get, const char *name) {
	auto entry = get.named_parameters.find(name);
	if (entry == get.named_parameters.end() || entry->second.IsNull()) {
		return false;
	}
	return entry->second.type().id() != LogicalTypeId::BOOLEAN || BooleanValue::Get(entry->second);
}

//! A scan that stores the rows it rejects writes them as a side effect of each scan
static bool RecordsRejects(const LogicalGet &get) {
	return HasOption(get, "store_rejects") || HasOption(get, "rejects_table") || HasOption(get, "rejects_scan") ||
	       HasOption(get, "rejects_limit");
}

//! Two scans that one scan reading both of their columns can replace: the same relation, and the rows a scan returns do
//! not depend on the columns it reads. A scan that drops the rows it fails to parse (ignore_errors) returns a row when
//! every column it reads parses, so such scans are only shared when they read the same columns.
//! HashScan must hash compatible scans alike; keep it consistent with changes to this comparison.
static bool CanShareScan(const LogicalGet &a, const LogicalGet &b) {
	if (!IsSameRelation(a, b) || RecordsRejects(a)) {
		return false;
	}
	return !HasOption(a, "ignore_errors") || ReadColumns(a) == ReadColumns(b);
}

static hash_t HashScanParameter(const Value &value) {
	auto result = value.type().Hash();
	// VARIANT equality can equate different representations that Value::Hash distinguishes.
	if (TypeVisitor::Contains(value.type(), LogicalTypeId::VARIANT)) {
		return result;
	}
	return CombineHash(result, value.Hash());
}

//! Equal relations must hash alike. Bind data without arguments has no generic hash and is omitted.
//! Column selection is excluded: a target may gain columns without changing which relation it reads.
static hash_t HashRelation(const LogicalGet &scan) {
	auto result = CombineHash(scan.function.GetName().Hash(), Hash(CastPointerToValue(scan.GetTable().get())));
	for (auto &parameter : scan.parameters) {
		result = CombineHash(Hash(result), HashScanParameter(parameter));
	}
	hash_t named_parameters = 0;
	for (auto &entry : scan.named_parameters) {
		named_parameters += Hash(CombineHash(entry.first.Hash(), HashScanParameter(entry.second)));
	}
	return CombineHash(result, named_parameters);
}

//! Compatible scans must hash alike. CanShareWith checks collisions before grouping branches.
static hash_t HashScan(const LogicalGet &scan) {
	auto result = HashRelation(scan);
	if (HasOption(scan, "ignore_errors")) {
		for (auto column : ReadColumns(scan)) {
			result = CombineHash(Hash(result), Hash(column));
		}
	}
	return result;
}

//! Borrows the ordered scans of one branch while matching and merging, indexed by relation identity.
//! Adding columns during merging does not change relation identity or invalidate the index.
class ScalarAggregateMerger::ScanCollection {
public:
	//! One plain scan, or a cross product of scans of different relations (self-joins do not pair one to one).
	bool Collect(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			for (auto &child : op.children) {
				if (!Collect(*child)) {
					return false;
				}
			}
			return true;
		}
		if (op.type != LogicalOperatorType::LOGICAL_GET) {
			return false;
		}
		auto &scan = op.Cast<LogicalGet>();
		// Reject-recording scans cannot share even with an identical scan; exclude them before candidate grouping.
		return IsPlainScan(scan) && !RecordsRejects(scan) && TryAdd(scan);
	}

	//! Each stored scan must have a compatible scan in other, preserving the comparison's left/right order.
	bool CanShareWith(const ScanCollection &other) const {
		if (scans.size() != other.scans.size()) {
			return false;
		}
		for (auto &scan : scans) {
			auto candidates = other.FindCandidates(scan.get());
			if (!candidates) {
				return false;
			}
			bool found = false;
			for (auto index : *candidates) {
				if (CanShareScan(scan.get(), other.scans[index].get())) {
					found = true;
					break;
				}
			}
			if (!found) {
				return false;
			}
		}
		return true;
	}

	//! The last compatible stored scan, in iteration order, matching the original target-selection order.
	optional_idx FindCompatible(const LogicalGet &scan) const {
		auto candidates = FindCandidates(scan);
		if (!candidates) {
			return optional_idx();
		}
		optional_idx result;
		for (auto index : *candidates) {
			if (CanShareScan(scans[index].get(), scan)) {
				result = index;
			}
		}
		return result;
	}

	idx_t size() const {
		return scans.size();
	}

	using const_iterator = vector<reference<LogicalGet>>::const_iterator;
	const_iterator begin() const {
		return scans.begin();
	}
	const_iterator end() const {
		return scans.end();
	}

private:
	bool TryAdd(LogicalGet &scan) {
		auto &candidates = relation_index[HashRelation(scan)];
		for (auto index : candidates) {
			if (IsSameRelation(scans[index].get(), scan)) {
				return false;
			}
		}
		candidates.push_back(scans.size());
		scans.push_back(scan);
		return true;
	}

	optional_ptr<const vector<idx_t>> FindCandidates(const LogicalGet &scan) const {
		auto entry = relation_index.find(HashRelation(scan));
		if (entry == relation_index.end()) {
			return nullptr;
		}
		return &entry->second;
	}

	vector<reference<LogicalGet>> scans;
	unordered_map<hash_t, vector<idx_t>> relation_index;
};

//===--------------------------------------------------------------------===//
// Branch: [projection ->] ungrouped aggregate -> filters -> plain scan(s)
//===--------------------------------------------------------------------===//
//! One leaf of a cross-product tree that can be merged with its siblings
struct ScalarAggregateMerger::Branch {
	//! The leaf's position in the collected cross-product leaves
	idx_t leaf_index;
	//! The projection over the aggregate, if any
	optional_ptr<LogicalProjection> projection;
	reference<LogicalAggregate> aggregate;
	//! The slot under the filters: the scan, or the cross product of scans of different tables (a FROM list)
	reference<unique_ptr<LogicalOperator>> relation;
	ScanCollection scans;
	//! The filters' predicates, with ANDs split into their conjuncts
	vector<reference<const Expression>> conjuncts;
};

//! One of the first branch's scans, which the merged leaf keeps, and the columns it gains for the other branches
struct ScalarAggregateMerger::MergeTarget {
	explicit MergeTarget(LogicalGet &scan_p) : scan(scan_p) {
		auto &column_ids = scan_p.GetColumnIds();
		column_bindings.reserve(column_ids.size());
		for (idx_t i = 0; i < column_ids.size(); i++) {
			// Keep the first position if a column occurs more than once.
			column_bindings.emplace(column_ids[i].GetPrimaryIndex(),
			                        ColumnBinding(scan_p.table_index, ProjectionIndex(i)));
		}
	}

	//! Where this target reads column, allocating a position without changing the scan until the merge is accepted.
	ColumnBinding BindingFor(column_t column) {
		auto entry = column_bindings.find(column);
		if (entry != column_bindings.end()) {
			return entry->second;
		}
		ColumnBinding binding(scan.get().table_index,
		                      ProjectionIndex(scan.get().GetColumnIds().size() + added_columns.size()));
		column_bindings.emplace(column, binding);
		added_columns.emplace_back(column, binding);
		return binding;
	}

	void ApplyAddedColumns() {
		for (auto &entry : added_columns) {
			auto position = scan.get().AddColumnId(entry.first);
			D_ASSERT(position == entry.second.column_index);
			(void)position;
		}
		added_columns.clear();
	}

private:
	reference<LogicalGet> scan;
	//! A column the scan gains, and the binding that column lands on
	vector<std::pair<column_t, ColumnBinding>> added_columns;
	unordered_map<column_t, ColumnBinding> column_bindings;
};

static hash_t HashBranch(const ScalarAggregateMerger::Branch &branch) {
	auto result = Hash(branch.scans.size());
	// CanShareWith matches scans irrespective of their order.
	for (auto &scan : branch.scans) {
		result += Hash(HashScan(scan.get()));
	}
	return result;
}

//! Equal conjuncts, or the same comparison with its sides swapped (a = b and b = a)
static bool IsSameConjunct(const Expression &a, const Expression &b) {
	if (a.Equals(b)) {
		return true;
	}
	if (!BoundComparisonExpression::IsComparison(a) || !BoundComparisonExpression::IsComparison(b)) {
		return false;
	}
	auto &left = a.Cast<BoundFunctionExpression>();
	auto &right = b.Cast<BoundFunctionExpression>();
	return left.GetExpressionType() == FlipComparisonExpression(right.GetExpressionType()) &&
	       BoundComparisonExpression::Left(left).Equals(BoundComparisonExpression::Right(right)) &&
	       BoundComparisonExpression::Right(left).Equals(BoundComparisonExpression::Left(right));
}

//! Borrows immutable expressions. Keep the index within the lifetime of the expressions it searches.
class ScalarAggregateMerger::ConjunctIndex {
public:
	void Add(const Expression &expr) {
		conjuncts[HashConjunct(expr)].push_back(expr);
	}

	bool Contains(const Expression &expr) const {
		auto entry = conjuncts.find(HashConjunct(expr));
		return entry != conjuncts.end() && Contains(entry->second, expr);
	}

private:
	static hash_t HashConjunct(const Expression &expr) {
		if (!BoundComparisonExpression::IsComparison(expr)) {
			return expr.Hash();
		}
		// IsSameConjunct also matches flipped comparisons, without comparing their function bind data.
		auto type = expr.GetExpressionType();
		auto flipped_type = FlipComparisonExpression(type);
		if (flipped_type < type) {
			type = flipped_type;
		}
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto operands = Hash(BoundComparisonExpression::Left(comparison).Hash()) +
		                Hash(BoundComparisonExpression::Right(comparison).Hash());
		return CombineHash(Hash(static_cast<uint32_t>(type)), operands);
	}

	static bool Contains(const vector<const_reference<Expression>> &bucket, const Expression &expr) {
		for (auto &conjunct : bucket) {
			if (IsSameConjunct(conjunct.get(), expr)) {
				return true;
			}
		}
		return false;
	}

	unordered_map<hash_t, vector<const_reference<Expression>>> conjuncts;
};

static void CollectBindings(const Expression &expr, column_binding_set_t &bindings) {
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &column_ref) { bindings.insert(column_ref.Binding()); });
}

//! Points every column reference of `expr` that `remap` names at its new binding
static void RemapBindings(unique_ptr<Expression> &expr, const column_binding_map_t<ColumnBinding> &remap) {
	expr = ExpressionIterator::PreorderReplace<BoundColumnRefExpression>(
	    std::move(expr), [&](const BoundColumnRefExpression &colref) -> unique_ptr<Expression> {
		    auto entry = remap.find(colref.Binding());
		    if (entry == remap.end()) {
			    return nullptr;
		    }
		    return colref.WithBinding(entry->second);
	    });
}

static unique_ptr<Expression> MakeConjunction(ExpressionType type, vector<unique_ptr<Expression>> terms) {
	if (terms.size() == 1) {
		return std::move(terms[0]);
	}
	auto result = make_uniq<BoundConjunctionExpression>(type);
	for (auto &term : terms) {
		result->GetChildrenMutable().push_back(std::move(term));
	}
	return std::move(result);
}

static bool TreeHasProjectionMap(const LogicalOperator &op) {
	if (op.HasProjectionMap()) {
		return true;
	}
	for (auto &child : op.children) {
		if (TreeHasProjectionMap(*child)) {
			return true;
		}
	}
	return false;
}

//===--------------------------------------------------------------------===//
// ScalarAggregateMerger
//===--------------------------------------------------------------------===//
ScalarAggregateMerger::ScalarAggregateMerger(Optimizer &optimizer) : optimizer(optimizer) {
}

void ScalarAggregateMerger::Optimize(unique_ptr<LogicalOperator> &plan) {
	// Merging or removing leaves moves the positions a projection map names: only plans without them.
	if (TreeHasProjectionMap(*plan)) {
		return;
	}
	VisitOperator(plan);
	if (replacements.Empty()) {
		return;
	}
	auto binding_replacements = std::move(replacements);
	replacements = BindingReplacementGraph();
	ColumnBindingReplacer replacer(binding_replacements);
	replacer.VisitOperator(*plan);
	plan->ResolveOperatorTypes();
}

void ScalarAggregateMerger::VisitOperator(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		MergeCrossProduct(op);
		return;
	}
	for (auto &child : op->children) {
		VisitOperator(child);
	}
}

static void CollectLeaves(unique_ptr<LogicalOperator> &op, vector<reference<unique_ptr<LogicalOperator>>> &leaves) {
	if (op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		for (auto &child : op->children) {
			CollectLeaves(child, leaves);
		}
		return;
	}
	leaves.push_back(op);
}

//! Remove empty cross products and collapse those with only one surviving child.
static void PruneCrossProduct(unique_ptr<LogicalOperator> &op) {
	if (!op || op->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return;
	}
	D_ASSERT(op->children.size() == 2);
	for (auto &child : op->children) {
		PruneCrossProduct(child);
	}
	if (!op->children[0] || !op->children[1]) {
		auto child = std::move(op->children[op->children[0] ? 0 : 1]);
		op = std::move(child);
	}
}

bool ScalarAggregateMerger::IsWorthMerging(const Branch &branch) {
	auto threshold = Settings::Get<ScalarAggregateMergeThresholdSetting>(optimizer.context);
	for (auto &scan : branch.scans) {
		if (!scan.get().GetTable()) {
			return true;
		}
		if (threshold != 0 && scan.get().EstimateCardinality(optimizer.context) >= threshold) {
			return true;
		}
	}
	return false;
}

void ScalarAggregateMerger::MergeCrossProduct(unique_ptr<LogicalOperator> &op) {
	vector<reference<unique_ptr<LogicalOperator>>> leaves;
	CollectLeaves(op, leaves);
	for (auto &leaf : leaves) {
		VisitOperator(leaf.get());
	}
	vector<Branch> branches;
	for (idx_t i = 0; i < leaves.size(); i++) {
		TryMatchBranch(leaves, i, branches);
	}
	// groups of branches that read the same tables, in the order their first branch appears
	vector<vector<idx_t>> groups;
	unordered_map<hash_t, vector<idx_t>> group_index;
	for (idx_t i = 0; i < branches.size(); i++) {
		bool placed = false;
		auto &candidates = group_index[HashBranch(branches[i])];
		for (auto group_idx : candidates) {
			auto &group = groups[group_idx];
			if (branches[group[0]].scans.CanShareWith(branches[i].scans)) {
				group.push_back(i);
				placed = true;
				break;
			}
		}
		if (!placed) {
			candidates.push_back(groups.size());
			groups.push_back({i});
		}
	}
	// the merged leaf takes the place of a group's first branch; the other branches' leaves go
	vector<unique_ptr<LogicalOperator>> merged_leaves(leaves.size());
	vector<bool> removed(leaves.size(), false);
	bool changed = false;
	for (auto &group : groups) {
		if (group.size() < 2 || !IsWorthMerging(branches[group[0]])) {
			continue;
		}
		auto merged_leaf = MergeBranches(branches, group);
		if (!merged_leaf) {
			continue;
		}
		merged_leaves[branches[group[0]].leaf_index] = std::move(merged_leaf);
		for (idx_t k = 1; k < group.size(); k++) {
			removed[branches[group[k]].leaf_index] = true;
		}
		changed = true;
	}
	if (!changed) {
		return;
	}
	// Keep the surviving tree's shape instead of rebuilding it as a left-deep chain.
	// Finish all leaf-slot updates before pruning, which invalidates the references in leaves and branches.
	for (idx_t i = 0; i < leaves.size(); i++) {
		if (removed[i]) {
			leaves[i].get().reset();
		} else if (merged_leaves[i]) {
			leaves[i].get() = std::move(merged_leaves[i]);
		}
	}
	PruneCrossProduct(op);
	D_ASSERT(op);
}

void ScalarAggregateMerger::TryMatchBranch(const vector<reference<unique_ptr<LogicalOperator>>> &leaves,
                                           idx_t leaf_index, vector<Branch> &branches) {
	reference<LogicalOperator> op = *leaves[leaf_index].get();
	optional_ptr<LogicalProjection> projection;
	if (op.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		projection = &op.get().Cast<LogicalProjection>();
		op = *op.get().children[0];
	}
	if (op.get().type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto &aggregate = op.get().Cast<LogicalAggregate>();
	// an aggregate without groups or grouping sets returns exactly one row
	if (!aggregate.groups.empty() || !aggregate.grouping_functions.empty() || aggregate.grouping_sets.size() > 1 ||
	    (aggregate.grouping_sets.size() == 1 && !aggregate.grouping_sets[0].empty()) || aggregate.expressions.empty()) {
		return;
	}
	for (auto &expr : aggregate.expressions) {
		if (!IsMergeableAggregate(*expr)) {
			return;
		}
	}
	vector<reference<const Expression>> conjuncts;
	reference<unique_ptr<LogicalOperator>> relation = aggregate.children[0];
	while (relation.get()->type == LogicalOperatorType::LOGICAL_FILTER) {
		for (auto &expr : relation.get()->Cast<LogicalFilter>().expressions) {
			SplitConjunction(*expr, conjuncts);
		}
		relation = relation.get()->children[0];
	}
	for (auto &conjunct : conjuncts) {
		if (!IsSafeOnOtherRows(conjunct.get())) {
			return;
		}
	}
	ScanCollection scans;
	if (!scans.Collect(*relation.get())) {
		return;
	}
	branches.push_back(Branch {leaf_index, projection, aggregate, relation, std::move(scans), std::move(conjuncts)});
}

unique_ptr<LogicalOperator> ScalarAggregateMerger::MergeBranches(vector<Branch> &branches, const vector<idx_t> &group) {
	auto &binder = optimizer.binder;
	auto &first = branches[group[0]];
	// every branch's scan bindings land on the first branch's scan of the same table, which gains the columns it
	// lacks once the group is known to merge
	// Targets have the first branch's scan order, which FindCompatible returns.
	vector<MergeTarget> targets;
	targets.reserve(first.scans.size());
	for (auto &scan : first.scans) {
		targets.emplace_back(scan.get());
	}
	vector<column_binding_map_t<ColumnBinding>> remaps(group.size());
	for (idx_t k = 1; k < group.size(); k++) {
		for (auto &branch_scan : branches[group[k]].scans) {
			auto &scan = branch_scan.get();
			auto target_index = first.scans.FindCompatible(scan);
			D_ASSERT(target_index.IsValid());
			auto &target = targets[target_index.GetIndex()];
			auto &column_ids = scan.GetColumnIds();
			for (idx_t i = 0; i < column_ids.size(); i++) {
				// nothing refers to the empty placeholder
				if (column_ids[i].IsEmptyColumn()) {
					continue;
				}
				remaps[k][ColumnBinding(scan.table_index, ProjectionIndex(i))] =
				    target.BindingFor(column_ids[i].GetPrimaryIndex());
			}
		}
	}
	// the conjuncts on the target bindings, then split into the ones every branch has and what each has beyond them
	vector<vector<unique_ptr<Expression>>> conjuncts(group.size());
	for (idx_t k = 0; k < group.size(); k++) {
		for (auto &conjunct : branches[group[k]].conjuncts) {
			auto copy = conjunct.get().Copy();
			RemapBindings(copy, remaps[k]);
			conjuncts[k].push_back(std::move(copy));
		}
	}
	vector<unique_ptr<Expression>> shared;
	vector<vector<unique_ptr<Expression>>> own(group.size());
	{
		vector<ConjunctIndex> conjunct_indexes(group.size());
		for (idx_t k = 1; k < group.size(); k++) {
			for (auto &conjunct : conjuncts[k]) {
				conjunct_indexes[k].Add(*conjunct);
			}
		}
		ConjunctIndex shared_index;
		expression_set_t tested;
		for (auto &conjunct : conjuncts[0]) {
			// Do not repeat failed searches for identical conjuncts either.
			if (!tested.insert(*conjunct).second || shared_index.Contains(*conjunct)) {
				continue;
			}
			bool everywhere = true;
			for (idx_t k = 1; everywhere && k < group.size(); k++) {
				everywhere = conjunct_indexes[k].Contains(*conjunct);
			}
			if (everywhere) {
				shared.push_back(conjunct->Copy());
				shared_index.Add(*shared.back());
			}
		}
		for (idx_t k = 0; k < group.size(); k++) {
			for (auto &conjunct : conjuncts[k]) {
				if (!shared_index.Contains(*conjunct)) {
					own[k].push_back(std::move(conjunct));
				}
			}
		}
	}
	// over several tables, every own conjunct must read one and the same table: the tables are then joined by the
	// shared conjuncts alone, and the OR filters that table. A join condition only some branches have would turn the
	// merged join into a filter over the tables' product.
	bool own_table_known = false;
	TableIndex own_table;
	for (auto &branch_own : own) {
		for (auto &conjunct : branch_own) {
			column_binding_set_t bindings;
			CollectBindings(*conjunct, bindings);
			for (auto &binding : bindings) {
				if (!own_table_known) {
					own_table_known = true;
					own_table = binding.table_index;
				} else if (binding.table_index != own_table) {
					return nullptr;
				}
			}
		}
	}

	// the group merges: the target scans read the added columns
	for (auto &target : targets) {
		target.ApplyAddedColumns();
	}
	// each branch's own predicate (none: every row the shared conjuncts keep)
	vector<unique_ptr<Expression>> predicates(group.size());
	bool every_branch_restricted = true;
	for (idx_t k = 0; k < group.size(); k++) {
		if (own[k].empty()) {
			every_branch_restricted = false;
			continue;
		}
		predicates[k] = MakeConjunction(ExpressionType::CONJUNCTION_AND, std::move(own[k]));
	}
	// the merged aggregate: every branch's aggregates, each FILTERed to its branch's rows
	auto aggregate_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> aggregates;
	vector<idx_t> offsets(group.size());
	for (idx_t k = 0; k < group.size(); k++) {
		offsets[k] = aggregates.size();
		for (auto &expr : branches[group[k]].aggregate.get().expressions) {
			auto copy = expr->Copy();
			RemapBindings(copy, remaps[k]);
			auto &aggregate = copy->Cast<BoundAggregateExpression>();
			if (predicates[k]) {
				auto filter = predicates[k]->Copy();
				if (aggregate.GetFilter()) {
					vector<unique_ptr<Expression>> both;
					both.push_back(std::move(filter));
					both.push_back(std::move(aggregate.GetFilterMutable()));
					filter = MakeConjunction(ExpressionType::CONJUNCTION_AND, std::move(both));
				}
				aggregate.GetFilterMutable() = std::move(filter);
			}
			aggregates.push_back(std::move(copy));
		}
	}
	// the merged projection: every branch's columns, which the plan above reads in their place
	auto projection_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> columns;
	for (idx_t k = 0; k < group.size(); k++) {
		auto &branch = branches[group[k]];
		auto &aggregate = branch.aggregate.get();
		column_binding_map_t<ColumnBinding> to_merged;
		for (idx_t j = 0; j < aggregate.expressions.size(); j++) {
			to_merged[ColumnBinding(aggregate.aggregate_index, ProjectionIndex(j))] =
			    ColumnBinding(aggregate_index, ProjectionIndex(offsets[k] + j));
		}
		if (branch.projection) {
			auto &expressions = branch.projection->expressions;
			for (idx_t i = 0; i < expressions.size(); i++) {
				auto copy = expressions[i]->Copy();
				RemapBindings(copy, to_merged);
				replacements.Add(ColumnBinding(branch.projection->table_index, ProjectionIndex(i)),
				                 ColumnBinding(projection_index, ProjectionIndex(columns.size())));
				columns.push_back(std::move(copy));
			}
		} else {
			for (idx_t j = 0; j < aggregate.expressions.size(); j++) {
				replacements.Add(ColumnBinding(aggregate.aggregate_index, ProjectionIndex(j)),
				                 ColumnBinding(projection_index, ProjectionIndex(columns.size())));
				columns.push_back(make_uniq<BoundColumnRefExpression>(
				    aggregate.expressions[j]->GetReturnType(),
				    ColumnBinding(aggregate_index, ProjectionIndex(offsets[k] + j))));
			}
		}
	}

	// projection -> aggregate -> filter [shared AND (OR of the branches' own predicates)] -> the first branch's scan(s)
	unique_ptr<LogicalOperator> relation = std::move(first.relation.get());
	vector<unique_ptr<Expression>> filters = std::move(shared);
	if (every_branch_restricted) {
		// branches with the same predicate (q9 has three per range) contribute one alternative
		vector<unique_ptr<Expression>> alternatives;
		{
			ConjunctIndex alternative_index;
			for (auto &predicate : predicates) {
				if (!alternative_index.Contains(*predicate)) {
					alternatives.push_back(predicate->Copy());
					alternative_index.Add(*alternatives.back());
				}
			}
		}
		// the OR is a new expression: the rewrite rules that ran before the merge (predicate factoring) derive from it
		// the single-column filters that the pushdown places on the scan and that the cardinality estimates see
		LogicalFilter rewritten;
		rewritten.expressions.push_back(MakeConjunction(ExpressionType::CONJUNCTION_OR, std::move(alternatives)));
		optimizer.rewriter.VisitOperator(rewritten);
		for (auto &expr : rewritten.expressions) {
			filters.push_back(std::move(expr));
		}
	}
	if (!filters.empty()) {
		auto filter = make_uniq<LogicalFilter>();
		filter->expressions = std::move(filters);
		filter->AddChild(std::move(relation));
		relation = std::move(filter);
	}
	auto aggregate = make_uniq<LogicalAggregate>(binder.GenerateTableIndex(), aggregate_index, std::move(aggregates));
	aggregate->groupings_index = binder.GenerateTableIndex();
	aggregate->grouping_sets = first.aggregate.get().grouping_sets;
	aggregate->AddChild(std::move(relation));
	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(columns));
	projection->AddChild(std::move(aggregate));
	projection->ResolveOperatorTypes();
	return std::move(projection);
}

} // namespace duckdb
