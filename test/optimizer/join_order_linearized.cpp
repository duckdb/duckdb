#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/join_order/greedy_join_enumerator.hpp"
#include "duckdb/optimizer/join_order/join_order_linearizer.hpp"
#include "duckdb/optimizer/join_order/join_order_precedence.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/plan_enumerator.hpp"
#include "duckdb/optimizer/join_order/linearized_plan_enumerator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

struct LinearizedTestContext {
	LinearizedTestContext(QueryGraphManager &graph, CostModel &costs, const QueryGraphEdges &edges)
	    : query_graph_manager(graph), cost_model(costs), candidate_evaluator(graph, costs, edges) {
	}
	void InitLeafPlans() {
		candidate_evaluator.InitLeafPlans(plans, query_graph_manager.relation_manager.GetRelationStats());
	}
	const JoinOrderPlanMap &GetPlans() const {
		return plans;
	}

	QueryGraphManager &query_graph_manager;
	CostModel &cost_model;
	JoinOrderCandidateEvaluator candidate_evaluator;
	JoinOrderPlanMap plans;
};

class LinearizedTestHelper {
public:
	static bool ForOrder(LinearizedTestContext &enumerator, const vector<RelationIndex> &order);
	static bool Approximate(LinearizedTestContext &enumerator) {
		LinearizedPlanEnumerator search(enumerator.query_graph_manager, enumerator.cost_model,
		                                enumerator.candidate_evaluator);
		return search.SolveApproximately(enumerator.plans);
	}
	static bool Greedy(LinearizedTestContext &enumerator) {
		enumerator.candidate_evaluator.InitInputPlan(enumerator.plans);
		GreedyJoinEnumerator search(enumerator.query_graph_manager, enumerator.candidate_evaluator);
		return search.Solve(enumerator.plans);
	}

	static void Input(LinearizedTestContext &enumerator) {
		enumerator.candidate_evaluator.InitInputPlan(enumerator.plans);
	}
	static void Leaves(LinearizedTestContext &enumerator) {
		enumerator.plans = CopyJoinOrderLeafPlans(enumerator.plans);
	}
	static JoinOrderPlanMap Capture(LinearizedTestContext &enumerator, JoinRelationSet &root) {
		return CapturedJoinOrderPlan::Capture(enumerator.plans, root, enumerator.cost_model)->TakePlans();
	}
	static void Swap(LinearizedTestContext &enumerator, JoinOrderPlanMap &plans) {
		enumerator.plans.swap(plans);
	}
	static bool Prefer(LinearizedTestContext &enumerator, const DPJoinNode &candidate,
	                   const JoinOrderPlanMap &candidate_plans, const DPJoinNode &incumbent,
	                   const JoinOrderPlanMap &incumbent_plans) {
		return enumerator.candidate_evaluator.PreferPlan(candidate, candidate_plans, incumbent, incumbent_plans);
	}
};

} // namespace duckdb

using namespace duckdb;

static unique_ptr<LogicalOperator> PlanLinearizedQuery(Connection &connection, const string &query) {
	Parser parser(*connection.context);
	parser.ParseQuery(query);
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	Optimizer optimizer(*planner.binder, *connection.context);
	FilterPushdown pushdown(optimizer);
	return pushdown.Rewrite(std::move(planner.plan));
}

static LogicalOperator &LinearizedJoinRoot(LogicalOperator &plan) {
	optional_ptr<LogicalOperator> root = plan;
	while (root->children.size() == 1) {
		root = *root->children[0];
	}
	return *root;
}

static LogicalRecursiveCTE &LinearizedRecursiveCTE(LogicalOperator &plan) {
	vector<reference<LogicalOperator>> pending {plan};
	while (!pending.empty()) {
		auto &op = pending.back().get();
		pending.pop_back();
		if (op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
			return op.Cast<LogicalRecursiveCTE>();
		}
		for (auto &child : op.children) {
			pending.push_back(*child);
		}
	}
	throw InternalException("Expected a recursive CTE");
}

static void InitializeLinearizedRecursiveMember(JoinOrderOptimizer &optimizer, LogicalRecursiveCTE &cte) {
	auto anchor_optimizer = optimizer.CreateChildOptimizer();
	RelationStats anchor_stats;
	cte.children[0] = anchor_optimizer.Optimize(std::move(cte.children[0]), &anchor_stats);
	REQUIRE(anchor_stats.stats_initialized);
	optimizer.AddMaterializedCTEStats(cte.table_index, std::move(anchor_stats));
	optimizer.recursive_cte_indexes.insert(cte.table_index);
}

static JoinRelationSet &LinearizedTotalSet(QueryGraphManager &graph) {
	unordered_set<RelationIndex> bindings;
	for (idx_t i = 0; i < graph.relation_manager.NumRelations(); i++) {
		bindings.emplace(i);
	}
	return graph.set_manager.GetJoinRelation(bindings);
}

static void VerifyLinearizedClosure(const DPJoinNode &node,
                                    const reference_map_t<JoinRelationSet, unique_ptr<DPJoinNode>> &plans,
                                    CostModel &costs) {
	if (node.is_leaf) {
		REQUIRE(node.cost == 0);
		return;
	}
	auto &left = *plans.at(node.left_set);
	auto &right = *plans.at(node.right_set);
	VerifyLinearizedClosure(left, plans, costs);
	VerifyLinearizedClosure(right, plans, costs);
	REQUIRE(node.cost == Approx(costs.ComputeCost(left, right, node.join_operator, node.set)));
	auto expected = node;
	expected.recursive_properties = {};
	expected.retained_build_size = 0;
	expected.peak_build_size = 0;
	costs.UpdateBuildMemory(expected, left, right);
	REQUIRE(node.peak_build_size == Approx(expected.peak_build_size));
	REQUIRE(node.retained_build_size == Approx(expected.retained_build_size));
	REQUIRE(node.recursive_properties.depends_on_recursive_input ==
	        expected.recursive_properties.depends_on_recursive_input);
	REQUIRE(node.recursive_properties.build.repeatable == expected.recursive_properties.build.repeatable);
	REQUIRE(node.recursive_properties.build.correlation_sensitive ==
	        expected.recursive_properties.build.correlation_sensitive);
}

bool duckdb::LinearizedTestHelper::ForOrder(LinearizedTestContext &enumerator, const vector<RelationIndex> &order) {
	IntervalJoinEnumerator trial(enumerator.query_graph_manager, enumerator.candidate_evaluator, enumerator.plans);
	auto solved = trial.Solve(order);
	if (solved) {
		unordered_set<RelationIndex> bindings(order.begin(), order.end());
		auto &root = enumerator.query_graph_manager.set_manager.GetJoinRelation(bindings);
		VerifyLinearizedClosure(*trial.GetPlans().at(root), trial.GetPlans(), enumerator.cost_model);
		enumerator.plans = CapturedJoinOrderPlan::Capture(trial.GetPlans(), root, enumerator.cost_model)->TakePlans();
	}
	return solved;
}

// An exhaustive oracle retains every legal alternative, with no memo pruning.
static vector<unique_ptr<DPJoinNode>> ExhaustiveIntervals(QueryGraphManager &graph, CardinalityEstimator &estimator,
                                                          CostModel &costs, const vector<RelationIndex> &order,
                                                          idx_t start, idx_t end) {
	vector<unique_ptr<DPJoinNode>> result;
	if (end == start + 1) {
		auto &set = graph.set_manager.GetJoinRelation(order[start]);
		auto leaf = make_uniq<DPJoinNode>(set);
		leaf->cardinality = estimator.EstimateCardinality(set);
		leaf->cost = 0;
		result.push_back(std::move(leaf));
		return result;
	}
	for (idx_t split = start + 1; split < end; split++) {
		auto left_plans = ExhaustiveIntervals(graph, estimator, costs, order, start, split);
		auto right_plans = ExhaustiveIntervals(graph, estimator, costs, order, split, end);
		for (auto &left : left_plans) {
			for (auto &right : right_plans) {
				for (bool reverse : {false, true}) {
					auto &lhs = reverse ? *right : *left;
					auto &rhs = reverse ? *left : *right;
					auto &set = graph.set_manager.Union(lhs.set, rhs.set);
					for (auto &connection : graph.GetQueryGraphEdges().GetConnections(lhs.set, rhs.set)) {
						auto &edge = connection.get();
						if (!graph.IsConnectionApplicable(edge, lhs.set, rhs.set) ||
						    !graph.IsJoinOrderCandidate(edge.join_operator, edge.generated_cross_product, lhs.set,
						                                rhs.set)) {
							continue;
						}
						auto rows = estimator.EstimateCardinality(set);
						auto node = make_uniq<DPJoinNode>(set, edge.join_operator, edge.predicates,
						                                  edge.generated_cross_product, lhs.set, rhs.set,
						                                  costs.ComputeCost(lhs, rhs, edge.join_operator, set));
						node->cardinality = rows;
						result.push_back(std::move(node));
					}
				}
			}
		}
	}
	return result;
}

TEST_CASE("Linearized interval DP matches exhaustive additive costs", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	for (idx_t i = 0; i < 5; i++) {
		REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE t" + to_string(i) + " AS SELECT i%17 k FROM range(" +
		                                  to_string(20 + i * 73) + ") t(i)"));
	}
	connection.BeginTransaction();
	auto plan = PlanLinearizedQuery(connection, "SELECT * FROM t0 JOIN t1 USING(k) JOIN t2 USING(k) "
	                                            "JOIN t3 USING(k) JOIN t4 USING(k)");
	JoinOrderOptimizer optimizer(*connection.context);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel costs(estimator);
	LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
	enumerator.InitLeafPlans();
	auto &root = LinearizedTotalSet(graph);
	vector<RelationIndex> order {RelationIndex(0), RelationIndex(1), RelationIndex(2), RelationIndex(3),
	                             RelationIndex(4)};
	idx_t permutations = 0;
	do {
		auto alternatives = ExhaustiveIntervals(graph, estimator, costs, order, 0, order.size());
		auto solved = LinearizedTestHelper::ForOrder(enumerator, order);
		REQUIRE(solved == !alternatives.empty());
		if (solved) {
			auto minimum = std::numeric_limits<double>::infinity();
			for (auto &alternative : alternatives) {
				minimum = MinValue(minimum, alternative->cost);
			}
			auto &plans = enumerator.GetPlans();
			REQUIRE(plans.at(root)->cost == Approx(minimum));
			auto actual_right = estimator.EstimateCardinality(plans.at(root)->right_set);
			for (auto &alternative : alternatives) {
				if (alternative->cost == plans.at(root)->cost) {
					auto possible_right = estimator.EstimateCardinality(alternative->right_set);
					REQUIRE_FALSE(possible_right > actual_right);
				}
			}
			VerifyLinearizedClosure(*plans.at(root), plans, costs);
		}
		permutations++;
	} while (std::next_permutation(order.begin(), order.end()));
	REQUIRE(permutations == 120);
	connection.Rollback();
}

TEST_CASE("Linearized cost bound preserves rounded cardinality ties", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	QueryGraphManager graph(*connection.context);
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel costs(estimator);
	JoinOrderCandidateEvaluator evaluator(graph, costs, graph.GetQueryGraphEdges());
	JoinOrderPlanMap plans;
	auto &a = graph.set_manager.GetJoinRelation(RelationIndex(0));
	auto &b = graph.set_manager.GetJoinRelation(RelationIndex(1));
	auto &c = graph.set_manager.GetJoinRelation(RelationIndex(2));
	auto &ab = graph.set_manager.Union(a, b);
	auto &bc = graph.set_manager.Union(b, c);
	auto &root = graph.set_manager.Union(ab, c);
	auto left = make_uniq<DPJoinNode>(ab, nullptr, vector<reference<JoinPredicate>> {}, false, a, b,
	                                  std::numeric_limits<double>::infinity());
	left->cardinality = CardinalityValue(4566650022153684.0) / CardinalityValue(4566650022153683.0);
	auto right = make_uniq<DPJoinNode>(c);
	right->cardinality = CardinalityValue(8935141660703064.0) / CardinalityValue(8935141660703064.0);
	auto incumbent_right = make_uniq<DPJoinNode>(bc, nullptr, vector<reference<JoinPredicate>> {}, false, b, c,
	                                             std::numeric_limits<double>::infinity());
	incumbent_right->cardinality = CardinalityValue(1);
	// Rounded cross-products can tie with each other but compare differently against a third cardinality.
	REQUIRE_FALSE(left->cardinality > right->cardinality);
	REQUIRE_FALSE(right->cardinality > incumbent_right->cardinality);
	REQUIRE(left->cardinality > incumbent_right->cardinality);
	plans[ab] = std::move(left);
	plans[c] = std::move(right);
	plans[bc] = std::move(incumbent_right);
	DPJoinNode incumbent(root, nullptr, {}, false, a, bc, std::numeric_limits<double>::infinity());
	incumbent.cardinality = CardinalityValue(1);
	DPJoinNode reversed(root, nullptr, {}, false, c, ab, incumbent.cost);
	reversed.cardinality = incumbent.cardinality;
	REQUIRE(evaluator.PreferPlan(reversed, plans, incumbent, plans));
	REQUIRE(evaluator.CanImprovePair(plans, *plans.at(ab), *plans.at(c), incumbent));
}

TEST_CASE("Linearized ranks retain order through composition and extreme cardinalities", "[optimizer][join_order]") {
	for (double left : {0.0, 0.1, 0.9, 1.0, 1.1, 10.0}) {
		for (double right : {0.0, 0.1, 0.9, 1.0, 1.1, 10.0}) {
			CAPTURE(left, right);
			JoinOrderRank lhs(std::log(left));
			JoinOrderRank rhs(std::log(right));
			// Comparing adjacent jobs is equivalent to comparing the cost of their two concatenations.
			REQUIRE((lhs < rhs) == (left + left * right < right + right * left));
			lhs.Append(rhs);
			if (left > 0 && right > 0) {
				REQUIRE(lhs.log_multiplier == Approx(std::log(left * right)).margin(1e-12));
				REQUIRE(lhs.log_work == Approx(std::log(left + left * right)));
			} else if (left == 0) {
				REQUIRE(lhs.log_work == -std::numeric_limits<double>::infinity());
			}
		}
	}
	auto huge = CardinalityValue(1e200) * CardinalityValue(1e200);
	auto tiny = CardinalityValue(1e-200) * CardinalityValue(1e-200);
	REQUIRE(std::isinf(huge.GetDouble()));
	REQUIRE(tiny.GetDouble() == 0);
	REQUIRE(std::isfinite(huge.GetLog()));
	REQUIRE(std::isfinite(tiny.GetLog()));
	REQUIRE(JoinOrderRank(tiny.GetLog()) < JoinOrderRank(huge.GetLog()));
	REQUIRE(JoinOrderRank(CardinalityValue(0).GetLog()) < JoinOrderRank(tiny.GetLog()));
}

TEST_CASE("Rooted IKKBZ orders minimize the tree proxy on small trees", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	for (idx_t i = 0; i < 6; i++) {
		REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE t" + to_string(i) + " AS SELECT i k FROM range(" +
		                                  to_string(2 + i * 17) + ") t(i)"));
	}
	connection.BeginTransaction();
	for (idx_t shape = 0; shape < 3; shape++) {
		string query = "SELECT * FROM t0";
		for (idx_t i = 1; i < 6; i++) {
			auto parent = shape == 0 ? i - 1 : shape == 1 ? 0 : (i - 1) / 2;
			query += " JOIN t" + to_string(i) + " ON t" + to_string(parent) + ".k<t" + to_string(i) + ".k";
		}
		auto plan = PlanLinearizedQuery(connection, query);
		JoinOrderOptimizer optimizer(*connection.context);
		QueryGraphManager graph(*connection.context);
		REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
		CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
		CostModel costs(estimator);
		LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
		enumerator.InitLeafPlans();
		vector<vector<idx_t>> adjacency(6);
		graph.GetQueryGraphEdges().EnumerateEdges([&](JoinRelationSet &left, NeighborInfo &edge) {
			REQUIRE(left.count == 1);
			REQUIRE(edge.neighbor->count == 1);
			auto lhs = left.relations[0].index;
			auto rhs = edge.neighbor->relations[0].index;
			if (std::find(adjacency[lhs].begin(), adjacency[lhs].end(), rhs) == adjacency[lhs].end()) {
				adjacency[lhs].push_back(rhs);
			}
			return false;
		});
		idx_t edges = 0;
		for (auto &neighbors : adjacency) {
			edges += neighbors.size();
		}
		REQUIRE(edges == 10);
		JoinOrderLinearizer linearizer(graph, estimator);
		REQUIRE(linearizer.BuildTree());
		double global_minimum = std::numeric_limits<double>::infinity();
		for (idx_t root = 0; root < 6; root++) {
			CAPTURE(shape, root);
			vector<idx_t> parent(6, 6);
			vector<double> multipliers(6);
			vector<idx_t> preorder {root};
			parent[root] = root;
			auto &root_set = graph.set_manager.GetJoinRelation(RelationIndex(root));
			multipliers[root] = estimator.EstimateCardinality(root_set).GetDouble();
			for (idx_t i = 0; i < preorder.size(); i++) {
				auto node = preorder[i];
				for (auto child : adjacency[node]) {
					if (parent[child] != 6) {
						continue;
					}
					parent[child] = node;
					auto &node_set = graph.set_manager.GetJoinRelation(RelationIndex(node));
					auto &child_set = graph.set_manager.GetJoinRelation(RelationIndex(child));
					auto &pair_set = graph.set_manager.Union(node_set, child_set);
					multipliers[child] = estimator.EstimateCardinality(pair_set).GetDouble() /
					                     estimator.EstimateCardinality(node_set).GetDouble();
					preorder.push_back(child);
				}
			}
			auto proxy_cost = [&](const vector<RelationIndex> &order) {
				vector<bool> visited(6, false);
				double product = 1;
				double work = 0;
				for (auto relation : order) {
					if (relation.index != root && !visited[parent[relation.index]]) {
						return std::numeric_limits<double>::infinity();
					}
					visited[relation.index] = true;
					product *= multipliers[relation.index];
					if (relation.index != root) {
						work += product;
					}
				}
				return work;
			};
			vector<RelationIndex> order {RelationIndex(root)};
			for (idx_t i = 0; i < 6; i++) {
				if (i != root) {
					order.emplace_back(i);
				}
			}
			auto minimum = std::numeric_limits<double>::infinity();
			do {
				minimum = MinValue(minimum, proxy_cost(order));
			} while (std::next_permutation(order.begin() + 1, order.end()));
			auto selected = linearizer.GetOrder(RelationIndex(root));
			REQUIRE(selected.relations.size() == 6);
			REQUIRE(selected.relations.front().index == root);
			REQUIRE(proxy_cost(selected.relations) == Approx(minimum));
			REQUIRE(std::exp(selected.log_cost) == Approx(minimum));
			if (minimum < global_minimum) {
				global_minimum = minimum;
			}
		}
		auto best = linearizer.GetBestOrder();
		auto selected = linearizer.GetOrder(best.front());
		REQUIRE(best == selected.relations);
		REQUIRE(std::exp(selected.log_cost) == Approx(global_minimum));
	}
	connection.Rollback();
}

// Synthetic populations replace the table observations, including independent bounds and key evidence.
static void ResetLinearizedInputStats(RelationStats &stats) {
	RelationStats result;
	result.table_name = stats.table_name;
	result.stats_initialized = true;
	for (auto &column : stats.columns) {
		result.columns.emplace_back(column.binding, column.total_domain, column.current_domain, column.name);
	}
	stats = std::move(result);
}

TEST_CASE("Linearized selectivity ties survive fractional input populations", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	for (auto name : {"a", "b", "c"}) {
		REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE " + string(name) + " AS SELECT 1 k"));
	}
	connection.BeginTransaction();
	auto plan = PlanLinearizedQuery(connection, "SELECT * FROM a JOIN b USING(k) JOIN c USING(k)");
	JoinOrderOptimizer optimizer(*connection.context);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
	auto stats = graph.relation_manager.GetRelationStats();
	vector<CardinalityValue> rows {135086, 7248868.8, 2967144};
	REQUIRE(stats.size() == rows.size());
	for (idx_t i = 0; i < stats.size(); i++) {
		ResetLinearizedInputStats(stats[i]);
		stats[i].cardinality = rows[i];
		for (auto &column : stats[i].columns) {
			column.total_domain = DistinctCount(2248305, DistinctCountSource::HLL);
			column.current_domain =
			    DistinctCount(MinValue(CardinalityValue(2248305), rows[i]), DistinctCountSource::HLL);
		}
	}
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel costs(estimator);
	LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
	enumerator.candidate_evaluator.InitLeafPlans(enumerator.plans, stats);
	JoinOrderLinearizer linearizer(graph, estimator);
	REQUIRE(linearizer.BuildTree());
	// Equal pair selectivities form the same ranking tree. Subtracting the three logs
	// perturbed their ties and incorrectly forced the larger input ahead of the smaller one.
	vector<RelationIndex> expected {RelationIndex(0), RelationIndex(2), RelationIndex(1)};
	REQUIRE(linearizer.GetOrder(RelationIndex(0)).relations == expected);
	connection.Rollback();
}

TEST_CASE("IKKBZ chooses the cheapest rooted order rather than the smallest input", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	for (auto name : {"a", "b", "c", "d"}) {
		REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE " + string(name) + " AS SELECT 1 ab, 1 bc, 1 cd"));
	}
	connection.BeginTransaction();
	auto plan = PlanLinearizedQuery(connection, "SELECT * FROM a JOIN b ON a.ab=b.ab "
	                                            "JOIN c ON b.bc=c.bc JOIN d ON c.cd=d.cd");
	JoinOrderOptimizer optimizer(*connection.context);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
	auto stats = graph.relation_manager.GetRelationStats();
	vector<idx_t> cardinalities {1, 100, 10000, 100};
	REQUIRE(stats.size() == cardinalities.size());
	for (idx_t i = 0; i < stats.size(); i++) {
		ResetLinearizedInputStats(stats[i]);
		stats[i].cardinality = cardinalities[i];
		for (auto &column : stats[i].columns) {
			auto domain = column.name.EndsWith(".ab") ? 1 : 100000000;
			column.total_domain = DistinctCount(domain, DistinctCountSource::HLL);
			column.current_domain =
			    DistinctCount(MinValue(CardinalityValue(domain), stats[i].cardinality), DistinctCountSource::HLL);
		}
	}
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel costs(estimator);
	LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
	enumerator.candidate_evaluator.InitLeafPlans(enumerator.plans, stats);
	JoinOrderLinearizer linearizer(graph, estimator);
	REQUIRE(linearizer.BuildTree());
	auto order = linearizer.GetBestOrder();
	REQUIRE(order.front().index != 0);
	REQUIRE(linearizer.GetOrder(order.front()).log_cost < linearizer.GetOrder(RelationIndex(0)).log_cost);
	IntervalJoinEnumerator trial(graph, enumerator.candidate_evaluator, enumerator.plans);
	REQUIRE(trial.Solve(order));
	auto &root = LinearizedTotalSet(graph);
	VerifyLinearizedClosure(*trial.GetPlans().at(root), trial.GetPlans(), costs);
	// Both ranking and interval enumeration must still honor query cancellation.
	connection.Interrupt();
	REQUIRE_THROWS_WITH(linearizer.GetBestOrder(), Catch::Contains("Interrupted!"));
	IntervalJoinEnumerator interrupted(graph, enumerator.candidate_evaluator, enumerator.plans);
	REQUIRE_THROWS_WITH(interrupted.Solve(order), Catch::Contains("Interrupted!"));
	connection.Rollback();
}

TEST_CASE("Linearized eligibility preserves complete plans and greedy fallbacks", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	for (auto name : {"a", "b", "c"}) {
		REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE " + string(name) + " AS SELECT i k FROM range(10)t(i)"));
	}
	connection.BeginTransaction();
	for (auto query :
	     {"SELECT * FROM a JOIN b USING(k) JOIN c USING(k)", "SELECT * FROM a,b,c",
	      "SELECT * FROM a LEFT JOIN b USING(k) LEFT JOIN c USING(k)", "SELECT * FROM a,b,c WHERE a.k+b.k=c.k"}) {
		CAPTURE(query);
		auto plan = PlanLinearizedQuery(connection, query);
		JoinOrderOptimizer optimizer(*connection.context);
		QueryGraphManager graph(*connection.context);
		REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
		CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
		CostModel costs(estimator);
		LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
		enumerator.InitLeafPlans();
		JoinOrderLinearizer linearizer(graph, estimator);
		auto eligible = linearizer.BuildTree();
		REQUIRE(eligible == (string(query) == "SELECT * FROM a JOIN b USING(k) JOIN c USING(k)"));
		REQUIRE(LinearizedTestHelper::Approximate(enumerator));
		auto &root = LinearizedTotalSet(graph);
		VerifyLinearizedClosure(*enumerator.GetPlans().at(root), enumerator.GetPlans(), costs);
		if (eligible) {
			auto incumbent = LinearizedTestHelper::Capture(enumerator, root);
			auto before = costs.GetCost(*incumbent.at(root));
			REQUIRE(LinearizedTestHelper::Approximate(enumerator));
			REQUIRE_FALSE(before < costs.GetCost(*enumerator.GetPlans().at(root)));
			VerifyLinearizedClosure(*incumbent.at(root), incumbent, costs);
			VerifyLinearizedClosure(*enumerator.GetPlans().at(root), enumerator.GetPlans(), costs);
		}
	}
	connection.Rollback();
}

TEST_CASE("Recursive memory preference can invert above an interval", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE a AS SELECT i k FROM range(20)t(i)"));
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE b AS SELECT i k FROM range(100)t(i)"));
	connection.BeginTransaction();
	auto plan = PlanLinearizedQuery(connection, R"(
	    WITH RECURSIVE r(k) AS (
	        SELECT 1 UNION ALL SELECT r.k+1 FROM r JOIN a ON r.k=a.k JOIN b ON r.k=b.k
	    ) SELECT * FROM r)");
	auto &cte = LinearizedRecursiveCTE(*plan);
	JoinOrderOptimizer optimizer(*connection.context);
	InitializeLinearizedRecursiveMember(optimizer, cte);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*cte.children[1])));
	REQUIRE(graph.relation_manager.HasRecursiveRelations());
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel costs(estimator);
	costs.InitializeRecursiveMemory(*connection.context, graph.relation_manager);
	LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
	enumerator.InitLeafPlans();
	REQUIRE(LinearizedTestHelper::Greedy(enumerator));
	auto &root = LinearizedTotalSet(graph);
	auto seed = LinearizedTestHelper::Capture(enumerator, root);
	VerifyLinearizedClosure(*seed.at(root), seed, costs);
	auto seed_cost = costs.GetCost(*seed.at(root));
	REQUIRE(LinearizedTestHelper::Approximate(enumerator));
	auto &plans = enumerator.GetPlans();
	REQUIRE_FALSE(seed_cost < costs.GetCost(*plans.at(root)));
	VerifyLinearizedClosure(*plans.at(root), plans, costs);
	REQUIRE(plans.at(root)->recursive_properties.depends_on_recursive_input);

	// Use the real final hash-build increment to construct two equivalent, individually fitting child
	// alternatives. Their work preference reverses when the parent's extra build exceeds the budget.
	auto parent = *seed.at(root);
	auto left = *seed.at(parent.left_set);
	auto right = *seed.at(parent.right_set);
	if (!left.recursive_properties.depends_on_recursive_input) {
		std::swap(left.recursive_properties, right.recursive_properties);
	}
	left.peak_build_size = left.retained_build_size = 0;
	right.peak_build_size = right.retained_build_size = 0;
	costs.UpdateBuildMemory(parent, left, right);
	auto increment = parent.peak_build_size;
	REQUIRE(increment > 0);
	auto memory = static_cast<double>(BufferManager::GetBufferManager(*connection.context).GetOperatorMemoryLimit());
	REQUIRE(memory > 2 * increment);
	auto lower_work = left;
	auto lower_memory = left;
	lower_work.cost = 1;
	lower_memory.cost = 2;
	lower_work.peak_build_size = lower_work.retained_build_size = memory - increment / 2;
	lower_memory.peak_build_size = lower_memory.retained_build_size = memory - 2 * increment;
	REQUIRE(costs.GetCost(lower_work) < costs.GetCost(lower_memory));
	auto first_parent = parent;
	auto second_parent = parent;
	first_parent.cost = costs.ComputeCost(lower_work, right, parent.join_operator, parent.set);
	second_parent.cost = costs.ComputeCost(lower_memory, right, parent.join_operator, parent.set);
	costs.UpdateBuildMemory(first_parent, lower_work, right);
	costs.UpdateBuildMemory(second_parent, lower_memory, right);
	REQUIRE(costs.GetCost(second_parent) < costs.GetCost(first_parent));
	REQUIRE(LinearizedTestHelper::Prefer(enumerator, second_parent, seed, first_parent, seed));
	REQUIRE(costs.ComputeCostLowerBound(lower_memory, right, parent.cardinality.GetDouble()) > first_parent.cost);
	REQUIRE(costs.GetCost(first_parent).memory_excess > 0);
	REQUIRE(enumerator.candidate_evaluator.CanImprovePair(seed, lower_memory, right, first_parent));
	connection.Rollback();
}

TEST_CASE("Linearized dispatch completes the chosen order and retains the best complete contender",
          "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE t AS SELECT i k FROM range(10)t(i)"));
	connection.BeginTransaction();
	for (idx_t count : {idx_t(12), idx_t(17), idx_t(32), idx_t(33)}) {
		CAPTURE(count);
		string query = "SELECT * FROM t t0";
		for (idx_t i = 1; i < count; i++) {
			query += " JOIN t t" + to_string(i) + " ON t0.k=t" + to_string(i) + ".k";
		}
		auto plan = PlanLinearizedQuery(connection, query);
		JoinOrderOptimizer optimizer(*connection.context);
		QueryGraphManager graph(*connection.context);
		REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
		CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
		CostModel costs(estimator);
		PlanEnumerator coordinator(graph, costs, graph.GetQueryGraphEdges());
		coordinator.InitLeafPlans();
		REQUIRE(coordinator.SolveJoinOrder());
		auto &root = LinearizedTotalSet(graph);
		VerifyLinearizedClosure(*coordinator.GetPlans().at(root), coordinator.GetPlans(), costs);

		CardinalityEstimator greedy_estimator(graph.set_manager, graph.GetPredicateModel());
		CostModel greedy_costs(greedy_estimator);
		LinearizedTestContext greedy(graph, greedy_costs, graph.GetQueryGraphEdges());
		greedy.InitLeafPlans();
		REQUIRE(LinearizedTestHelper::Greedy(greedy));
		auto captured_greedy = LinearizedTestHelper::Capture(greedy, root);
		auto result_cost = costs.GetCost(*coordinator.GetPlans().at(root));
		if (count == 33) {
			REQUIRE(coordinator.GetPlans().size() == greedy.GetPlans().size());
			REQUIRE(result_cost == costs.GetCost(*captured_greedy.at(root)));
			continue;
		}
		REQUIRE_FALSE(costs.GetCost(*captured_greedy.at(root)) < result_cost);
		REQUIRE(coordinator.GetPlans().size() == 2 * count - 1);
		JoinOrderLinearizer linearizer(graph, greedy_estimator);
		REQUIRE(linearizer.BuildTree());
		IntervalJoinEnumerator trial(graph, greedy.candidate_evaluator, greedy.plans);
		REQUIRE(trial.Solve(linearizer.GetBestOrder()));
		REQUIRE_FALSE(costs.GetCost(*trial.GetPlans().at(root)) < result_cost);
		VerifyLinearizedClosure(*trial.GetPlans().at(root), trial.GetPlans(), greedy_costs);
	}
	connection.Rollback();
}

TEST_CASE("Completed linearized search handles empty inputs", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE t(k INTEGER)"));
	connection.BeginTransaction();
	string query = "SELECT * FROM t t0";
	for (idx_t i = 1; i < 15; i++) {
		query += " JOIN t t" + to_string(i) + " ON t0.k=t" + to_string(i) + ".k";
	}
	auto plan = PlanLinearizedQuery(connection, query);
	JoinOrderOptimizer optimizer(*connection.context);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel costs(estimator);
	LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
	enumerator.InitLeafPlans();
	LinearizedPlanEnumerator search(graph, costs, enumerator.candidate_evaluator);
	REQUIRE(search.SolveApproximately(enumerator.plans));
	auto &root = LinearizedTotalSet(graph);
	auto &node = *enumerator.GetPlans().at(root);
	REQUIRE(costs.GetCost(node).work == 0);
	REQUIRE(costs.GetCost(node).memory_excess == 0);
	VerifyLinearizedClosure(node, enumerator.GetPlans(), costs);
	connection.Rollback();
}

TEST_CASE("Zero work with recursive memory excess still needs interval search", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE a AS SELECT 1::BIGINT ra, 1::BIGINT ab"));
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE b AS SELECT 1::BIGINT rb, 1::BIGINT ab"));
	connection.BeginTransaction();
	auto plan = PlanLinearizedQuery(connection, R"(
	    WITH RECURSIVE r(ra, rb) AS (
	        SELECT 1::BIGINT, 1::BIGINT WHERE false
	        UNION ALL SELECT r.ra+1, r.rb+1 FROM r JOIN a ON r.ra=a.ra
	            JOIN b ON a.ab=b.ab AND r.rb=b.rb
	    ) SELECT * FROM r)");
	auto &cte = LinearizedRecursiveCTE(*plan);
	JoinOrderOptimizer optimizer(*connection.context);
	InitializeLinearizedRecursiveMember(optimizer, cte);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*cte.children[1])));
	REQUIRE(graph.relation_manager.HasRecursiveRelations());
	auto stats = graph.relation_manager.GetRelationStats();
	auto large_rows = BufferManager::GetBufferManager(*connection.context).GetOperatorMemoryLimit() * 4;
	for (idx_t i = 0; i < stats.size(); i++) {
		auto &relation = stats[i];
		ResetLinearizedInputStats(relation);
		relation.row_retention = 1;
		if (graph.relation_manager.GetRelation(RelationIndex(i)).recursive_properties.depends_on_recursive_input) {
			relation.cardinality = 0;
		} else {
			relation.cardinality = relation.table_name == "a" ? large_rows : 1;
		}
		for (auto &column : relation.columns) {
			column.total_domain = DistinctCount(large_rows, DistinctCountSource::HLL);
			column.current_domain = DistinctCount(relation.cardinality, DistinctCountSource::HLL);
		}
	}
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel costs(estimator);
	costs.InitializeRecursiveMemory(*connection.context, graph.relation_manager);
	LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
	enumerator.candidate_evaluator.InitLeafPlans(enumerator.plans, stats);
	REQUIRE(LinearizedTestHelper::Greedy(enumerator));
	auto &root = LinearizedTotalSet(graph);
	auto seed = LinearizedTestHelper::Capture(enumerator, root);
	REQUIRE(costs.GetCost(*seed.at(root)).work == 0);
	REQUIRE(costs.GetCost(*seed.at(root)).memory_excess > 0);
	VerifyLinearizedClosure(*seed.at(root), seed, costs);

	REQUIRE(LinearizedTestHelper::Approximate(enumerator));
	auto &node = *enumerator.GetPlans().at(root);
	REQUIRE(costs.GetCost(node).work > 0);
	REQUIRE(costs.GetCost(node).memory_excess == 0);
	REQUIRE(costs.GetCost(node) < costs.GetCost(*seed.at(root)));
	VerifyLinearizedClosure(node, enumerator.GetPlans(), costs);

	// A subsequent search must preserve this complete plan even when input and greedy plans are worse.
	auto incoming = LinearizedTestHelper::Capture(enumerator, root);
	auto incoming_cost = costs.GetCost(*incoming.at(root));
	LinearizedTestHelper::Leaves(enumerator);
	LinearizedTestHelper::Input(enumerator);
	auto input = LinearizedTestHelper::Capture(enumerator, root);
	REQUIRE(incoming_cost < costs.GetCost(*input.at(root)));
	LinearizedTestHelper::Leaves(enumerator);
	REQUIRE(LinearizedTestHelper::Greedy(enumerator));
	auto greedy = LinearizedTestHelper::Capture(enumerator, root);
	REQUIRE(incoming_cost < costs.GetCost(*greedy.at(root)));
	LinearizedTestHelper::Swap(enumerator, incoming);
	LinearizedPlanEnumerator search(graph, costs, enumerator.candidate_evaluator);
	REQUIRE(search.SolveApproximately(enumerator.plans));
	REQUIRE(costs.GetCost(*enumerator.GetPlans().at(root)) == incoming_cost);
	VerifyLinearizedClosure(*enumerator.GetPlans().at(root), enumerator.GetPlans(), costs);
	connection.Rollback();
}

TEST_CASE("Complete contenders preserve the input plan across recursive memory inversion", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE a AS SELECT 1::INT ab, 1::INT ra"));
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE b AS SELECT 1::INT ab, 1::INT bc"));
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE c AS SELECT 1::INT bc"));
	connection.BeginTransaction();
	auto plan = PlanLinearizedQuery(connection, R"(
	    WITH RECURSIVE r(ra) AS (
	        SELECT 1 UNION ALL SELECT r.ra+1 FROM r JOIN
	            (a JOIN (b JOIN c ON b.bc=c.bc) ON a.ab=b.ab) ON r.ra=a.ra
	    ) SELECT * FROM r)");
	auto &cte = LinearizedRecursiveCTE(*plan);
	JoinOrderOptimizer optimizer(*connection.context);
	InitializeLinearizedRecursiveMember(optimizer, cte);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*cte.children[1])));
	REQUIRE(graph.relation_manager.NumRelations() == 4);
	auto &root = LinearizedTotalSet(graph);
	auto memory = static_cast<double>(BufferManager::GetBufferManager(*connection.context).GetOperatorMemoryLimit());
	idx_t scale = 1;
	for (idx_t phase = 0; phase < 2; phase++) {
		auto stats = graph.relation_manager.GetRelationStats();
		for (idx_t i = 0; i < stats.size(); i++) {
			auto &relation = stats[i];
			ResetLinearizedInputStats(relation);
			relation.row_retention = 1;
			if (graph.relation_manager.GetRelation(RelationIndex(i)).recursive_properties.depends_on_recursive_input) {
				relation.cardinality = 10000 * scale;
			} else if (relation.table_name == "a" || relation.table_name == "b") {
				relation.cardinality = 100 * scale;
			} else {
				REQUIRE(relation.table_name == "c");
				relation.cardinality = scale;
			}
			// Filtered source domains: AB=10*scale, BC=20*scale, ABC=2*scale. The alternative
			// ((A B) C) does less work but retains a larger hash build than the input (A (B C)).
			for (auto &column : relation.columns) {
				auto domain = column.name.EndsWith(".ab")   ? 1000 * scale
				              : column.name.EndsWith(".bc") ? 5 * scale
				                                            : scale;
				column.total_domain = DistinctCount(domain, DistinctCountSource::HLL);
				column.current_domain =
				    DistinctCount(MinValue(CardinalityValue(domain), relation.cardinality), DistinctCountSource::HLL);
			}
		}
		CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
		CostModel costs(estimator);
		costs.InitializeRecursiveMemory(*connection.context, graph.relation_manager);
		LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
		enumerator.candidate_evaluator.InitLeafPlans(enumerator.plans, stats);
		LinearizedTestHelper::Input(enumerator);
		auto input = LinearizedTestHelper::Capture(enumerator, root);
		VerifyLinearizedClosure(*input.at(root), input, costs);
		LinearizedTestHelper::Leaves(enumerator);
		REQUIRE(LinearizedTestHelper::Greedy(enumerator));
		auto greedy = LinearizedTestHelper::Capture(enumerator, root);
		auto &greedy_root = *greedy.at(root);
		auto &left = *greedy.at(greedy_root.left_set);
		auto &right = *greedy.at(greedy_root.right_set);
		auto &static_child = left.recursive_properties.depends_on_recursive_input ? right : left;
		REQUIRE_FALSE(static_child.recursive_properties.depends_on_recursive_input);
		if (phase == 0) {
			REQUIRE(greedy_root.cost < input.at(root)->cost);
			REQUIRE(static_child.peak_build_size > input.at(root)->peak_build_size);
			REQUIRE(greedy_root.peak_build_size > static_child.peak_build_size);
			// Put the budget above the replacement child but below its enclosing complete plan.
			scale = static_cast<idx_t>(memory / ((static_child.peak_build_size + greedy_root.peak_build_size) / 2));
			REQUIRE(scale > 100);
			continue;
		}
		REQUIRE(costs.GetCost(greedy_root).memory_excess > 0);
		REQUIRE(costs.GetCost(*input.at(root)).memory_excess == 0);
		REQUIRE(costs.GetCost(*input.at(root)) < costs.GetCost(greedy_root));
		LinearizedTestHelper::Leaves(enumerator);
		REQUIRE(LinearizedTestHelper::Approximate(enumerator));
		REQUIRE_FALSE(costs.GetCost(*input.at(root)) < costs.GetCost(*enumerator.GetPlans().at(root)));
		VerifyLinearizedClosure(*enumerator.GetPlans().at(root), enumerator.GetPlans(), costs);
	}
	connection.Rollback();
}

TEST_CASE("Interval DP can use a cyclic edge omitted from the ranking tree", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE a AS SELECT 1::INT ab, 1::INT ac"));
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE b AS SELECT 1::INT ab, 1::INT bc"));
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE c AS SELECT 1::INT bc, 1::INT ac"));
	connection.BeginTransaction();
	auto plan =
	    PlanLinearizedQuery(connection, "SELECT * FROM a JOIN b ON a.ab=b.ab JOIN c ON b.bc=c.bc AND a.ac=c.ac");
	JoinOrderOptimizer optimizer(*connection.context);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
	auto stats = graph.relation_manager.GetRelationStats();
	REQUIRE(stats.size() == 3);
	for (idx_t i = 0; i < stats.size(); i++) {
		REQUIRE(stats[i].table_name == Identifier(i == 0 ? "a" : i == 1 ? "b" : "c"));
		ResetLinearizedInputStats(stats[i]);
		stats[i].cardinality = i == 1 ? 1000000 : 1;
		for (auto &column : stats[i].columns) {
			auto domain = column.name.EndsWith(".ab") ? 1000 : column.name.EndsWith(".bc") ? 500 : 1;
			column.total_domain = DistinctCount(domain, DistinctCountSource::HLL);
			column.current_domain =
			    DistinctCount(MinValue(CardinalityValue(domain), stats[i].cardinality), DistinctCountSource::HLL);
		}
	}
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel costs(estimator);
	LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
	enumerator.candidate_evaluator.InitLeafPlans(enumerator.plans, stats);
	JoinOrderLinearizer linearizer(graph, estimator);
	REQUIRE(linearizer.BuildTree());
	// AB and BC have selectivities 1/1000 and 1/500; AC has selectivity 1.
	// The MST is the path A-B-C, which forces these two rooted orders.
	REQUIRE(linearizer.GetOrder(RelationIndex(0)).relations ==
	        vector<RelationIndex> {RelationIndex(0), RelationIndex(1), RelationIndex(2)});
	REQUIRE(linearizer.GetOrder(RelationIndex(2)).relations ==
	        vector<RelationIndex> {RelationIndex(2), RelationIndex(1), RelationIndex(0)});
	auto order = linearizer.GetOrder(RelationIndex(1)).relations;
	REQUIRE(order.front().index == 1);
	REQUIRE(LinearizedTestHelper::ForOrder(enumerator, order));
	auto &root = LinearizedTotalSet(graph);
	auto &node = *enumerator.GetPlans().at(root);
	auto &ac = graph.set_manager.GetJoinRelation({RelationIndex(0), RelationIndex(2)});
	REQUIRE((&node.left_set == &ac || &node.right_set == &ac));
	VerifyLinearizedClosure(node, enumerator.GetPlans(), costs);
	connection.Rollback();
}

TEST_CASE("Interval trials skip infeasible splits without mutating their inputs", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE t AS SELECT i a, i b FROM range(10)t(i)"));
	connection.BeginTransaction();
	auto plan = PlanLinearizedQuery(connection, "SELECT * FROM t a JOIN t b ON a.a=b.a JOIN t c ON b.b=c.b");
	JoinOrderOptimizer optimizer(*connection.context);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel costs(estimator);
	LinearizedTestContext inputs(graph, costs, graph.GetQueryGraphEdges());
	inputs.InitLeafPlans();
	auto &root = LinearizedTotalSet(graph);
	auto &ac = graph.set_manager.GetJoinRelation({RelationIndex(0), RelationIndex(2)});
	const vector<RelationIndex> order {RelationIndex(0), RelationIndex(2), RelationIndex(1)};
	IntervalJoinEnumerator trial(graph, inputs.candidate_evaluator, inputs.plans);
	REQUIRE(trial.Solve(order));
	REQUIRE(trial.GetPlans().find(ac) == trial.GetPlans().end());
	REQUIRE(trial.GetPlans().find(root) != trial.GetPlans().end());
	REQUIRE(inputs.plans.size() == 3);
	for (auto &entry : inputs.plans) {
		REQUIRE(entry.second->is_leaf);
		REQUIRE(trial.GetPlans().at(entry.first).get() != entry.second.get());
	}
	VerifyLinearizedClosure(*trial.GetPlans().at(root), trial.GetPlans(), costs);
	connection.Rollback();
}

TEST_CASE("Generalized schedules retain a legal interval parenthesization", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	idx_t rows = 5;
	for (auto name : {"a", "b", "c", "d", "e", "f", "x"}) {
		REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE " + string(name) + " AS SELECT i%7 k FROM range(" +
		                                  to_string(rows) + ") t(i)"));
		rows *= 2;
	}
	connection.BeginTransaction();
	for (auto query : {"SELECT * FROM (a JOIN b ON a.k=b.k) JOIN (c JOIN d ON c.k=d.k) ON a.k+b.k=c.k+d.k",
	                   "SELECT * FROM a LEFT JOIN b ON a.k=b.k LEFT JOIN c ON a.k=c.k",
	                   "SELECT * FROM a SEMI JOIN b ON a.k=b.k ANTI JOIN c ON a.k=c.k",
	                   "SELECT * FROM a LEFT JOIN (b SEMI JOIN c ON b.k=c.k) ON a.k=b.k",
	                   "SELECT * FROM (a JOIN b ON a.k=b.k) JOIN "
	                   "(((c JOIN x ON c.k=x.k) JOIN d ON x.k=d.k) JOIN (e JOIN f ON e.k=f.k) ON x.k=e.k+f.k) "
	                   "ON a.k+b.k=c.k+d.k"}) {
		CAPTURE(query);
		auto plan = PlanLinearizedQuery(connection, query);
		JoinOrderOptimizer optimizer(*connection.context);
		QueryGraphManager graph(*connection.context);
		REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
		CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
		CostModel costs(estimator);
		LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
		enumerator.InitLeafPlans();
		LinearizedTestHelper::Input(enumerator);
		auto &root = LinearizedTotalSet(graph);
		auto witness = CapturedJoinOrderPlan::Capture(enumerator.plans, root, costs);
		JoinOrderPrecedence precedence(graph, estimator, enumerator.candidate_evaluator, *witness);
		auto order = precedence.GetBestOrder();
		REQUIRE(order.size() == graph.relation_manager.NumRelations());
		REQUIRE(precedence.GetBestOrder() == order);
		LinearizedTestHelper::Leaves(enumerator);
		REQUIRE(LinearizedTestHelper::ForOrder(enumerator, order));
		if (order.size() <= 4) {
			auto alternatives = ExhaustiveIntervals(graph, estimator, costs, order, 0, order.size());
			REQUIRE_FALSE(alternatives.empty());
			auto best = std::numeric_limits<double>::infinity();
			for (auto &alternative : alternatives) {
				best = MinValue(best, alternative->cost);
			}
			REQUIRE(enumerator.plans.at(root)->cost == Approx(best));
		}
		VerifyLinearizedClosure(*enumerator.plans.at(root), enumerator.plans, costs);
		auto trial_cost = costs.GetCost(*enumerator.plans.at(root));
		REQUIRE(LinearizedTestHelper::Approximate(enumerator));
		REQUIRE_FALSE(trial_cost < costs.GetCost(*enumerator.plans.at(root)));
		VerifyLinearizedClosure(*enumerator.plans.at(root), enumerator.plans, costs);
	}
	connection.Rollback();
}

TEST_CASE("Nonseparable hyperedges retain a complete witness through approximate search", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	for (auto name : {"a", "b", "c", "d"}) {
		REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE " + string(name) + " AS SELECT i::INT k FROM range(10) t(i)"));
	}
	connection.BeginTransaction();
	auto plan = PlanLinearizedQuery(connection,
	                                "SELECT * FROM a JOIN b ON a.k=b.k JOIN c ON a.k+b.k=c.k JOIN d ON a.k+c.k=d.k");
	JoinOrderOptimizer optimizer(*connection.context);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel costs(estimator);
	LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
	enumerator.InitLeafPlans();
	LinearizedTestHelper::Input(enumerator);
	auto &root = LinearizedTotalSet(graph);
	auto witness = CapturedJoinOrderPlan::Capture(enumerator.plans, root, costs);
	REQUIRE(JoinOrderPrecedence(graph, estimator, enumerator.candidate_evaluator, *witness).GetBestOrder().empty());
	LinearizedTestHelper::Leaves(enumerator);
	REQUIRE(LinearizedTestHelper::Approximate(enumerator));
	REQUIRE_FALSE(costs.GetCost(witness->GetRootNode()) < costs.GetCost(*enumerator.plans.at(root)));
	VerifyLinearizedClosure(*enumerator.plans.at(root), enumerator.plans, costs);
	connection.Interrupt();
	REQUIRE_THROWS_WITH(JoinOrderPrecedence(graph, estimator, enumerator.candidate_evaluator, *witness).GetBestOrder(),
	                    Catch::Contains("Interrupted!"));
	connection.Rollback();
}

TEST_CASE("Generalized search obtains a witness after required cross-product activation", "[optimizer][join_order]") {
	DuckDB db;
	Connection connection(db);
	for (auto name : {"a", "b", "c"}) {
		REQUIRE_NO_FAIL(*connection.Query("CREATE TABLE " + string(name) + " AS SELECT i::INT k FROM range(10) t(i)"));
	}
	connection.BeginTransaction();
	auto plan = PlanLinearizedQuery(connection, "SELECT * FROM a,b,c WHERE a.k+b.k=c.k");
	JoinOrderOptimizer optimizer(*connection.context);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, LinearizedJoinRoot(*plan)));
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel costs(estimator);
	LinearizedTestContext enumerator(graph, costs, graph.GetQueryGraphEdges());
	enumerator.InitLeafPlans();
	LinearizedTestHelper::Input(enumerator);
	auto &root = LinearizedTotalSet(graph);
	REQUIRE(enumerator.plans.find(root) == enumerator.plans.end());
	LinearizedTestHelper::Leaves(enumerator);
	REQUIRE(LinearizedTestHelper::Approximate(enumerator));
	VerifyLinearizedClosure(*enumerator.plans.at(root), enumerator.plans, costs);
	connection.Rollback();
}
