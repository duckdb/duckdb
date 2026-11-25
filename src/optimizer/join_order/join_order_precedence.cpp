#include "duckdb/optimizer/join_order/join_order_precedence.hpp"

#include "duckdb/optimizer/join_order/query_graph_manager.hpp"
#include "duckdb/optimizer/join_order/join_order_plan.hpp"
#include "duckdb/main/client_context.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

JoinOrderPrecedence::Atom::Atom(vector<RelationIndex> relations, double multiplier, double preparation, idx_t group)
    : relations(std::move(relations)), rank(multiplier), log_preparation(preparation), group(group) {
}

JoinOrderPrecedence::Block::Block(Atom atom, vector<RelationIndex> prerequisites)
    : rank(atom.rank), prerequisites(std::move(prerequisites)) {
	atoms.push_back(std::move(atom));
}

JoinOrderPrecedence::Node::Node(Block block) : block(std::move(block)) {
}

JoinOrderPrecedence::JoinOrderPrecedence(QueryGraphManager &graph, CardinalityEstimator &estimator,
                                         JoinOrderCandidateEvaluator &evaluator, const CapturedJoinOrderPlan &witness)
    : graph(graph), estimator(estimator), evaluator(evaluator), witness(witness) {
}

bool JoinOrderPrecedence::Build() {
	const auto &plans = witness.GetPlans();
	auto &root = witness.GetRoot();
	for (idx_t i = 0; i < graph.relation_manager.NumRelations(); i++) {
		graph.context.InterruptCheck();
		cardinalities.push_back(plans.at(graph.set_manager.GetJoinRelation(RelationIndex(i)))->cardinality.GetLog());
	}
	vector<pair<reference<JoinRelationSet>, reference<NeighborInfo>>> connections;
	graph.GetQueryGraphEdges().EnumerateEdges([&](JoinRelationSet &left, NeighborInfo &edge) {
		graph.context.InterruptCheck();
		connections.emplace_back(left, edge);
		return false;
	});
	vector<reference<const DPJoinNode>> pending {*plans.at(root)}, nodes;
	while (!pending.empty()) {
		graph.context.InterruptCheck();
		auto &node = pending.back().get();
		pending.pop_back();
		if (!node.is_leaf) {
			nodes.push_back(node);
			pending.push_back(*plans.at(node.left_set));
			pending.push_back(*plans.at(node.right_set));
		}
	}
	for (idx_t node_idx = nodes.size(); node_idx > 0; node_idx--) {
		auto &node = nodes[node_idx - 1].get();
		idx_t best = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < connections.size(); i++) {
			graph.context.InterruptCheck();
			auto &left = connections[i].first.get();
			auto &edge = connections[i].second.get();
			if (edge.join_operator != node.join_operator ||
			    edge.generated_cross_product != node.generated_cross_product ||
			    !JoinRelationSet::IsSubset(node.left_set, left) ||
			    !JoinRelationSet::IsSubset(node.right_set, *edge.neighbor) ||
			    !graph.IsConnectionApplicable(edge, node.left_set, node.right_set) ||
			    !graph.IsJoinOrderCandidate(edge.join_operator, edge.generated_cross_product, node.left_set,
			                                node.right_set)) {
				continue;
			}
			// Each connection here uses the same witnessed join estimate. Break proxy ties by full endpoints.
			if (best == DConstants::INVALID_INDEX ||
			    EndpointLess(left, *edge.neighbor, connections[best].first, *connections[best].second.get().neighbor)) {
				best = i;
			}
		}
		if (best == DConstants::INVALID_INDEX) {
			return false;
		}
		auto left_log = plans.at(node.left_set)->cardinality.GetLog();
		auto right_log = plans.at(node.right_set)->cardinality.GetLog();
		double selectivity = 0;
		if (std::isfinite(left_log) && std::isfinite(right_log)) {
			selectivity = node.cardinality.GetLog() - left_log - right_log;
		}
		auto &edge = connections[best];
		edges.push_back({edge.first, *edge.second.get().neighbor, selectivity});
	}
	return CheckComponent(Mask(cardinalities.size(), true));
}

RankedJoinOrder JoinOrderPrecedence::GetOrder(RelationIndex root) {
	Mask component(cardinalities.size(), true);
	auto chain = Solve(root, component);
	if (chain.empty()) {
		return {};
	}
	RankedJoinOrder result;
	result.log_cost = TotalWork(chain);
	vector<Atom> atoms;
	for (auto &block : chain) {
		if (!block.prerequisites.empty()) {
			return {};
		}
		for (auto &atom : block.atoms) {
			result.relations.insert(result.relations.end(), atom.relations.begin(), atom.relations.end());
			atoms.push_back(std::move(atom));
		}
	}
	// Check this prescribed construction, not alternative parenthesizations of the order.
	auto plans = CopyJoinOrderLeafPlans(witness.GetPlans());
	auto constructed = Materialize(atoms, plans);
	if (!constructed || constructed.get() != &witness.GetRoot()) {
		return {};
	}
	return result;
}

bool JoinOrderPrecedence::EndpointLess(const JoinRelationSet &left, const JoinRelationSet &right,
                                       const JoinRelationSet &other_left, const JoinRelationSet &other_right) {
	for (auto pair :
	     {make_pair(reference<const JoinRelationSet>(left), reference<const JoinRelationSet>(other_left)),
	      make_pair(reference<const JoinRelationSet>(right), reference<const JoinRelationSet>(other_right))}) {
		auto &a = pair.first;
		auto &b = pair.second;
		for (idx_t i = 0; i < MinValue(a.count, b.count); i++) {
			if (a.relations[i] != b.relations[i]) {
				return a.relations[i] < b.relations[i];
			}
		}
		if (a.count != b.count) {
			return a.count < b.count;
		}
	}
	return false;
}

bool JoinOrderPrecedence::Contains(const Mask &mask, const JoinRelationSet &set) {
	for (idx_t i = 0; i < set.count; i++) {
		if (!mask[set.relations[i].index]) {
			return false;
		}
	}
	return true;
}

idx_t JoinOrderPrecedence::Count(const Mask &mask) {
	return std::count(mask.begin(), mask.end(), true);
}

idx_t JoinOrderPrecedence::FindRoot(vector<idx_t> &parents, idx_t node) {
	while (parents[node] != node) {
		parents[node] = parents[parents[node]];
		node = parents[node];
	}
	return node;
}

void JoinOrderPrecedence::Components(const Mask &component, idx_t omitted, vector<idx_t> &labels) {
	graph.context.InterruptCheck();
	labels.resize(component.size());
	for (idx_t i = 0; i < labels.size(); i++) {
		labels[i] = i;
	}
	for (idx_t edge_idx = 0; edge_idx < edges.size(); edge_idx++) {
		auto &edge = edges[edge_idx];
		graph.context.InterruptCheck();
		if (edge_idx == omitted || !Contains(component, edge.left) || !Contains(component, edge.right)) {
			continue;
		}
		auto root = FindRoot(labels, edge.left.get().relations[0].index);
		for (auto endpoint : {edge.left, edge.right}) {
			for (idx_t i = 0; i < endpoint.get().count; i++) {
				labels[FindRoot(labels, endpoint.get().relations[i].index)] = root;
			}
		}
	}
	for (idx_t i = 0; i < labels.size(); i++) {
		labels[i] = component[i] ? FindRoot(labels, i) : DConstants::INVALID_INDEX;
	}
}

bool JoinOrderPrecedence::CheckComponent(const Mask &component) {
	for (auto &checked : checked_components) {
		graph.context.InterruptCheck();
		if (checked == component) {
			return true;
		}
	}
	vector<idx_t> labels;
	Components(component, DConstants::INVALID_INDEX, labels);
	unordered_set<idx_t> roots;
	for (idx_t i = 0; i < labels.size(); i++) {
		if (component[i]) {
			roots.insert(labels[i]);
		}
	}
	if (roots.size() != 1) {
		return false;
	}
	vector<idx_t> ordinary_parents(component.size());
	for (idx_t i = 0; i < ordinary_parents.size(); i++) {
		ordinary_parents[i] = i;
	}
	for (idx_t edge_idx = 0; edge_idx < edges.size(); edge_idx++) {
		auto &edge = edges[edge_idx];
		graph.context.InterruptCheck();
		if (!Contains(component, edge.left) || !Contains(component, edge.right)) {
			continue;
		}
		if (edge.left.get().count == 1 && edge.right.get().count == 1) {
			auto left = FindRoot(ordinary_parents, edge.left.get().relations[0].index);
			auto right = FindRoot(ordinary_parents, edge.right.get().relations[0].index);
			if (left == right) {
				return false;
			}
			ordinary_parents[right] = left;
			continue;
		}
		Components(component, edge_idx, labels);
		roots.clear();
		for (idx_t i = 0; i < labels.size(); i++) {
			if (component[i]) {
				roots.insert(labels[i]);
			}
		}
		auto left = labels[edge.left.get().relations[0].index];
		auto right = labels[edge.right.get().relations[0].index];
		bool separated = roots.size() == 2 && left != right;
		for (auto endpoint : {edge.left, edge.right}) {
			auto expected = labels[endpoint.get().relations[0].index];
			for (idx_t i = 0; i < endpoint.get().count; i++) {
				separated &= labels[endpoint.get().relations[i].index] == expected;
			}
		}
		if (!separated) {
			return false;
		}
	}
	graph.context.InterruptCheck();
	checked_components.push_back(component);
	return true;
}

void JoinOrderPrecedence::Mark(const Block &block, Mask &mask) {
	for (auto &atom : block.atoms) {
		for (auto relation : atom.relations) {
			mask[relation.index] = true;
		}
	}
}

double JoinOrderPrecedence::TotalWork(const vector<Block> &chain) {
	D_ASSERT(!chain.empty());
	auto rank = chain.front().rank;
	rank.log_work = -std::numeric_limits<double>::infinity();
	for (idx_t i = 0; i < chain.size(); i++) {
		if (i) {
			rank.Append(chain[i].rank);
		}
		for (auto &atom : chain[i].atoms) {
			rank.AddIndependentWork(atom.log_preparation);
		}
	}
	return rank.log_work;
}

idx_t JoinOrderPrecedence::SolveGroup(const JoinRelationSet &group, const Mask &component) {
	Mask group_mask(component.size(), false);
	for (idx_t i = 0; i < group.count; i++) {
		group_mask[group.relations[i].index] = true;
	}
	for (idx_t i = 0; i < groups.size(); i++) {
		auto &cached = groups[i];
		graph.context.InterruptCheck();
		if (cached.component == component && cached.group == group_mask) {
			return i;
		}
	}
	if (!CheckComponent(component)) {
		return DConstants::INVALID_INDEX;
	}
	vector<Block> best;
	double best_work = std::numeric_limits<double>::infinity();
	for (idx_t i = 0; i < group.count; i++) {
		graph.context.InterruptCheck();
		auto chain = Solve(group.relations[i], component);
		if (chain.empty()) {
			continue;
		}
		auto work = TotalWork(chain);
		if (best.empty() || work < best_work) {
			best_work = work;
			best = std::move(chain);
		}
	}
	if (best.empty()) {
		return DConstants::INVALID_INDEX;
	}
	auto result = BuildGroup(best, group_mask);
	// The component already excludes visited outer inputs; their traversal history cannot affect this group.
	groups.push_back({component, group_mask, std::move(result)});
	return groups.size() - 1;
}

JoinOrderPrecedence::Group JoinOrderPrecedence::BuildGroup(vector<Block> &chain, const Mask &group_mask) {
	// Undo rank-normalization compounds; nested forward groups remain atomic.
	vector<Atom> atoms;
	idx_t prefix = 0;
	for (auto &block : chain) {
		for (auto &atom : block.atoms) {
			for (auto relation : atom.relations) {
				if (group_mask[relation.index]) {
					prefix = atoms.size() + 1;
				}
			}
			atoms.push_back(std::move(atom));
		}
	}
	D_ASSERT(prefix > 0);
	Group result;
	auto rank = atoms.front().rank;
	rank.log_work = -std::numeric_limits<double>::infinity();
	for (idx_t i = 0; i < prefix; i++) {
		graph.context.InterruptCheck();
		if (i) {
			rank.Append(atoms[i].rank);
		}
		rank.AddIndependentWork(atoms[i].log_preparation);
		result.relations.insert(result.relations.end(), atoms[i].relations.begin(), atoms[i].relations.end());
		result.atoms.push_back(std::move(atoms[i]));
	}
	graph.context.InterruptCheck();
	unordered_set<RelationIndex> bindings(result.relations.begin(), result.relations.end());
	auto &set = graph.set_manager.GetJoinRelation(bindings);
	result.log_cardinality = estimator.EstimateCardinality(set).GetLog();
	result.log_preparation = rank.log_work;
	return result;
}

idx_t JoinOrderPrecedence::SolveForwardGroup(idx_t edge_idx, const JoinRelationSet &head, const Mask &component,
                                             const Mask &marked) {
	vector<idx_t> labels;
	Components(component, edge_idx, labels);
	Mask child(component.size(), false);
	auto label = labels[head.relations[0].index];
	for (idx_t i = 0; i < child.size(); i++) {
		child[i] = component[i] && labels[i] == label && !marked[i];
	}
	if (Count(child) >= Count(component) || !Contains(child, head)) {
		return DConstants::INVALID_INDEX;
	}
	return SolveGroup(head, child);
}

bool JoinOrderPrecedence::Visit(idx_t node_idx, const Mask &component, Mask &marked, Mask path, vector<Node> &nodes) {
	Mask own(component.size(), false);
	Mark(nodes[node_idx].block, own);
	Mark(nodes[node_idx].block, path);
	Mark(nodes[node_idx].block, marked);
	for (idx_t edge_idx = 0; edge_idx < edges.size(); edge_idx++) {
		auto &edge = edges[edge_idx];
		for (bool reverse : {false, true}) {
			auto &tail = reverse ? edge.right.get() : edge.left.get();
			auto &head = reverse ? edge.left.get() : edge.right.get();
			graph.context.InterruptCheck();
			if (!Contains(component, tail) || !Contains(component, head) || !Contains(marked, tail)) {
				continue;
			}
			bool intersects = false, unvisited = true;
			for (idx_t i = 0; i < tail.count; i++) {
				intersects |= own[tail.relations[i].index];
			}
			for (idx_t i = 0; i < head.count; i++) {
				unvisited &= !marked[head.relations[i].index];
			}
			if (!intersects || !unvisited) {
				continue;
			}
			vector<RelationIndex> prerequisites;
			for (idx_t i = 0; i < tail.count; i++) {
				if (!path[tail.relations[i].index]) {
					prerequisites.push_back(tail.relations[i]);
				}
			}
			vector<RelationIndex> relations;
			double cardinality, preparation = -std::numeric_limits<double>::infinity();
			idx_t group_index = DConstants::INVALID_INDEX;
			if (head.count == 1) {
				relations.push_back(head.relations[0]);
				cardinality = cardinalities[head.relations[0].index];
			} else {
				group_index = SolveForwardGroup(edge_idx, head, component, marked);
				if (group_index == DConstants::INVALID_INDEX) {
					return false;
				}
				auto &group = groups[group_index].result;
				relations = group.relations;
				cardinality = group.log_cardinality;
				preparation = group.log_preparation;
			}
			auto child_idx = nodes.size();
			nodes[node_idx].children.push_back(child_idx);
			nodes.emplace_back(
			    Block(Atom(std::move(relations), cardinality + edge.log_selectivity, preparation, group_index),
			          std::move(prerequisites)));
			if (!Visit(child_idx, component, marked, path, nodes)) {
				return false;
			}
		}
	}
	return true;
}

void JoinOrderPrecedence::Normalize(vector<Block> &chain) {
	vector<Block> normalized;
	for (auto &block : chain) {
		graph.context.InterruptCheck();
		normalized.push_back(std::move(block));
		while (normalized.size() > 1 && normalized.back().prerequisites.empty() &&
		       normalized.back().rank < normalized[normalized.size() - 2].rank) {
			auto &left = normalized[normalized.size() - 2];
			auto &right = normalized.back();
			graph.context.InterruptCheck();
			left.rank.Append(right.rank);
			for (auto &atom : right.atoms) {
				left.atoms.push_back(std::move(atom));
			}
			normalized.pop_back();
		}
	}
	chain = std::move(normalized);
}

vector<JoinOrderPrecedence::Block> JoinOrderPrecedence::MergeChildren(Node &node, vector<vector<Block>> &chains) {
	vector<Block> chain;
	Mask inside(cardinalities.size(), false), emitted(cardinalities.size(), false);
	Mark(node.block, inside);
	Mark(node.block, emitted);
	chain.push_back(std::move(node.block));
	for (auto child : node.children) {
		Normalize(chains[child]);
		for (auto &block : chains[child]) {
			Mark(block, inside);
		}
	}
	vector<idx_t> positions(node.children.size(), 0);
	while (true) {
		bool unfinished = false;
		idx_t best = DConstants::INVALID_INDEX;
		for (idx_t child = 0; child < node.children.size(); child++) {
			auto &child_chain = chains[node.children[child]];
			graph.context.InterruptCheck();
			if (positions[child] == child_chain.size()) {
				continue;
			}
			unfinished = true;
			auto &candidate = child_chain[positions[child]];
			bool available = true;
			for (auto prerequisite : candidate.prerequisites) {
				available &= !inside[prerequisite.index] || emitted[prerequisite.index];
			}
			if (!available) {
				continue;
			}
			if (best != DConstants::INVALID_INDEX) {
				auto &incumbent = chains[node.children[best]][positions[best]];
				if (incumbent.rank < candidate.rank ||
				    (!(candidate.rank < incumbent.rank) &&
				     !(candidate.atoms[0].relations[0] < incumbent.atoms[0].relations[0]))) {
					continue;
				}
			}
			best = child;
		}
		if (best == DConstants::INVALID_INDEX) {
			if (unfinished) {
				return {};
			}
			break;
		}
		auto block = std::move(chains[node.children[best]][positions[best]++]);
		block.prerequisites.erase(std::remove_if(block.prerequisites.begin(), block.prerequisites.end(),
		                                         [&](RelationIndex relation) { return emitted[relation.index]; }),
		                          block.prerequisites.end());
		Mark(block, emitted);
		chain.push_back(std::move(block));
	}
	return chain;
}

vector<JoinOrderPrecedence::Block> JoinOrderPrecedence::Solve(RelationIndex root, const Mask &component) {
	D_ASSERT(component[root.index]);
	graph.context.InterruptCheck();
	vector<Node> nodes;
	nodes.emplace_back(Block(Atom({root}, cardinalities[root.index], -std::numeric_limits<double>::infinity()), {}));
	Mask marked(component.size(), false), path(component.size(), false);
	if (!Visit(0, component, marked, path, nodes) || marked != component) {
		return {};
	}
	vector<vector<Block>> chains(nodes.size());
	for (idx_t i = nodes.size(); i > 0; i--) {
		chains[i - 1] = MergeChildren(nodes[i - 1], chains);
		if (chains[i - 1].empty()) {
			return {};
		}
	}
	Mask covered(component.size(), false);
	for (auto &block : chains[0]) {
		if (!block.prerequisites.empty()) {
			return {};
		}
		for (auto &atom : block.atoms) {
			for (auto relation : atom.relations) {
				if (covered[relation.index]) {
					return {};
				}
				covered[relation.index] = true;
			}
		}
	}
	D_ASSERT(covered == component);
	return std::move(chains[0]);
}

optional_ptr<JoinRelationSet> JoinOrderPrecedence::Materialize(const vector<Atom> &atoms, JoinOrderPlanMap &plans) {
	optional_ptr<JoinRelationSet> root;
	for (auto &atom : atoms) {
		graph.context.InterruptCheck();
		optional_ptr<JoinRelationSet> next;
		if (atom.group == DConstants::INVALID_INDEX) {
			D_ASSERT(atom.relations.size() == 1);
			next = graph.set_manager.GetJoinRelation(atom.relations[0]);
		} else {
			next = Materialize(groups[atom.group].result.atoms, plans);
		}
		if (!next) {
			return nullptr;
		}
		if (!root) {
			root = next;
			continue;
		}
		auto &combined = graph.set_manager.Union(*root, *next);
		auto &edges = graph.GetQueryGraphEdges();
		evaluator.EmitPair(plans, combined, *root, *next, edges.GetConnections(*root, *next));
		evaluator.EmitPair(plans, combined, *next, *root, edges.GetConnections(*next, *root));
		if (plans.find(combined) == plans.end()) {
			return nullptr;
		}
		root = combined;
	}
	return root;
}

vector<RelationIndex> JoinOrderPrecedence::GetBestOrder() {
	cardinalities.clear();
	edges.clear();
	checked_components.clear();
	groups.clear();
	if (!Build()) {
		return {};
	}
	RankedJoinOrder best;
	for (idx_t root = 0; root < graph.relation_manager.NumRelations(); root++) {
		auto candidate = GetOrder(RelationIndex(root));
		if (!candidate.relations.empty() && (best.relations.empty() || candidate.log_cost < best.log_cost)) {
			best = std::move(candidate);
		}
	}
	return std::move(best.relations);
}

} // namespace duckdb
