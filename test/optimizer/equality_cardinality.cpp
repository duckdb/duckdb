#include "catch.hpp"
#include "duckdb/optimizer/relation_statistics/equality_cardinality.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"

#include <algorithm>
#include <cmath>

using namespace duckdb;

static ColumnBinding EqualityBinding(idx_t relation, idx_t column) {
	return ColumnBinding(TableIndex(relation), ProjectionIndex(column));
}

static RelationStats EqualityInput(double rows, const vector<double> &totals, const vector<double> &currents = {}) {
	RelationStats result;
	result.cardinality = rows;
	result.stats_initialized = true;
	for (idx_t c = 0; c < totals.size(); c++) {
		auto total = DistinctCount(totals[c], DistinctCountSource::EXACT);
		auto current = DistinctCount(currents.empty() ? totals[c] : currents[c], DistinctCountSource::EXACT);
		result.columns.emplace_back(EqualityBinding(0, c), total, current, Identifier("key"));
	}
	return result;
}

static double EqualityRows(vector<RelationStats> inputs, const vector<pair<ColumnBinding, ColumnBinding>> &edges,
                           const join_column_sources_t &sources = {}) {
	EqualityCardinalityModel model(inputs, sources);
	vector<idx_t> relations;
	CardinalityValue rows(1);
	for (idx_t r = 0; r < inputs.size(); r++) {
		relations.push_back(r);
		rows *= inputs[r].cardinality;
	}
	auto estimate = model.EstimateJoin(edges, relations);
	rows *= estimate.selectivity;
	return (estimate.maximum ? MinValue(rows, *estimate.maximum) : rows).GetDouble();
}

static vector<pair<ColumnBinding, ColumnBinding>> EqualityChain(idx_t relations, idx_t keys) {
	vector<pair<ColumnBinding, ColumnBinding>> result;
	for (idx_t r = 1; r < relations; r++) {
		for (idx_t c = 0; c < keys; c++) {
			result.emplace_back(EqualityBinding(r - 1, c), EqualityBinding(r, c));
		}
	}
	return result;
}

static void EqualityPopulation(RelationStats &stats, vector<idx_t> axes, double total, double current,
                               bool unique = true) {
	if (unique) {
		stats.AddUniqueKey(axes);
	}
	stats.joint_stats.push_back({std::move(axes), DistinctCount(total, DistinctCountSource::EXACT),
	                             DistinctCount(current, DistinctCountSource::EXACT), nullptr});
}

static RelationStats UnknownEqualityInput(double rows, const vector<double> &populations) {
	auto result = EqualityInput(rows, populations);
	for (auto &column : result.columns) {
		column.total_domain = DistinctCount(column.total_domain.distinct_count, DistinctCountSource::CARDINALITY);
		column.current_domain = column.total_domain;
	}
	return result;
}

static join_column_sources_t EqualityScanSources(const vector<RelationStats> &inputs, double scan_population = 100) {
	join_column_sources_t result;
	for (idx_t r = 0; r < inputs.size(); r++) {
		for (idx_t c = 0; c < inputs[r].columns.size(); c++) {
			auto binding = EqualityBinding(r, c);
			result.emplace(
			    binding, JoinColumnSources({EqualityBinding(100 + r, c), idx_t(scan_population), inputs[r].cardinality,
			                                DistinctCount(scan_population, DistinctCountSource::CARDINALITY)}));
		}
	}
	return result;
}

TEST_CASE("Composite join cost priors retain producer populations and equality closure",
          "[optimizer][equality_cardinality]") {
	vector<RelationStats> inputs {EqualityInput(100000, {1000, 2000}), EqualityInput(10000, {1000, 2000})};
	auto edges = EqualityChain(2, 2);
	auto sources = EqualityScanSources(inputs);
	for (idx_t r = 0; r < inputs.size(); r++) {
		for (idx_t c = 0; c < inputs[r].columns.size(); c++) {
			auto &source = sources.at(EqualityBinding(r, c)).direct;
			source.source_cardinality = r == 0 ? 100000 : 10000;
			source.domain = inputs[r].columns[c].total_domain;
		}
	}
	REQUIRE(EqualityRows(inputs, edges, sources) == Approx(100000));
	SECTION("Duplicate predicates cannot charge the prior twice") {
		edges.push_back(edges[0]);
		std::reverse(edges.begin(), edges.end());
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(100000));
	}
	SECTION("Filters retain their selectivity instead of shrinking the tuple population") {
		inputs[1].cardinality = 1000;
		inputs[1].CapCurrentDomainsToCardinality();
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(10000));
	}
	SECTION("Missing or mixed producer origins retain independent factors") {
		REQUIRE(EqualityRows(inputs, edges) == Approx(500));
		sources.at(EqualityBinding(1, 1)).direct.binding.table_index = TableIndex(200);
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(500));
	}
	SECTION("A second coordinate can close through a dimension") {
		inputs.push_back(EqualityInput(1000, {1000}));
		inputs.push_back(EqualityInput(2000, {2000}));
		edges = {{EqualityBinding(0, 0), EqualityBinding(1, 0)},
		         {EqualityBinding(1, 0), EqualityBinding(2, 0)},
		         {EqualityBinding(0, 1), EqualityBinding(3, 0)},
		         {EqualityBinding(1, 1), EqualityBinding(3, 0)}};
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(100000));
	}
	SECTION("Competing pairs remain independent") {
		inputs.push_back(EqualityInput(10000, {1000, 2000}));
		for (idx_t c = 0; c < 2; c++) {
			auto binding = EqualityBinding(2, c);
			sources.emplace(
			    binding, JoinColumnSources({EqualityBinding(102, c), 10000, 10000, inputs[2].columns[c].total_domain}));
			edges.emplace_back(EqualityBinding(1, c), binding);
		}
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(2.5));
	}
	SECTION("Complete joint evidence has priority") {
		for (auto &input : inputs) {
			EqualityPopulation(input, {0, 1}, 5000, 5000, false);
		}
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(200000));
		inputs.push_back(EqualityInput(1000, {1000}));
		edges.emplace_back(EqualityBinding(0, 0), EqualityBinding(2, 0));
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(200000));
		// Marginal restrictions still limit compatible tuples when a dimension prevents a full bundle.
		inputs[1].columns[0].total_domain = DistinctCount(100, DistinctCountSource::EXACT);
		inputs[1].columns[0].current_domain = inputs[1].columns[0].total_domain;
		sources.at(EqualityBinding(1, 0)).direct.domain = inputs[1].columns[0].total_domain;
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(20000));
	}
	SECTION("Partial joint evidence cannot be replaced with a guessed tuple") {
		EqualityPopulation(inputs[0], {0, 1}, 5000, 5000, false);
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(500));
	}
	SECTION("Broad fact-to-fact joins retain independent factors") {
		for (auto &entry : sources) {
			entry.second.direct.source_cardinality = 100000;
		}
		inputs[1].cardinality = 100000;
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(5000));
	}
}

TEST_CASE("Unknown equality coordinates share a connected tuple prior", "[optimizer][equality_cardinality]") {
	for (idx_t keys : {1, 2, 16}) {
		vector<RelationStats> inputs(2, UnknownEqualityInput(100, vector<double>(keys, 100)));
		auto sources = EqualityScanSources(inputs);
		auto edges = EqualityChain(2, keys);
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(500));
		edges.push_back(edges[0]);
		std::reverse(edges.begin(), edges.end());
		for (auto &edge : edges) {
			std::swap(edge.first, edge.second);
		}
		REQUIRE(EqualityRows(inputs, edges, sources) == Approx(500));
		// Absence of a captured scan basis is a different evidence condition, not an inferred scan.
		REQUIRE(EqualityRows(inputs, edges) == Approx(100));
	}
	for (idx_t count : {3, 4}) {
		vector<RelationStats> inputs(count, UnknownEqualityInput(100, {100, 100, 100}));
		auto sources = EqualityScanSources(inputs);
		auto same_key = EqualityChain(count, 1);
		auto expected = count == 3 ? 2500 : 12500;
		REQUIRE(EqualityRows(inputs, same_key, sources) == Approx(expected));
		vector<pair<ColumnBinding, ColumnBinding>> independent;
		for (idx_t r = 1; r < count; r++) {
			independent.emplace_back(EqualityBinding(r - 1, r - 1), EqualityBinding(r, r - 1));
		}
		REQUIRE(EqualityRows(inputs, independent, sources) == Approx(expected));
		for (idx_t a = 0; a < count; a++) {
			for (idx_t b = a + 1; b < count; b++) {
				independent.emplace_back(EqualityBinding(a, a % 3), EqualityBinding(b, a % 3));
			}
		}
		REQUIRE(EqualityRows(inputs, independent, sources) == Approx(expected));
		std::reverse(independent.begin(), independent.end());
		REQUIRE(EqualityRows(inputs, independent, sources) == Approx(expected));
	}
}

TEST_CASE("Unknown blocks keep informed factors and local components separate", "[optimizer][equality_cardinality]") {
	vector<RelationStats> inputs(4, UnknownEqualityInput(100, {100, 100}));
	auto sources = EqualityScanSources(inputs);
	vector<pair<ColumnBinding, ColumnBinding>> edges {{EqualityBinding(0, 0), EqualityBinding(1, 0)},
	                                                  {EqualityBinding(2, 0), EqualityBinding(3, 0)}};
	REQUIRE(EqualityRows(inputs, edges, sources) == Approx(250000));
	for (auto r : {1, 2}) {
		inputs[r].columns[1].total_domain = DistinctCount(4, DistinctCountSource::EXACT);
		inputs[r].columns[1].current_domain = inputs[r].columns[1].total_domain;
	}
	edges.emplace_back(EqualityBinding(1, 1), EqualityBinding(2, 1));
	REQUIRE(EqualityRows(inputs, edges, sources) == Approx(62500));

	inputs = {UnknownEqualityInput(100, {20, 100, 100}), UnknownEqualityInput(100, {20})};
	edges = {{EqualityBinding(0, 0), EqualityBinding(1, 0)}};
	REQUIRE(EqualityRows(inputs, edges) == Approx(500));
	edges.emplace_back(EqualityBinding(0, 1), EqualityBinding(0, 2));
	REQUIRE(EqualityRows(inputs, edges) == Approx(500));
	// Within a cross-input component, multiple unknown bindings of A deliberately share its tuple prior.
	edges.emplace_back(EqualityBinding(0, 0), EqualityBinding(0, 1));
	REQUIRE(EqualityRows(inputs, edges) == Approx(100));
	vector<RelationStats> informed(2, EqualityInput(100, {20, 20}));
	edges = {{EqualityBinding(0, 0), EqualityBinding(1, 0)}, {EqualityBinding(0, 0), EqualityBinding(0, 1)}};
	REQUIRE(EqualityRows(informed, edges) == Approx(25));
	for (auto &input : informed) {
		for (auto &column : input.columns) {
			column.total_domain.has_usable_domain = false;
		}
	}
	REQUIRE(EqualityRows(informed, edges) == Approx(500));
}

TEST_CASE("Unknown tuple priors retain unequal binding populations", "[optimizer][equality_cardinality]") {
	for (auto populations : {vector<double> {4000, 4000, 6}, vector<double> {10, 10, 20}}) {
		vector<RelationStats> inputs;
		for (auto point : populations) {
			inputs.push_back(UnknownEqualityInput(10000, {point}));
		}
		auto expected = populations[0] == 4000 ? 1e12 / 16000000 : 1e12 / 200;
		for (idx_t permutation = 0; permutation < 3; permutation++) {
			REQUIRE(EqualityRows(inputs, EqualityChain(3, 1)) == Approx(expected));
			std::rotate(inputs.begin(), inputs.begin() + 1, inputs.end());
		}
	}
	vector<RelationStats> filtered {UnknownEqualityInput(10, {10}), UnknownEqualityInput(100, {100})};
	REQUIRE(EqualityRows(filtered, EqualityChain(2, 1), EqualityScanSources(filtered)) == Approx(50));
	filtered[1] = filtered[0];
	REQUIRE(EqualityRows(filtered, EqualityChain(2, 1), EqualityScanSources(filtered)) == Approx(10));
	// Different unknown coordinates of B share a latent tuple, not independent edge probabilities.
	vector<RelationStats> overlap {UnknownEqualityInput(100, {20}), UnknownEqualityInput(100, {20, 100}),
	                               UnknownEqualityInput(100, {100})};
	vector<pair<ColumnBinding, ColumnBinding>> edges {{EqualityBinding(0, 0), EqualityBinding(1, 0)},
	                                                  {EqualityBinding(1, 1), EqualityBinding(2, 0)}};
	REQUIRE(EqualityRows(overlap, edges) == Approx(100));
}

TEST_CASE("Unknown closure is invariant to the spanning-forest counterexample", "[optimizer][equality_cardinality]") {
	vector<RelationStats> inputs {UnknownEqualityInput(100, {100, 20}), UnknownEqualityInput(100, {100, 20}),
	                              UnknownEqualityInput(100, {1, 20}), UnknownEqualityInput(100, {1, 20})};
	vector<pair<ColumnBinding, ColumnBinding>> edges {{EqualityBinding(0, 0), EqualityBinding(1, 0)},
	                                                  {EqualityBinding(2, 0), EqualityBinding(3, 0)},
	                                                  {EqualityBinding(0, 1), EqualityBinding(2, 1)},
	                                                  {EqualityBinding(1, 1), EqualityBinding(3, 1)}};
	REQUIRE(EqualityRows(inputs, edges) == Approx(500));
	edges.emplace_back(EqualityBinding(0, 0), EqualityBinding(2, 0));
	REQUIRE(EqualityRows(inputs, edges) == Approx(500));
}

TEST_CASE("Factor-local peer selection preserves original population evidence", "[optimizer][equality_cardinality]") {
	vector<RelationStats> inputs {UnknownEqualityInput(100, {100}), UnknownEqualityInput(100, {100}),
	                              EqualityInput(100, {4})};
	auto sources = EqualityScanSources(inputs);
	sources.at(EqualityBinding(2, 0)).direct.domain = inputs[2].columns[0].total_domain;
	EqualityCardinalityModel model(inputs, sources);
	// Source observations are owned by the model; subsequent caller changes cannot alter it.
	sources.clear();
	auto ab = model.Components(EqualityChain(2, 1));
	auto abc = model.Components(EqualityChain(3, 1));
	for (idx_t iteration = 0; iteration < 3; iteration++) {
		REQUIRE((10000 * model.Estimate(ab, {0, 1}).selectivity).GetDouble() == Approx(500));
		REQUIRE((1000000 * model.Estimate(abc, {0, 1, 2}).selectivity).GetDouble() == Approx(62500));
		REQUIRE(inputs[0].columns[0].total_domain.distinct_count == 100);
		REQUIRE_FALSE(inputs[0].columns[0].total_domain.has_usable_domain);
		REQUIRE_FALSE(model.ColumnSupport(EqualityBinding(0, 0)).has_usable_domain);
	}
	// Identical identity is not an observation of how many values that support contains.
	inputs[0].EnsureMaterializedProducerIdentity();
	inputs[1] = inputs[0];
	inputs.resize(2);
	REQUIRE(EqualityRows(inputs, EqualityChain(2, 1), EqualityScanSources(inputs)) == Approx(500));
}

TEST_CASE("Usable local populations are distinct from raw peer observations", "[optimizer][equality_cardinality]") {
	auto known = EqualityInput(20, {1});
	known.columns[0].total_domain = DistinctCount(1, DistinctCountSource::DERIVED, true);
	auto unknown = UnknownEqualityInput(40, {1000});
	vector<RelationStats> inputs {known, unknown};
	auto edges = EqualityChain(2, 1);
	REQUIRE(EqualityRows(inputs, edges) == Approx(0.8));
	auto sources = EqualityScanSources(inputs, 200);
	sources.at(EqualityBinding(0, 0)).direct.domain = DistinctCount(10, DistinctCountSource::EXACT);
	REQUIRE(EqualityRows(inputs, edges, sources) == Approx(80));
	known.columns[0].total_domain = DistinctCount(2, DistinctCountSource::EXACT);
	known.columns[0].current_domain = known.columns[0].total_domain;
	REQUIRE(EqualityRows({known, unknown}, edges) == Approx(400));
}

TEST_CASE("Factor source selection deduplicates origins and preserves uplift guards",
          "[optimizer][equality_cardinality]") {
	for (bool duplicate : {false, true}) {
		for (bool independent : {false, true}) {
			JoinDomainEstimate domain;
			auto a = EqualityBinding(0, 0);
			auto b = EqualityBinding(1, 0);
			auto c = EqualityBinding(2, 0);
			JoinColumnSources observed(
			    {EqualityBinding(10, 0), 10000, 10000, DistinctCount(50, DistinctCountSource::HLL)});
			if (duplicate) {
				observed.equivalents = {observed.direct, observed.direct};
			}
			domain.Update(a, observed.direct.domain, &observed);
			auto second = observed;
			if (independent) {
				second.direct.binding = EqualityBinding(11, 0);
			}
			domain.Update(b, second.direct.domain, &second);
			JoinColumnSources range(
			    {EqualityBinding(12, 0), 1000, 1000, DistinctCount(1000, DistinctCountSource::MIN_MAX)});
			domain.Update(c, range.direct.domain, &range);
			REQUIRE(domain.Resolve({c}).distinct_count == (independent ? 50 : 1000));
			domain.Update(b, DistinctCount(40, DistinctCountSource::DERIVED, true), &second);
			REQUIRE(domain.Resolve({c}).distinct_count == 50);
		}
	}
	JoinDomainEstimate selected;
	JoinColumnSources large(
	    {EqualityBinding(10, 0), 100000, 10, DistinctCount(100000, DistinctCountSource::CARDINALITY)});
	JoinColumnSources small(
	    {EqualityBinding(11, 0), 20000, 10, DistinctCount(20000, DistinctCountSource::CARDINALITY)});
	selected.Update(EqualityBinding(0, 0), DistinctCount(10, DistinctCountSource::CARDINALITY), &large);
	selected.Update(EqualityBinding(1, 0), DistinctCount(10, DistinctCountSource::CARDINALITY), &small);
	REQUIRE(selected.UnknownScanBasis());
	REQUIRE(*selected.UnknownScanBasis() == 20000);
	REQUIRE(selected.Resolve({EqualityBinding(0, 0)}).distinct_count == 10);
}

TEST_CASE("Usable cardinality observations precede unknown source fallbacks", "[optimizer][equality_cardinality]") {
	vector<DistinctCount> domains {DistinctCount(100, DistinctCountSource::CARDINALITY, true),
	                               DistinctCount(10, DistinctCountSource::CARDINALITY),
	                               DistinctCount(200, DistinctCountSource::CARDINALITY)};
	vector<idx_t> order {0, 1, 2};
	do {
		CAPTURE(order);
		vector<DistinctCount> reordered;
		JoinDomainEstimate source;
		for (auto i : order) {
			reordered.push_back(domains[i]);
			auto binding = EqualityBinding(i, 0);
			JoinColumnSources observation({EqualityBinding(10 + i, 0), domains[i].distinct_count.GetIndex(),
			                               domains[i].distinct_count, domains[i]});
			source.Update(binding, domains[i], &observation);
		}
		auto selected = RelationStatisticsHelper::SelectTotalDomain(reordered);
		REQUIRE(selected);
		REQUIRE(selected->distinct_count == 100);
		REQUIRE(selected->has_usable_domain);
		REQUIRE(selected->source == DistinctCountSource::CARDINALITY);
		REQUIRE_FALSE(source.UnknownScanBasis());
		for (idx_t i = 0; i < domains.size(); i++) {
			auto resolved = source.Resolve({EqualityBinding(i, 0)});
			REQUIRE(resolved.distinct_count == 100);
			REQUIRE(resolved.has_usable_domain);
		}
	} while (std::next_permutation(order.begin(), order.end()));
	// Existing informed-source preferences remain unchanged within the usable category.
	auto selected = RelationStatisticsHelper::SelectTotalDomain(
	    {domains[0], domains[1], DistinctCount(20, DistinctCountSource::MIN_MAX)});
	REQUIRE(selected);
	REQUIRE(selected->distinct_count == 20);
	REQUIRE(selected->source == DistinctCountSource::MIN_MAX);
	selected = RelationStatisticsHelper::SelectTotalDomain(
	    {domains[0], DistinctCount(200, DistinctCountSource::MIN_MAX), DistinctCount(40, DistinctCountSource::HLL)});
	REQUIRE(selected);
	REQUIRE(selected->distinct_count == 40);
	REQUIRE(selected->source == DistinctCountSource::HLL);
	selected = RelationStatisticsHelper::SelectTotalDomain({domains[1], domains[2]});
	REQUIRE(selected);
	REQUIRE(selected->distinct_count == 10);
	REQUIRE_FALSE(selected->has_usable_domain);
}

TEST_CASE("Captured raw scalar keys inform only factor-local pair populations", "[optimizer][equality_cardinality]") {
	for (auto kind : {DistinctCountSource::CARDINALITY, DistinctCountSource::EXACT, DistinctCountSource::HLL}) {
		vector<RelationStats> originals {UnknownEqualityInput(100, {100}), UnknownEqualityInput(10, {10}),
		                                 UnknownEqualityInput(10, {10})};
		// CaptureUniqueKeys keeps the original provider kind while supplying actual key-population evidence.
		auto &key = originals[0];
		key.columns[0].total_domain = DistinctCount(100, kind, true);
		key.columns[0].current_domain = DistinctCount(100, DistinctCountSource::CARDINALITY, true);
		key.AddUniqueKey({0});
		key.joint_stats.push_back({{0}, key.columns[0].total_domain, key.columns[0].current_domain, nullptr});
		vector<idx_t> order {0, 1, 2};
		do {
			CAPTURE(kind, order);
			vector<RelationStats> inputs;
			join_column_sources_t sources;
			for (idx_t r = 0; r < order.size(); r++) {
				const auto &original = originals[order[r]];
				inputs.push_back(original);
				sources.emplace(EqualityBinding(r, 0),
				                JoinColumnSources({EqualityBinding(100 + order[r], 0), original.cardinality.GetIndex(),
				                                   original.cardinality, original.columns[0].total_domain}));
			}
			EqualityCardinalityModel model(inputs, sources);
			auto estimate = model.Estimate(model.Components(EqualityChain(3, 1)), {0, 1, 2});
			// Pair totals are 100,100,100: 100*10*10 / 100^2 = 1, not the unknown-peer result 10.
			REQUIRE((10000 * estimate.selectivity).GetDouble() == Approx(1));
			REQUIRE(EqualityRows(inputs, EqualityChain(3, 1), sources) == Approx(1));
			for (idx_t r = 0; r < order.size(); r++) {
				const auto &column = inputs[r].columns[0];
				REQUIRE(column.total_domain.distinct_count == (order[r] == 0 ? 100 : 10));
				REQUIRE(column.total_domain.has_usable_domain == (order[r] == 0));
				REQUIRE(model.ColumnSupport(EqualityBinding(r, 0)).has_usable_domain == (order[r] == 0));
				const auto &raw = sources.at(EqualityBinding(r, 0)).direct.domain;
				REQUIRE(raw.source == (order[r] == 0 ? kind : DistinctCountSource::CARDINALITY));
				REQUIRE(raw.has_usable_domain == (order[r] == 0));
			}
		} while (std::next_permutation(order.begin(), order.end()));
	}
}

TEST_CASE("Conditioned unknown points remain unknown through aliases and row retention",
          "[optimizer][equality_cardinality]") {
	for (bool known : {false, true}) {
		auto input = EqualityInput(100, {100, 100});
		for (auto &column : input.columns) {
			column.total_domain = DistinctCount(100, DistinctCountSource::DERIVED, known);
			column.current_domain = column.total_domain;
		}
		input.equivalent_columns = {{0, 1}};
		RelationStatisticsHelper::ApplyCardinality(input, 10);
		input.EnsureMaterializedProducerIdentity();
		vector<RelationStats> inputs {input};
		EqualityCardinalityModel model(inputs);
		for (auto &column : inputs[0].columns) {
			REQUIRE(column.total_domain.has_usable_domain == known);
			REQUIRE(column.current_domain.has_usable_domain == known);
		}
		// An actual informed alternative of the same population can supply its point to both aliases.
		inputs[0].columns[1].total_domain = DistinctCount(40, DistinctCountSource::EXACT);
		EqualityCardinalityModel reconciled(inputs);
		REQUIRE(inputs[0].columns[0].total_domain.has_usable_domain);
		REQUIRE(inputs[0].columns[0].total_domain.distinct_count == 40);
	}
	auto source = UnknownEqualityInput(100, {10, 10});
	source.equivalent_columns = {{0, 1}};
	source.columns[1].total_domain = DistinctCount(40, DistinctCountSource::EXACT);
	auto projected = EqualityInput(100, {10});
	projected.CopyEvidence(source, {optional_idx(0)});
	REQUIRE(projected.columns[0].total_domain.has_usable_domain);
	REQUIRE(projected.columns[0].total_domain.distinct_count == 40);
}

TEST_CASE("Scalar and singleton alternatives retain the actual usable population",
          "[optimizer][equality_cardinality]") {
	for (double unknown_population : {10, 40, 100}) {
		for (bool scalar_informed : {false, true}) {
			for (idx_t aliases : {1, 2}) {
				for (bool reverse : {false, true}) {
					CAPTURE(unknown_population, scalar_informed, aliases, reverse);
					auto input = UnknownEqualityInput(100, vector<double>(aliases, unknown_population));
					auto informed_alias = reverse ? aliases - 1 : 0;
					if (scalar_informed) {
						input.columns[informed_alias].total_domain = DistinctCount(40, DistinctCountSource::EXACT);
						input.columns[informed_alias].current_domain = input.columns[informed_alias].total_domain;
					}
					for (idx_t c = 0; c < aliases; c++) {
						auto point = !scalar_informed && c == informed_alias
						                 ? DistinctCount(40, DistinctCountSource::EXACT)
						                 : DistinctCount(unknown_population, DistinctCountSource::CARDINALITY);
						input.joint_stats.push_back({{c}, point, point, nullptr});
					}
					if (aliases == 2) {
						input.equivalent_columns = {reverse ? vector<idx_t> {1, 0} : vector<idx_t> {0, 1}};
					}
					if (reverse) {
						std::reverse(input.joint_stats.begin(), input.joint_stats.end());
					}
					vector<RelationStats> inputs {input, UnknownEqualityInput(100, {100})};
					for (idx_t iteration = 0; iteration < 2; iteration++) {
						EqualityCardinalityModel model(inputs);
						REQUIRE_FALSE(inputs[0].HasUniqueKey({0}));
						REQUIRE(inputs[0].joint_stats.size() == 1);
						for (const auto &column : inputs[0].columns) {
							REQUIRE(column.total_domain.distinct_count == 40);
							REQUIRE(column.current_domain.distinct_count == 40);
							REQUIRE(column.total_domain.has_usable_domain);
							REQUIRE(column.current_domain.has_usable_domain);
							REQUIRE(column.total_domain.source == DistinctCountSource::EXACT);
							REQUIRE(column.current_domain.source == DistinctCountSource::EXACT);
						}
						const auto &joint = inputs[0].joint_stats[0];
						REQUIRE(joint.total_domain.distinct_count == 40);
						REQUIRE(joint.current_domain.distinct_count == 40);
						REQUIRE(joint.total_domain.has_usable_domain);
						REQUIRE(joint.current_domain.has_usable_domain);
						REQUIRE(joint.total_domain.source == DistinctCountSource::EXACT);
						REQUIRE(joint.current_domain.source == DistinctCountSource::EXACT);
						REQUIRE_FALSE(model.ColumnSupport(EqualityBinding(1, 0)).has_usable_domain);
						auto estimate = model.Estimate(model.Components(EqualityChain(2, 1)), {0, 1});
						// The actual 40-value alternative informs the pair prior: 100*100 / 40 = 250.
						REQUIRE((10000 * estimate.selectivity).GetDouble() == Approx(250));
					}
				}
			}
		}
	}
	auto unknown = UnknownEqualityInput(100, {40});
	unknown.joint_stats.push_back({{0}, unknown.columns[0].total_domain, unknown.columns[0].current_domain, nullptr});
	vector<RelationStats> inputs {unknown, UnknownEqualityInput(100, {100})};
	EqualityCardinalityModel model(inputs);
	REQUIRE_FALSE(inputs[0].HasUniqueKey({0}));
	REQUIRE_FALSE(inputs[0].columns[0].total_domain.has_usable_domain);
	REQUIRE_FALSE(inputs[0].joint_stats[0].total_domain.has_usable_domain);
	REQUIRE(EqualityRows(inputs, EqualityChain(2, 1)) == Approx(100));

	// A weaker current alternative cannot enlarge the selected informed total population.
	auto informed_total = UnknownEqualityInput(100, {40});
	informed_total.columns[0].total_domain = DistinctCount(40, DistinctCountSource::EXACT);
	informed_total.joint_stats.push_back({{0},
	                                      DistinctCount(100, DistinctCountSource::CARDINALITY),
	                                      DistinctCount(100, DistinctCountSource::CARDINALITY),
	                                      nullptr});
	inputs = {informed_total, UnknownEqualityInput(100, {100})};
	EqualityCardinalityModel bounded(inputs);
	REQUIRE(inputs[0].columns[0].total_domain.distinct_count == 40);
	REQUIRE(inputs[0].columns[0].current_domain.distinct_count == 40);
	REQUIRE(inputs[0].columns[0].total_domain.has_usable_domain);
	REQUIRE_FALSE(inputs[0].columns[0].current_domain.has_usable_domain);
	REQUIRE(inputs[0].joint_stats[0].total_domain.distinct_count == 40);
	REQUIRE(inputs[0].joint_stats[0].current_domain.distinct_count == 40);
	REQUIRE(inputs[0].joint_stats[0].total_domain.has_usable_domain);
	REQUIRE_FALSE(inputs[0].joint_stats[0].current_domain.has_usable_domain);
	REQUIRE(EqualityRows(inputs, EqualityChain(2, 1)) == Approx(250));
}

TEST_CASE("Retained singleton populations survive changes to current uniqueness", "[optimizer][equality_cardinality]") {
	for (double total : {64, 128}) {
		for (bool reverse : {false, true}) {
			CAPTURE(total, reverse);
			auto input = EqualityInput(32, {32, 32});
			input.equivalent_columns = {reverse ? vector<idx_t> {1, 0} : vector<idx_t> {0, 1}};
			EqualityPopulation(input, {reverse ? idx_t(1) : idx_t(0)}, total, 16);
			vector<RelationStats> inputs {input};
			for (idx_t stage = 0; stage < 3; stage++) {
				CAPTURE(stage);
				if (stage == 1) {
					inputs[0].unique_keys.clear();
					inputs[0].cardinality = 64;
				} else if (stage == 2) {
					inputs[0].AddUniqueKey({0});
					inputs[0].cardinality = 16;
				}
				EqualityCardinalityModel model(inputs);
				auto current = stage == 2 ? 16 : 32;
				REQUIRE(inputs[0].HasUniqueKey({0}) == (stage != 1));
				for (const auto &column : inputs[0].columns) {
					REQUIRE(column.total_domain.distinct_count == total);
					REQUIRE(column.current_domain.distinct_count == current);
					REQUIRE(column.total_domain.has_usable_domain);
					REQUIRE(column.current_domain.has_usable_domain);
				}
				REQUIRE(inputs[0].joint_stats.size() == 1);
				REQUIRE(inputs[0].joint_stats[0].total_domain.distinct_count == total);
				REQUIRE(inputs[0].joint_stats[0].current_domain.distinct_count == current);
			}
		}
	}
}

TEST_CASE("Mixed evidence component merges are outside unknown-only monotonicity",
          "[optimizer][equality_cardinality]") {
	vector<RelationStats> inputs(2, UnknownEqualityInput(100, {20, 4}));
	for (auto &input : inputs) {
		input.columns[1].total_domain = DistinctCount(4, DistinctCountSource::EXACT);
	}
	auto edges = EqualityChain(2, 2);
	REQUIRE(EqualityRows(inputs, edges) == Approx(125));
	edges.emplace_back(EqualityBinding(0, 0), EqualityBinding(0, 1));
	REQUIRE(EqualityRows(inputs, edges) == Approx(156.25));
	// The new related evidence changes pair imputation, never the raw population's evidence.
	REQUIRE_FALSE(inputs[0].columns[0].total_domain.has_usable_domain);
}

TEST_CASE("Complete informed joint populations do not require known marginal overlap",
          "[optimizer][equality_cardinality]") {
	auto a = UnknownEqualityInput(100, {100, 100});
	auto b = UnknownEqualityInput(100, {10, 10});
	EqualityPopulation(a, {0, 1}, 100, 100);
	EqualityPopulation(b, {0, 1}, 100, 100);
	REQUIRE(EqualityRows({a, b}, EqualityChain(2, 2)) == Approx(100));
	REQUIRE_FALSE(a.columns[0].total_domain.has_usable_domain);
}

TEST_CASE("Equality components count bindings independently of edge shape and numeric NDVs",
          "[optimizer][equality_cardinality]") {
	vector<RelationStats> nested {EqualityInput(2, {2}), EqualityInput(4, {4}), EqualityInput(8, {8})};
	auto chain = EqualityChain(3, 1);
	auto star = vector<pair<ColumnBinding, ColumnBinding>> {{EqualityBinding(0, 0), EqualityBinding(1, 0)},
	                                                        {EqualityBinding(0, 0), EqualityBinding(2, 0)}};
	for (auto edges : {chain, star}) {
		REQUIRE(EqualityRows(nested, edges) == Approx(2));
		edges.emplace_back(EqualityBinding(0, 0), EqualityBinding(2, 0));
		std::reverse(edges.begin(), edges.end());
		for (auto &edge : edges) {
			std::swap(edge.first, edge.second);
		}
		REQUIRE(EqualityRows(nested, edges) == Approx(2));
	}
	for (idx_t relations : {2, 3, 4, 6}) {
		for (double duplicates : {1, 2, 4, 8, 9, 16}) {
			vector<RelationStats> inputs(relations, EqualityInput(16 * duplicates, {4, 4}));
			auto edges = EqualityChain(relations, 2);
			auto expected = 16 * std::pow(duplicates, relations);
			REQUIRE(EqualityRows(inputs, edges) == Approx(expected));
			auto duplicates_edges = vector<pair<ColumnBinding, ColumnBinding>>(edges.begin(), edges.begin() + 2);
			edges.insert(edges.end(), duplicates_edges.begin(), duplicates_edges.end());
			REQUIRE(EqualityRows(inputs, edges) == Approx(expected));
		}
	}
	vector<RelationStats> dense(6, EqualityInput(512, {64, 4}));
	REQUIRE(EqualityRows(dense, EqualityChain(6, 2)) == Approx(16384));
	vector<RelationStats> observed(6, EqualityInput(512, {59, 4}));
	REQUIRE(EqualityRows(observed, EqualityChain(6, 2)) == Approx(std::pow(512, 6) / std::pow(59 * 4, 5)));
}

TEST_CASE("Retained unique populations replace a complete bundle once", "[optimizer][equality_cardinality]") {
	auto edges = EqualityChain(2, 2);
	auto a = EqualityInput(32, {64, 1}, {32, 1});
	EqualityPopulation(a, {0}, 64, 32);
	REQUIRE(EqualityRows({a, a}, edges) == Approx(16));
	auto composite = EqualityInput(32, {8, 8});
	EqualityPopulation(composite, {0, 1}, 64, 32);
	REQUIRE(EqualityRows({composite, composite}, edges) == Approx(16));
	// A late proof cannot invent the original population for an incomplete multi-column key.
	auto late = composite;
	late.joint_stats.clear();
	REQUIRE(EqualityRows({late, late}, edges) == Approx(16));
	auto sparse = EqualityInput(8, {8, 8});
	EqualityPopulation(sparse, {0, 1}, 8, 8);
	REQUIRE(EqualityRows({sparse, sparse}, edges) == Approx(8));
	auto unknown = sparse;
	unknown.unique_keys.clear();
	unknown.joint_stats.clear();
	REQUIRE(EqualityRows({unknown, unknown}, edges) == Approx(1));
	// Fanout removes uniqueness, not the retained tuple population.
	auto fanout = sparse;
	fanout.cardinality = 32;
	fanout.unique_keys.clear();
	REQUIRE(EqualityRows({fanout, fanout}, edges) == Approx(128));
	// A one-sided unique key bounds matches even without evidence on the other side.
	auto weak = EqualityInput(128, {2, 2});
	auto unique = EqualityInput(32, {2, 2});
	unique.AddUniqueKey({0, 1});
	REQUIRE(EqualityRows({unique, weak}, edges) == Approx(128));
}

TEST_CASE("Joint population choice is canonical and exact axes take precedence", "[optimizer][equality_cardinality]") {
	auto input = EqualityInput(32, {32, 32, 1});
	EqualityPopulation(input, {0}, 64, 32);
	EqualityPopulation(input, {1}, 128, 32);
	auto edges = EqualityChain(2, 3);
	REQUIRE(EqualityRows({input, input}, edges) == Approx(16));
	std::reverse(input.unique_keys.begin(), input.unique_keys.end());
	std::reverse(input.joint_stats.begin(), input.joint_stats.end());
	REQUIRE(EqualityRows({input, input}, edges) == Approx(16));
	EqualityPopulation(input, {0, 1, 2}, 128, 32, false);
	REQUIRE(EqualityRows({input, input}, edges) == Approx(8));
}

TEST_CASE("Current bounds preserve original joint populations", "[optimizer][equality_cardinality]") {
	auto filtered = EqualityInput(16, {8, 8}, {2, 8});
	EqualityPopulation(filtered, {0, 1}, 64, 16);
	auto edges = EqualityChain(2, 2);
	REQUIRE(EqualityRows({filtered, filtered}, edges) == Approx(4));
	filtered.columns[0].semi_anti_join_domain_evidence.TightenDomainBound(2);
	REQUIRE(EqualityRows({filtered, filtered}, edges) == Approx(4));
	vector<RelationStats> inputs {filtered, filtered};
	EqualityCardinalityModel first(inputs);
	REQUIRE(inputs[0].joint_stats[0].total_domain.distinct_count == 64);
	EqualityCardinalityModel second(inputs);
	REQUIRE((256 * second.Estimate(second.Components(edges), {0, 1}).selectivity).GetDouble() == Approx(4));
}

TEST_CASE("Overlapping complete bundles fall back as a cluster", "[optimizer][equality_cardinality]") {
	auto input = EqualityInput(64, {4, 4, 4});
	EqualityPopulation(input, {0, 1}, 8, 8, false);
	EqualityPopulation(input, {1, 2}, 8, 8, false);
	auto edges = EqualityChain(2, 3);
	REQUIRE(EqualityRows({input, input}, edges) == Approx(64));
	std::reverse(input.joint_stats.begin(), input.joint_stats.end());
	REQUIRE(EqualityRows({input, input}, edges) == Approx(64));
	EqualityPopulation(input, {0, 1, 2}, 16, 16, false);
	REQUIRE(EqualityRows({input, input}, edges) == Approx(256));
}

TEST_CASE("Alias normalization preserves populations and every support origin", "[optimizer][equality_cardinality]") {
	auto a = EqualityInput(32, {64, 64, 1}, {32, 32, 1});
	a.equivalent_columns = {{0, 1}};
	EqualityPopulation(a, {1}, 64, 32);
	auto x = make_shared_ptr<SupportOrigin>();
	auto y = make_shared_ptr<SupportOrigin>();
	auto xs = make_shared_ptr<vector<SupportIdentity>>();
	auto ys = make_shared_ptr<vector<SupportIdentity>>();
	xs->push_back({x, {0}});
	ys->push_back({y, {0}});
	a.columns[0].support_identities = xs;
	a.columns[1].support_identities = ys;
	auto b = EqualityInput(32, {64, 1}, {32, 1});
	b.columns[0].support_identities = ys;
	EqualityPopulation(b, {0}, 64, 32);
	vector<pair<ColumnBinding, ColumnBinding>> edges {{EqualityBinding(0, 0), EqualityBinding(1, 0)},
	                                                  {EqualityBinding(0, 1), EqualityBinding(1, 0)}};
	REQUIRE(EqualityRows({a, b}, edges) == Approx(32));
	auto c = EqualityInput(16, {64}, {16});
	edges.emplace_back(EqualityBinding(1, 0), EqualityBinding(2, 0));
	REQUIRE(EqualityRows({a, b, c}, edges) == Approx(8));
	// Dropping the original alias retains the alternative key and both scalar origins.
	vector<RelationStats> normalized {a, b};
	EqualityCardinalityModel model(normalized);
	auto projected = RelationStatisticsHelper::ProjectOutputStats(
	    normalized[0], {normalized[0].columns[1].binding, normalized[0].columns[2].binding});
	REQUIRE(projected);
	REQUIRE(projected->HasUniqueKey({0}));
	REQUIRE(projected->columns[0].support_identities->size() == 2);
	REQUIRE(model.Normalize(EqualityBinding(0, 0)) == model.Normalize(EqualityBinding(0, 1)));
	REQUIRE(normalized[0].columns[0].support_identities == normalized[0].columns[1].support_identities);
}

TEST_CASE("Tuple support identity preserves coordinate orientation without scalar reconstruction",
          "[optimizer][equality_cardinality]") {
	auto original = EqualityInput(8, {8, 8});
	EqualityPopulation(original, {0, 1}, 64, 8);
	original.EnsureMaterializedProducerIdentity();
	auto copy = original;
	auto aligned = EqualityChain(2, 2);
	REQUIRE(EqualityRows({original, copy}, aligned) == Approx(8));
	vector<pair<ColumnBinding, ColumnBinding>> swapped {{EqualityBinding(0, 0), EqualityBinding(1, 1)},
	                                                    {EqualityBinding(0, 1), EqualityBinding(1, 0)}};
	REQUIRE(EqualityRows({original, copy}, swapped) == Approx(1));
	copy.tuple_identities.reset();
	// Identical scalar supports do not say which pairs occur.
	REQUIRE(EqualityRows({original, copy}, aligned) == Approx(1));
}

TEST_CASE("Tuple identity does not propagate through an omitted producer bridge", "[optimizer][equality_cardinality]") {
	auto x = make_shared_ptr<SupportOrigin>();
	auto y = make_shared_ptr<SupportOrigin>();
	auto a = EqualityInput(8, {8, 8});
	EqualityPopulation(a, {0, 1}, 64, 8);
	auto b = a;
	auto bridge = a;
	auto add_tuple = [](RelationStats &stats, shared_ptr<const SupportOrigin> origin) {
		auto tuples = make_shared_ptr<vector<RelationTupleIdentity>>();
		if (stats.tuple_identities) {
			*tuples = *stats.tuple_identities;
		}
		tuples->push_back({{0, 1}, {std::move(origin), {0, 1}}});
		stats.tuple_identities = tuples;
	};
	add_tuple(a, x);
	add_tuple(b, y);
	add_tuple(bridge, x);
	add_tuple(bridge, y);
	vector<RelationStats> inputs {a, b, bridge};
	EqualityCardinalityModel model(inputs);
	auto components = model.Components(EqualityChain(2, 2));
	auto estimate = model.Estimate(components, {0, 1});
	REQUIRE((64 * estimate.selectivity).GetDouble() == Approx(1));
	// Distinct direct origins do not establish shared tuple occurrence through another producer.
	REQUIRE(inputs[0].tuple_identities->size() == 1);
	REQUIRE(inputs[1].tuple_identities->size() == 1);
	REQUIRE(inputs[2].tuple_identities->size() == 2);
	vector<RelationStats> exported {inputs[0], inputs[1]};
	EqualityCardinalityModel imported(exported);
	REQUIRE((64 * imported.Estimate(imported.Components(EqualityChain(2, 2)), {0, 1}).selectivity).GetDouble() ==
	        Approx(1));
	REQUIRE((64 * model.Estimate(components, {0, 1}).selectivity).GetDouble() == Approx(1));
}

TEST_CASE("Complete keys bound the multiplicity of a whole equality region", "[optimizer][equality_cardinality]") {
	auto original = EqualityInput(10, {12, 12}, {10, 10});
	EqualityPopulation(original, {0, 1}, 144, 10);
	original.EnsureMaterializedProducerIdentity();
	vector<RelationStats> inputs(3, original);
	for (idx_t r = 0; r < inputs.size(); r++) {
		auto rows = double(10 + r);
		inputs[r].cardinality = rows;
		for (auto &column : inputs[r].columns) {
			column.current_domain = DistinctCount(rows, DistinctCountSource::HLL);
		}
		inputs[r].joint_stats[0].current_domain = DistinctCount(rows, DistinctCountSource::HLL);
	}
	auto edges = EqualityChain(3, 2);
	for (bool fanout : {false, true}) {
		auto current = inputs;
		if (fanout) {
			current[2].unique_keys.clear();
			current[2].cardinality = 120;
		}
		vector<idx_t> order {0, 1, 2};
		do {
			CAPTURE(fanout, order);
			vector<RelationStats> reordered {current[order[0]], current[order[1]], current[order[2]]};
			EqualityCardinalityModel model(reordered);
			auto components = model.Components(edges);
			for (idx_t r = 0; r < order.size(); r++) {
				REQUIRE(model.PreservesMultiplicity(r, {0, 1, 2}, components) == (!fanout || order[r] == 2));
			}
			// The raw identity factor gives 13.2 (132 with fanout). Complete keys bound the
			// former by the smallest input, while the nonunique input can still fan out.
			REQUIRE(EqualityRows(reordered, edges) == Approx(fanout ? 120 : 10));
			auto redundant = edges;
			redundant.emplace_back(EqualityBinding(0, 0), EqualityBinding(2, 0));
			REQUIRE(EqualityRows(reordered, redundant) == Approx(fanout ? 120 : 10));
		} while (std::next_permutation(order.begin(), order.end()));
	}
}

TEST_CASE("Direct tuple identity does not expand coordinate permutations", "[optimizer][equality_cardinality]") {
	auto input = EqualityInput(128, {4, 4, 4, 4, 4, 4, 4});
	vector<idx_t> axes {0, 1, 2, 3, 4, 5, 6};
	EqualityPopulation(input, axes, 256, 128);
	auto origin = make_shared_ptr<SupportOrigin>();
	auto tuples = make_shared_ptr<vector<RelationTupleIdentity>>();
	tuples->push_back({axes, {origin, axes}});
	for (idx_t i = 1; i < axes.size(); i++) {
		auto coordinates = axes;
		std::swap(coordinates[i - 1], coordinates[i]);
		tuples->push_back({axes, {origin, std::move(coordinates)}});
	}
	input.tuple_identities = tuples;
	vector<RelationStats> inputs {input, input};
	EqualityCardinalityModel model(inputs);
	auto edges = EqualityChain(2, axes.size());
	auto first = model.Estimate(model.Components(edges), {0, 1});
	// The direct aligned identity is available without constructing its permutation closure.
	REQUIRE((128 * 128 * first.selectivity).GetDouble() == Approx(128));
	std::reverse(edges.begin(), edges.end());
	for (auto &edge : edges) {
		std::swap(edge.first, edge.second);
	}
	REQUIRE((128 * 128 * model.Estimate(model.Components(edges), {1, 0}).selectivity).GetDouble() == Approx(128));
	REQUIRE(inputs[0].HasUniqueKey(axes));
	REQUIRE(inputs[0].joint_stats[0].total_domain.distinct_count == 256);
	REQUIRE(inputs[0].joint_stats[0].current_domain.distinct_count == 128);
}

TEST_CASE("Equality factors retain positive estimates below double range",
          "[optimizer][equality_cardinality][cardinality_arithmetic]") {
	vector<RelationStats> inputs(32, EqualityInput(2, {1e18}, {2}));
	for (auto &input : inputs) {
		input.AddUniqueKey({0});
	}
	EqualityCardinalityModel model(inputs);
	vector<idx_t> relations;
	for (idx_t i = 0; i < inputs.size(); i++) {
		relations.push_back(i);
	}
	auto estimate = model.Estimate(model.Components(EqualityChain(32, 1)), relations);
	auto rows = CardinalityValue(4294967296.0) * estimate.selectivity;
	REQUIRE_FALSE(rows.IsZero());
	REQUIRE(rows.GetDouble() == 0);
	auto expected = CardinalityValue(4294967296.0);
	for (idx_t i = 1; i < inputs.size(); i++) {
		expected = expected / CardinalityValue(1e18);
	}
	REQUIRE((rows / expected).GetDouble() == Approx(1));
}

TEST_CASE("Projection separates fanout from retained tuple populations", "[optimizer][equality_cardinality]") {
	auto source = EqualityInput(8, {8});
	EqualityPopulation(source, {0}, 8, 8);
	source.EnsureMaterializedProducerIdentity();
	auto many = EqualityInput(16, {8});
	vector<RelationStats> inputs {source, many};
	EqualityCardinalityModel model(inputs);
	auto components = model.Components(EqualityChain(2, 1));
	REQUIRE_FALSE(model.PreservesMultiplicity(0, {0, 1}, components));
	auto output = EqualityInput(16, {8});
	output.columns[0].binding = EqualityBinding(1, 0);
	model.ProjectEvidence(output, components, {{}, {}});
	REQUIRE(output.unique_keys.empty());
	REQUIRE(output.joint_stats.size() == 1);
	REQUIRE(output.joint_stats[0].columns == vector<idx_t> {0});
	REQUIRE(output.joint_stats[0].total_domain.distinct_count == 8);
	REQUIRE(output.joint_stats[0].current_domain.distinct_count == 8);
	REQUIRE_FALSE(output.tuple_identities);

	many.AddUniqueKey({0});
	vector<RelationStats> one_to_one {source, many};
	EqualityCardinalityModel lookup(one_to_one);
	auto lookup_components = lookup.Components(EqualityChain(2, 1));
	REQUIRE(lookup.PreservesMultiplicity(0, {0, 1}, lookup_components));
	auto unique_output = EqualityInput(8, {8});
	unique_output.columns[0].binding = EqualityBinding(1, 0);
	lookup.ProjectEvidence(unique_output, lookup_components, {{EqualitySelection::MATCHED, {}, true}, {}});
	REQUIRE(unique_output.HasUniqueKey({0}));
}

TEST_CASE("Joint projection exports matched support independently of matching fanout",
          "[optimizer][equality_cardinality]") {
	auto source = EqualityInput(512, {16, 16});
	EqualityPopulation(source, {0, 1}, 256, 200, false);
	vector<RelationStats> inputs {source, source};
	EqualityCardinalityModel model(inputs);
	auto components = model.Components(EqualityChain(2, 2));
	auto estimate = model.Estimate(components, {0, 1});
	REQUIRE((512 * 512 * estimate.selectivity).GetDouble() == Approx(1024));
	auto output = EqualityInput(1024, {16, 16});
	model.ProjectEvidence(output, components, {{}, {}});
	REQUIRE(output.unique_keys.empty());
	REQUIRE(output.joint_stats.size() == 2);
	for (auto &joint : output.joint_stats) {
		REQUIRE(joint.total_domain.distinct_count == 256);
		REQUIRE(joint.current_domain.distinct_count.GetDouble() == Approx(156.25));
	}
	vector<RelationStats> next {output, source};
	EqualityCardinalityModel downstream(next);
	auto support = downstream.JointSupport(0, {0, 1});
	REQUIRE(support);
	REQUIRE(support->current.GetDouble() == Approx(156.25));
	REQUIRE(support->total == 256);
}

TEST_CASE("Joint projection distinguishes key constraints from payload sampling", "[optimizer][equality_cardinality]") {
	for (bool key_constraint : {false, true}) {
		auto source = EqualityInput(8, {2, 2, 2});
		EqualityPopulation(source, {0, 1}, 4, 4, false);
		auto lookup = EqualityInput(2, {1});
		vector<RelationStats> inputs {source, lookup};
		EqualityCardinalityModel model(inputs);
		auto components = model.Components({{EqualityBinding(0, key_constraint ? 0 : 2), EqualityBinding(1, 0)}});
		auto output = EqualityInput(8, {key_constraint ? 1.0 : 2.0, 2});
		model.ProjectEvidence(output, components, {{}, {}});
		REQUIRE(output.joint_stats.size() == 1);
		auto &joint = output.joint_stats[0];
		// Restricting a key axis keeps two whole tuples. Independent row sampling keeps 3 of 4 duplicated tuples.
		REQUIRE(joint.current_domain.distinct_count.GetDouble() == Approx(key_constraint ? 2 : 3));
		REQUIRE(joint.total_domain.distinct_count.GetDouble() == Approx(key_constraint ? 2 : 4));
	}
}

TEST_CASE("Joint projection declines crossing conditional tuple distributions", "[optimizer][equality_cardinality]") {
	auto source = EqualityInput(8, {4, 4, 4});
	EqualityPopulation(source, {0, 1}, 8, 8, false);
	EqualityPopulation(source, {1, 2}, 8, 8, false);
	auto other = EqualityInput(8, {4, 4, 4});
	EqualityPopulation(other, {1, 2}, 8, 8, false);
	vector<RelationStats> inputs {source, other};
	EqualityCardinalityModel model(inputs);
	auto components = model.Components(
	    {{EqualityBinding(0, 1), EqualityBinding(1, 1)}, {EqualityBinding(0, 2), EqualityBinding(1, 2)}});
	auto output = EqualityInput(8, {4, 4, 4});
	model.ProjectEvidence(output, components, {{}, {}});
	REQUIRE(output.joint_stats.size() == 2);
	for (auto &joint : output.joint_stats) {
		REQUIRE(joint.columns == vector<idx_t> {1, 2});
	}
}

TEST_CASE("Projected single-key populations follow the common equality population",
          "[optimizer][equality_cardinality]") {
	auto source = EqualityInput(64, {64});
	EqualityPopulation(source, {0}, 64, 64);
	auto other = EqualityInput(64, {32});
	vector<RelationStats> inputs {source, other};
	EqualityCardinalityModel model(inputs);
	auto components = model.Components(EqualityChain(2, 1));
	auto output = EqualityInput(64, {32});
	model.ProjectEvidence(output, components, {{}, {}});
	vector<RelationStats> normalized {output};
	EqualityCardinalityModel next(normalized);
	REQUIRE(normalized[0].columns[0].total_domain.distinct_count == 32);
	REQUIRE(normalized[0].columns[0].current_domain.distinct_count == 32);
	REQUIRE(normalized[0].joint_stats[0].total_domain.distinct_count == 32);
}

TEST_CASE("Fanout retains source dependencies without determining introduced payload",
          "[optimizer][equality_cardinality]") {
	auto source = EqualityInput(64, {64, 4});
	EqualityPopulation(source, {0}, 64, 64);
	auto copies = EqualityInput(2, {2});
	REQUIRE(EqualityRows({source, copies, source}, {{EqualityBinding(0, 0), EqualityBinding(2, 0)},
	                                                {EqualityBinding(0, 1), EqualityBinding(2, 1)}}) == Approx(128));
	vector<RelationStats> inputs {source, copies};
	EqualityCardinalityModel model(inputs);
	auto output = EqualityInput(128, {64, 4, 2});
	output.columns[2].binding = EqualityBinding(1, 0);
	model.ProjectEvidence(output, {}, {{}, {}});
	REQUIRE(output.unique_keys.empty());
	REQUIRE(output.joint_stats.size() == 1);
	REQUIRE(output.joint_stats[0].dependent_columns == vector<idx_t> {0, 1});
	// The dependent (k,p) bundle must still be selected alongside independent introduced payload x.
	REQUIRE(EqualityRows({source, copies, source, copies}, {{EqualityBinding(0, 0), EqualityBinding(2, 0)},
	                                                        {EqualityBinding(0, 1), EqualityBinding(2, 1)},
	                                                        {EqualityBinding(1, 0), EqualityBinding(3, 0)}}) ==
	        Approx(128));
	auto all_keys = EqualityChain(2, 3);
	REQUIRE(EqualityRows({output, output}, all_keys) == Approx(128));
	std::reverse(all_keys.begin(), all_keys.end());
	REQUIRE(EqualityRows({output, output}, all_keys) == Approx(128));
	for (bool reorder_aliases : {false, true}) {
		auto projected = output;
		if (reorder_aliases) {
			// Rebind the semantic output before applying the ordinary projection map.
			for (idx_t c = 0; c < projected.columns.size(); c++) {
				projected.columns[c].binding = EqualityBinding(0, c);
			}
			auto projection = RelationStatisticsHelper::ProjectOutputStats(
			    projected, vector<ColumnBinding> {EqualityBinding(0, 1), EqualityBinding(0, 0), EqualityBinding(0, 1)});
			REQUIRE(projection);
			projected = *projection;
		}
		vector<RelationStats> downstream {projected, source};
		EqualityCardinalityModel next(downstream);
		auto key = reorder_aliases ? 1 : 0;
		auto payload = reorder_aliases ? 2 : 1;
		auto support = next.JointSupport(0, {idx_t(key), idx_t(payload)});
		REQUIRE(support);
		REQUIRE(support->total == 64);
		REQUIRE(support->current == 64);
		auto components = next.Components(
		    {{EqualityBinding(0, key), EqualityBinding(1, 0)}, {EqualityBinding(0, payload), EqualityBinding(1, 1)}});
		REQUIRE((128 * 64 * next.Estimate(components, {0, 1}).selectivity).GetDouble() == Approx(128));
		if (!reorder_aliases) {
			REQUIRE_FALSE(next.JointSupport(0, {0, 2}));
		}
	}
	vector<RelationStats> repeated_inputs {output, copies};
	EqualityCardinalityModel repeated(repeated_inputs);
	auto repeated_output = EqualityInput(256, {64, 4, 2, 2});
	repeated_output.columns[3].binding = EqualityBinding(1, 0);
	repeated.ProjectEvidence(repeated_output, {}, {{}, {}});
	vector<RelationStats> repeated_stats {repeated_output};
	EqualityCardinalityModel normalized(repeated_stats);
	REQUIRE(normalized.JointSupport(0, {0, 1}));
	REQUIRE(normalized.JointSupport(0, {0, 1})->current == 64);
	REQUIRE_FALSE(normalized.JointSupport(0, {0, 2}));
	REQUIRE_FALSE(normalized.JointSupport(0, {0, 3}));

	auto nullable_output = EqualityInput(128, {64, 4});
	model.ProjectEvidence(nullable_output, {}, {{EqualitySelection::MATCHED, {}, false, false, true}, {}});
	vector<RelationStats> nullable_stats {nullable_output};
	EqualityCardinalityModel nullable(nullable_stats);
	REQUIRE_FALSE(nullable.JointSupport(0, {0, 1}));
}

TEST_CASE("Dependent payload constraints remove whole source keys after fanout", "[optimizer][equality_cardinality]") {
	auto source = EqualityInput(64, {8, 8, 4});
	EqualityPopulation(source, {0, 1}, 64, 64);
	vector<RelationStats> inputs {source, EqualityInput(2, {2})};
	EqualityCardinalityModel fanout(inputs);
	auto duplicated = EqualityInput(128, {8, 8, 4});
	fanout.ProjectEvidence(duplicated, {}, {{}, {}});
	for (bool complete_bundle : {false, true}) {
		auto lookup = complete_bundle ? EqualityInput(32, {8, 8, 4}) : EqualityInput(2, {4}, {2});
		if (complete_bundle) {
			EqualityPopulation(lookup, {0, 1}, 64, 32);
		}
		vector<RelationStats> joined_inputs {duplicated, lookup};
		EqualityCardinalityModel model(joined_inputs);
		auto components = model.Components(complete_bundle ? EqualityChain(2, 3)
		                                                   : vector<pair<ColumnBinding, ColumnBinding>> {
		                                                         {EqualityBinding(0, 2), EqualityBinding(1, 0)}});
		auto output = EqualityInput(64, {8, 8, 4}, {8, 8, complete_bundle ? 4.0 : 2.0});
		model.ProjectEvidence(output, components, {{}, {}});
		vector<RelationStats> downstream {output};
		EqualityCardinalityModel next(downstream);
		auto support = next.JointSupport(0, {0, 1, 2});
		REQUIRE(support);
		REQUIRE(support->total == 64);
		// Filtering the determined payload keeps 32 complete key values, not 48 sampled duplicate values.
		REQUIRE(support->current == 32);
	}
}

TEST_CASE("Empty equality projections clear column evidence", "[optimizer][equality_cardinality][empty_projection]") {
	auto with_proof = []() {
		auto source = EqualityInput(8, {8, 8});
		auto tuples = make_shared_ptr<vector<RelationTupleIdentity>>();
		tuples->push_back({{0, 1}, {make_shared_ptr<SupportOrigin>(), {0, 1}}});
		tuples->push_back({{0, 1}, {make_shared_ptr<SupportOrigin>(), {0, 1}}});
		source.tuple_identities = tuples;
		vector<RelationStats> inputs {source};
		EqualityCardinalityModel model(inputs);
		return inputs[0];
	};
	auto stale = with_proof();
	REQUIRE(stale.tuple_identities);
	EqualityPopulation(stale, {0, 1}, 8, 8);
	stale.joint_stats.back().dependent_columns = {0, 1};
	stale.equivalent_columns = {{0, 1}};
	stale.value_equivalent_columns = {{0, 1}};
	// Reusing an output after dropping its columns must discard every old column-index proof.
	stale.columns.clear();
	stale.row_retention = 0.375;
	stale.table_name = Identifier("empty projection");
	struct RowCase {
		double point;
		optional_idx maximum;
		double cost;
	};
	vector<RowCase> row_cases {{0.25, optional_idx(), 1},
	                           {0, optional_idx(), 1},
	                           {10, optional_idx(2), 2},
	                           {10, optional_idx(0), 0},
	                           {0.25, optional_idx(1), 1}};
	for (bool inherit_proof : {false, true}) {
		vector<RelationStats> inputs {EqualityInput(8, {8, 8}), EqualityInput(4, {4, 4})};
		for (auto &input : inputs) {
			EqualityPopulation(input, {0, 1}, 8, 4, false);
		}
		EqualityCardinalityModel model(inputs);
		auto components = model.Components(EqualityChain(2, 2));
		for (auto selection : {EqualitySelection::ALL, EqualitySelection::MATCHED, EqualitySelection::UNMATCHED}) {
			vector<EqualityProjection> projections(2);
			for (auto &projection : projections) {
				projection.selection = selection;
				projection.matched_rows = CardinalityValue(2);
				projection.preserve_keys = true;
				projection.preserve_identity = true;
			}
			projections[1].null_extended = true;
			for (auto &row_case : row_cases) {
				auto output = stale;
				output.cardinality = row_case.point;
				output.max_cardinality = row_case.maximum;
				output.stats_initialized = inherit_proof;
				model.Project(output, components, projections, components);
				REQUIRE(output.columns.empty());
				REQUIRE(output.unique_keys.empty());
				REQUIRE(output.equivalent_columns.empty());
				REQUIRE(output.value_equivalent_columns.empty());
				REQUIRE(output.joint_stats.empty());
				REQUIRE_FALSE(output.tuple_identities);
				REQUIRE(output.cardinality.GetDouble() == Approx(row_case.point));
				REQUIRE(output.GetCardinalityForCost().GetDouble() == Approx(row_case.cost));
				REQUIRE(output.max_cardinality.IsValid() == row_case.maximum.IsValid());
				if (row_case.maximum.IsValid()) {
					REQUIRE(output.max_cardinality.GetIndex() == row_case.maximum.GetIndex());
				}
				REQUIRE(output.IsEmpty() == (row_case.cost == 0));
				REQUIRE(output.row_retention.GetDouble() == Approx(0.375));
				REQUIRE(output.stats_initialized == inherit_proof);
				REQUIRE(output.table_name == Identifier("empty projection"));
			}
		}
	}
}

TEST_CASE("Local producer aliases do not rewrite an untouched reference", "[optimizer][equality_cardinality]") {
	auto producer = EqualityInput(100, {10, 10});
	producer.EnsureMaterializedProducerIdentity();
	auto filtered = producer;
	filtered.equivalent_columns = {{0, 1}};
	filtered.cardinality = 10;
	vector<RelationStats> inputs {filtered, producer};
	EqualityCardinalityModel model(inputs);
	REQUIRE(model.Normalize(EqualityBinding(0, 0)) == model.Normalize(EqualityBinding(0, 1)));
	REQUIRE(model.Normalize(EqualityBinding(1, 0)) != model.Normalize(EqualityBinding(1, 1)));
	REQUIRE(inputs[1].equivalent_columns.empty());
	REQUIRE(producer.equivalent_columns.empty());
}
