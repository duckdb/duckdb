#include "catch.hpp"
#include "duckdb/common/types/geometry.hpp"

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

using namespace duckdb; // NOLINT

// Ground-truth longitude membership per the geospatial bounding box spec:
//   x_min <= x_max  ->  x matches iff x_min <= x <= x_max
//   x_min >  x_max  ->  x matches iff x >= x_min OR x <= x_max   (antimeridian wraparound)
// plus circle equivalence: +180 and -180 are the same physical longitude.
static bool LonExtentContains(double lo, double hi, double p) {
	auto contains = [](double l, double h, double x) {
		return l <= h ? (l <= x && x <= h) : (x >= l || x <= h);
	};
	if (contains(lo, hi, p)) {
		return true;
	}
	if (p == 180.0) {
		return contains(lo, hi, -180.0);
	}
	if (p == -180.0) {
		return contains(lo, hi, 180.0);
	}
	return false;
}

static GeometryExtent MakeLonArc(double lo, double hi) {
	auto e = GeometryExtent::Empty();
	e.x_min = lo;
	e.x_max = hi;
	e.y_min = -90;
	e.y_max = 90;
	return e;
}

static void ExtendLon(GeometryExtent &e, double x) {
	VertexXY v {x, 0.0};
	e.Extend(v, true);
}

TEST_CASE("Geodetic extent: point extension covers every inserted longitude exactly", "[geometry]") {
	// Random point clouds, inserted one vertex at a time like statistics collection does.
	// Every inserted coordinate must test as contained in the final extent: any exclusion means a
	// stored row could be pruned away when queried with its own coordinates.
	std::mt19937_64 gen(42);
	std::uniform_real_distribution<double> dist(-180.0, 180.0);
	for (int iter = 0; iter < 20000; iter++) {
		const int n = 1 + int(gen() % 8);
		std::vector<double> xs;
		auto e = GeometryExtent::Empty();
		for (int i = 0; i < n; i++) {
			const int roll = int(gen() % 10);
			const double x = roll == 0 ? 180.0 : (roll == 1 ? -180.0 : dist(gen));
			xs.push_back(x);
			ExtendLon(e, x);
		}
		for (auto x : xs) {
			// The stored extent covers the coordinate...
			REQUIRE(LonExtentContains(e.x_min, e.x_max, x));
			// ...and the pruning predicate agrees when queried with a point extent at the coordinate
			auto q = GeometryExtent::Empty();
			ExtendLon(q, x);
			REQUIRE(q.IntersectsXY(e, true));
		}
		// Endpoints are actual coordinates (or the canonical full circle)
		const bool full = e.x_min == -180.0 && e.x_max == 180.0;
		if (!full) {
			REQUIRE(std::find(xs.begin(), xs.end(), e.x_min) != xs.end());
			REQUIRE(std::find(xs.begin(), xs.end(), e.x_max) != xs.end());
		}
	}
}

TEST_CASE("Geodetic extent: merge covers both inputs", "[geometry]") {
	// Split a point cloud over two extents (like separate row groups), merge, verify coverage.
	std::mt19937_64 gen(1337);
	std::uniform_real_distribution<double> dist(-180.0, 180.0);
	for (int iter = 0; iter < 20000; iter++) {
		auto e1 = GeometryExtent::Empty();
		auto e2 = GeometryExtent::Empty();
		std::vector<double> xs;
		const int n = 2 + int(gen() % 8);
		for (int i = 0; i < n; i++) {
			const int roll = int(gen() % 10);
			const double x = roll == 0 ? 180.0 : (roll == 1 ? -180.0 : dist(gen));
			xs.push_back(x);
			ExtendLon(i % 2 ? e1 : e2, x);
		}
		e1.Merge(e2, true);
		for (auto x : xs) {
			REQUIRE(LonExtentContains(e1.x_min, e1.x_max, x));
		}
	}
}

TEST_CASE("Geodetic extent: intersection is exact on integer arcs", "[geometry]") {
	// All pairs of arcs over a 15-degree grid (plus seam-adjacent values): IntersectsXY must have
	// no false negatives (unsound pruning) and no false positives beyond the epsilon guard band.
	std::vector<double> grid;
	for (int d = -180; d <= 180; d += 15) {
		grid.push_back(d);
	}
	grid.push_back(-179);
	grid.push_back(-1);
	grid.push_back(1);
	grid.push_back(179);

	std::vector<double> pts;
	for (int d = -180; d <= 180; d++) {
		pts.push_back(d);
	}

	for (auto alo : grid) {
		for (auto ahi : grid) {
			for (auto blo : grid) {
				for (auto bhi : grid) {
					auto a = MakeLonArc(alo, ahi);
					auto b = MakeLonArc(blo, bhi);
					bool oracle = false;
					for (auto p : pts) {
						if (LonExtentContains(alo, ahi, p) && LonExtentContains(blo, bhi, p)) {
							oracle = true;
							break;
						}
					}
					// Arc endpoints all lie on the integer grid, so the sweep decides intersection
					// exactly, and the epsilon guard band cannot flip any of these cases.
					if (a.IntersectsXY(b, true) != oracle) {
						INFO("A=[" << alo << "," << ahi << "] B=[" << blo << "," << bhi << "]");
						REQUIRE(a.IntersectsXY(b, true) == oracle);
					}
				}
			}
		}
	}
}

TEST_CASE("Geodetic extent: containment has no false positives", "[geometry]") {
	// A false "contains" can turn a filter into FILTER_ALWAYS_TRUE, returning wrong rows.
	std::vector<double> grid;
	for (int d = -180; d <= 180; d += 15) {
		grid.push_back(d);
	}
	std::vector<double> pts;
	for (int d = -180; d <= 180; d++) {
		pts.push_back(d);
	}
	for (auto alo : grid) {
		for (auto ahi : grid) {
			for (auto blo : grid) {
				for (auto bhi : grid) {
					auto a = MakeLonArc(alo, ahi);
					auto b = MakeLonArc(blo, bhi);
					if (!a.ContainsXY(b, true)) {
						continue;
					}
					for (auto p : pts) {
						if (LonExtentContains(blo, bhi, p)) {
							INFO("A=[" << alo << "," << ahi << "] B=[" << blo << "," << bhi << "] p=" << p);
							REQUIRE(LonExtentContains(alo, ahi, p));
						}
					}
				}
			}
		}
	}
}

TEST_CASE("Geodetic extent: antimeridian seam and degenerate cases", "[geometry]") {
	// +180 and -180 are the same longitude
	auto e180 = GeometryExtent::Empty();
	ExtendLon(e180, 180.0);
	auto eneg180 = GeometryExtent::Empty();
	ExtendLon(eneg180, -180.0);
	REQUIRE(e180.IntersectsXY(eneg180, true));
	REQUIRE(eneg180.IntersectsXY(e180, true));

	// Merging the two seam representations yields a point extent, not the full circle
	auto seam = GeometryExtent::Empty();
	ExtendLon(seam, -180.0);
	ExtendLon(seam, 180.0);
	REQUIRE(seam.x_min == seam.x_max);

	// A wrapped arc contains the seam but not the far side
	auto wrapped = GeometryExtent::Empty();
	ExtendLon(wrapped, 170.0);
	ExtendLon(wrapped, -170.0);
	REQUIRE(wrapped.x_min == 170.0);
	REQUIRE(wrapped.x_max == -170.0);
	auto probe = [](double x) {
		auto q = GeometryExtent::Empty();
		ExtendLon(q, x);
		return q;
	};
	REQUIRE(wrapped.IntersectsXY(probe(180.0), true));
	REQUIRE(wrapped.IntersectsXY(probe(-180.0), true));
	REQUIRE(wrapped.IntersectsXY(probe(175.0), true));
	REQUIRE(wrapped.IntersectsXY(probe(-175.0), true));
	REQUIRE(!wrapped.IntersectsXY(probe(0.0), true));
	REQUIRE(!wrapped.IntersectsXY(probe(90.0), true));

	// Two half-circle arcs merge into the canonical full circle
	auto h1 = MakeLonArc(-90, 90);
	auto h2 = MakeLonArc(90, -90);
	h1.Merge(h2, true);
	REQUIRE(h1.x_min == -180.0);
	REQUIRE(h1.x_max == 180.0);
	// ...which intersects and contains everything
	REQUIRE(h1.IntersectsXY(probe(123.0), true));
	REQUIRE(h1.ContainsXY(MakeLonArc(170, -170), true));
}

TEST_CASE("Extent: NaN ordinates degrade the axis to unknown instead of losing coverage", "[geometry]") {
	// A NaN ordinate must not shrink-wrap the extent around the remaining values: min/max comparisons
	// silently drop NaN, which used to let a NaN row erase previously merged coverage.
	for (bool geodetic : {false, true}) {
		auto e = GeometryExtent::Empty();
		VertexXY v1 {0.0, 0.0};
		e.Extend(v1, geodetic);
		VertexXY v2 {std::numeric_limits<double>::quiet_NaN(), 1.0};
		e.Extend(v2, geodetic);
		VertexXY v3 {5.0, 2.0};
		e.Extend(v3, geodetic);
		// X axis is unknown (covers everything), Y axis is still exact
		REQUIRE(!e.HasX());
		REQUIRE(e.y_min == 0.0);
		REQUIRE(e.y_max == 2.0);
		// Unknown intersects everything: no row can be lost
		REQUIRE(e.IntersectsXY(MakeLonArc(0, 1), geodetic));

		// Merging a NaN extent into an accumulated extent must not drop the accumulated range
		auto acc = GeometryExtent::Empty();
		acc.Merge(MakeLonArc(0, 10), geodetic);
		auto poisoned = GeometryExtent::Empty();
		poisoned.x_min = std::numeric_limits<double>::quiet_NaN();
		poisoned.x_max = std::numeric_limits<double>::quiet_NaN();
		acc.Merge(poisoned, geodetic);
		acc.Merge(MakeLonArc(50, 60), geodetic);
		REQUIRE(acc.IntersectsXY(MakeLonArc(5, 5), geodetic));
	}
}

TEST_CASE("Geodetic extent: known ULP regression cases", "[geometry]") {
	// These coordinate sets previously produced extents that excluded their own inputs, because the
	// merged endpoint was reconstructed as start + width instead of reusing the exact coordinate.
	const std::vector<std::vector<double>> cases = {
	    {-128.20366710520983, 22.404613198913019},
	    {-137.73085882757348, 141.08874361649151},
	    {11.81190708480235, -71.706062737400671},
	    {180.0, -34.436061507319117, -21.644696034872688, -33.974438731270403},
	    {180.0, -27.63411843978664, -180.0, -107.54340180174924},
	    {177.3248255377766, 40.546503500952689, -180.0, -10.573325779081593, -25.899807397694246, -6.2727017577805677,
	     180.0},
	};
	for (auto &coords : cases) {
		auto e = GeometryExtent::Empty();
		for (auto x : coords) {
			ExtendLon(e, x);
		}
		for (auto x : coords) {
			REQUIRE(LonExtentContains(e.x_min, e.x_max, x));
			auto q = GeometryExtent::Empty();
			ExtendLon(q, x);
			REQUIRE(q.IntersectsXY(e, true));
		}
	}
}
