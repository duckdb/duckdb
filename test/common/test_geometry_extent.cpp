#include "catch.hpp"
#include "duckdb/common/types/geometry.hpp"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <random>
#include <utility>
#include <vector>

static constexpr double TEST_PI = 3.14159265358979323846;

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

// ---------------------------------------------------------------------------------------------------------------------
// Edge-aware (coverage) extents: helpers to build WKB and sample geodesics
// ---------------------------------------------------------------------------------------------------------------------

static void AppendDouble(std::string &buf, double v) {
	char raw[sizeof(double)];
	memcpy(raw, &v, sizeof(double));
	buf.append(raw, sizeof(double));
}

static void AppendUint32(std::string &buf, uint32_t v) {
	char raw[sizeof(uint32_t)];
	memcpy(raw, &v, sizeof(uint32_t));
	buf.append(raw, sizeof(uint32_t));
}

static std::string MakeLineStringWKB(const std::vector<std::pair<double, double>> &pts) {
	std::string buf;
	buf.push_back(1); // little-endian
	AppendUint32(buf, 2);
	AppendUint32(buf, uint32_t(pts.size()));
	for (auto &p : pts) {
		AppendDouble(buf, p.first);
		AppendDouble(buf, p.second);
	}
	return buf;
}

static std::string MakePolygonWKB(const std::vector<std::pair<double, double>> &ring) {
	std::string buf;
	buf.push_back(1);
	AppendUint32(buf, 3);
	AppendUint32(buf, 1);
	AppendUint32(buf, uint32_t(ring.size()));
	for (auto &p : ring) {
		AppendDouble(buf, p.first);
		AppendDouble(buf, p.second);
	}
	return buf;
}

struct Vec3 {
	double x, y, z;
};

static Vec3 ToVec3(double lon_deg, double lat_deg) {
	const double lam = lon_deg * TEST_PI / 180.0;
	const double phi = lat_deg * TEST_PI / 180.0;
	return {cos(phi) * cos(lam), cos(phi) * sin(lam), sin(phi)};
}

static std::pair<double, double> ToLonLat(const Vec3 &v) {
	const double norm = sqrt(v.x * v.x + v.y * v.y + v.z * v.z);
	return {atan2(v.y, v.x) * 180.0 / TEST_PI, asin(v.z / norm) * 180.0 / TEST_PI};
}

// Spherical linear interpolation along the geodesic between a and b
static Vec3 Slerp(const Vec3 &a, const Vec3 &b, double t) {
	double dot = a.x * b.x + a.y * b.y + a.z * b.z;
	dot = std::max(-1.0, std::min(1.0, dot));
	const double theta = acos(dot);
	if (theta < 1e-12) {
		return a;
	}
	const double s = sin(theta);
	const double wa = sin((1.0 - t) * theta) / s;
	const double wb = sin(t * theta) / s;
	return {wa * a.x + wb * b.x, wa * a.y + wb * b.y, wa * a.z + wb * b.z};
}

TEST_CASE("Geodetic extent: covers sampled points along geodesic edges", "[geometry]") {
	// Random geodesic edges: every point sampled along the great-circle arc must lie within the
	// computed extent (the edge-aware "bulge" handling). This is the coverage contract of the
	// geodetic extents.
	std::mt19937_64 gen(2024);
	std::normal_distribution<double> gauss(0.0, 1.0);
	auto random_point = [&]() {
		Vec3 v {gauss(gen), gauss(gen), gauss(gen)};
		const double norm = sqrt(v.x * v.x + v.y * v.y + v.z * v.z);
		return Vec3 {v.x / norm, v.y / norm, v.z / norm};
	};
	for (int iter = 0; iter < 20000; iter++) {
		const auto a = random_point();
		const auto b = random_point();
		const double dot = a.x * b.x + a.y * b.y + a.z * b.z;
		if (dot < -0.99) {
			continue; // skip (near-)antipodal pairs: those are covered by the full-globe fallback
		}
		const auto ll_a = ToLonLat(a);
		const auto ll_b = ToLonLat(b);
		const auto wkb = MakeLineStringWKB({ll_a, ll_b});

		auto extent = GeometryExtent::Empty();
		Geometry::GetExtent(string_t(wkb.data(), uint32_t(wkb.size())), extent, true);

		for (int s = 0; s <= 32; s++) {
			const auto p = ToLonLat(Slerp(a, b, double(s) / 32.0));
			INFO("edge (" << ll_a.first << " " << ll_a.second << ") -> (" << ll_b.first << " " << ll_b.second
			              << ") sample (" << p.first << " " << p.second << ")");
			// Latitude must be within the (bulge-aware) Y range; tiny slack for the sampling math itself
			REQUIRE(p.second >= extent.y_min - 1e-9);
			REQUIRE(p.second <= extent.y_max + 1e-9);
			// Longitude must be within the arc
			REQUIRE(LonExtentContains(extent.x_min, extent.x_max, p.first));
		}
	}
}

TEST_CASE("Geodetic extent: polygon interiors follow the interior-on-the-left rule", "[geometry]") {
	auto get_extent = [](const std::string &wkb) {
		auto extent = GeometryExtent::Empty();
		Geometry::GetExtent(string_t(wkb.data(), uint32_t(wkb.size())), extent, true);
		return extent;
	};

	// A shell winding east around the globe at 80N: interior is the northern cap
	auto north = get_extent(MakePolygonWKB({{0, 80}, {90, 80}, {180, 80}, {-90, 80}, {0, 80}}));
	REQUIRE(north.x_min == -180.0);
	REQUIRE(north.x_max == 180.0);
	REQUIRE(north.y_max == 90.0);
	REQUIRE(north.y_min == 80.0);

	// The same ring wound west: interior is everything EXCEPT the northern cap. The boundary edges
	// still bulge to ~82.9N, but the north pole itself is not covered.
	auto north_cw = get_extent(MakePolygonWKB({{0, 80}, {-90, 80}, {180, 80}, {90, 80}, {0, 80}}));
	REQUIRE(north_cw.x_min == -180.0);
	REQUIRE(north_cw.x_max == 180.0);
	REQUIRE(north_cw.y_min == -90.0);
	REQUIRE(north_cw.y_max < 84.0);

	// Antarctica-style: a west-wound shell at 80S encloses the south pole, the north stays bounded
	auto south = get_extent(MakePolygonWKB({{0, -80}, {-90, -80}, {180, -80}, {90, -80}, {0, -80}}));
	REQUIRE(south.y_min == -90.0);
	REQUIRE(south.y_max < -70.0);

	// An east-winding shell straddling the equator encloses the north side only
	auto straddle = get_extent(MakePolygonWKB({{0, 10}, {90, -10}, {180, 10}, {-90, -10}, {0, 10}}));
	REQUIRE(straddle.y_max == 90.0);
	REQUIRE(straddle.y_min > -90.0);
	REQUIRE(straddle.x_min == -180.0);
	REQUIRE(straddle.x_max == 180.0);

	// A counterclockwise shell encloses its bounded side: tight bounds
	auto small_ccw = get_extent(MakePolygonWKB({{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}}));
	REQUIRE(small_ccw.y_max < 11.0);
	REQUIRE(LonExtentContains(small_ccw.x_min, small_ccw.x_max, 5.0));
	REQUIRE(!LonExtentContains(small_ccw.x_min, small_ccw.x_max, 50.0));

	// A clockwise shell encloses the complement of its bounded side: (almost) the whole globe
	auto small_cw = get_extent(MakePolygonWKB({{0, 0}, {0, 10}, {10, 10}, {10, 0}, {0, 0}}));
	REQUIRE(small_cw.x_min == -180.0);
	REQUIRE(small_cw.x_max == 180.0);
	REQUIRE(small_cw.y_min == -90.0);
	REQUIRE(small_cw.y_max == 90.0);

	// Holes never extend the interior: a clockwise hole inside a counterclockwise shell must not
	// trigger the complement rule
	std::string with_hole;
	{
		std::string buf;
		buf.push_back(1);
		AppendUint32(buf, 3);
		AppendUint32(buf, 2);
		const std::vector<std::pair<double, double>> shell = {{0, 0}, {20, 0}, {20, 20}, {0, 20}, {0, 0}};
		const std::vector<std::pair<double, double>> hole = {{5, 5}, {5, 15}, {15, 15}, {15, 5}, {5, 5}};
		AppendUint32(buf, uint32_t(shell.size()));
		for (auto &p : shell) {
			AppendDouble(buf, p.first);
			AppendDouble(buf, p.second);
		}
		AppendUint32(buf, uint32_t(hole.size()));
		for (auto &p : hole) {
			AppendDouble(buf, p.first);
			AppendDouble(buf, p.second);
		}
		with_hole = buf;
	}
	auto holed = get_extent(with_hole);
	REQUIRE(holed.y_max < 21.0);
	REQUIRE(!LonExtentContains(holed.x_min, holed.x_max, 50.0));

	// Antipodal endpoints make the geodesic ambiguous: full globe
	auto ambiguous = get_extent(MakeLineStringWKB({{-90, 0}, {90, 0}}));
	REQUIRE(ambiguous.y_min == -90.0);
	REQUIRE(ambiguous.y_max == 90.0);
	REQUIRE(ambiguous.x_min == -180.0);
	REQUIRE(ambiguous.x_max == 180.0);
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
