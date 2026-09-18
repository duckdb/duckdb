#include "astronomy.hpp"

#include "grego.hpp"

#include <cmath>
#include <limits>

namespace duckdb {
namespace datetime {

static constexpr double PI = 3.14159265358979323846;
static constexpr double PI2 = PI * 2.0;
static constexpr double DEGREES = PI / 180;

//! The instant of the Julian day epoch, in milliseconds
static constexpr double JULIAN_EPOCH_MS = -210866760000000.0;
//! The Julian day of the epoch that the formulas are relative to (1990-01-01 00:00 UTC)
static constexpr double JD_EPOCH = 2447891.5;

//! The ecliptic longitude of the sun at the epoch, and of its perigee
static constexpr double SUN_ETA_G = 279.403303 * DEGREES;
static constexpr double SUN_OMEGA_G = 282.768422 * DEGREES;
//! The eccentricity of the orbit of the earth
static constexpr double SUN_E = 0.016713;

//! The mean longitude of the moon at the epoch, of its perigee and of its node
static constexpr double MOON_L0 = 318.351648 * DEGREES;
static constexpr double MOON_P0 = 36.340410 * DEGREES;
static constexpr double MOON_N0 = 318.510107 * DEGREES;
//! The inclination of the orbit of the moon
static constexpr double MOON_I = 5.145366 * DEGREES;

//! Reduces an angle to a single turn
static double Normalize(double value, double range) {
	return value - range * FloorDiv::Divide(value, range);
}
static double Norm2PI(double angle) {
	return Normalize(angle, PI2);
}
static double NormPI(double angle) {
	return Normalize(angle + PI, PI2) - PI;
}

void Astronomer::ClearCache() {
	const auto invalid = std::numeric_limits<double>::quiet_NaN();
	julian_day = invalid;
	sun_longitude = invalid;
	sun_mean_anomaly = invalid;
	moon_longitude = invalid;
	moon_longitude_set = false;
}

double Astronomer::GetJulianDay() {
	if (std::isnan(julian_day)) {
		julian_day = (time - JULIAN_EPOCH_MS) / double(MILLIS_PER_DAY);
	}
	return julian_day;
}

//! Solves Kepler's equation for the true anomaly of an orbit
static double TrueAnomaly(double mean_anomaly, double eccentricity) {
	double delta;
	auto e = mean_anomaly;
	do {
		delta = e - eccentricity * std::sin(e) - mean_anomaly;
		e = e - delta / (1 - eccentricity * std::cos(e));
	} while (std::fabs(delta) > 1e-5);
	return 2.0 * std::atan(std::tan(e / 2) * std::sqrt((1 + eccentricity) / (1 - eccentricity)));
}

void Astronomer::ComputeSunLongitude(double julian_day, double &longitude, double &mean_anomaly) {
	const auto day = julian_day - JD_EPOCH;
	const auto epoch_angle = Norm2PI(PI2 / TROPICAL_YEAR * day);
	mean_anomaly = Norm2PI(epoch_angle + SUN_ETA_G - SUN_OMEGA_G);
	longitude = Norm2PI(TrueAnomaly(mean_anomaly, SUN_E) + SUN_OMEGA_G);
}

double Astronomer::GetSunLongitude() {
	if (std::isnan(sun_longitude)) {
		ComputeSunLongitude(GetJulianDay(), sun_longitude, sun_mean_anomaly);
	}
	return sun_longitude;
}

double Astronomer::GetMoonLongitude() {
	if (moon_longitude_set) {
		return moon_longitude;
	}
	GetSunLongitude();
	const auto day = GetJulianDay() - JD_EPOCH;

	const auto mean_longitude = Norm2PI(13.1763966 * DEGREES * day + MOON_L0);
	auto mean_anomaly = Norm2PI(mean_longitude - 0.1114041 * DEGREES * day - MOON_P0);

	// the corrections for the pull of the sun on the orbit of the moon
	const auto evection = 1.2739 * DEGREES * std::sin(2 * (mean_longitude - sun_longitude) - mean_anomaly);
	const auto annual = 0.1858 * DEGREES * std::sin(sun_mean_anomaly);
	const auto a3 = 0.3700 * DEGREES * std::sin(sun_mean_anomaly);
	mean_anomaly += evection - annual - a3;

	const auto center = 6.2886 * DEGREES * std::sin(mean_anomaly);
	const auto a4 = 0.2140 * DEGREES * std::sin(2 * mean_anomaly);
	auto longitude = mean_longitude + evection + center - annual + a4;
	longitude += 0.6583 * DEGREES * std::sin(2 * (longitude - sun_longitude));

	auto node_longitude = Norm2PI(MOON_N0 - 0.0529539 * DEGREES * day);
	node_longitude -= 0.16 * DEGREES * std::sin(sun_mean_anomaly);

	// project the position onto the ecliptic
	const auto y = std::sin(longitude - node_longitude);
	const auto x = std::cos(longitude - node_longitude);
	moon_longitude = std::atan2(y * std::cos(MOON_I), x) + node_longitude;
	moon_longitude_set = true;
	return moon_longitude;
}

double Astronomer::GetMoonAge() {
	const auto longitude = GetMoonLongitude();
	return Norm2PI(longitude - sun_longitude);
}

template <class ANGLE>
double Astronomer::TimeOfAngle(ANGLE angle_of, double desired, double period_days, double epsilon, bool next) {
	// start from a linear estimate of how far away the angle is, then refine it
	auto last_angle = angle_of(*this);
	const auto delta_angle = Norm2PI(desired - last_angle);
	auto delta = (delta_angle + (next ? 0.0 : -PI2)) * (period_days * double(MILLIS_PER_DAY)) / PI2;
	auto last_delta = delta;
	const auto start = time;

	SetTime(time + std::ceil(delta));
	do {
		const auto angle = angle_of(*this);
		const auto factor = std::fabs(delta / NormPI(angle - last_angle));
		delta = NormPI(desired - angle) * factor;
		if (std::fabs(delta) > std::fabs(last_delta)) {
			// the estimate is diverging, so restart an eighth of a period along
			const auto step = std::ceil(period_days * double(MILLIS_PER_DAY) / 8.0);
			SetTime(start + (next ? step : -step));
			return TimeOfAngle(angle_of, desired, period_days, epsilon, next);
		}
		last_delta = delta;
		last_angle = angle;
		SetTime(time + std::ceil(delta));
	} while (std::fabs(delta) > epsilon);
	return time;
}

double Astronomer::GetSunTime(double desired, bool next) {
	return TimeOfAngle([](Astronomer &a) { return a.GetSunLongitude(); }, desired, TROPICAL_YEAR,
	                   double(MILLIS_PER_MINUTE), next);
}

double Astronomer::GetMoonTime(double desired, bool next) {
	return TimeOfAngle([](Astronomer &a) { return a.GetMoonAge(); }, desired, SYNODIC_MONTH, double(MILLIS_PER_MINUTE),
	                   next);
}

} // namespace datetime
} // namespace duckdb
