//===----------------------------------------------------------------------===//
//                         DuckDB
//
// astronomy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
namespace datetime {

//! The positions of the sun and the moon at an instant, which the lunisolar calendars use to
//! decide where their months and years start.
//!
//! The formulas are the ones from Duffett-Smith's Practical Astronomy, which is what ICU uses.
//! They are only accurate to a few minutes, but the calendars round to whole days and every
//! implementation has to agree on the same approximation for the dates to line up.
class Astronomer {
public:
	//! The average length of a lunar month, in days
	static constexpr double SYNODIC_MONTH = 29.530588853;
	//! The average length of a solar year, in days
	static constexpr double TROPICAL_YEAR = 365.242191;
	//! The longitude of the sun at the winter solstice
	static constexpr double WINTER_SOLSTICE = (3.14159265358979323846 * 3) / 2;

	explicit Astronomer(double millis) : time(millis) {
		ClearCache();
	}

	void SetTime(double millis) {
		time = millis;
		ClearCache();
	}
	double GetTime() const {
		return time;
	}

	//! The ecliptic longitude of the sun, in radians
	double GetSunLongitude();
	//! The angle between the moon and the sun, in radians, which is zero at a new moon
	double GetMoonAge();

	//! The instant at which the sun reaches a longitude, searching forwards or backwards
	double GetSunTime(double desired, bool next);
	//! The instant at which the moon reaches an age, searching forwards or backwards
	double GetMoonTime(double desired, bool next);

private:
	//! The instant as a Julian day, including the fraction of the day
	double GetJulianDay();
	//! Computes both the longitude and the mean anomaly of the sun on a Julian day
	static void ComputeSunLongitude(double julian_day, double &longitude, double &mean_anomaly);
	//! The ecliptic longitude of the moon, in radians
	double GetMoonLongitude();
	//! Searches for the instant at which an angle reaches the value it is asked for
	template <class ANGLE>
	double TimeOfAngle(ANGLE angle_of, double desired, double period_days, double epsilon, bool next);

	void ClearCache();

	double time;
	double julian_day;
	double sun_longitude;
	double sun_mean_anomaly;
	double moon_longitude;
	bool moon_longitude_set;
};

} // namespace datetime
} // namespace duckdb
