#include "catch.hpp"
#include "duckdb/storage/statistics/stats_writer.hpp"

#include <cmath>
#include <limits>

using namespace duckdb;

template <class T>
static void UpdateRawInputMinMax(StatsWriter<T> &writer, const T &value) {
	writer.UpdateMinMaxFromInput(NumericStatsTraits<T>::LoadInput(&value));
}

TEST_CASE("StatsWriter canonicalizes float signed zero stats", "[stats_writer]") {
	StatsWriter<float> writer;
	writer.Update(-0.0f);
	writer.Update(0.0f);

	auto stats = BaseStatistics::CreateEmpty(LogicalType::FLOAT);
	writer.Merge(stats);

	auto min = NumericStats::GetMin<float>(stats);
	auto max = NumericStats::GetMax<float>(stats);
	REQUIRE(min == 0.0f);
	REQUIRE(max == 0.0f);
	REQUIRE_FALSE(std::signbit(min));
	REQUIRE_FALSE(std::signbit(max));
}

TEST_CASE("StatsWriter canonicalizes double signed zero stats", "[stats_writer]") {
	StatsWriter<double> writer;
	writer.Update(-0.0);
	writer.Update(0.0);

	auto stats = BaseStatistics::CreateEmpty(LogicalType::DOUBLE);
	writer.Merge(stats);

	auto min = NumericStats::GetMin<double>(stats);
	auto max = NumericStats::GetMax<double>(stats);
	REQUIRE(min == 0.0);
	REQUIRE(max == 0.0);
	REQUIRE_FALSE(std::signbit(min));
	REQUIRE_FALSE(std::signbit(max));
}

TEST_CASE("StatsWriter merges float NaN into valid target as max", "[stats_writer]") {
	StatsWriter<float> finite_writer;
	finite_writer.Update(1.0f);
	finite_writer.Update(10.0f);

	auto stats = BaseStatistics::CreateEmpty(LogicalType::FLOAT);
	finite_writer.Merge(stats);

	StatsWriter<float> nan_writer;
	nan_writer.Update(std::numeric_limits<float>::quiet_NaN());
	nan_writer.Merge(stats);

	auto min = NumericStats::GetMin<float>(stats);
	auto max = NumericStats::GetMax<float>(stats);
	REQUIRE(min == 1.0f);
	REQUIRE(std::isnan(max));
}

TEST_CASE("StatsWriter merges double NaN into valid target as max", "[stats_writer]") {
	StatsWriter<double> finite_writer;
	finite_writer.Update(1.0);
	finite_writer.Update(10.0);

	auto stats = BaseStatistics::CreateEmpty(LogicalType::DOUBLE);
	finite_writer.Merge(stats);

	StatsWriter<double> nan_writer;
	nan_writer.Update(std::numeric_limits<double>::quiet_NaN());
	nan_writer.Merge(stats);

	auto min = NumericStats::GetMin<double>(stats);
	auto max = NumericStats::GetMax<double>(stats);
	REQUIRE(min == 1.0);
	REQUIRE(std::isnan(max));
}

TEST_CASE("StatsWriter folds float raw input min max values", "[stats_writer]") {
	float one = 1.0f;
	float ten = 10.0f;
	float nan = std::numeric_limits<float>::quiet_NaN();

	StatsWriter<float> writer;
	writer.SetHasValid();
	UpdateRawInputMinMax(writer, one);
	UpdateRawInputMinMax(writer, ten);
	UpdateRawInputMinMax(writer, nan);

	auto stats = BaseStatistics::CreateEmpty(LogicalType::FLOAT);
	writer.Merge(stats);

	auto min = NumericStats::GetMin<float>(stats);
	auto max = NumericStats::GetMax<float>(stats);
	REQUIRE(min == 1.0f);
	REQUIRE(std::isnan(max));
}

TEST_CASE("StatsWriter folds double raw input min max values", "[stats_writer]") {
	double one = 1.0;
	double ten = 10.0;
	double nan = std::numeric_limits<double>::quiet_NaN();

	StatsWriter<double> writer;
	writer.SetHasValid();
	UpdateRawInputMinMax(writer, one);
	UpdateRawInputMinMax(writer, ten);
	UpdateRawInputMinMax(writer, nan);

	auto stats = BaseStatistics::CreateEmpty(LogicalType::DOUBLE);
	writer.Merge(stats);

	auto min = NumericStats::GetMin<double>(stats);
	auto max = NumericStats::GetMax<double>(stats);
	REQUIRE(min == 1.0);
	REQUIRE(std::isnan(max));
}
