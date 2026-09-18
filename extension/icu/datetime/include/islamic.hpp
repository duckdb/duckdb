//===----------------------------------------------------------------------===//
//                         DuckDB
//
// islamic.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "calendar.hpp"

namespace duckdb {
namespace datetime {

//! The tabular Islamic (Hijri) calendar: twelve alternating months of 30 and 29 days, with the
//! last month gaining a day in eleven years of every thirty year cycle.
class IslamicCivilCalendar : public FieldCalendar {
public:
	explicit IslamicCivilCalendar(unique_ptr<TimeZone> zone) : FieldCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "islamic-civil";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new IslamicCivilCalendar(*this));
	}

protected:
	IslamicCivilCalendar(const IslamicCivilCalendar &other) = default;

	//! The Julian day that the epoch falls on. The civil calendar starts on the Friday.
	virtual int32_t GetEpoch() const {
		return 1948440;
	}
	//! The day the year starts on, counted from the epoch
	virtual int64_t YearStart(int32_t year) const;
	//! The day a 0-based month starts on, counted from the epoch
	virtual int64_t MonthStart(int32_t year, int32_t month) const;

	int64_t HandleComputeMonthStart(int32_t eyear, int32_t month, bool use_month) const override;
	int32_t HandleGetMonthLength(int32_t eyear, int32_t month) const override;
	int32_t HandleGetYearLength(int32_t eyear) const override;
	void HandleComputeFields(int32_t julian_day) override;
	int32_t HandleGetExtendedYear() override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
};

//! The tabular Islamic calendar reckoned from the Thursday rather than the Friday
class IslamicTBLACalendar : public IslamicCivilCalendar {
public:
	explicit IslamicTBLACalendar(unique_ptr<TimeZone> zone) : IslamicCivilCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "islamic-tbla";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new IslamicTBLACalendar(*this));
	}

protected:
	IslamicTBLACalendar(const IslamicTBLACalendar &other) = default;

	int32_t GetEpoch() const override {
		return 1948439;
	}
};

//! The Islamic calendar in which a month starts on the day the new moon is first visible, which
//! is what the religious calendar follows
class IslamicCalendar : public IslamicCivilCalendar {
public:
	explicit IslamicCalendar(unique_ptr<TimeZone> zone) : IslamicCivilCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "islamic";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new IslamicCalendar(*this));
	}

protected:
	IslamicCalendar(const IslamicCalendar &other) = default;

	int64_t YearStart(int32_t year) const override;
	int64_t MonthStart(int32_t year, int32_t month) const override;
	int32_t HandleGetMonthLength(int32_t eyear, int32_t month) const override;
	int32_t HandleGetYearLength(int32_t eyear) const override;
	void HandleComputeFields(int32_t julian_day) override;
};

//! The Islamic calendar as observed by the Umm al-Qura observatory in Saudi Arabia, which uses
//! the same sighting rule
class IslamicRGSACalendar : public IslamicCalendar {
public:
	explicit IslamicRGSACalendar(unique_ptr<TimeZone> zone) : IslamicCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "islamic-rgsa";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new IslamicRGSACalendar(*this));
	}

protected:
	IslamicRGSACalendar(const IslamicRGSACalendar &other) = default;
};

//! The Umm al-Qura calendar of Saudi Arabia, which follows a table of observed month lengths for
//! the years 1300 to 1600 and the tabular calendar outside of that range
class IslamicUmalquraCalendar : public IslamicCivilCalendar {
public:
	//! The range of years that the table covers
	static constexpr int32_t TABLE_START = 1300;
	static constexpr int32_t TABLE_END = 1600;

	explicit IslamicUmalquraCalendar(unique_ptr<TimeZone> zone) : IslamicCivilCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "islamic-umalqura";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new IslamicUmalquraCalendar(*this));
	}

protected:
	IslamicUmalquraCalendar(const IslamicUmalquraCalendar &other) = default;

	int64_t YearStart(int32_t year) const override;
	int64_t MonthStart(int32_t year, int32_t month) const override;
	int32_t HandleGetMonthLength(int32_t eyear, int32_t month) const override;
	int32_t HandleGetYearLength(int32_t eyear) const override;
	void HandleComputeFields(int32_t julian_day) override;
};

} // namespace datetime
} // namespace duckdb
