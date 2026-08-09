//===----------------------------------------------------------------------===//
//                         DuckDB
//
// coptic.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "calendar.hpp"

namespace duckdb {
namespace datetime {

//! The calendar that the Coptic and the Ethiopic calendars share: twelve months of thirty days
//! followed by a thirteenth month of five days, or six in every fourth year. The systems differ
//! only in the day their first year starts on and in how they name their years.
class CopticEthiopicCalendar : public FieldCalendar {
public:
	explicit CopticEthiopicCalendar(unique_ptr<TimeZone> zone) : FieldCalendar(std::move(zone)) {
	}

protected:
	CopticEthiopicCalendar(const CopticEthiopicCalendar &other) = default;

	//! The Julian day that the first day of the first year falls on
	virtual int32_t GetEpochOffset() const = 0;
	//! The era that an extended year belongs to
	virtual int32_t ExtendedYearToEra(int32_t eyear) const = 0;
	//! The year within its era that an extended year corresponds to
	virtual int32_t ExtendedYearToYear(int32_t eyear) const = 0;

	int64_t HandleComputeMonthStart(int32_t eyear, int32_t month, bool use_month) const override;
	void HandleComputeFields(int32_t julian_day) override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
};

//! The Coptic calendar, counting from the Era of the Martyrs
class CopticCalendar : public CopticEthiopicCalendar {
public:
	static constexpr int32_t BCE = 0;
	static constexpr int32_t CE = 1;

	explicit CopticCalendar(unique_ptr<TimeZone> zone) : CopticEthiopicCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "coptic";
	}
	bool IsEra0CountingBackward() const override {
		return true;
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new CopticCalendar(*this));
	}

protected:
	CopticCalendar(const CopticCalendar &other) = default;

	int32_t GetEpochOffset() const override {
		return 1824665;
	}
	int32_t ExtendedYearToEra(int32_t eyear) const override {
		return eyear <= 0 ? BCE : CE;
	}
	int32_t ExtendedYearToYear(int32_t eyear) const override {
		return eyear <= 0 ? 1 - eyear : eyear;
	}
	int32_t HandleGetExtendedYear() override;
};

//! The Ethiopic calendar, which numbers its years from the Incarnation (Amete Mihret) and the
//! years before it from the creation of the world (Amete Alem)
class EthiopicCalendar : public CopticEthiopicCalendar {
public:
	static constexpr int32_t AMETE_ALEM = 0;
	static constexpr int32_t AMETE_MIHRET = 1;
	//! Amete Alem 5501 is Amete Mihret 1
	static constexpr int32_t AMETE_MIHRET_DELTA = 5500;

	explicit EthiopicCalendar(unique_ptr<TimeZone> zone) : CopticEthiopicCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "ethiopic";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new EthiopicCalendar(*this));
	}

protected:
	EthiopicCalendar(const EthiopicCalendar &other) = default;

	int32_t GetEpochOffset() const override {
		return 1723856;
	}
	int32_t ExtendedYearToEra(int32_t eyear) const override {
		return eyear <= 0 ? AMETE_ALEM : AMETE_MIHRET;
	}
	int32_t ExtendedYearToYear(int32_t eyear) const override {
		return eyear <= 0 ? eyear + AMETE_MIHRET_DELTA : eyear;
	}
	int32_t HandleGetExtendedYear() override;
};

//! The Ethiopic calendar counting every year from the creation of the world, in a single era
class EthiopicAmeteAlemCalendar : public EthiopicCalendar {
public:
	explicit EthiopicAmeteAlemCalendar(unique_ptr<TimeZone> zone) : EthiopicCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "ethiopic-amete-alem";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new EthiopicAmeteAlemCalendar(*this));
	}

protected:
	EthiopicAmeteAlemCalendar(const EthiopicAmeteAlemCalendar &other) = default;

	int32_t GetEpochOffset() const override {
		return -285019;
	}
	int32_t ExtendedYearToEra(int32_t eyear) const override {
		return AMETE_ALEM;
	}
	int32_t ExtendedYearToYear(int32_t eyear) const override {
		return eyear;
	}
	int32_t HandleGetExtendedYear() override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
};

} // namespace datetime
} // namespace duckdb
