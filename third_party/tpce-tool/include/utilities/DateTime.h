/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Sergey Vasilevskiy
 */

#ifndef DATE_TIME_H
#define DATE_TIME_H

#ifdef WIN32
#include <windows.h>
#endif

#ifdef COMPILE_ODBC_LOAD
// ODBC headers
#include <sql.h>
#include <sqlext.h>
#include <odbcss.h>
#endif // COMPILE_ODBC_LOAD

#include <sstream>

#include "EGenStandardTypes.h"

namespace TPCE {

// Common datetime structure.
// Identical to ODBC's TIMESTAMP_STRUCT
//
typedef struct tagTIMESTAMP_STRUCT {
	INT16 year;
	UINT16 month;
	UINT16 day;
	UINT16 hour;
	UINT16 minute;
	UINT16 second;
	UINT32 fraction;
} TIMESTAMP_STRUCT;

// Date/Time constants
const double NsPerSecondDivisor = 1000000000.0;
const INT32 NsPerSecond = 1000000000;
const double MsPerSecondDivisor = 1000.000;
const INT32 MsPerSecond = 1000;
const INT32 SecondsPerMinute = 60;
const INT32 MinutesPerHour = 60;
const INT32 HoursPerDay = 24;
const INT32 HoursPerWorkDay = 8;
const INT32 DaysPerWorkWeek = 5;
const INT32 DaysPerWeek = 7;

const INT32 SecondsPerHour = SecondsPerMinute * MinutesPerHour;
const INT32 SecondsPerDay = SecondsPerMinute * MinutesPerHour * HoursPerDay;
const INT32 SecondsPerWorkDay = SecondsPerMinute * MinutesPerHour * HoursPerWorkDay;
const INT32 MsPerDay = SecondsPerDay * MsPerSecond;
const INT32 MsPerWorkDay = SecondsPerWorkDay * MsPerSecond;

#define RoundToNearestNsec(d_Seconds) (((INT64)(((d_Seconds)*NsPerSecond) + 0.5)) / NsPerSecondDivisor)

class CDateTime {
private:
	INT32 m_dayno;  // absolute day number since 1-Jan-0001, starting from zero
	INT32 m_msec;   // milliseconds from the beginning of the day
	char *m_szText; // text representation; only allocated if needed

	friend bool operator>(const CDateTime &l_dt, const CDateTime &r_dt);

	//
	// Ranges used for date/time validation
	//
	static const INT32 minValidYear = 1;
	static const INT32 maxValidYear = 9999;
	static const INT32 minValidMonth = 1;
	static const INT32 maxValidMonth = 12;
	static const INT32 minValidDay = 1;
	static const INT32 maxValidDay = 31;
	static const INT32 minValidHour = 0;
	static const INT32 maxValidHour = 23;
	static const INT32 minValidMinute = 0;
	static const INT32 maxValidMinute = 59;
	static const INT32 minValidSecond = 0;
	static const INT32 maxValidSecond = 59;
	static const INT32 minValidMilliSecond = 0;
	static const INT32 maxValidMilliSecond = 999;

	static const INT32 minValidDayNumber = 0;

	// days in 1-year period (not including any leap year exceptions) = 365
	static const INT32 dy1 = 365;
	// days in 4-year period (not including 400 and 100-year exceptions) = 1,461
	static const INT32 dy4 = 4 * dy1 + 1; // fourth year is a leap year
	// days in 100-year period (not including 400-year exception) = 36,524
	static const INT32 dy100 = 25 * dy4 - 1; // 100th year is not a leap year
	// days in 400-year period = 146,097
	static const INT32 dy400 = 4 * dy100 + 1; // 400th year is a leap year

	// month array contains days of months for months in a non leap-year
	static const INT32 monthArray[12];

	// month array contains days of months for months in a leap-year
	static const INT32 monthArrayLY[12];

	// MonthArray contains cumulative days for months in a non leap-year
	static const INT32 cumulativeMonthArray[];

	//
	// Utility routine to calculate the day number for a given year/month/day.
	//
	static INT32 CalculateDayNumber(INT32 year, INT32 month, INT32 day);

	//
	// Validation routines used to check inputs to constructors and Set methods.
	//
	static bool IsValid(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec);
	static void Validate(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec);
	static void Validate(INT32 dayNumber);
	static void Validate(INT32 dayNumber, INT32 msecSoFarToday);

	//
	// Leap Year determinination routine.
	//
	static bool IsLeapYear(INT32 year);

public:
	CDateTime(void);        // current local date/time
	CDateTime(INT32 dayno); // date as specified; time set to 0:00:00 (midnight)
	CDateTime(INT32 year, INT32 month,
	          INT32 day); // date as specified; time set to 0:00:00 (midnight)
	CDateTime(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec);

	CDateTime(TIMESTAMP_STRUCT *ts); // date specified in the TIMESTAMP struct

	CDateTime(const CDateTime &dt); // proper copy constructor - does not copy m_szText
	~CDateTime(void);

	void Set(void);        // set to current local date/time
	void Set(INT32 dayno); // set to specified day number
	void Set(INT32 year, INT32 month,
	         INT32 day); // set to specified date; time set to 0:00:00 (midnight)
	void Set(INT32 hour, INT32 minute, INT32 second,
	         INT32 msec); // set to specified time, date not changed.
	void Set(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec);

	inline INT32 DayNo(void) {
		return m_dayno;
	};
	inline INT32 MSec(void) {
		return m_msec;
	};
	void GetYMD(INT32 *year, INT32 *month, INT32 *day);
	void GetYMDHMS(INT32 *year, INT32 *month, INT32 *day, INT32 *hour, INT32 *minute, INT32 *second, INT32 *msec);
	void GetHMS(INT32 *hour, INT32 *minute, INT32 *second, INT32 *msec);

	void GetTimeStamp(TIMESTAMP_STRUCT *ts);

#ifdef COMPILE_ODBC_LOAD
	void GetDBDATETIME(DBDATETIME *dt);
#endif // COMPILE_ODBC_LOAD

	static INT32 YMDtoDayno(INT32 yr, INT32 mm, INT32 dd);
	char *ToStr(INT32 style);

	void Add(INT32 days, INT32 msec, bool adjust_weekend = false);
	void AddMinutes(INT32 Minutes);
	void AddWorkMs(INT64 WorkMs);

	bool operator<(const CDateTime &);
	bool operator<=(const CDateTime &);
	// operator > is defined as an external (not in-class) operator in
	// CDateTime.cpp
	bool operator>=(const CDateTime &);
	bool operator==(const CDateTime &);

	// compute the difference between two DateTimes;
	// result in seconds
	double operator-(const CDateTime &dt);
	INT32 DiffInMilliSeconds(const CDateTime &BaseTime);
	INT32 DiffInMilliSeconds(CDateTime *pBaseTime);
	// add another DateTime to this one
	CDateTime &operator+=(const CDateTime &dt);
	// Proper assignment operator - does not copy szText
	CDateTime &operator=(const CDateTime &dt);
};

} // namespace TPCE

#endif // DATE_TIME_H
