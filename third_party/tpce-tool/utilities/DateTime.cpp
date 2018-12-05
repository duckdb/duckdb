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
 * - Charles Levine
 */

#include "utilities/DateTime.h"

#include <stdio.h>
#include <stdexcept>
#include <chrono>

// DJ: perhaps all unixes need this so maybe we want #ifndef WIN32 or something
// like that?
#if (__unix) || (_AIX)
#include <sys/time.h> // for gettimeofday
#endif

using namespace TPCE;

// Initialize static const member arrays.
const INT32 CDateTime::monthArray[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
const INT32 CDateTime::monthArrayLY[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
const INT32 CDateTime::cumulativeMonthArray[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

bool CDateTime::IsLeapYear(INT32 year) {
	if ((year % 4) != 0) // only years which are multiples of 4 can be leap years
	{
		return false;
	}

	if ((year % 400) == 0) // years divisible by 400 are leap years
	{
		return true;
	}

	if ((year % 100) == 0) // years divisible by 100 but not 400 are not leap years
	{
		return false;
	}

	// must be a leap year if you get here
	return true;
}

// Checks whether the date/time is valid.
bool CDateTime::IsValid(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec) {
	// check all values that have static, absolute bounds
	if (hour < minValidHour || maxValidHour < hour || minute < minValidMinute || maxValidMinute < minute ||
	    second < minValidSecond || maxValidSecond < second || msec < minValidMilliSecond ||
	    maxValidMilliSecond < msec || year < minValidYear || maxValidYear < year || month < minValidMonth ||
	    maxValidMonth < month || day < minValidDay) {
		return false;
	}

	// check the day of the month
	// optimize for common case.  if check passes, we're done
	if (day <= monthArray[month - 1]) {
		return true;
	}

	// Only one possibility left -- February 29th
	// Feb 29th valid only if a leap year; invalid otherwise
	if ((month == 2) && (day == 29)) {
		return IsLeapYear(year);
	}

	// exhausted all possibilities; can't be valid
	return false;
}

// Validate the specified date/time and throw an out_of_range exception if it
// isn't valid.
void CDateTime::Validate(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec) {
	if (!IsValid(year, month, day, hour, minute, second, msec)) {
		std::ostringstream msg;
		msg << "The specified Date/Time is not valid.";
		throw std::out_of_range(msg.str());
	}
}

// Validate the specified day number and throw an out_of_range exception if it
// isn't valid.
void CDateTime::Validate(INT32 dayNumber) {
	if ((dayNumber < minValidDayNumber) || (CalculateDayNumber(maxValidYear, maxValidMonth, maxValidDay) < dayNumber)) {
		std::ostringstream msg;
		msg << "The specified day-number is not valid.";
		throw std::out_of_range(msg.str());
	}
}

// Validate a day-number/msec pair.
void CDateTime::Validate(INT32 dayNumber, INT32 msecSoFarToday) {
	if ((msecSoFarToday < minValidMilliSecond) || (maxValidMilliSecond < msecSoFarToday) ||
	    (dayNumber < minValidDayNumber) || (CalculateDayNumber(maxValidYear, maxValidMonth, maxValidDay) < dayNumber)) {
		std::ostringstream msg;
		msg << "The specified (day-number, msec) pair is not valid.";
		throw std::out_of_range(msg.str());
	}
}

// Computes the number of days since Jan 1, 0001.  (Year 1 AD)
// 1-Jan-0001 = 0
INT32 CDateTime::CalculateDayNumber(INT32 yr, INT32 mm, INT32 dd) {
	// compute day of year
	INT32 jd = cumulativeMonthArray[mm - 1] + dd - 1;

	// adjust day of year if this is a leap year and it is after February
	if ((mm > 2) && IsLeapYear(yr)) {
		jd++;
	}

	// compute number of days from 1/1/0001 to beginning of present year
	yr--; // start counting from year 1 AD (1-based instead of 0-based)
	jd += yr / 400 * dy400;
	yr %= 400;
	jd += yr / 100 * dy100;
	yr %= 100;
	jd += yr / 4 * dy4;
	yr %= 4;
	jd += yr * dy1;

	return jd;
}

INT32 CDateTime::YMDtoDayno(INT32 yr, INT32 mm, INT32 dd) {
	// Validate year, month and day (use known safe values for hours -
	// milliseconds).
	Validate(yr, mm, dd, minValidHour, minValidMinute, minValidSecond, minValidMilliSecond);

	return CalculateDayNumber(yr, mm, dd);
}

// returns text representation of DateTime
// the style argument is interpreted as a two digit field, where the first digit
// (in the tens place) is the date format and the second digit (in the ones
// place) is the time format.
//
// The following formats are provided:
//      STYLE   DATE            TIME
//      -----   ----            ----
//      0       <omit>          <omit>
//      1       YYYY-MM-DD      HH:MM:SS        (24hr)
//      2       MM/DD/YY        HH:MM:SS.mmm    (24hr)
//      3       MM/DD/YYYY      HH:MM           (24hr)
//      4       DD-MON-YYYY     HH:MM:SS [AM|PM]
//      5       DD-MON-YY       HH:MM:SS.mmm [AM|PM]
//      6       MM-DD-YY        HH:MM [AM|PM]
//      7       MON DD YYYY
//      8       Month DD, YYYY
//
char *CDateTime::ToStr(INT32 style = 11) {
	static const char *szMonthsShort[] = {"JAN", "FEB", "MAR", "APR", "MAY", "JUN",
	                                      "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};
	static const char *szMonthsFull[] = {"January", "February", "March",     "April",   "May",      "June",
	                                     "July",    "August",   "September", "October", "November", "December"};
	static const char *szAmPm[] = {"AM", "PM"};
	// the following array is used to map from 24-hour to 12-hour time
	static const INT32 iHr12[] = {12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

	INT32 year, month, day, hour, minute, second, msec;
	INT32 p = 0;
	static const INT32 iMaxStrLen = 40;

	if (m_szText == NULL)
		m_szText = new char[iMaxStrLen];
	m_szText[0] = '\0';

	GetYMDHMS(&year, &month, &day, &hour, &minute, &second, &msec);

	size_t lengthTotal = iMaxStrLen;
	char *pszText = m_szText;

	// DATE portion
	switch (style / 10) {
	case 1:
		// YYYY-MM-DD
		p = snprintf(pszText, lengthTotal, "%04d-%02d-%02d ", year, month, day);
		break;
	case 2:
		// MM/DD/YY
		p = snprintf(pszText, lengthTotal, "%02d/%02d/%02d ", month, day, year % 100);
		break;
	case 3:
		// MM/DD/YYYY
		p = snprintf(pszText, lengthTotal, "%02d/%02d/%04d ", month, day, year);
		break;
	case 4:
		// DD-MON-YYYY
		p = snprintf(pszText, lengthTotal, "%02d-%s-%04d ", day, szMonthsShort[month - 1], year);
		break;
	case 5:
		// DD-MON-YY
		p = snprintf(pszText, lengthTotal, "%02d-%s-%02d ", day, szMonthsShort[month - 1], year % 100);
		break;
	case 6:
		// MM-DD-YY
		p = snprintf(pszText, lengthTotal, "%02d-%02d-%02d ", month, day, year % 100);
		break;
	case 7:
		// MON DD YYYY
		p = snprintf(pszText, lengthTotal, "%s %02d %04d ", szMonthsShort[month - 1], day, year);
		break;
	case 8:
		// Month DD, YYYY
		p = snprintf(pszText, lengthTotal, "%s %02d, %04d ", szMonthsFull[month - 1], day, year);
		break;
	}

	size_t lengthRemaining = lengthTotal - p;
	pszText = m_szText + (char)p;

	// TIME portion
	switch (style % 10) {
	case 1:
		// HH:MM:SS     (24hr)
		p += snprintf(pszText, lengthRemaining, "%02d:%02d:%02d", hour, minute, second);
		break;
	case 2:
		// HH:MM:SS.mmm (24hr)
		p += snprintf(pszText, lengthRemaining, "%02d:%02d:%02d.%03d", hour, minute, second, msec);
		break;
	case 3:
		// HH:MM        (24hr)
		p += snprintf(pszText, lengthRemaining, "%02d:%02d", hour, minute);
		break;
	case 4:
		// HH:MM:SS [AM|PM]
		p += snprintf(pszText, lengthRemaining, "%02d:%02d:%02d %s", iHr12[hour], minute, second, szAmPm[hour / 12]);
		break;
	case 5:
		// HHH:MM:SS.mmm [AM|PM]
		p += snprintf(pszText, lengthRemaining, "%02d:%02d:%02d.%03d %s", iHr12[hour], minute, second, msec,
		              szAmPm[hour / 12]);
		break;
	case 6:
		// HH:MM [AM|PM]
		p += snprintf(pszText, lengthRemaining, "%02d:%02d %s", iHr12[hour], minute, szAmPm[hour / 12]);
		break;
	}

	// trim trailing blank, if there is one.
	if (p > 0 && m_szText[p - 1] == ' ')
		m_szText[p - 1] = '\0';

	return m_szText;
}

// set to current local time
CDateTime::CDateTime(void) {
	m_szText = NULL;
	Set();
}

CDateTime::CDateTime(INT32 dayno) {
	Validate(dayno);
	m_szText = NULL;
	m_dayno = dayno;
	m_msec = 0;
}

CDateTime::CDateTime(INT32 year, INT32 month, INT32 day) {
	Validate(year, month, day, minValidHour, minValidMinute, minValidSecond, minValidMilliSecond);
	m_dayno = CalculateDayNumber(year, month, day);
	m_szText = NULL;
	m_msec = 0;
}

// Copy constructor
CDateTime::CDateTime(const CDateTime &dt) {
	// Assume source is valid.
	// Validate( dt.m_dayno, dt.m_msec );
	m_dayno = dt.m_dayno;
	m_msec = dt.m_msec;
	m_szText = NULL;
}

CDateTime::~CDateTime(void) {
	if (m_szText)
		delete[] m_szText;
}

CDateTime::CDateTime(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec) {
	// Validate specified date/time
	Validate(year, month, day, hour, minute, second, msec);

	m_szText = NULL;
	m_dayno = CalculateDayNumber(year, month, day);
	m_msec = ((hour * MinutesPerHour + minute) * SecondsPerMinute + second) * MsPerSecond + msec;
}

CDateTime::CDateTime(TPCE::TIMESTAMP_STRUCT *ts) {
	Validate(ts->year, ts->month, ts->day, ts->hour, ts->minute, ts->second, ts->fraction / 1000000);

	m_szText = NULL;
	m_dayno = CalculateDayNumber(ts->year, ts->month, ts->day);
	m_msec = ((ts->hour * MinutesPerHour + ts->minute) * SecondsPerMinute + ts->second) * MsPerSecond +
	         ts->fraction / 1000000;
}

// set to current local time
void CDateTime::Set(void) {
	// assert(0); // why is this necessary?
	// //UNIX-specific code to get the current time with 1ms resolution
	//     struct timeval tv;
	//     struct tm ltr;
	//     int secs;
	//     gettimeofday(&tv, NULL);
	//     struct tm* lt = localtime_r(&tv.tv_sec, &ltr);  //expand into
	//     year/month/day/...
	//     // NOTE: 1 is added to tm_mon because it is 0 based, but
	//     CalculateDayNumber expects it to
	//     // be 1 based.
	//     m_dayno = CalculateDayNumber(lt->tm_year+1900, lt->tm_mon+1,
	//     lt->tm_mday);  // tm_year is based on 1900, not 0.

	//     secs = (lt->tm_hour * MinutesPerHour + lt->tm_min)*SecondsPerMinute +
	//             lt->tm_sec;
	//     m_msec = static_cast<INT32>((long)secs * MsPerSecond + tv.tv_usec /
	//     1000);
}

void CDateTime::Set(INT32 dayno) {
	Validate(dayno);
	m_dayno = dayno;
	m_msec = 0;
}

void CDateTime::Set(INT32 year, INT32 month, INT32 day) {
	Validate(year, month, day, minValidHour, minValidMinute, minValidSecond, minValidMilliSecond);

	m_dayno = CalculateDayNumber(year, month, day);
	m_msec = 0;
}

void CDateTime::Set(INT32 hour, INT32 minute, INT32 second, INT32 msec) {
	Validate(minValidYear, minValidMonth, minValidDay, hour, minute, second, msec);

	m_msec = ((hour * MinutesPerHour + minute) * SecondsPerMinute + second) * MsPerSecond + msec;
}

void CDateTime::Set(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec) {
	Validate(year, month, day, hour, minute, second, msec);

	m_dayno = CalculateDayNumber(year, month, day);
	m_msec = ((hour * MinutesPerHour + minute) * SecondsPerMinute + second) * MsPerSecond + msec;
}

// DaynoToYMD converts a day index to
// its corresponding calendar value (mm/dd/yr).  The valid range for days
// is { 0 .. ~3.65M } for dates from 1-Jan-0001 to 31-Dec-9999.
void CDateTime::GetYMD(INT32 *year, INT32 *month, INT32 *day) {
	INT32 dayno = m_dayno;

	// local variables
	INT32 y, m;

	y = 1; // based on year 1 AD
	y += (dayno / dy400) * 400;
	dayno %= dy400;
	if (dayno == dy400 - 1) // special case for last day of 400-year leap-year
	{
		y += 399;
		dayno -= 3 * dy100 + 24 * dy4 + 3 * dy1;
	} else {
		y += (dayno / dy100) * 100;
		dayno %= dy100;
		y += (dayno / dy4) * 4;
		dayno %= dy4;
		if (dayno == dy4 - 1) // special case for last day of 4-year leap-year
		{
			y += 3;
			dayno -= 3 * dy1;
		} else {
			y += dayno / dy1;
			dayno %= dy1;
		}
	}

	m = 1;
	dayno++;
	if (IsLeapYear(y)) {
		while (dayno > monthArrayLY[m - 1]) {
			dayno -= monthArrayLY[m - 1];
			m++;
		}
	} else {
		while (dayno > monthArray[m - 1]) {
			dayno -= monthArray[m - 1];
			m++;
		}
	}

	*year = y;
	*month = m;
	*day = dayno;
}

void CDateTime::GetYMDHMS(INT32 *year, INT32 *month, INT32 *day, INT32 *hour, INT32 *minute, INT32 *second,
                          INT32 *msec) {
	GetYMD(year, month, day);
	GetHMS(hour, minute, second, msec);
}

void CDateTime::GetHMS(INT32 *hour, INT32 *minute, INT32 *second, INT32 *msec) {
	INT32 ms = m_msec;

	*msec = ms % MsPerSecond;
	ms /= MsPerSecond;
	*second = ms % SecondsPerMinute;
	ms /= SecondsPerMinute;
	*minute = ms % MinutesPerHour;
	*hour = ms / MinutesPerHour;
}

void CDateTime::GetTimeStamp(TPCE::TIMESTAMP_STRUCT *ts) {
	INT32 year, month, day, hour, minute, second, msec;

	GetYMDHMS(&year, &month, &day, &hour, &minute, &second, &msec);
	ts->year = (INT16)year;
	ts->month = (UINT16)month;
	ts->day = (UINT16)day;
	ts->hour = (UINT16)hour;
	ts->minute = (UINT16)minute;
	ts->second = (UINT16)second;
	ts->fraction = (UINT32)msec * 1000000; // because "fraction" is 1/billion'th of a second
}

#ifdef COMPILE_ODBC_LOAD
static const INT32 dayno_1Jan1900 = CDateTime::YMDtoDayno(1900, 1, 1);

void CDateTime::GetDBDATETIME(DBDATETIME *dt) {
	dt->dtdays = m_dayno - dayno_1Jan1900;
	dt->dttime = m_msec * 3 / 10;
}
#endif // COMPILE_ODBC_LOAD

void CDateTime::Add(INT32 days, INT32 msec, bool adjust_weekend /* =false */) {
	if (adjust_weekend) {
		days = ((days / DaysPerWorkWeek) * DaysPerWeek) + (days % DaysPerWorkWeek);
	}

	m_dayno += days;

	m_msec += msec;
	m_dayno += m_msec / MsPerDay;
	m_msec %= MsPerDay;
	if (m_msec < 0) {
		m_dayno--;
		m_msec += MsPerDay;
	}
}
void CDateTime::AddMinutes(INT32 Minutes) {
	Add(0, Minutes * SecondsPerMinute * MsPerSecond);
}
void CDateTime::AddWorkMs(INT64 WorkMs) {
	INT32 WorkDays = (INT32)(WorkMs / (INT64)MsPerWorkDay);
	Add(WorkDays, (INT32)(WorkMs % MsPerWorkDay), true);
}
bool CDateTime::operator<(const CDateTime &dt) {
	return (m_dayno == dt.m_dayno) ? (m_msec < dt.m_msec) : (m_dayno < dt.m_dayno);
}

bool CDateTime::operator<=(const CDateTime &dt) {
	return (m_dayno == dt.m_dayno) ? (m_msec <= dt.m_msec) : (m_dayno <= dt.m_dayno);
}

namespace TPCE {

// Need const reference left argument for greater<CDateTime> comparison function
bool operator>(const CDateTime &l_dt, const CDateTime &r_dt) {
	return (l_dt.m_dayno == r_dt.m_dayno) ? (l_dt.m_msec > r_dt.m_msec) : (l_dt.m_dayno > r_dt.m_dayno);
}

} // namespace TPCE

bool CDateTime::operator>=(const CDateTime &dt) {
	return (m_dayno == dt.m_dayno) ? (m_msec >= dt.m_msec) : (m_dayno >= dt.m_dayno);
}

bool CDateTime::operator==(const CDateTime &dt) {
	return m_dayno == dt.m_dayno ? m_msec == dt.m_msec : false;
}

// compute the difference between two DateTimes;
// result in seconds
double CDateTime::operator-(const CDateTime &dt) {
	double dSecs;
	dSecs = (double)((m_dayno - dt.m_dayno) * SecondsPerMinute * MinutesPerHour * HoursPerDay);
	dSecs += (double)(m_msec - dt.m_msec) / MsPerSecondDivisor;
	return dSecs;
}

INT32 CDateTime::DiffInMilliSeconds(const CDateTime &BaseTime) {
	INT32 mSecs;
	mSecs = (m_dayno - BaseTime.m_dayno) * MsPerSecond * SecondsPerMinute * MinutesPerHour * HoursPerDay;
	mSecs += (m_msec - BaseTime.m_msec);
	return mSecs;
}

INT32 CDateTime::DiffInMilliSeconds(CDateTime *pBaseTime) {
	INT32 mSecs;
	mSecs = (m_dayno - pBaseTime->m_dayno) * MsPerSecond * SecondsPerMinute * MinutesPerHour * HoursPerDay;
	mSecs += (m_msec - pBaseTime->m_msec);
	return mSecs;
}

CDateTime &CDateTime::operator=(const CDateTime &dt) {
	// Assume source is valid.
	// Validate( dt.m_dayno, dt.m_msec );
	m_dayno = dt.m_dayno;
	m_msec = dt.m_msec;

	return *this;
}

CDateTime &CDateTime::operator+=(const CDateTime &dt) {
	Add(dt.m_dayno, dt.m_msec);

	return *this;
}
