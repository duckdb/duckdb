/**************************************************************
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 *************************************************************/

#include <stdio.h>
#include <string.h>
#include <math.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/time.h>
#endif
#include <ctime> 
#include "nf_calendar.h"
#include "nf_localedata.h"


namespace duckdb_excel {

//------------- calendar.cxx ------------------------------------

#define erDUMP_ICU_CALENDAR  0
#define erDUMP_I18N_CALENDAR 0
#define DUMP_ICU_CAL_MSG(x)
#define DUMP_I18N_CAL_MSG(x)

namespace CalendarDisplayIndex {
/// name of an AM/PM value
const short CDI_AM_PM = 0;
/// name of a day of week
const short CDI_DAY = 1;
/// name of a month
const short CDI_MONTH = 2;
/// name of a year (if used for a specific calendar)
const short CDI_YEAR = 3;
/// name of an era, like BC/AD
const short CDI_ERA = 4;
} // namespace CalendarDisplayIndex

const double MILLISECONDS_PER_DAY = 1000.0 * 60.0 * 60.0 * 24.0;

Calendar::Calendar(LocaleData *pFormatterP) {
	pFormatter = pFormatterP;
	init(NULL);
	pNullDate = new Date(30, 12, 1899);
	memset(fieldValue, 0, sizeof(fieldValue));
}
Calendar::Calendar(Era *_eraArray) {
	init(_eraArray);
	pNullDate = new Date(30, 12, 1899);
	memset(fieldValue, 0, sizeof(fieldValue));
}
void Calendar::init(Era *_eraArray) {
	eraArray = _eraArray;
	aEpochStart = DateTime(Date(1, 1, 1970));
	timeInDays = 0.0;
}

Calendar::~Calendar() {
	delete pNullDate;
}

void Calendar::setValue(sal_Int16 fieldIndex, sal_Int16 value) {
	if (fieldIndex < 0 || FIELD_INDEX_COUNT <= fieldIndex) {
		return;
	}
	fieldSet |= (1 << fieldIndex);
	fieldValue[fieldIndex] = value;
}

bool Calendar::getCombinedOffset(sal_Int32 &o_nOffset, sal_Int16 nParentFieldIndex, sal_Int16 nChildFieldIndex) const {
	o_nOffset = 0;
	bool bFieldsSet = false;
	if (fieldSet & (1 << nParentFieldIndex)) {
		bFieldsSet = true;
		o_nOffset = static_cast<sal_Int32>(fieldValue[nParentFieldIndex]) * 60000;
	}
	if (fieldSet & (1 << nChildFieldIndex)) {
		bFieldsSet = true;
		if (o_nOffset < 0)
			o_nOffset -= static_cast<sal_uInt16>(fieldValue[nChildFieldIndex]);
		else
			o_nOffset += static_cast<sal_uInt16>(fieldValue[nChildFieldIndex]);
	}
	return bFieldsSet;
}

bool Calendar::getZoneOffset(sal_Int32 &o_nOffset) const {
	return getCombinedOffset(o_nOffset, CalendarFieldIndex::ZONE_OFFSET, CalendarFieldIndex::ZONE_OFFSET_SECOND_MILLIS);
}

bool Calendar::getDSTOffset(sal_Int32 &o_nOffset) const {
	return getCombinedOffset(o_nOffset, CalendarFieldIndex::DST_OFFSET, CalendarFieldIndex::DST_OFFSET_SECOND_MILLIS);
}

void Calendar::submitFields() {
	sal_Int32 nZoneOffset, nDSTOffset;
	getZoneOffset(nZoneOffset);
	getDSTOffset(nDSTOffset);
}

void Calendar::submitValues(sal_Int32 nYear, sal_Int32 nMonth, sal_Int32 nDay, sal_Int32 nHour, sal_Int32 nMinute,
                            sal_Int32 nSecond, sal_Int32 nMilliSecond, sal_Int32 nZone, sal_Int32 nDST) {
	submitFields();
}

#if (0)
static void lcl_setCombinedOffsetFieldValues(sal_Int32 nValue, sal_Int16 rFieldSetValue[], sal_Int16 rFieldValue[],
                                             sal_Int16 nParentFieldIndex, sal_Int16 nChildFieldIndex) {
	sal_Int32 nTrunc = nValue / 60000;
	rFieldSetValue[nParentFieldIndex] = rFieldValue[nParentFieldIndex] = static_cast<sal_Int16>(nTrunc);
	sal_uInt16 nMillis = static_cast<sal_uInt16>(abs(nValue - nTrunc * 60000));
	rFieldSetValue[nChildFieldIndex] = rFieldValue[nChildFieldIndex] = static_cast<sal_Int16>(nMillis);
}
#endif

sal_Int16 Calendar::getValue(sal_Int16 fieldIndex) {
	if (fieldIndex < 0 || FIELD_INDEX_COUNT <= fieldIndex) {
		return -1;
	}

	return fieldValue[fieldIndex];
}

bool Calendar::isValid() {
	return true;
}

static sal_Int32 DisplayCode2FieldIndex(sal_Int32 nCalendarDisplayCode) {
	switch (nCalendarDisplayCode) {
	case CalendarDisplayCode::SHORT_DAY:
	case CalendarDisplayCode::LONG_DAY:
		return CalendarFieldIndex::DAY_OF_MONTH;
	case CalendarDisplayCode::SHORT_DAY_NAME:
	case CalendarDisplayCode::LONG_DAY_NAME:
		return CalendarFieldIndex::DAY_OF_WEEK;
	case CalendarDisplayCode::SHORT_QUARTER:
	case CalendarDisplayCode::LONG_QUARTER:
	case CalendarDisplayCode::SHORT_MONTH:
	case CalendarDisplayCode::LONG_MONTH:
	case CalendarDisplayCode::SHORT_MONTH_NAME:
	case CalendarDisplayCode::LONG_MONTH_NAME:
		return CalendarFieldIndex::CFI_MONTH;
	case CalendarDisplayCode::SHORT_YEAR:
	case CalendarDisplayCode::LONG_YEAR:
		return CalendarFieldIndex::CFI_YEAR;
	case CalendarDisplayCode::SHORT_ERA:
	case CalendarDisplayCode::LONG_ERA:
		return CalendarFieldIndex::ERA;
	case CalendarDisplayCode::SHORT_YEAR_AND_ERA:
	case CalendarDisplayCode::LONG_YEAR_AND_ERA:
		return CalendarFieldIndex::CFI_YEAR;
	default:
		return 0;
	}
}

std::wstring Calendar::getDisplayName(sal_Int16 displayIndex, sal_Int16 idx, sal_Int16 nameType) {
	std::wstring aStr;

	switch (displayIndex) {
	case CalendarDisplayIndex::CDI_AM_PM: /* ==0 */
		if (idx == 0)
			aStr = pFormatter->getTimeAM();
		else if (idx == 1)
			aStr = pFormatter->getTimePM();
		else
			return L"";
		break;
	case CalendarDisplayIndex::CDI_DAY:
		if (idx >= pFormatter->getDayOfWeekSize())
			return L"";
		if (nameType == 0)
			aStr = pFormatter->getDayOfWeekAbbrvName(idx);
		else if (nameType == 1)
			aStr = pFormatter->getDayOfWeekFullName(idx);
		else
			return L"";
		break;
	case CalendarDisplayIndex::CDI_MONTH:
		if (idx >= pFormatter->getMonthsOfYearSize())
			return L"";
		if (nameType == 0)
			aStr = pFormatter->getMonthsOfYearAbbrvName(idx);
		else if (nameType == 1)
			aStr = pFormatter->getMonthsOfYearFullName(idx);
		else
			return L"";
		break;
	case CalendarDisplayIndex::CDI_ERA:
		if (idx >= pFormatter->getEraSize())
			return L"";
		if (nameType == 0)
			aStr = pFormatter->getEraAbbrvName(idx);
		else if (nameType == 1)
			aStr = pFormatter->getEraFullName(idx);
		else
			return L"";
		break;
	case CalendarDisplayIndex::CDI_YEAR:
		break;
	default:
		return L"";
	}
	return aStr;
}

// Methods in XExtendedCalendar
std::wstring Calendar::getDisplayString(sal_Int32 nCalendarDisplayCode, sal_Int16 nNativeNumberMode) {
	sal_Int16 value = getValue((sal_Int16)(DisplayCode2FieldIndex(nCalendarDisplayCode)));
	std::wstring aOUStr;

	if (nCalendarDisplayCode == CalendarDisplayCode::SHORT_QUARTER ||
	    nCalendarDisplayCode == CalendarDisplayCode::LONG_QUARTER) {
		sal_Int16 quarter = value / 3;
		// Since this base class method may be called by derived calendar
		// classes where a year consists of more than 12 months we need a check
		// to not run out of bounds of reserved quarter words. Perhaps a more
		// clean way (instead of dividing by 3) would be to first get the
		// number of months, divide by 4 and then use that result to divide the
		// actual month value.
		if (quarter > 3)
			quarter = 3;
		quarter = (sal_Int16)(quarter + ((nCalendarDisplayCode == CalendarDisplayCode::SHORT_QUARTER)
		                                     ? reservedWords::QUARTER1_ABBREVIATION
		                                     : reservedWords::QUARTER1_WORD));
		aOUStr = pFormatter->getReservedWord(quarter);
	} else {
		// The "#100211# - checked" comments serve for detection of "use of
		// sprintf is safe here" conditions. An sprintf encountered without
		// having that comment triggers alarm ;-)
		wchar_t aStr[10];
		switch (nCalendarDisplayCode) {
		case CalendarDisplayCode::SHORT_MONTH:
			value += 1; // month is zero based
			            // fall thru
		case CalendarDisplayCode::SHORT_DAY:
			swprintf(aStr, sizeof(aStr) / sizeof(wchar_t), L"%d", value); // #100211# - checked
			break;
		case CalendarDisplayCode::LONG_YEAR:
			// if (aCalendar.Name.equalsAscii("gengou"))
			//    swprintf(aStr, L"%02d", value);     // #100211# - checked
			// else
			swprintf(aStr, sizeof(aStr) / sizeof(wchar_t), L"%d", value); // #100211# - checked
			break;
		case CalendarDisplayCode::LONG_MONTH:
			value += 1;                                                     // month is zero based
			swprintf(aStr, sizeof(aStr) / sizeof(wchar_t), L"%02d", value); // #100211# - checked
			break;
		case CalendarDisplayCode::SHORT_YEAR:
			// Take last 2 digits, or only one if value<10, for example,
			// in case of the Gengou calendar.
			// #i116701# For values in non-Gregorian era years use all
			// digits.
			if (value < 100 || eraArray)
				swprintf(aStr, sizeof(aStr) / sizeof(wchar_t), L"%d", value); // #100211# - checked
			else
				swprintf(aStr, sizeof(aStr) / sizeof(wchar_t), L"%02d", value % 100); // #100211# - checked
			break;
		case CalendarDisplayCode::LONG_DAY:
			swprintf(aStr, sizeof(aStr) / sizeof(wchar_t), L"%02d", value); // #100211# - checked
			break;

		case CalendarDisplayCode::SHORT_DAY_NAME:
			return getDisplayName(CalendarDisplayIndex::CDI_DAY, value, 0);
		case CalendarDisplayCode::LONG_DAY_NAME:
			return getDisplayName(CalendarDisplayIndex::CDI_DAY, value, 1);
		case CalendarDisplayCode::SHORT_MONTH_NAME:
			return getDisplayName(CalendarDisplayIndex::CDI_MONTH, value, 0);
		case CalendarDisplayCode::LONG_MONTH_NAME:
			return getDisplayName(CalendarDisplayIndex::CDI_MONTH, value, 1);
		case CalendarDisplayCode::SHORT_ERA:
			return getDisplayName(CalendarDisplayIndex::CDI_ERA, value, 0);
		case CalendarDisplayCode::LONG_ERA:
			return getDisplayName(CalendarDisplayIndex::CDI_ERA, value, 1);

		case CalendarDisplayCode::SHORT_YEAR_AND_ERA:
			return getDisplayString(CalendarDisplayCode::SHORT_ERA, nNativeNumberMode) +
			       getDisplayString(CalendarDisplayCode::SHORT_YEAR, nNativeNumberMode);

		case CalendarDisplayCode::LONG_YEAR_AND_ERA:
			return getDisplayString(CalendarDisplayCode::LONG_ERA, nNativeNumberMode) +
			       getDisplayString(CalendarDisplayCode::LONG_YEAR, nNativeNumberMode);

		default:
			return L"";
		}
		aOUStr = aStr;
	}
	return aOUStr;
}

void Calendar::setDateTime(double timeInDays_val) {
	double fDiff = DateTime(*(GetNullDate())) - getEpochStart();
	timeInDays = timeInDays_val - fDiff;
	DateTime dt;
	dt += timeInDays;
	setValue(CalendarFieldIndex::AM_PM, dt.GetHour() < 12 ? 0 : 1);
	setValue(CalendarFieldIndex::DAY_OF_MONTH, dt.GetDay());
	setValue(CalendarFieldIndex::DAY_OF_WEEK, dt.GetDayOfWeek() + 1);
	setValue(CalendarFieldIndex::DAY_OF_YEAR, dt.GetDayOfYear());
	setValue(CalendarFieldIndex::CFI_HOUR, dt.GetHour());
	setValue(CalendarFieldIndex::CFI_MINUTE, dt.GetMin());
	setValue(CalendarFieldIndex::CFI_SECOND, dt.GetSec());
	setValue(CalendarFieldIndex::CFI_MILLISECOND, dt.Get100Sec() * 10);
	setValue(CalendarFieldIndex::WEEK_OF_YEAR, dt.GetWeekOfYear());
	setValue(CalendarFieldIndex::CFI_YEAR, dt.GetYear());
	setValue(CalendarFieldIndex::CFI_MONTH, dt.GetMonth() - 1);
}

double Calendar::getDateTime() {
	return timeInDays;
}

sal_Int32 Calendar::getCombinedOffsetInMillis(sal_Int16 nParentFieldIndex, sal_Int16 nChildFieldIndex) {
	sal_Int32 nOffset = 0;
	nOffset = (sal_Int32)(getValue(nParentFieldIndex)) * 60000;
	sal_Int16 nSecondMillis = getValue(nChildFieldIndex);
	if (nOffset < 0)
		nOffset -= static_cast<sal_uInt16>(nSecondMillis);
	else
		nOffset += static_cast<sal_uInt16>(nSecondMillis);
	return nOffset;
}

sal_Int32 Calendar::getZoneOffsetInMillis() {
	return getCombinedOffsetInMillis(CalendarFieldIndex::ZONE_OFFSET, CalendarFieldIndex::ZONE_OFFSET_SECOND_MILLIS);
}

sal_Int32 Calendar::getDSTOffsetInMillis() {
	return getCombinedOffsetInMillis(CalendarFieldIndex::DST_OFFSET, CalendarFieldIndex::DST_OFFSET_SECOND_MILLIS);
}

void Calendar::setLocalDateTime(double nTimeInDays) {
	setDateTime(nTimeInDays);
	sal_Int32 nZone1 = getZoneOffsetInMillis();
	sal_Int32 nDST1 = getDSTOffsetInMillis();
	double nLoc = nTimeInDays - (double)(nZone1 + nDST1) / MILLISECONDS_PER_DAY;
	setDateTime(nLoc);
	sal_Int32 nZone2 = getZoneOffsetInMillis();
	sal_Int32 nDST2 = getDSTOffsetInMillis();
	if (nDST1 != nDST2) {
		nLoc = nTimeInDays - (double)(nZone2 + nDST2) / MILLISECONDS_PER_DAY;
		setDateTime(nLoc);
		sal_Int32 nDST3 = getDSTOffsetInMillis();
		if (nDST2 != nDST3 && !nDST3) {
			nLoc = nTimeInDays - (double)(nZone2 + nDST3) / MILLISECONDS_PER_DAY;
			setDateTime(nLoc);
		}
	}
}

double Calendar::getLocalDateTime() {
	double nTimeInDays = getDateTime();
	sal_Int32 nZone = getZoneOffsetInMillis();
	sal_Int32 nDST = getDSTOffsetInMillis();
	nTimeInDays += (double)(nZone + nDST) / MILLISECONDS_PER_DAY;
	return nTimeInDays;
}

void Calendar::ChangeNullDate(const sal_uInt16 Day, const sal_uInt16 Month, const sal_uInt16 Year) {
	if (pNullDate)
		*pNullDate = Date(Day, Month, Year);
	else
		pNullDate = new Date(Day, Month, Year);
}


//------------- datetime.cxx ------------------------------------

bool DateTime::IsBetween( const DateTime& rFrom,
                          const DateTime& rTo ) const
{
    if ( (*this >= rFrom) && (*this <= rTo) )
        return true;
    else
        return false;
}

bool DateTime::operator >( const DateTime& rDateTime ) const
{
    if ( (Date::operator>( rDateTime )) ||
         (Date::operator==( rDateTime ) && Time::operator>( rDateTime )) )
        return true;
    else
        return false;
}

bool DateTime::operator <( const DateTime& rDateTime ) const
{
    if ( (Date::operator<( rDateTime )) ||
         (Date::operator==( rDateTime ) && Time::operator<( rDateTime )) )
        return true;
    else
        return false;
}

bool DateTime::operator >=( const DateTime& rDateTime ) const
{
    if ( (Date::operator>( rDateTime )) ||
         (Date::operator==( rDateTime ) && Time::operator>=( rDateTime )) )
        return true;
    else
        return false;
}

bool DateTime::operator <=( const DateTime& rDateTime ) const
{
    if ( (Date::operator<( rDateTime )) ||
         (Date::operator==( rDateTime ) && Time::operator<=( rDateTime )) )
        return true;
    else
        return false;
}

long DateTime::GetSecFromDateTime( const Date& rDate ) const
{
    if ( Date::operator<( rDate ) )
        return 0;
    else
    {
        long nSec = Date( *this ) - rDate;
        nSec *= 24UL*60*60;
        long nHour = GetHour();
        long nMin  = GetMin();
        nSec += (nHour*3600)+(nMin*60)+GetSec();
        return nSec;
    }
}

void DateTime::MakeDateTimeFromSec( const Date& rDate, sal_uIntPtr nSec )
{
    long nDays = nSec / (24UL*60*60);
	((Date*)this)->operator=( rDate );
    nSec -= nDays * (24UL*60*60);
	sal_uInt16 nMin = (sal_uInt16)(nSec / 60);
	nSec -= nMin * 60;
    ((Time*)this)->operator=( Time( 0, nMin, (sal_uInt16)nSec ) );
    operator+=( nDays );
}

DateTime& DateTime::operator +=( const Time& rTime )
{
    Time aTime = *this;
    aTime += rTime;
    sal_uInt16 nHours = aTime.GetHour();
    if ( aTime.GetTime() > 0 )
    {
        while ( nHours >= 24 )
        {
            Date::operator++();
            nHours -= 24;
        }
        aTime.SetHour( nHours );
    }
    else if ( aTime.GetTime() != 0 )
    {
        while ( nHours >= 24 )
        {
            Date::operator--();
            nHours -= 24;
        }
        Date::operator--();
        aTime = Time( 24, 0, 0 )+aTime;
    }
    Time::operator=( aTime );

    return *this;
}

DateTime& DateTime::operator -=( const Time& rTime )
{
    Time aTime = *this;
    aTime -= rTime;
    sal_uInt16 nHours = aTime.GetHour();
    if ( aTime.GetTime() > 0 )
    {
        while ( nHours >= 24 )
        {
            Date::operator++();
            nHours -= 24;
        }
        aTime.SetHour( nHours );
    }
    else if ( aTime.GetTime() != 0 )
    {
        while ( nHours >= 24 )
        {
            Date::operator--();
            nHours -= 24;
        }
        Date::operator--();
        aTime = Time( 24, 0, 0 )+aTime;
    }
    Time::operator=( aTime );

    return *this;
}

DateTime operator +( const DateTime& rDateTime, long nDays )
{
    DateTime aDateTime( rDateTime );
    aDateTime += nDays;
    return aDateTime;
}

DateTime operator -( const DateTime& rDateTime, long nDays )
{
    DateTime aDateTime( rDateTime );
    aDateTime -= nDays;
    return aDateTime;
}

DateTime operator +( const DateTime& rDateTime, const Time& rTime )
{
    DateTime aDateTime( rDateTime );
    aDateTime += rTime;
    return aDateTime;
}

DateTime operator -( const DateTime& rDateTime, const Time& rTime )
{
    DateTime aDateTime( rDateTime );
    aDateTime -= rTime;
    return aDateTime;
}

DateTime& DateTime::operator +=( double fTimeInDays )
{
	double fInt, fFrac;
	if ( fTimeInDays < 0.0 )
	{
		fInt = ceil( fTimeInDays );
		fFrac = fInt <= fTimeInDays ? 0.0 : fTimeInDays - fInt;
	}
	else
	{
		fInt = floor( fTimeInDays );
		fFrac = fInt >= fTimeInDays ? 0.0 : fTimeInDays - fInt;
	}
	Date::operator+=( long(fInt) );		// full days
	if ( fFrac )
	{
		Time aTime(0);	// default ctor calls system time, we don't need that
		fFrac *= 24UL * 60 * 60 * 1000;		// time expressed in milliseconds
		aTime.MakeTimeFromMS( long(fFrac) );	// method handles negative ms
		operator+=( aTime );
	}
	return *this;
}

DateTime operator +( const DateTime& rDateTime, double fTimeInDays )
{
    DateTime aDateTime( rDateTime );
	aDateTime += fTimeInDays;
	return aDateTime;
}

double operator -( const DateTime& rDateTime1, const DateTime& rDateTime2 )
{
	long nDays = (const Date&) rDateTime1 - (const Date&) rDateTime2;
	long nTime = rDateTime1.GetMSFromTime() - rDateTime2.GetMSFromTime();
	if ( nTime )
	{
		double fTime = double(nTime);
		fTime /= 24UL * 60 * 60 * 1000;	// convert from milliseconds to fraction
		if ( nDays < 0 && fTime > 0.0 )
			fTime = 1.0 - fTime;
		return double(nDays) + fTime;
	}
	return double(nDays);
}

void DateTime::GetWin32FileDateTime( sal_uInt32 & rLower, sal_uInt32 & rUpper )
{
    const sal_Int64 a100nPerSecond = SAL_CONST_INT64( 10000000 );
    const sal_Int64 a100nPerDay = a100nPerSecond * sal_Int64( 60 * 60 * 24 );

    sal_Int64 nYears = GetYear() - 1601;
    sal_Int64 nDays =
        nYears * 365 +
        nYears / 4 - nYears / 100 + nYears / 400 +
        GetDayOfYear() - 1;

    sal_Int64 aTime =
        a100nPerDay * nDays +
        a100nPerSecond * (
                sal_Int64( GetSec() ) +
                60 * sal_Int64( GetMin() ) +
                60 * 60 * sal_Int64( GetHour() ) );

    rLower = sal_uInt32( aTime % SAL_CONST_UINT64( 0x100000000 ) );
    rUpper = sal_uInt32( aTime / SAL_CONST_UINT64( 0x100000000 ) );
}

DateTime DateTime::CreateFromWin32FileDateTime( const sal_uInt32 & rLower, const sal_uInt32 & rUpper )
{
    const sal_Int64 a100nPerSecond = SAL_CONST_INT64( 10000000 );
    const sal_Int64 a100nPerDay = a100nPerSecond * sal_Int64( 60 * 60 * 24 );

    sal_Int64 aTime = sal_Int64(
            sal_uInt64( rUpper ) * SAL_CONST_UINT64( 0x100000000 ) +
            sal_uInt64( rLower ) );

    sal_Int64 nDays = aTime / a100nPerDay;
    sal_Int64 nYears =
        ( nDays -
          ( nDays / ( 4 * 365 ) ) +
          ( nDays / ( 100 * 365 ) ) -
          ( nDays / ( 400 * 365 ) ) ) / 365;
    nDays -= nYears * 365 + nYears / 4 - nYears / 100 + nYears / 400;

    sal_uInt16 nMonths = 0;
    for( sal_Int64 nDaysCount = nDays; nDaysCount >= 0; )
    {
        nDays = nDaysCount;
        nMonths ++;
        nDaysCount -= Date(
            1, nMonths, (sal_uInt16)(1601 + nYears) ).
            GetDaysInMonth();
    }

    Date _aDate(
        (sal_uInt16)( nDays + 1 ), nMonths,
        (sal_uInt16)(nYears + 1601) );
    Time _aTime( sal_uIntPtr( ( aTime / ( a100nPerSecond * 60 * 60 ) ) % sal_Int64( 24 ) ),
            sal_uIntPtr( ( aTime / ( a100nPerSecond * 60 ) ) % sal_Int64( 60 ) ),
            sal_uIntPtr( ( aTime / ( a100nPerSecond ) ) % sal_Int64( 60 ) ) );

    return DateTime( _aDate, _aTime );
}


// ----------------------- tdate.cxx -------------------------------------

static sal_uInt16 aDaysInMonth[12] = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

#define MAX_DAYS	3636532

inline bool ImpIsLeapYear( sal_uInt16 nYear )
{
	return (
                 ( ((nYear % 4) == 0) && ((nYear % 100) != 0) ) ||
                 ( (nYear % 400) == 0 )
               );
}

inline sal_uInt16 DaysInMonth( sal_uInt16 nMonth, sal_uInt16 nYear )
{
	if ( nMonth != 2 )
		return aDaysInMonth[nMonth-1];
	else
	{
		if (ImpIsLeapYear(nYear))
			return aDaysInMonth[nMonth-1] + 1;
		else
			return aDaysInMonth[nMonth-1];
	}
}

long Date::DateToDays( sal_uInt16 nDay, sal_uInt16 nMonth, sal_uInt16 nYear )
{
	long nDays;

	nDays = ((sal_uIntPtr)nYear-1) * 365;
	nDays += ((nYear-1) / 4) - ((nYear-1) / 100) + ((nYear-1) / 400);
	for( sal_uInt16 i = 1; i < nMonth; i++ )
		nDays += DaysInMonth(i,nYear);
	nDays += nDay;
	return nDays;
}

static void DaysToDate( long nDays,
						sal_uInt16& rDay, sal_uInt16& rMonth, sal_uInt16& rYear )
{
	long	nTempDays;
	long	i = 0;
	bool	bCalc;

	do
	{
		nTempDays = (long)nDays;
		rYear = (sal_uInt16)((nTempDays / 365) - i);
		nTempDays -= ((sal_uIntPtr)rYear-1) * 365;
		nTempDays -= ((rYear-1) / 4) - ((rYear-1) / 100) + ((rYear-1) / 400);
		bCalc = false;
		if ( nTempDays < 1 )
		{
			i++;
			bCalc = true;
		}
		else
		{
			if ( nTempDays > 365 )
			{
				if ( (nTempDays != 366) || !ImpIsLeapYear( rYear ) )
				{
					i--;
					bCalc = true;
				}
			}
		}
	}
	while ( bCalc );

	rMonth = 1;
	while ( (sal_uIntPtr)nTempDays > DaysInMonth( rMonth, rYear ) )
	{
		nTempDays -= DaysInMonth( rMonth, rYear );
		rMonth++;
	}
	rDay = (sal_uInt16)nTempDays;
}

Date::Date()
{
	time_t	   nTmpTime;
	struct tm aTime;

	nTmpTime = time( 0 );
	nDate = 30 + 1200 + (((sal_uIntPtr)1899)*10000);
}

void Date::SetDay( sal_uInt16 nNewDay )
{
	sal_uIntPtr  nMonth  = GetMonth();
	sal_uIntPtr  nYear   = GetYear();

	nDate = ((sal_uIntPtr)(nNewDay%100)) + (nMonth*100) + (nYear*10000);
}

void Date::SetMonth( sal_uInt16 nNewMonth )
{
	sal_uIntPtr  nDay 	 = GetDay();
	sal_uIntPtr  nYear	 = GetYear();

	nDate = nDay + (((sal_uIntPtr)(nNewMonth%100))*100) + (nYear*10000);
}

void Date::SetYear( sal_uInt16 nNewYear )
{
	sal_uIntPtr  nDay 	= GetDay();
	sal_uIntPtr  nMonth	= GetMonth();

	nDate = nDay + (nMonth*100) + (((sal_uIntPtr)(nNewYear%10000))*10000);
}

DayOfWeek Date::GetDayOfWeek() const
{
	return (DayOfWeek)((sal_uIntPtr)(DateToDays( GetDay(), GetMonth(), GetYear() )-1) % 7);
}

sal_uInt16 Date::GetDayOfYear() const
{
	sal_uInt16 nDay = GetDay();
	for( sal_uInt16 i = 1; i < GetMonth(); i++ )
         nDay = nDay + DaysInMonth( i, GetYear() );   // += yields a warning on MSVC, so don't use it
	return nDay;
}

sal_uInt16 Date::GetWeekOfYear( DayOfWeek eStartDay,
							sal_Int16 nMinimumNumberOfDaysInWeek ) const
{
	short nWeek;
	short n1WDay = (short)Date( 1, 1, GetYear() ).GetDayOfWeek();
	short nDayOfYear = (short)GetDayOfYear();

	// Days of the week start at 0, so subtract one
	nDayOfYear--;
	// StartDay consider
	n1WDay = (n1WDay+(7-(short)eStartDay)) % 7;

    if (nMinimumNumberOfDaysInWeek < 1 || 7 < nMinimumNumberOfDaysInWeek)
    {
        //DBG_ERRORFILE("Date::GetWeekOfYear: invalid nMinimumNumberOfDaysInWeek");
        nMinimumNumberOfDaysInWeek = 4;
    }

	if ( nMinimumNumberOfDaysInWeek == 1 )
	{
		nWeek = ((n1WDay+nDayOfYear)/7) + 1;
		// 53rd week only if we are not already in the first
		// week of the new year lie
		if ( nWeek == 54 )
			nWeek = 1;
		else if ( nWeek == 53 )
		{
			short nDaysInYear = (short)GetDaysInYear();
			short nDaysNextYear = (short)Date( 1, 1, GetYear()+1 ).GetDayOfWeek();
			nDaysNextYear = (nDaysNextYear+(7-(short)eStartDay)) % 7;
			if ( nDayOfYear > (nDaysInYear-nDaysNextYear-1) )
				nWeek = 1;
		}
	}
	else if ( nMinimumNumberOfDaysInWeek == 7 )
	{
		nWeek = ((n1WDay+nDayOfYear)/7);
		// First week of a year corresponds to the last week of previous year
		if ( nWeek == 0 )
		{
			Date aLastDatePrevYear( 31, 12, GetYear()-1 );
			nWeek = aLastDatePrevYear.GetWeekOfYear( eStartDay, nMinimumNumberOfDaysInWeek );
		}
	}
	else // ( nMinimumNumberOfDaysInWeek == somehing_else, commentary examples for 4 )
	{
		// x_monday - thursday
		if ( n1WDay < nMinimumNumberOfDaysInWeek )
			nWeek = 1;
		// friday
		else if ( n1WDay == nMinimumNumberOfDaysInWeek )
			nWeek = 53;
		// saturday
		else if ( n1WDay == nMinimumNumberOfDaysInWeek + 1 )
		{
			// year after leap year
			if ( Date( 1, 1, GetYear()-1 ).IsLeapYear() )
				nWeek = 53;
			else
				nWeek = 52;
		}
		// sunday
		else
			nWeek = 52;

		if ( (nWeek == 1) || (nDayOfYear + n1WDay > 6) )
		{
			if ( nWeek == 1 )
				nWeek += (nDayOfYear + n1WDay) / 7;
			else
				nWeek = (nDayOfYear + n1WDay) / 7;
			if ( nWeek == 53 )
			{
				// next x_Sunday == first x_Sunday in the new year
				//					   == same week
				long nTempDays = DateToDays( GetDay(), GetMonth(), GetYear() );
				nTempDays +=  6 - (GetDayOfWeek()+(7-(short)eStartDay)) % 7;
				sal_uInt16	nDay;
				sal_uInt16	nMonth;
				sal_uInt16	nYear;
				DaysToDate( nTempDays, nDay, nMonth, nYear );
				nWeek = Date( nDay, nMonth, nYear ).GetWeekOfYear( eStartDay, nMinimumNumberOfDaysInWeek );
			}
		}
	}

	return (sal_uInt16)nWeek;
}

sal_uInt16 Date::GetDaysInMonth() const
{
	return DaysInMonth( GetMonth(), GetYear() );
}

bool Date::IsLeapYear() const
{
	sal_uInt16 nYear = GetYear();
	return ImpIsLeapYear( nYear );
}

bool Date::IsValid() const
{
	sal_uInt16 nDay   = GetDay();
	sal_uInt16 nMonth = GetMonth();
	sal_uInt16 nYear  = GetYear();

	if ( !nMonth || (nMonth > 12) )
		return false;
	if ( !nDay || (nDay > DaysInMonth( nMonth, nYear )) )
		return false;
	else if ( nYear <= 1582 )
	{
		if ( nYear < 1582 )
			return false;
		else if ( nMonth < 10 )
			return false;
		else if ( (nMonth == 10) && (nDay < 15) )
			return false;
	}

	return true;
}

Date& Date::operator +=( long nDays )
{
	sal_uInt16	nDay;
	sal_uInt16	nMonth;
	sal_uInt16	nYear;
	long	nTempDays = DateToDays( GetDay(), GetMonth(), GetYear() );

	nTempDays += nDays;
	if ( nTempDays > MAX_DAYS )
		nDate = 31 + (12*100) + (((sal_uIntPtr)9999)*10000);
	else if ( nTempDays <= 0 )
		nDate = 1 + 100;
	else
	{
		DaysToDate( nTempDays, nDay, nMonth, nYear );
		nDate = ((sal_uIntPtr)nDay) + (((sal_uIntPtr)nMonth)*100) + (((sal_uIntPtr)nYear)*10000);
	}

	return *this;
}

Date& Date::operator -=( long nDays )
{
	sal_uInt16	nDay;
	sal_uInt16	nMonth;
	sal_uInt16	nYear;
	long	nTempDays = DateToDays( GetDay(), GetMonth(), GetYear() );

	nTempDays -= nDays;
	if ( nTempDays > MAX_DAYS )
		nDate = 31 + (12*100) + (((sal_uIntPtr)9999)*10000);
	else if ( nTempDays <= 0 )
		nDate = 1 + 100;
	else
	{
		DaysToDate( nTempDays, nDay, nMonth, nYear );
		nDate = ((sal_uIntPtr)nDay) + (((sal_uIntPtr)nMonth)*100) + (((sal_uIntPtr)nYear)*10000);
	}

	return *this;
}

Date& Date::operator ++()
{
	sal_uInt16	nDay;
	sal_uInt16	nMonth;
	sal_uInt16	nYear;
	long	nTempDays = DateToDays( GetDay(), GetMonth(), GetYear() );

	if ( nTempDays < MAX_DAYS )
	{
		nTempDays++;
		DaysToDate( nTempDays, nDay, nMonth, nYear );
		nDate = ((sal_uIntPtr)nDay) + (((sal_uIntPtr)nMonth)*100) + (((sal_uIntPtr)nYear)*10000);
	}

	return *this;
}

Date& Date::operator --()
{
	sal_uInt16	nDay;
	sal_uInt16	nMonth;
	sal_uInt16	nYear;
	long	nTempDays = DateToDays( GetDay(), GetMonth(), GetYear() );

	if ( nTempDays > 1 )
	{
		nTempDays--;
		DaysToDate( nTempDays, nDay, nMonth, nYear );
		nDate = ((sal_uIntPtr)nDay) + (((sal_uIntPtr)nMonth)*100) + (((sal_uIntPtr)nYear)*10000);
	}
	return *this;
}

#ifndef MPW33

Date Date::operator ++( int )
{
	Date aOldDate = *this;
	Date::operator++();
	return aOldDate;
}

Date Date::operator --( int )
{
	Date aOldDate = *this;
	Date::operator--();
	return aOldDate;
}

#endif

Date operator +( const Date& rDate, long nDays )
{
	Date aDate( rDate );
	aDate += nDays;
	return aDate;
}

Date operator -( const Date& rDate, long nDays )
{
	Date aDate( rDate );
	aDate -= nDays;
	return aDate;
}

long operator -( const Date& rDate1, const Date& rDate2 )
{
    sal_uIntPtr  nTempDays1 = Date::DateToDays( rDate1.GetDay(), rDate1.GetMonth(),
									rDate1.GetYear() );
    sal_uIntPtr  nTempDays2 = Date::DateToDays( rDate2.GetDay(), rDate2.GetMonth(),
									rDate2.GetYear() );
	return nTempDays1 - nTempDays2;
}


// -------------------------- ttime.cxx ----------------------------------------

static sal_Int32 TimeToSec100( const Time& rTime )
{
	short  nSign   = (rTime.GetTime() >= 0) ? +1 : -1;
	sal_Int32   nHour   = rTime.GetHour();
	sal_Int32   nMin    = rTime.GetMin();
	sal_Int32   nSec    = rTime.GetSec();
	sal_Int32   n100Sec = rTime.Get100Sec();

//	Wegen Interal Compiler Error bei MSC, etwas komplizierter
//	return (n100Sec + (nSec*100) + (nMin*60*100) + (nHour*60*60*100) * nSign);

	sal_Int32 nRet = n100Sec;
	nRet	 += nSec*100;
	nRet	 += nMin*60*100;
	nRet	 += nHour*60*60*100;

	return (nRet * nSign);
}

static Time Sec100ToTime( sal_Int32 nSec100 )
{
	short nSign;
	if ( nSec100 < 0 )
	{
		nSec100 *= -1;
		nSign = -1;
	}
	else
		nSign = 1;

	Time aTime( 0, 0, 0, nSec100 );
	aTime.SetTime( aTime.GetTime() * nSign );
	return aTime;
}

Time::Time()
{
	nTime = 0;
}

Time::Time( const Time& rTime )
{
	nTime = rTime.nTime;
}

Time::Time( sal_uIntPtr nHour, sal_uIntPtr nMin, sal_uIntPtr nSec, sal_uIntPtr n100Sec )
{
	nSec	+= n100Sec / 100;
	n100Sec  = n100Sec % 100;
	nMin	+= nSec / 60;
	nSec	 = nSec % 60;
	nHour	+= nMin / 60;
	nMin	 = nMin % 60;

	nTime = (sal_Int32)(n100Sec + (nSec*100) + (nMin*10000) + (nHour*1000000));
}

void Time::SetHour( sal_uInt16 nNewHour )
{
	short  nSign	  = (nTime >= 0) ? +1 : -1;
	sal_Int32   nMin 	  = GetMin();
	sal_Int32   nSec 	  = GetSec();
	sal_Int32   n100Sec	  = Get100Sec();

	nTime = (n100Sec + (nSec*100) + (nMin*10000) +
			(((sal_Int32)nNewHour)*1000000)) * nSign;
}

void Time::SetMin( sal_uInt16 nNewMin )
{
	short  nSign	  = (nTime >= 0) ? +1 : -1;
	sal_Int32   nHour	  = GetHour();
	sal_Int32   nSec 	  = GetSec();
	sal_Int32   n100Sec	  = Get100Sec();

	nNewMin = nNewMin % 60;

	nTime = (n100Sec + (nSec*100) + (((sal_Int32)nNewMin)*10000) +
			(nHour*1000000)) * nSign;
}

void Time::SetSec( sal_uInt16 nNewSec )
{
	short       nSign	  = (nTime >= 0) ? +1 : -1;
	sal_Int32   nHour	  = GetHour();
	sal_Int32   nMin 	  = GetMin();
	sal_Int32   n100Sec	  = Get100Sec();

	nNewSec = nNewSec % 60;

	nTime = (n100Sec + (((sal_Int32)nNewSec)*100) + (nMin*10000) +
			(nHour*1000000)) * nSign;
}

void Time::Set100Sec( sal_uInt16 nNew100Sec )
{
	short       nSign	  = (nTime >= 0) ? +1 : -1;
	sal_Int32   nHour	  = GetHour();
	sal_Int32   nMin 	  = GetMin();
	sal_Int32   nSec 	  = GetSec();

	nNew100Sec = nNew100Sec % 100;

	nTime = (((sal_Int32)nNew100Sec) + (nSec*100) + (nMin*10000) +
			(nHour*1000000)) * nSign;
}

sal_Int32 Time::GetMSFromTime() const
{
	short       nSign	  = (nTime >= 0) ? +1 : -1;
	sal_Int32   nHour	  = GetHour();
	sal_Int32   nMin 	  = GetMin();
	sal_Int32   nSec 	  = GetSec();
	sal_Int32   n100Sec	  = Get100Sec();

	return (((nHour*3600000)+(nMin*60000)+(nSec*1000)+(n100Sec*10))*nSign);
}

void Time::MakeTimeFromMS( sal_Int32 nMS )
{
	short nSign;
	if ( nMS < 0 )
	{
		nMS *= -1;
		nSign = -1;
	}
	else
		nSign = 1;

	Time aTime( 0, 0, 0, nMS/10 );
	SetTime( aTime.GetTime() * nSign );
}

double Time::GetTimeInDays() const
{
	short  nSign	  = (nTime >= 0) ? +1 : -1;
    double nHour      = GetHour();
    double nMin       = GetMin();
    double nSec       = GetSec();
    double n100Sec    = Get100Sec();

    return (nHour+(nMin/60)+(nSec/(60*60))+(n100Sec/(60*60*100))) / 24 * nSign;
}

Time& Time::operator =( const Time& rTime )
{
	nTime = rTime.nTime;
	return *this;
}

Time& Time::operator +=( const Time& rTime )
{
	nTime = Sec100ToTime( TimeToSec100( *this ) +
						  TimeToSec100( rTime ) ).GetTime();
	return *this;
}

Time& Time::operator -=( const Time& rTime )
{
	nTime = Sec100ToTime( TimeToSec100( *this ) -
						  TimeToSec100( rTime ) ).GetTime();
	return *this;
}

Time operator +( const Time& rTime1, const Time& rTime2 )
{
	return Sec100ToTime( TimeToSec100( rTime1 ) +
						 TimeToSec100( rTime2 ) );
}

Time operator -( const Time& rTime1, const Time& rTime2 )
{
	return Sec100ToTime( TimeToSec100( rTime1 ) -
						 TimeToSec100( rTime2 ) );
}

bool Time::IsEqualIgnore100Sec( const Time& rTime ) const
{
    sal_Int32 n1 = (nTime < 0 ? -Get100Sec() : Get100Sec() );
    sal_Int32 n2 = (rTime.nTime < 0 ? -rTime.Get100Sec() : rTime.Get100Sec() );
    return (nTime - n1) == (rTime.nTime - n2);
}

Time Time::GetUTCOffset()
{
#if defined( OS2 )
#undef timezone
	PM_DATETIME aDateTime;
	DosGetDateTime( &aDateTime );

	// Zeit zusammenbauen
	if ( aDateTime.timezone != -1  )
	{
		short nTempTime = (short)Abs( aDateTime.timezone );
		Time aTime( 0, (sal_uInt16)nTempTime );
		if ( aDateTime.timezone > 0 )
			aTime = -aTime;
		return aTime;
	}
	else
		return Time( 0 );
#elif defined( _WIN32 )
	TIME_ZONE_INFORMATION	aTimeZone;
	aTimeZone.Bias = 0;
	DWORD nTimeZoneRet = GetTimeZoneInformation( &aTimeZone );
	sal_Int32 nTempTime = aTimeZone.Bias;
	if ( nTimeZoneRet == TIME_ZONE_ID_STANDARD )
		nTempTime += aTimeZone.StandardBias;
	else if ( nTimeZoneRet == TIME_ZONE_ID_DAYLIGHT )
		nTempTime += aTimeZone.DaylightBias;
	Time aTime( 0, (sal_uInt16)abs( nTempTime ) );
	if ( nTempTime > 0 )
		aTime = -aTime;
	return aTime;
#else
	static sal_uIntPtr	nCacheTicks = 0;
	static sal_Int32 	nCacheSecOffset = -1;
	sal_uIntPtr			nTicks = Time::GetSystemTicks();
	time_t			nTime;
	tm 			    aTM;
	sal_Int32			nLocalTime;
	sal_Int32			nUTC;
	short			nTempTime;

	// Evt. Wert neu ermitteln
	if ( (nCacheSecOffset == -1)			||
		 ((nTicks - nCacheTicks) > 360000)	||
		 ( nTicks < nCacheTicks ) // handle overflow
		 )
	{
		nTime = time( 0 );
		localtime_r( &nTime, &aTM );
		nLocalTime = mktime( &aTM );
#if defined( SOLARIS )
		// Solaris gmtime_r() seems not to handle daylight saving time
		// flags correctly
		nUTC = nLocalTime + ( aTM.tm_isdst == 0 ? timezone : altzone );
#elif defined( LINUX )
		// Linux mktime() seems not to handle tm_isdst correctly
		nUTC = nLocalTime - aTM.tm_gmtoff;
#else
 		gmtime_r( &nTime, &aTM );
 		nUTC = mktime( &aTM );
#endif
 		nCacheTicks = nTicks;
 		nCacheSecOffset = (nLocalTime-nUTC) / 60;
	}

	nTempTime = (short)abs( nCacheSecOffset );
	Time aTime( 0, (sal_uInt16)nTempTime );
	if ( nCacheSecOffset < 0 )
		aTime = -aTime;
	return aTime;
#endif
}


sal_uIntPtr Time::GetSystemTicks()
{
#if defined _WIN32
	return (sal_uIntPtr)GetTickCount();
#elif defined( OS2 )
	sal_uIntPtr nClock;
	DosQuerySysInfo( QSV_MS_COUNT, QSV_MS_COUNT, &nClock, sizeof( nClock ) );
	return (sal_uIntPtr)nClock;
#else
	timeval tv;
	gettimeofday(&tv, 0);

	double fTicks = tv.tv_sec;
	fTicks *= 1000;
	fTicks += ((tv.tv_usec + 500) / 1000);

	fTicks = fmod(fTicks, double(ULONG_MAX));
	return sal_uIntPtr(fTicks);
#endif
}

sal_uIntPtr Time::GetProcessTicks()
{
#if defined _WIN32
	return (sal_uIntPtr)GetTickCount();
#elif defined( OS2 )
	sal_uIntPtr nClock;
	DosQuerySysInfo( QSV_MS_COUNT, QSV_MS_COUNT, &nClock, sizeof( nClock ) );
	return (sal_uIntPtr)nClock;
#else
	static sal_uIntPtr	nImplTicksPerSecond = 0;
	static double	dImplTicksPerSecond;
	static double	dImplTicksULONGMAX;
	sal_uIntPtr			nTicks = (sal_uIntPtr)clock();

	if ( !nImplTicksPerSecond )
	{
		nImplTicksPerSecond = CLOCKS_PER_SEC;
		dImplTicksPerSecond = nImplTicksPerSecond;
		dImplTicksULONGMAX	= (double)(sal_uIntPtr)ULONG_MAX;
	}

	double fTicks = nTicks;
	fTicks *= 1000;
	fTicks /= dImplTicksPerSecond;
	fTicks = fmod (fTicks, dImplTicksULONGMAX);
	return (sal_uIntPtr)fTicks;
#endif
}

}   // namespace duckdb_excel