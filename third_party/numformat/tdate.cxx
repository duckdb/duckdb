// MARKER(update_precomp.py): autogen include statement, do not remove

#if defined( OS2 )
#define INCL_DOSDATETIME
#include <svpm.h>
#elif defined( _WIN32 )
#ifdef _MSC_VER
#pragma warning (push,1)
#endif
#ifdef _MSC_VER
#pragma warning (pop)
#endif
#else
#include <time.h>
#endif
#include <ctime> 

#include "define.h"
#include "date.hxx"
#ifdef  MACOSX
extern "C" {
struct tm *localtime_r(const time_t *timep, struct tm *buffer);
}
#endif

// =======================================================================

static sal_uInt16 aDaysInMonth[12] = { 31, 28, 31, 30, 31, 30,
								   31, 31, 30, 31, 30, 31 };

#define MAX_DAYS	3636532

// =======================================================================

inline sal_Bool ImpIsLeapYear( sal_uInt16 nYear )
{
	return (
                 ( ((nYear % 4) == 0) && ((nYear % 100) != 0) ) ||
                 ( (nYear % 400) == 0 )
               );
}

// -----------------------------------------------------------------------

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

// -----------------------------------------------------------------------

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

// -----------------------------------------------------------------------

static void DaysToDate( long nDays,
						sal_uInt16& rDay, sal_uInt16& rMonth, sal_uInt16& rYear )
{
	long	nTempDays;
	long	i = 0;
	sal_Bool	bCalc;

	do
	{
		nTempDays = (long)nDays;
		rYear = (sal_uInt16)((nTempDays / 365) - i);
		nTempDays -= ((sal_uIntPtr)rYear-1) * 365;
		nTempDays -= ((rYear-1) / 4) - ((rYear-1) / 100) + ((rYear-1) / 400);
		bCalc = sal_False;
		if ( nTempDays < 1 )
		{
			i++;
			bCalc = sal_True;
		}
		else
		{
			if ( nTempDays > 365 )
			{
				if ( (nTempDays != 366) || !ImpIsLeapYear( rYear ) )
				{
					i--;
					bCalc = sal_True;
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

// =======================================================================

Date::Date()
{
#if defined( OS2 )
	PM_DATETIME aDateTime;
	DosGetDateTime( &aDateTime );

	// Datum zusammenbauen
	nDate = ((sal_uIntPtr)aDateTime.day) +
			(((sal_uIntPtr)aDateTime.month)*100) +
			(((sal_uIntPtr)aDateTime.year)*10000);
#else
	time_t	   nTmpTime;
	struct tm aTime;

	// Zeit ermitteln
	nTmpTime = time( 0 );
	
	// Datum zusammenbauen
	if ( localtime_r( &nTmpTime, &aTime ) )
	{
		nDate = ((sal_uIntPtr)aTime.tm_mday) +
				(((sal_uIntPtr)(aTime.tm_mon+1))*100) +
				(((sal_uIntPtr)(aTime.tm_year+2000))*10000);
	}
	else
		nDate = 30 + 1200 + (((sal_uIntPtr)1899)*10000);
#endif
}

// -----------------------------------------------------------------------

void Date::SetDay( sal_uInt16 nNewDay )
{
	sal_uIntPtr  nMonth  = GetMonth();
	sal_uIntPtr  nYear   = GetYear();

	nDate = ((sal_uIntPtr)(nNewDay%100)) + (nMonth*100) + (nYear*10000);
}

// -----------------------------------------------------------------------

void Date::SetMonth( sal_uInt16 nNewMonth )
{
	sal_uIntPtr  nDay 	 = GetDay();
	sal_uIntPtr  nYear	 = GetYear();

	nDate = nDay + (((sal_uIntPtr)(nNewMonth%100))*100) + (nYear*10000);
}

// -----------------------------------------------------------------------

void Date::SetYear( sal_uInt16 nNewYear )
{
	sal_uIntPtr  nDay 	= GetDay();
	sal_uIntPtr  nMonth	= GetMonth();

	nDate = nDay + (nMonth*100) + (((sal_uIntPtr)(nNewYear%10000))*10000);
}

// -----------------------------------------------------------------------

DayOfWeek Date::GetDayOfWeek() const
{
	return (DayOfWeek)((sal_uIntPtr)(DateToDays( GetDay(), GetMonth(), GetYear() )-1) % 7);
}

// -----------------------------------------------------------------------

sal_uInt16 Date::GetDayOfYear() const
{
	sal_uInt16 nDay = GetDay();
	for( sal_uInt16 i = 1; i < GetMonth(); i++ )
         nDay = nDay + ::DaysInMonth( i, GetYear() );   // += yields a warning on MSVC, so don't use it
	return nDay;
}

// -----------------------------------------------------------------------

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

// -----------------------------------------------------------------------

sal_uInt16 Date::GetDaysInMonth() const
{
	return DaysInMonth( GetMonth(), GetYear() );
}

// -----------------------------------------------------------------------

sal_Bool Date::IsLeapYear() const
{
	sal_uInt16 nYear = GetYear();
	return ImpIsLeapYear( nYear );
}

// -----------------------------------------------------------------------

sal_Bool Date::IsValid() const
{
	sal_uInt16 nDay   = GetDay();
	sal_uInt16 nMonth = GetMonth();
	sal_uInt16 nYear  = GetYear();

	if ( !nMonth || (nMonth > 12) )
		return sal_False;
	if ( !nDay || (nDay > DaysInMonth( nMonth, nYear )) )
		return sal_False;
	else if ( nYear <= 1582 )
	{
		if ( nYear < 1582 )
			return sal_False;
		else if ( nMonth < 10 )
			return sal_False;
		else if ( (nMonth == 10) && (nDay < 15) )
			return sal_False;
	}

	return sal_True;
}

// -----------------------------------------------------------------------

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

// -----------------------------------------------------------------------

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

// -----------------------------------------------------------------------

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

// -----------------------------------------------------------------------

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

// -----------------------------------------------------------------------

Date Date::operator ++( int )
{
	Date aOldDate = *this;
	Date::operator++();
	return aOldDate;
}

// -----------------------------------------------------------------------

Date Date::operator --( int )
{
	Date aOldDate = *this;
	Date::operator--();
	return aOldDate;
}

#endif

// -----------------------------------------------------------------------

Date operator +( const Date& rDate, long nDays )
{
	Date aDate( rDate );
	aDate += nDays;
	return aDate;
}

// -----------------------------------------------------------------------

Date operator -( const Date& rDate, long nDays )
{
	Date aDate( rDate );
	aDate -= nDays;
	return aDate;
}

// -----------------------------------------------------------------------

long operator -( const Date& rDate1, const Date& rDate2 )
{
    sal_uIntPtr  nTempDays1 = Date::DateToDays( rDate1.GetDay(), rDate1.GetMonth(),
									rDate1.GetYear() );
    sal_uIntPtr  nTempDays2 = Date::DateToDays( rDate2.GetDay(), rDate2.GetMonth(),
									rDate2.GetYear() );
	return nTempDays1 - nTempDays2;
}
