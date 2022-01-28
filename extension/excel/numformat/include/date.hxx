#ifndef _DATE_HXX
#define _DATE_HXX

#include "define.h"

class ResId;

// --------------
// - Date-Types -
// --------------

enum DayOfWeek { MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY,
				 SATURDAY, SUNDAY };

// --------
// - Date -
// --------

class TOOLS_DLLPUBLIC Date
{
private:
	sal_uInt32		nDate;

public:
					Date();
					Date( const ResId & rResId );
                    Date( sal_uInt32 _nDate ) { Date::nDate = _nDate; }
					Date( const Date& rDate )
						{ nDate = rDate.nDate; }
					Date( sal_uInt16 nDay, sal_uInt16 nMonth, sal_uInt16 nYear )
						{ nDate = (   sal_uInt32( nDay   % 100 ) ) +
								  ( ( sal_uInt32( nMonth % 100 ) ) * 100 ) +
								  ( ( sal_uInt32( nYear  % 10000 ) ) * 10000); }

	void			SetDate( sal_uInt32 nNewDate ) { nDate = nNewDate; }
	sal_uInt32		GetDate() const { return nDate; }

	void			SetDay( sal_uInt16 nNewDay );
	void			SetMonth( sal_uInt16 nNewMonth );
	void			SetYear( sal_uInt16 nNewYear );
	sal_uInt16			GetDay() const { return (sal_uInt16)(nDate % 100); }
	sal_uInt16			GetMonth() const { return (sal_uInt16)((nDate / 100) % 100); }
	sal_uInt16			GetYear() const { return (sal_uInt16)(nDate / 10000); }

	DayOfWeek		GetDayOfWeek() const;
	sal_uInt16			GetDayOfYear() const;
    /** nMinimumNumberOfDaysInWeek: how many days of a week must reside in the
        first week of a year. */
	sal_uInt16			GetWeekOfYear( DayOfWeek eStartDay = MONDAY,
								   sal_Int16 nMinimumNumberOfDaysInWeek = 4 ) const;

	sal_uInt16			GetDaysInMonth() const;
	sal_uInt16			GetDaysInYear() const { return (IsLeapYear()) ? 366 : 365; }
	bool			IsLeapYear() const;
	bool			IsValid() const;

	bool			IsBetween( const Date& rFrom, const Date& rTo ) const
						{ return ((nDate >= rFrom.nDate) &&
								 (nDate <= rTo.nDate)); }

	bool			operator ==( const Date& rDate ) const
						{ return (nDate == rDate.nDate); }
	bool			operator !=( const Date& rDate ) const
						{ return (nDate != rDate.nDate); }
	bool			operator  >( const Date& rDate ) const
						{ return (nDate > rDate.nDate); }
	bool			operator  <( const Date& rDate ) const
						{ return (nDate < rDate.nDate); }
	bool			operator >=( const Date& rDate ) const
						{ return (nDate >= rDate.nDate); }
	bool			operator <=( const Date& rDate ) const
						{ return (nDate <= rDate.nDate); }

	Date&			operator =( const Date& rDate )
						{ nDate = rDate.nDate; return *this; }
	Date&			operator +=( long nDays );
	Date&			operator -=( long nDays );
	Date&			operator ++();
	Date&			operator --();
#ifndef MPW33
	Date			operator ++( int );
	Date			operator --( int );
#endif

	TOOLS_DLLPUBLIC friend Date 	operator +( const Date& rDate, long nDays );
	TOOLS_DLLPUBLIC friend Date 	operator -( const Date& rDate, long nDays );
	TOOLS_DLLPUBLIC friend long 	operator -( const Date& rDate1, const Date& rDate2 );

    static long DateToDays( sal_uInt16 nDay, sal_uInt16 nMonth, sal_uInt16 nYear );

};

#endif // _DATE_HXX
