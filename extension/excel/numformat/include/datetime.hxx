#ifndef _DATETIME_HXX
#define _DATETIME_HXX

#include "define.h"
#include "date.hxx"
#include "time.hxx"

// ------------
// - DateTime -
// ------------

class TOOLS_DLLPUBLIC DateTime : public Date, public Time
{
public:
                    DateTime() : Date(), Time() {}
                    DateTime( const DateTime& rDateTime ) :
                        Date( rDateTime ), Time( rDateTime ) {}
                    DateTime( const Date& rDate ) : Date( rDate ), Time(0) {}
                    DateTime( const Time& rTime ) : Date(0), Time( rTime ) {}
                    DateTime( const Date& rDate, const Time& rTime ) :
                        Date( rDate ), Time( rTime ) {}

    bool            IsBetween( const DateTime& rFrom,
                               const DateTime& rTo ) const;

    bool            IsEqualIgnore100Sec( const DateTime& rDateTime ) const
                        {
                            if ( Date::operator!=( rDateTime ) )
                                return false;
                            return Time::IsEqualIgnore100Sec( rDateTime );
                        }

    bool            operator ==( const DateTime& rDateTime ) const
                        { return (Date::operator==( rDateTime ) &&
                                  Time::operator==( rDateTime )); }
    bool            operator !=( const DateTime& rDateTime ) const
                        { return (Date::operator!=( rDateTime ) ||
                                  Time::operator!=( rDateTime )); }
    bool            operator  >( const DateTime& rDateTime ) const;
    bool            operator  <( const DateTime& rDateTime ) const;
    bool            operator >=( const DateTime& rDateTime ) const;
    bool            operator <=( const DateTime& rDateTime ) const;

    long            GetSecFromDateTime( const Date& rDate ) const;
    void            MakeDateTimeFromSec( const Date& rDate, sal_uIntPtr nSec );

    void            ConvertToUTC()       { *this -= Time::GetUTCOffset(); }
    void            ConvertToLocalTime() { *this += Time::GetUTCOffset(); }

    DateTime&       operator +=( long nDays )
                        { Date::operator+=( nDays ); return *this; }
    DateTime&       operator -=( long nDays )
                        { Date::operator-=( nDays ); return *this; }
	DateTime&		operator +=( double fTimeInDays );
	DateTime&		operator -=( double fTimeInDays )
						{ return operator+=( -fTimeInDays ); }
    DateTime&       operator +=( const Time& rTime );
    DateTime&       operator -=( const Time& rTime );

    TOOLS_DLLPUBLIC friend DateTime operator +( const DateTime& rDateTime, long nDays );
    TOOLS_DLLPUBLIC friend DateTime operator -( const DateTime& rDateTime, long nDays );
    TOOLS_DLLPUBLIC friend DateTime operator +( const DateTime& rDateTime, double fTimeInDays );
    TOOLS_DLLPUBLIC friend DateTime operator -( const DateTime& rDateTime, double fTimeInDays )
						{ return operator+( rDateTime, -fTimeInDays ); }
    TOOLS_DLLPUBLIC friend DateTime operator +( const DateTime& rDateTime, const Time& rTime );
    TOOLS_DLLPUBLIC friend DateTime operator -( const DateTime& rDateTime, const Time& rTime );
	TOOLS_DLLPUBLIC friend double	operator -( const DateTime& rDateTime1, const DateTime& rDateTime2 );
	TOOLS_DLLPUBLIC friend long		operator -( const DateTime& rDateTime, const Date& rDate )
						{ return (const Date&) rDateTime - rDate; }

    DateTime&       operator =( const DateTime& rDateTime );

    void            GetWin32FileDateTime( sal_uInt32 & rLower, sal_uInt32 & rUpper );
    static DateTime CreateFromWin32FileDateTime( const sal_uInt32 & rLower, const sal_uInt32 & rUpper );
};

inline DateTime& DateTime::operator =( const DateTime& rDateTime )
{
    Date::operator=( rDateTime );
    Time::operator=( rDateTime );
    return *this;
}

#endif // _DATETIME_HXX
