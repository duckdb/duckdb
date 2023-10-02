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

#ifndef _NF_CALENDAR_H
#define _NF_CALENDAR_H

#include <stdint.h>
#include <string>
#include <algorithm>
#include "duckdb/common/vector.hpp"


namespace duckdb_excel {


// ------------------------------ define.h -------------------------------------------------

typedef int8_t sal_Char;
typedef int8_t sal_sChar;
typedef uint8_t sal_uChar;
typedef int8_t sal_Int8;
typedef uint8_t sal_uInt8;
typedef int16_t sal_Int16;
typedef uint16_t sal_uInt16;
typedef int32_t sal_Int32;
typedef uint32_t sal_uInt32;
typedef int64_t sal_Int64;
typedef uint64_t sal_uInt64;
typedef unsigned long sal_uLong;
typedef wchar_t sal_Unicode;
typedef std::wstring String;
typedef sal_Int64 sal_IntPtr;
typedef sal_uInt64 sal_uIntPtr;

#define SAL_CONST_INT64(x)  x
#define SAL_CONST_UINT64(x) x

#define TOOLS_DLLPUBLIC
#define SV_NUMBERFORMATTER_VERSION_ADDITIONAL_I18N_FORMATS 0x000e
#define SV_NUMBERFORMATTER_VERSION                         0x000e
#if defined(ULONG_MAX)
#undef ULONG_MAX
#endif
#define ULONG_MAX 0xffffffffffffffffUL

// Format types
#ifndef NUMBERFORMAT_ALL
//	also defined in com/sun/star/util/NumberFormat.hpp
//!	=> put in single .idl file and include here
#define NUMBERFORMAT_ALL        0x000 /// Just for Output of total list, not a real format type
#define NUMBERFORMAT_DEFINED    0x001 /// Format defined by user
#define NUMBERFORMAT_DATE       0x002 /// Number as date
#define NUMBERFORMAT_TIME       0x004 /// Number as time
#define NUMBERFORMAT_CURRENCY   0x008 /// Number as currency
#define NUMBERFORMAT_NUMBER     0x010 /// Any "normal" number format
#define NUMBERFORMAT_SCIENTIFIC 0x020 /// Number as scientific
#define NUMBERFORMAT_FRACTION   0x040 /// Number as fraction
#define NUMBERFORMAT_PERCENT    0x080 /// Number as percent
#define NUMBERFORMAT_TEXT       0x100 /// Text format
#define NUMBERFORMAT_DATETIME   0x006 /// Number as date and time
#define NUMBERFORMAT_LOGICAL    0x400 /// Number as boolean value
#define NUMBERFORMAT_UNDEFINED  0x800 /// Format undefined yet in analyzing
#endif
#define NUMBERFORMAT_ENTRY_NOT_FOUND (sal_uInt32)(0xffffffff) /// MAX_ULONG
#define STRING_NOTFOUND              ((uint16_t)-1)
#define SAL_MAX_ENUM                 0x7fffffff
#define SAL_MAX_UINT16               ((sal_uInt16)0xFFFF)
#define SAL_MAX_INT32                ((sal_Int32)0x7FFFFFFF)
#if defined(_WIN32)
#define localtime_r(A, B) localtime_s(B, A)
#define gmtime_r(A, B)    gmtime_s(B, A)
#endif

#define EraseAllChars(A, B)      A.erase(std::remove(A.begin(), A.end(), B), A.end())
#define EraseTrailingChars(A, B) A.erase(A.find_last_not_of(B) + 1, std::string::npos)
#define EraseLeadingChars(A, B)  A.erase(0, std::min(A.find_first_not_of(B), A.size() - 1))
#define ConvertToUpper(A)        std::transform(A.begin(), A.end(), A.begin(), ::toupper)
#define ConvertToLower(A)        std::transform(A.begin(), A.end(), A.begin(), ::tolower)

namespace CalendarFieldIndex {
/// Get     <type>AmPmValue</type>.
const short AM_PM = 0;
/// Get/Set day of month [1-31].
const short DAY_OF_MONTH = 1;
/// Get     day of week [0-6].
const short DAY_OF_WEEK = 2;
/// Get     day of  year.
const short DAY_OF_YEAR = 3;
const short DST_OFFSET = 4;
/// Get/Set hour [0-23].
const short CFI_HOUR = 5;
/// Get/Set minute [0-59].
const short CFI_MINUTE = 6;
/// Get/Set second [0-59].
const short CFI_SECOND = 7;
/// Get/Set milliseconds [0-999].
const short CFI_MILLISECOND = 8;
/// Get     week of month.
const short WEEK_OF_MONTH = 9;
/// Get     week of year.
const short WEEK_OF_YEAR = 10;
/// Get/Set year.
const short CFI_YEAR = 11;
const short CFI_MONTH = 12;
/// Get/Set era, for example, 0:= Before Christ, 1:= After Christ.
const short ERA = 13;
/// Get/Set time zone offset in minutes, e.g. [-14*60..14*60]
const short ZONE_OFFSET = 14;

/// Total number of fields for &lt; OpenOffice 3.1
const short FIELD_COUNT = 15;

const short ZONE_OFFSET_SECOND_MILLIS = 15;

const short DST_OFFSET_SECOND_MILLIS = 16;

const short FIELD_COUNT2 = 17;

} // namespace CalendarFieldIndex

enum NfEvalDateFormat {
	/** DateFormat only from International, default. */
	NF_EVALDATEFORMAT_INTL,

	/** DateFormat only from date format passed to function (if any).
	    If no date format is passed then the DateFormat is taken from International. */
	NF_EVALDATEFORMAT_FORMAT,

	/** First try the DateFormat from International. If it doesn't match a
	    valid date try the DateFormat from the date format passed. */
	NF_EVALDATEFORMAT_INTL_FORMAT,

	/** First try the DateFormat from the date format passed. If it doesn't
	    match a valid date try the DateFormat from International. */
	NF_EVALDATEFORMAT_FORMAT_INTL
};

enum DateFormat { MDY, DMY, YMD };

enum LocaleIndentifier {
	LocaleId_en_US = 0,
	LocaleId_fr_FR,

	LocaleIndentifierCount
};
typedef LocaleIndentifier LanguageType;

namespace CalendarDisplayCode {
/// Day of month, one or two digits, no leading zero.
const long SHORT_DAY = 1;
/// Day of month, two digits, with leading zero.
const long LONG_DAY = 2;
/// Day of week, abbreviated name.
const long SHORT_DAY_NAME = 3;
/// Day of week, full name.
const long LONG_DAY_NAME = 4;

/// Month of year, one or two digits, no leading zero.
const long SHORT_MONTH = 5;
/// Month of year, with leading zero.
const long LONG_MONTH = 6;
/// Full month name.
const long SHORT_MONTH_NAME = 7;
/// Abbreviated month name.
const long LONG_MONTH_NAME = 8;

/// Year, two digits.
const long SHORT_YEAR = 9;
/// Year, four digits.
const long LONG_YEAR = 10;
/// Full era name, for example, "Before Christ" or "Anno Dominus".
const long SHORT_ERA = 11;
/// Abbreviated era name, for example, BC or AD.
const long LONG_ERA = 12;
/// Combined short year and era, order depends on locale/calendar.
const long SHORT_YEAR_AND_ERA = 13;
/// Combined full year and era, order depends on locale/calendar.
const long LONG_YEAR_AND_ERA = 14;

/// Short quarter, for example, "Q1"
const long SHORT_QUARTER = 15;
/// Long quarter, for example, "1st quarter"
const long LONG_QUARTER = 16;
} // namespace CalendarDisplayCode

namespace reservedWords {
/// "true"
const short TRUE_WORD = 0;
/// "false"
const short FALSE_WORD = 1;
/// "1st quarter"
const short QUARTER1_WORD = 2;
/// "2nd quarter"
const short QUARTER2_WORD = 3;
/// "3rd quarter"
const short QUARTER3_WORD = 4;
/// "4th quarter"
const short QUARTER4_WORD = 5;
/// "above"
const short ABOVE_WORD = 6;
/// "below"
const short BELOW_WORD = 7;
/// "Q1"
const short QUARTER1_ABBREVIATION = 8;
/// "Q2"
const short QUARTER2_ABBREVIATION = 9;
/// "Q3"
const short QUARTER3_ABBREVIATION = 10;
/// "Q4"
const short QUARTER4_ABBREVIATION = 11;

//! Yes, this must be the count of known reserved words and one more than
//! the maximum number used above!
/// Count of known reserved words.
const short COUNT = 12;
} // namespace reservedWords

enum rtl_math_StringFormat {
	/** Like sprintf() %E.
	 */
	rtl_math_StringFormat_E,

	/** Like sprintf() %f.
	 */
	rtl_math_StringFormat_F,

	/** Like sprintf() %G, 'F' or 'E' format is used depending on which one is
	    more compact.
	*/
	rtl_math_StringFormat_G,

	/** Automatic, 'F' or 'E' format is used depending on the numeric value to
	    be formatted.
	 */
	rtl_math_StringFormat_Automatic,

	/** @internal
	 */
	rtl_math_StringFormat_FORCE_EQUAL_SIZE = SAL_MAX_ENUM
};

enum rtl_math_DecimalPlaces {
	/** Value to be used with rtl_math_StringFormat_Automatic.
	 */
	rtl_math_DecimalPlaces_Max = 0x7ffffff,

	/** Value to be used with rtl_math_StringFormat_G.
	    In fact the same value as rtl_math_DecimalPlaces_Max, just an alias for
	    better understanding.
	 */
	rtl_math_DecimalPlaces_DefaultSignificance = 0x7ffffff
};

/** Rounding modes for rtl_math_round.
 */
enum rtl_math_RoundingMode {
	/** Like HalfUp, but corrects roundoff errors, preferred.
	 */
	rtl_math_RoundingMode_Corrected,

	/** Floor of absolute value, signed return (commercial).
	 */
	rtl_math_RoundingMode_Down,

	/** Ceil of absolute value, signed return (commercial).
	 */
	rtl_math_RoundingMode_Up,

	/** Floor of signed value.
	 */
	rtl_math_RoundingMode_Floor,

	/** Ceil of signed value.
	 */
	rtl_math_RoundingMode_Ceiling,

	/** Frac <= 0.5 ? floor of abs : ceil of abs, signed return.
	 */
	rtl_math_RoundingMode_HalfDown,

	/** Frac < 0.5 ? floor of abs : ceil of abs, signed return (mathematical).
	 */
	rtl_math_RoundingMode_HalfUp,

	/** IEEE rounding mode (statistical).
	 */
	rtl_math_RoundingMode_HalfEven,

	/** @internal
	 */
	rtl_math_RoundingMode_FORCE_EQUAL_SIZE = SAL_MAX_ENUM
};

enum NfKeywordIndex
{
    NF_KEY_NONE = 0,
    NF_KEY_E,           // exponential symbol
    NF_KEY_AMPM,        // AM/PM
    NF_KEY_AP,          // a/p
    NF_KEY_MI,          // minute       (!)
    NF_KEY_MMI,         // minute 02    (!)
    NF_KEY_M,           // month        (!)
    NF_KEY_MM,          // month 02     (!)
    NF_KEY_MMM,         // month short name
    NF_KEY_MMMM,        // month long name
    NF_KEY_H,           // hour
    NF_KEY_HH,          // hour 02
    NF_KEY_S,           // second
    NF_KEY_SS,          // second 02
    NF_KEY_Q,           // quarter
    NF_KEY_QQ,          // quarter 02
    NF_KEY_D,           // day of month
    NF_KEY_DD,          // day of month 02
    NF_KEY_DDD,         // day of week short
    NF_KEY_DDDD,        // day of week long
    NF_KEY_YY,          // year two digits
    NF_KEY_YYYY,        // year four digits
    NF_KEY_NN,          // day of week short
    NF_KEY_NNNN,        // day of week long with separator
    NF_KEY_CCC,         // currency bank symbol (old version)
    NF_KEY_GENERAL,     // General / Standard
    NF_KEY_LASTOLDKEYWORD = NF_KEY_GENERAL,
    NF_KEY_NNN,         // day of week long without separator, as of version 6, 10.10.97
    NF_KEY_WW,          // week of year, as of version 8, 19.06.98
    NF_KEY_MMMMM,       // first letter of month name
    NF_KEY_LASTKEYWORD = NF_KEY_MMMMM,
    NF_KEY_UNUSED4,
    NF_KEY_QUARTER,     // was quarter word, not used anymore from SRC631 on (26.04.01)
    NF_KEY_TRUE,        // boolean true
    NF_KEY_FALSE,       // boolean false
    NF_KEY_BOOLEAN,     // boolean
    NF_KEY_COLOR,       // color
    NF_KEY_FIRSTCOLOR,
    NF_KEY_BLACK = NF_KEY_FIRSTCOLOR,   // you do know colors, don't you?
    NF_KEY_BLUE,
    NF_KEY_GREEN,
    NF_KEY_CYAN,
    NF_KEY_RED,
    NF_KEY_MAGENTA,
    NF_KEY_BROWN,
    NF_KEY_GREY,
    NF_KEY_YELLOW,
    NF_KEY_WHITE,
    NF_KEY_LASTCOLOR = NF_KEY_WHITE,
    NF_KEY_LASTKEYWORD_SO5 = NF_KEY_LASTCOLOR,
    //! Keys from here on can't be saved in SO5 file format and MUST be
    //! converted to string which means losing any information.
    NF_KEY_AAA,         // abbreviated day name from Japanese Xcl, same as DDD or NN English
    NF_KEY_AAAA,        // full day name from Japanese Xcl, same as DDDD or NNN English
    NF_KEY_EC,          // E non-gregorian calendar year without preceding 0
    NF_KEY_EEC,         // EE non-gregorian calendar year with preceding 0 (two digit)
    NF_KEY_G,           // abbreviated era name, latin characters M T S or H for Gengou calendar
    NF_KEY_GG,          // abbreviated era name
    NF_KEY_GGG,         // full era name
    NF_KEY_R,           // acts as EE (Xcl) => GR==GEE, GGR==GGEE, GGGR==GGGEE
    NF_KEY_RR,          // acts as GGGEE (Xcl)
    NF_KEY_THAI_T,      // Thai T modifier, speciality of Thai Excel, only used with Thai locale and converted to [NatNum1]
    NF_KEYWORD_ENTRIES_COUNT
};

class NfKeywordTable
{
    typedef duckdb::vector<String> Keywords_t;
    Keywords_t m_keywords;

public:
    NfKeywordTable() : m_keywords(NF_KEYWORD_ENTRIES_COUNT) {};
    virtual ~NfKeywordTable() {}

    String & operator[] (Keywords_t::size_type n) { return m_keywords[n]; }
    const String & operator[] (Keywords_t::size_type n) const { return m_keywords[n]; }
};

/// Number formatter's symbol types of a token, if not key words, which are >0
enum NfSymbolType
{
    NF_SYMBOLTYPE_STRING        = -1,   // literal string in output
    NF_SYMBOLTYPE_DEL           = -2,   // special character
    NF_SYMBOLTYPE_BLANK         = -3,   // blank for '_'
    NF_SYMBOLTYPE_STAR          = -4,   // *-character
    NF_SYMBOLTYPE_DIGIT         = -5,   // digit place holder
    NF_SYMBOLTYPE_DECSEP        = -6,   // decimal separator
    NF_SYMBOLTYPE_THSEP         = -7,   // group AKA thousand separator
    NF_SYMBOLTYPE_EXP           = -8,   // exponent E
    NF_SYMBOLTYPE_FRAC          = -9,   // fraction /
    NF_SYMBOLTYPE_EMPTY         = -10,  // deleted symbols
    NF_SYMBOLTYPE_FRACBLANK     = -11,  // delimiter between integer and fraction
    NF_SYMBOLTYPE_COMMENT       = -12,  // comment is following
    NF_SYMBOLTYPE_CURRENCY      = -13,  // currency symbol
    NF_SYMBOLTYPE_CURRDEL       = -14,  // currency symbol delimiter [$]
    NF_SYMBOLTYPE_CURREXT       = -15,  // currency symbol extension -xxx
    NF_SYMBOLTYPE_CALENDAR      = -16,  // calendar ID
    NF_SYMBOLTYPE_CALDEL        = -17,  // calendar delimiter [~]
    NF_SYMBOLTYPE_DATESEP       = -18,  // date separator
    NF_SYMBOLTYPE_TIMESEP       = -19,  // time separator
    NF_SYMBOLTYPE_TIME100SECSEP = -20,  // time 100th seconds separator
    NF_SYMBOLTYPE_PERCENT       = -21   // percent %
};


// ------------------------- digitgroupingiterator.hxx -----------------------------------------

class DigitGroupingIterator
{
	duckdb::vector<int32_t> maGroupings;
    sal_Int32   mnGroup;        // current active grouping
    sal_Int32   mnDigits;       // current active digits per group
    sal_Int32   mnNextPos;      // position (in digits) of next grouping

    void setInfinite()
    {
        mnGroup = maGroupings.size();
    }

    bool isInfinite() const
    {
        return mnGroup >= (sal_Int32)maGroupings.size();
    }

    sal_Int32 getGrouping() const
    {
        if (mnGroup < (sal_Int32)maGroupings.size())
        {
            sal_Int32 n = maGroupings[mnGroup];
            //OSL_ENSURE( 0 <= n && n <= SAL_MAX_UINT16, "DigitGroupingIterator::getGrouping: far out");
            if (n < 0)
                n = 0;                  // sanitize ...
            else if (n > SAL_MAX_UINT16)
                n = SAL_MAX_UINT16;     // limit for use with uint16_t
            return n;
        }
        return 0;
    }

    void setPos()
    {
        // someone might be playing jokes on us, so check for overflow
        if (mnNextPos <= SAL_MAX_INT32 - mnDigits)
            mnNextPos += mnDigits;
    }

    void setDigits()
    {
        sal_Int32 nPrev = mnDigits;
        mnDigits = getGrouping();
        if (!mnDigits)
        {
            mnDigits = nPrev;
            setInfinite();
        }
        setPos();
    }

    void initGrouping()
    {
        mnDigits = 3;       // just in case of constructed with empty grouping
        mnGroup = 0;
        mnNextPos = 0;
        setDigits();
    }

    // not implemented, prevent usage
    DigitGroupingIterator();
    DigitGroupingIterator( const DigitGroupingIterator & );
    DigitGroupingIterator & operator=( const DigitGroupingIterator & );

public:

    explicit DigitGroupingIterator(duckdb::vector<int32_t>& digit_grouping)
        : maGroupings(digit_grouping)
    {
        initGrouping();
    }

    /** Advance iterator to next grouping. */
    DigitGroupingIterator & advance()
    {
        if (isInfinite())
            setPos();
        else
        {
            ++mnGroup;
            setDigits();
        }
        return *this;
    }

    /** Obtain current grouping. Always > 0. */
    sal_Int32 get() const
    {
        return mnDigits;
    }

    /** The next position (in integer digits) from the right where to insert a
        group separator. */
    sal_Int32 getPos()
    {
        return mnNextPos;
    }

    /** Reset iterator to start again from the right beginning. */
    void reset()
    {
        initGrouping();
    }
};


// ------------------------------------------ date.hxx -------------------------------------------------------

class ResId;

enum DayOfWeek { MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY,
				 SATURDAY, SUNDAY };

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


// ----------------------------------------- time.hxx -----------------------------------------------------

class TOOLS_DLLPUBLIC Time
{
private:
	sal_Int32			nTime;

public:
					Time();
					Time( const ResId & rResId );
					Time( sal_Int32 _nTime ) { Time::nTime = _nTime; }
					Time( const Time& rTime );
					Time( sal_uIntPtr nHour, sal_uIntPtr nMin,
						  sal_uIntPtr nSec = 0, sal_uIntPtr n100Sec = 0 );

	void			SetTime( sal_Int32 nNewTime ) { nTime = nNewTime; }
	sal_Int32		GetTime() const { return nTime; }

	void			SetHour( sal_uInt16 nNewHour );
	void			SetMin( sal_uInt16 nNewMin );
	void			SetSec( sal_uInt16 nNewSec );
	void			Set100Sec( sal_uInt16 nNew100Sec );
	sal_uInt16			GetHour() const
						{ sal_uIntPtr nTempTime = (nTime >= 0) ? nTime : nTime*-1;
						  return (sal_uInt16)(nTempTime / 1000000); }
	sal_uInt16			GetMin() const
						{ sal_uIntPtr nTempTime = (nTime >= 0) ? nTime : nTime*-1;
						  return (sal_uInt16)((nTempTime / 10000) % 100); }
	sal_uInt16			GetSec() const
						{ sal_uIntPtr nTempTime = (nTime >= 0) ? nTime : nTime*-1;
						  return (sal_uInt16)((nTempTime / 100) % 100); }
	sal_uInt16			Get100Sec() const
						{ sal_uIntPtr nTempTime = (nTime >= 0) ? nTime : nTime*-1;
						  return (sal_uInt16)(nTempTime % 100); }

	sal_Int32		GetMSFromTime() const;
	void			MakeTimeFromMS( sal_Int32 nMS );

                    /// 12 hours == 0.5 days
    double          GetTimeInDays() const;

	bool			IsBetween( const Time& rFrom, const Time& rTo ) const
						{ return ((nTime >= rFrom.nTime) && (nTime <= rTo.nTime)); }

    bool            IsEqualIgnore100Sec( const Time& rTime ) const;

	bool			operator ==( const Time& rTime ) const
						{ return (nTime == rTime.nTime); }
	bool			operator !=( const Time& rTime ) const
						{ return (nTime != rTime.nTime); }
	bool			operator  >( const Time& rTime ) const
						{ return (nTime > rTime.nTime); }
	bool			operator  <( const Time& rTime ) const
						{ return (nTime < rTime.nTime); }
	bool			operator >=( const Time& rTime ) const
						{ return (nTime >= rTime.nTime); }
	bool			operator <=( const Time& rTime ) const
						{ return (nTime <= rTime.nTime); }

	static Time 	GetUTCOffset();
	static sal_uIntPtr	GetSystemTicks();		// Elapsed time
	static sal_uIntPtr	GetProcessTicks();		// CPU time

	void			ConvertToUTC()		 { *this -= Time::GetUTCOffset(); }
	void			ConvertToLocalTime() { *this += Time::GetUTCOffset(); }

	Time&			operator =( const Time& rTime );
	Time			operator -() const
						{ return Time( nTime * -1 ); }
	Time&			operator +=( const Time& rTime );
	Time&			operator -=( const Time& rTime );
	TOOLS_DLLPUBLIC friend Time 	operator +( const Time& rTime1, const Time& rTime2 );
	TOOLS_DLLPUBLIC friend Time 	operator -( const Time& rTime1, const Time& rTime2 );
};


// --------------------------------------------- datetime.hxx ---------------------------------------------------

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


// ---------------------------------------- calendar.hxx -----------------------------------------------------------

struct Era {
	sal_Int32 year;
	sal_Int32 month;
	sal_Int32 day;
};

const sal_Int16 FIELD_INDEX_COUNT = CalendarFieldIndex::FIELD_COUNT2;

class LocaleData;

class Calendar {
public:
	// Constructors
	Calendar(LocaleData *pFormatter);
	Calendar(Era *_eraArray);
	void init(Era *_eraArray);

	~Calendar();

	void setValue(sal_Int16 nFieldIndex, sal_Int16 nValue);
	sal_Int16 getValue(sal_Int16 nFieldIndex);
	bool isValid();
	std::wstring getDisplayName(sal_Int16 nCalendarDisplayIndex, sal_Int16 nIdx, sal_Int16 nNameType);

	// Methods in XExtendedCalendar
	std::wstring getDisplayString(sal_Int32 nCalendarDisplayCode, sal_Int16 nNativeNumberMode);
	void setDateTime(double timeInDays);
	double getDateTime();
	double getLocalDateTime();
	void setLocalDateTime(double nTimeInDays);
	DateTime getEpochStart() {
		return aEpochStart;
	}
	Date *GetNullDate() const {
		return pNullDate;
	}
	inline void setGregorianDateTime(const DateTime &rDateTime) {
		setLocalDateTime(rDateTime - aEpochStart);
	}
	/// set reference date for offset calculation
	void ChangeNullDate(const sal_uInt16 nDay, const sal_uInt16 nMonth,
	                    const sal_uInt16 nYear); // exchanges reference date

protected:
	Era *eraArray;
	// NativeNumberSupplier aNatNum;
	sal_uInt32 fieldSet;
	sal_Int16 fieldValue[FIELD_INDEX_COUNT];

private:
	LocaleData *pFormatter;
	// Calendar aCalendar;

	DateTime aEpochStart;
	Date *pNullDate; // 30Dec1899
	double timeInDays;

	/** Submit fieldSetValue array according to fieldSet. */
	void submitFields();
	/** Submit fieldSetValue array according to fieldSet, plus YMDhms if >=0,
	    plus zone and DST if != 0 */
	void submitValues(sal_Int32 nYear, sal_Int32 nMonth, sal_Int32 nDay, sal_Int32 nHour, sal_Int32 nMinute,
	                  sal_Int32 nSecond, sal_Int32 nMilliSecond, sal_Int32 nZone, sal_Int32 nDST);
	/** Obtain combined field values for timezone offset (minutes+secondmillis)
	    in milliseconds and whether fields were set. */
	bool getZoneOffset(sal_Int32 &o_nOffset) const;
	/** Obtain combined field values for DST offset (minutes+secondmillis) in
	    milliseconds and whether fields were set. */
	bool getDSTOffset(sal_Int32 &o_nOffset) const;
	/** Used by getZoneOffset() and getDSTOffset(). Parent is
	    CalendarFieldIndex for offset in minutes, child is CalendarFieldIndex
	    for offset in milliseconds. */
	bool getCombinedOffset(sal_Int32 &o_nOffset, sal_Int16 nParentFieldIndex, sal_Int16 nChildFieldIndex) const;
	sal_Int32 getCombinedOffsetInMillis(sal_Int16 nParentFieldIndex, sal_Int16 nChildFieldIndex);
	sal_Int32 getZoneOffsetInMillis();
	sal_Int32 getDSTOffsetInMillis();
};

}   // namespace duckdb_excel

#endif  // _NF_CALENDAR_H
