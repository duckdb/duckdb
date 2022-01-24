#ifndef _DEFINE_H
#define _DEFINE_H

#include "stdint.h"
#include <string>
#include <algorithm>

typedef unsigned char sal_Bool;
#   define sal_False ((sal_Bool)0)                
#   define sal_True  ((sal_Bool)1) 

typedef char                sal_Char;
typedef signed char         sal_sChar;
typedef unsigned char       sal_uChar;
typedef signed char         sal_Int8;
typedef unsigned char       sal_uInt8;
typedef signed short        sal_Int16;
typedef unsigned short      sal_uInt16;
typedef signed long         sal_Int32;
typedef unsigned long       sal_uInt32;
typedef int64_t             sal_Int64;
typedef uint64_t            sal_uInt64;
typedef unsigned long		sal_uLong;
typedef wchar_t             sal_Unicode;
typedef std::wstring		String;
typedef sal_Int64           sal_IntPtr;
typedef sal_uInt64          sal_uIntPtr;
typedef sal_uInt16          xub_StrLen;

#define SAL_CONST_INT64(x)       x
#define SAL_CONST_UINT64(x)      x

#define TOOLS_DLLPUBLIC
#define SV_NUMBERFORMATTER_VERSION_ADDITIONAL_I18N_FORMATS	0x000e
#define SV_NUMBERFORMATTER_VERSION				0x000e
#if defined(ULONG_MAX)
#undef ULONG_MAX
#endif
#define ULONG_MAX 0xffffffffffffffffUL

// Format types
#ifndef NUMBERFORMAT_ALL
//	also defined in com/sun/star/util/NumberFormat.hpp
//!	=> put in single .idl file and include here
#define NUMBERFORMAT_ALL			 0x000	/// Just for Output of total list, not a real format type
#define NUMBERFORMAT_DEFINED		 0x001	/// Format defined by user
#define NUMBERFORMAT_DATE			 0x002	/// Number as date
#define NUMBERFORMAT_TIME			 0x004	/// Number as time
#define NUMBERFORMAT_CURRENCY		 0x008	/// Number as currency
#define NUMBERFORMAT_NUMBER			 0x010	/// Any "normal" number format
#define NUMBERFORMAT_SCIENTIFIC		 0x020	/// Number as scientific
#define NUMBERFORMAT_FRACTION		 0x040	/// Number as fraction
#define NUMBERFORMAT_PERCENT		 0x080	/// Number as percent
#define NUMBERFORMAT_TEXT			 0x100	/// Text format
#define NUMBERFORMAT_DATETIME		 0x006	/// Number as date and time
#define NUMBERFORMAT_LOGICAL		 0x400	/// Number as boolean value
#define NUMBERFORMAT_UNDEFINED		 0x800	/// Format undefined yet in analyzing
#endif
#define NUMBERFORMAT_ENTRY_NOT_FOUND (sal_uInt32)(0xffffffff)	/// MAX_ULONG
#define STRING_NOTFOUND    ((xub_StrLen)-1)
#define SAL_MAX_ENUM 0x7fffffff
#define SAL_MAX_UINT16        ((sal_uInt16) 0xFFFF)
#define SAL_MAX_INT32         ((sal_Int32)  0x7FFFFFFF)
#if defined(_WIN32)
#define localtime_r(A, B)			localtime_s(B, A)
#define gmtime_r(A, B)				gmtime_s(B, A)
#endif

#define EraseAllChars(A, B)		A.erase(std::remove(A.begin(), A.end(), B ), A.end())
#define EraseTrailingChars(A, B)	A.erase (A.find_last_not_of(B) + 1, std::string::npos )
#define EraseLeadingChars(A, B)		A.erase(0, std::min(A.find_first_not_of(B), A.size() - 1))
#define ConvertToUpper(A)		std::transform(A.begin(), A.end(), A.begin(), ::toupper)
#define ConvertToLower(A)		std::transform(A.begin(), A.end(), A.begin(), ::tolower)

//=============================================================================


/**
	Field indices to be passed to various <type>XCalendar</type> methods.

	<p> Field is writable only if marked both Get/Set. </p>

	<p> ZONE_OFFSET and DST_OFFSET cooperate such that both values are added,
	for example, ZoneOffset=1*60 and DstOffset=1*60 results in a time
	difference of GMT+2. The calculation in minutes is
	GMT = LocalTime - ZoneOffset - DstOffset </p>

	<p> With introduction of ZONE_OFFSET_SECOND_MILLIS and
	DST_OFFSET_SECOND_MILLIS the exact calculation in milliseconds is
	GMT = LocalTime
		- (ZoneOffset*60000 + ZoneOffsetMillis * sign(ZoneOffset))
		- (DstOffset*60000 + DstOffsetMillis * sign(DstOffset))
	<p>
 */
namespace CalendarFieldIndex
{
	/// Get     <type>AmPmValue</type>.
const short AM_PM = 0;
/// Get/Set day of month [1-31].
const short DAY_OF_MONTH = 1;
/// Get     day of week [0-6].
const short DAY_OF_WEEK = 2;
/// Get     day of  year.
const short DAY_OF_YEAR = 3;
/** Get     daylight saving time offset in minutes, e.g. [0*60..1*60]
	<p> The DST offset value depends on the actual date set at the
	calendar and is determined according to the timezone rules of
	the locale used with the calendar. </p>
	<p> Note that there is a bug in OpenOffice.org 1.0 / StarOffice 6.0
	that prevents interpreting this value correctly. </p> */
const short DST_OFFSET = 4;
/// Get/Set hour [0-23].
const short HOUR = 5;
/// Get/Set minute [0-59].
const short MINUTE = 6;
/// Get/Set second [0-59].
const short SECOND = 7;
/// Get/Set milliseconds [0-999].
const short MILLISECOND = 8;
/// Get     week of month.
const short WEEK_OF_MONTH = 9;
/// Get     week of year.
const short WEEK_OF_YEAR = 10;
/// Get/Set year.
const short YEAR = 11;
/** Get/Set month [0-...].
	<p> Note that the maximum value is <b>not</b> necessarily 11 for
	December but depends on the calendar used instead. </p> */
const short MONTH = 12;
/// Get/Set era, for example, 0:= Before Christ, 1:= After Christ.
const short ERA = 13;
/// Get/Set time zone offset in minutes, e.g. [-14*60..14*60]
const short ZONE_OFFSET = 14;

/// Total number of fields for &lt; OpenOffice 3.1
const short FIELD_COUNT = 15;

/** Get/Set additional offset in milliseconds that <b>adds</b> to
	the value of ZONE_OFFSET. This may be necessary to correctly
	interpret historical timezone data that consists of fractions of
	minutes, e.g. seconds. 1 minute == 60000 milliseconds.

	@ATTENTION! Though the field's type is signed 16-bit, the field
	value is treated as unsigned 16-bit to allow for values up to
	60000 and expresses an absolute value that inherits its sign
	from the parent ZONE_OFFSET field.

	@since OpenOffice 3.1
 */
const short ZONE_OFFSET_SECOND_MILLIS = 15;

/** Get     additional offset in milliseconds that <b>adds</b> to
	the value of DST_OFFSET. This may be necessary to correctly
	interpret historical timezone data that consists of fractions of
	minutes, e.g. seconds. 1 minute == 60000 milliseconds.

	@ATTENTION! Though the field's type is signed 16-bit, the field
	value is treated as unsigned 16-bit to allow for values up to
	60000 and expresses an absolute value that inherits its sign
	from the parent DST_OFFSET field.

	@since OpenOffice 3.1
 */
const short DST_OFFSET_SECOND_MILLIS = 16;

/** Total number of fields as of OpenOffice 3.1

	@since OpenOffice 3.1
 */
const short FIELD_COUNT2 = 17;

};

//=============================================================================


// #45717# IsNumberFormat( "98-10-24", 30, x ), YMD Format set with DMY
// International settings doesn't recognize the string as a date.
/** enum values for <method>SvNumberFormatter::SetEvalDateFormat</method>

	<p>How <method>ImpSvNumberInputScan::GetDateRef</method> shall take the
	DateFormat order (YMD,DMY,MDY) into account, if called from IsNumberFormat
	with a date format to match against.
 */
enum NfEvalDateFormat
{
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

enum DateFormat {
	MDY,
	DMY,
	YMD
};

enum LocaleIndentifier
{
	LocaleId_en_US = 0,
	LocaleId_fr_FR,

	LocaleIndentifierCount
};
typedef LocaleIndentifier LanguageType;

namespace CalendarDisplayCode
{
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
};

//=============================================================================


/**
	Values to be passed to <member>XCalendar::getDisplayName()</member>.
 */
namespace CalendarDisplayIndex
{
	/// name of an AM/PM value
	const short AM_PM = 0;
/// name of a day of week
const short DAY = 1;
/// name of a month
const short MONTH = 2;
/// name of a year (if used for a specific calendar)
const short YEAR = 3;
/// name of an era, like BC/AD
const short ERA = 4;
};

//=============================================================================

//============================================================================

/**
	Offsets into the sequence of strings returned by
	<member>XLocaleData::getReservedWord()</member>.

	@see XLocaleData
		for links to DTD of XML locale data files.
 */

namespace reservedWords
{
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
};

//============================================================================

/** Formatting modes for rtl_math_doubleToString and rtl_math_doubleToUString
	and rtl_math_doubleToUStringBuffer.
 */
enum rtl_math_StringFormat
{
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

/** Special decimal places constants for rtl_math_doubleToString and
	rtl_math_doubleToUString and rtl_math_doubleToUStringBuffer.
 */
enum rtl_math_DecimalPlaces
{
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
enum rtl_math_RoundingMode
{
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

#endif // _DEFINE_H