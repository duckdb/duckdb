#ifndef _ZFORFIND_HXX
#define _ZFORFIND_HXX

#include "define.h"
#include "localedata.h"

namespace duckdb_numformat {

class Date;
class SvNumberformat;

#define SV_MAX_ANZ_INPUT_STRINGS  20    // max count of substrings in input scanner

class LocaleData;

class ImpSvNumberInputScan
{
public:
    ImpSvNumberInputScan( LocaleData* pFormatter );
    ~ImpSvNumberInputScan();

/*!*/   void ChangeIntl();                      // MUST be called if language changes

    /// convert input string to number
    sal_Bool IsNumberFormat(
            const String& rString,              /// input string
            short& F_Type,                      /// format type (in + out)
            double& fOutNumber,                 /// value determined (out)
            const SvNumberformat* pFormat = NULL    /// optional a number format to which compare against
            );

    /// after IsNumberFormat: get decimal position
    short   GetDecPos() const { return nDecPos; }
    /// after IsNumberFormat: get count of numeric substrings in input string
    sal_uInt16  GetAnzNums() const { return nAnzNums; }

    /// set threshold of two-digit year input
    void    SetYear2000( sal_uInt16 nVal ) { nYear2000 = nVal; }
    /// get threshold of two-digit year input
    sal_uInt16  GetYear2000() const { return nYear2000; }

private:
	LocaleData*  pFormatter;							// SvNumberFormatter *
    String* pUpperMonthText;                    // Array of month names, uppercase
    String* pUpperAbbrevMonthText;              // Array of month names, abbreviated, uppercase
    String* pUpperDayText;                      // Array of day of week names, uppercase
    String* pUpperAbbrevDayText;                // Array of day of week names, abbreviated, uppercase
    String  aUpperCurrSymbol;                   // Currency symbol, uppercase
    sal_Bool    bTextInitialized;                   // Whether days and months are initialized
                                                // Variables for provisional results:
    String sStrArray[SV_MAX_ANZ_INPUT_STRINGS]; // Array of scanned substrings
    sal_Bool   IsNum[SV_MAX_ANZ_INPUT_STRINGS];     // Whether a substring is numeric
    sal_uInt16 nNums[SV_MAX_ANZ_INPUT_STRINGS];     // Sequence of offsets to numeric strings
    sal_uInt16 nAnzStrings;                         // Total count of scanned substrings
    sal_uInt16 nAnzNums;                            // Count of numeric substrings
    sal_Bool   bDecSepInDateSeps;                   // True <=> DecSep in {.,-,/,DateSep}
    sal_uInt8   nMatchedAllStrings;                  // Scan...String() matched all substrings,
                                                // bit mask of nMatched... constants

    static const sal_uInt8 nMatchedEndString;        // 0x01
    static const sal_uInt8 nMatchedMidString;        // 0x02
    static const sal_uInt8 nMatchedStartString;      // 0x04
    static const sal_uInt8 nMatchedVirgin;           // 0x08
    static const sal_uInt8 nMatchedUsedAsReturn;     // 0x10

    int    nSign;                               // Sign of number
    short  nMonth;                              // Month (1..x) if date
                                                // negative => short format
    short  nMonthPos;                           // 1 = front, 2 = middle
                                                // 3 = end
    sal_uInt16 nTimePos;                            // Index of first time separator (+1)
    short  nDecPos;                             // Index of substring containing "," (+1)
    short  nNegCheck;                           // '( )' for negative
    short  nESign;                              // Sign of exponent
    short  nAmPm;                               // +1 AM, -1 PM, 0 if none
    short  nLogical;                            // -1 => False, 1 => True
    sal_uInt16 nThousand;                           // Count of group (AKA thousand) separators
    sal_uInt16 nPosThousandString;                  // Position of concatenaded 000,000,000 string
    short  eScannedType;                        // Scanned type
    short  eSetType;                            // Preset Type

    sal_uInt16 nStringScanNumFor;                   // Fixed strings recognized in
                                                // pFormat->NumFor[nNumForStringScan]
    short  nStringScanSign;                     // Sign resulting of FixString
    sal_uInt16 nYear2000;                           // Two-digit threshold
                                                // Year as 20xx
                                                // default 18
                                                // number <= nYear2000 => 20xx
                                                // number >  nYear2000 => 19xx
    sal_uInt16  nTimezonePos;                       // Index of timezone separator (+1)
    sal_uInt8    nMayBeIso8601;                      // 0:=dontknowyet, 1:=yes, 2:=no

//#ifdef _ZFORFIND_CXX        // methods private to implementation
    void Reset();                               // Reset all variables before start of analysis

    void InitText();                            // Init of months and days of week

    // Convert string to double.
    // Only simple unsigned floating point values without any error detection,
    // decimal separator has to be '.'
    // If bForceFraction==sal_True the string is taken to be the fractional part
    // of 0.1234 without the leading 0. (thus being just "1234").
    double StringToDouble(
            const String& rStr,
            sal_Bool bForceFraction = sal_False );

    sal_Bool NextNumberStringSymbol(                // Next number/string symbol
            const sal_Unicode*& pStr,
            String& rSymbol );

    sal_Bool SkipThousands(                         // Concatenate ,000,23 blocks
            const sal_Unicode*& pStr,           // in input to 000123
            String& rSymbol );

    void NumberStringDivision(                  // Divide numbers/strings into
            const String& rString );            // arrays and variables above.
                                                // Leading blanks and blanks
                                                // after numbers are thrown away


                                                // optimized substring versions

    static inline sal_Bool StringContains(          // Whether rString contains rWhat at nPos
            const String& rWhat,
            const String& rString,
            xub_StrLen nPos )
                {   // mostly used with one character
                    if ( rWhat.size() <= 0 || nPos >= rString.size() || rWhat.at(0) != rString.at(nPos) )
                        return sal_False;
                    return StringContainsImpl( rWhat, rString, nPos );
                }
    static inline sal_Bool StringPtrContains(       // Whether pString contains rWhat at nPos
            const String& rWhat,
            const sal_Unicode* pString,
            xub_StrLen nPos )                   // nPos MUST be a valid offset from pString
                {   // mostly used with one character
                    if ( rWhat.at(0) != *(pString+nPos) )
                        return sal_False;
                    return StringPtrContainsImpl( rWhat, pString, nPos );
                }
    static sal_Bool StringContainsImpl(             //! DO NOT use directly
            const String& rWhat,
            const String& rString,
            xub_StrLen nPos );
    static sal_Bool StringPtrContainsImpl(          //! DO NOT use directly
            const String& rWhat,
            const sal_Unicode* pString,
            xub_StrLen nPos );


    static inline sal_Bool SkipChar(                // Skip a special character
            sal_Unicode c,
            const String& rString,
            xub_StrLen& nPos );
    static inline void SkipBlanks(              // Skip blank
            const String& rString,
            xub_StrLen& nPos );
    static inline sal_Bool SkipString(              // Jump over rWhat in rString at nPos
            const String& rWhat,
            const String& rString,
            xub_StrLen& nPos );

    inline sal_Bool GetThousandSep(                 // Recognizes exactly ,111 as group separator
            const String& rString,
            xub_StrLen& nPos,
            sal_uInt16 nStringPos );
    short GetLogical(                           // Get boolean value
            const String& rString );
    short GetMonth(                             // Get month and advance string position
            const String& rString,
            xub_StrLen& nPos );
    int GetDayOfWeek(                           // Get day of week and advance string position
            const String& rString,
            xub_StrLen& nPos );
    sal_Bool GetCurrency(                           // Get currency symbol and advance string position
            const String& rString,
            xub_StrLen& nPos,
            const SvNumberformat* pFormat = NULL ); // optional number format to match against
    sal_Bool GetTimeAmPm(                           // Get symbol AM or PM and advance string position
            const String& rString,
            xub_StrLen& nPos );
    inline sal_Bool GetDecSep(                      // Get decimal separator and advance string position
            const String& rString,
            xub_StrLen& nPos );
    inline sal_Bool GetTime100SecSep(               // Get hundredth seconds separator and advance string position
            const String& rString,
            xub_StrLen& nPos );
    int GetSign(                                // Get sign  and advance string position
            const String& rString,              // Including special case '('
            xub_StrLen& nPos );
    short GetESign(                             // Get sign of exponent and advance string position
            const String& rString,
            xub_StrLen& nPos );

    inline sal_Bool GetNextNumber(                  // Get next number as array offset
            sal_uInt16& i,
            sal_uInt16& j );

    void GetTimeRef(                            // Converts time -> double (only decimals)
            double& fOutNumber,                 // result as double
            sal_uInt16 nIndex,                      // Index of hour in input
            sal_uInt16 nAnz );                      // Count of time substrings in input
    sal_uInt16 ImplGetDay  ( sal_uInt16 nIndex );       // Day input, 0 if no match
    sal_uInt16 ImplGetMonth( sal_uInt16 nIndex );       // Month input, zero based return, NumberOfMonths if no match
    sal_uInt16 ImplGetYear ( sal_uInt16 nIndex );       // Year input, 0 if no match
    sal_Bool GetDateRef(                            // Conversion of date to number
            double& fDays,                      // OUT: days diff to null date
            sal_uInt16& nCounter,                   // Count of date substrings
            const SvNumberformat* pFormat = NULL ); // optional number format to match against

    sal_Bool ScanStartString(                       // Analyze start of string
            const String& rString,
            const SvNumberformat* pFormat = NULL );
    sal_Bool ScanMidString(                         // Analyze middle substring
            const String& rString,
            sal_uInt16 nStringPos,
            const SvNumberformat* pFormat = NULL );
    sal_Bool ScanEndString(                         // Analyze end of string
            const String& rString,
            const SvNumberformat* pFormat = NULL );

    // Whether input may be a ISO 8601 date format, yyyy-mm-dd...
    // checks if at least 3 numbers and first number>31
    bool MayBeIso8601();

    // Compare rString to substring of array indexed by nString
    // nString == 0xFFFF => last substring
    sal_Bool ScanStringNumFor(
            const String& rString,
            xub_StrLen nPos,
            const SvNumberformat* pFormat,
            sal_uInt16 nString,
            sal_Bool bDontDetectNegation = sal_False );

    // if nMatchedAllStrings set nMatchedUsedAsReturn and return sal_True,
    // else do nothing and return sal_False
    sal_Bool MatchedReturn();

    //! Be sure that the string to be analyzed is already converted to upper
    //! case and if it contained native humber digits that they are already
    //! converted to ASCII.
    sal_Bool IsNumberFormatMain(                    // Main anlyzing function
            const String& rString,
            double& fOutNumber,                 // return value if string is numeric
            const SvNumberformat* pFormat = NULL    // optional number format to match against
            );

    static inline sal_Bool MyIsdigit( sal_Unicode c );

    // native number transliteration if necessary
    void TransformInput( String& rString );

//#endif  // _ZFORFIND_CXX
};
}       // namespace duckdb_numformat

#endif  // _ZFORFIND_HXX
