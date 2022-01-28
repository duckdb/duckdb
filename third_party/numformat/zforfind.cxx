// MARKER(update_precomp.py): autogen include statement, do not remove
#include "define.h"
#include <chrono>
#include <ctime> 
#include <ctype.h>
#include <stdlib.h>
#include <float.h>
#include <errno.h>
#include <math.h>
#include "date.hxx"
#include "datetime.hxx"
#include "digitgroupingiterator.hxx"
#include "zforfind.hxx"
#include "zforscan.hxx"


#ifndef DBG_UTIL
#define NF_TEST_CALENDAR 0
#else
#define NF_TEST_CALENDAR 0
#endif
#if NF_TEST_CALENDAR
#include <comphelper/processfactory.hxx>
#include <com/sun/star/i18n/XExtendedCalendar.hpp>
#endif

namespace duckdb_numformat {

const sal_uInt8 ImpSvNumberInputScan::nMatchedEndString    = 0x01;
const sal_uInt8 ImpSvNumberInputScan::nMatchedMidString    = 0x02;
const sal_uInt8 ImpSvNumberInputScan::nMatchedStartString  = 0x04;
const sal_uInt8 ImpSvNumberInputScan::nMatchedVirgin       = 0x08;
const sal_uInt8 ImpSvNumberInputScan::nMatchedUsedAsReturn = 0x10;

/* It is not clear how we want timezones to be handled. Convert them to local
 * time isn't wanted, as it isn't done in any other place and timezone
 * information isn't stored anywhere. Ignoring them and pretending local time
 * may be wrong too and might not be what the user expects. Keep the input as
 * string so that no information is lost.
 * Anyway, defining NF_RECOGNIZE_ISO8601_TIMEZONES to 1 would be the way how it
 * would work, together with the nTimezonePos handling in GetTimeRef(). */
#define NF_RECOGNIZE_ISO8601_TIMEZONES 0

//---------------------------------------------------------------------------
//      Konstruktor

ImpSvNumberInputScan::ImpSvNumberInputScan(LocaleData* pFormatterP )
        :
        pUpperMonthText( NULL ),
        pUpperAbbrevMonthText( NULL ),
        pUpperDayText( NULL ),
        pUpperAbbrevDayText( NULL )
{
    pFormatter = pFormatterP;
    nYear2000 = 1970;
	time_t	   nTmpTime;
	struct tm aTime;
	nTmpTime = time(0);
	if (localtime_r(&nTmpTime, &aTime))
	{
		nYear2000 = aTime.tm_year + 2000;
	}
	Reset();
    ChangeIntl();
}


//---------------------------------------------------------------------------
//      Destruktor

ImpSvNumberInputScan::~ImpSvNumberInputScan()
{
    Reset();
    delete [] pUpperMonthText;
    delete [] pUpperAbbrevMonthText;
    delete [] pUpperDayText;
    delete [] pUpperAbbrevDayText;
}


//---------------------------------------------------------------------------
//      Reset

void ImpSvNumberInputScan::Reset()
{
// ER 16.06.97 18:56 Vorbelegung erfolgt jetzt in NumberStringDivision,
// wozu immer alles loeschen wenn einiges wieder benutzt oder gar nicht
// gebraucht wird..
    for (sal_uInt16 i = 0; i < SV_MAX_ANZ_INPUT_STRINGS; i++)
    {
        sStrArray[i].erase();
        nNums[i] = SV_MAX_ANZ_INPUT_STRINGS-1;
        IsNum[i] = sal_False;
    }

    nMonth       = 0;
    nMonthPos    = 0;
    nTimePos     = 0;
    nSign        = 0;
    nESign       = 0;
    nDecPos      = 0;
    nNegCheck    = 0;
    nAnzStrings  = 0;
    nAnzNums     = 0;
    nThousand    = 0;
    eScannedType = NUMBERFORMAT_UNDEFINED;
    nAmPm        = 0;
    nPosThousandString = 0;
    nLogical     = 0;
    nStringScanNumFor = 0;
    nStringScanSign = 0;
    nMatchedAllStrings = nMatchedVirgin;
    nMayBeIso8601 = 0;
    nTimezonePos = 0;
}


//---------------------------------------------------------------------------
//
// static
inline sal_Bool ImpSvNumberInputScan::MyIsdigit( sal_Unicode c )
{
    // If the input string wouldn't be converted using TransformInput() we'd
    // to use something similar to the following and to adapt many places.
#if 0
    // use faster isdigit() if possible
    if ( c < 128 )
        return isdigit( (unsigned char) c ) != 0;
    if ( c < 256 )
        return sal_False;
    String aTmp( c );
    return pFormatter->GetCharClass()->isDigit( aTmp, 0 );
#else
    return c < 128 && isdigit( (unsigned char) c );
#endif
}


//---------------------------------------------------------------------------
//
void ImpSvNumberInputScan::TransformInput( String& rStr )
{
#if (0)
    xub_StrLen nPos, nLen;
    for ( nPos = 0, nLen = rStr.size(); nPos < nLen; ++nPos )
    {
        if ( 256 <= rStr.at( nPos ) &&
                pFormatter->GetCharClass()->isDigit( rStr, nPos ) )
            break;
    }
    if ( nPos < nLen )
        rStr = pFormatter->GetNatNum()->getNativeNumberString( rStr,
                pFormatter->GetLocale(), 0 );
#endif
}


//---------------------------------------------------------------------------
//      StringToDouble
//
// Only simple unsigned floating point values without any error detection,
// decimal separator has to be '.'

double ImpSvNumberInputScan::StringToDouble( const String& rStr, sal_Bool bForceFraction )
{
    double fNum = 0.0;
    double fFrac = 0.0;
    int nExp = 0;
    xub_StrLen nPos = 0;
    xub_StrLen nLen = rStr.size();
    sal_Bool bPreSep = !bForceFraction;

    while (nPos < nLen)
    {
        if (rStr.at(nPos) == L'.')
            bPreSep = sal_False;
        else if (bPreSep)
            fNum = fNum * 10.0 + (double) (rStr.at(nPos) - L'0');
        else
        {
            fFrac = fFrac * 10.0 + (double) (rStr.at(nPos) - L'0');
            --nExp;
        }
        nPos++;
    }
	if (fFrac)
		return fNum + fFrac * pow(10.0, nExp);
    return fNum;
}


//---------------------------------------------------------------------------
//       NextNumberStringSymbol
//
// Zerlegt die Eingabe in Zahlen und Strings fuer die weitere
// Verarbeitung (Turing-Maschine).
//---------------------------------------------------------------------------
// Ausgangs Zustand = GetChar
//---------------+-------------------+-----------------------+---------------
// Alter Zustand | gelesenes Zeichen | Aktion                | Neuer Zustand
//---------------+-------------------+-----------------------+---------------
// GetChar       | Ziffer            | Symbol=Zeichen        | GetValue
//               | Sonst             | Symbol=Zeichen        | GetString
//---------------|-------------------+-----------------------+---------------
// GetValue      | Ziffer            | Symbol=Symbol+Zeichen | GetValue
//               | Sonst             | Dec(CharPos)          | Stop
//---------------+-------------------+-----------------------+---------------
// GetString     | Ziffer            | Dec(CharPos)          | Stop
//               | Sonst             | Symbol=Symbol+Zeichen | GetString
//---------------+-------------------+-----------------------+---------------

enum ScanState              // States der Turing-Maschine
{
    SsStop      = 0,
    SsStart     = 1,
    SsGetValue  = 2,
    SsGetString = 3
};

sal_Bool ImpSvNumberInputScan::NextNumberStringSymbol(
        const sal_Unicode*& pStr,
        String& rSymbol )
{
    sal_Bool isNumber = sal_False;
    sal_Unicode cToken;
    ScanState eState = SsStart;
    const sal_Unicode* pHere = pStr;
    xub_StrLen nChars = 0;

    while ( ((cToken = *pHere) != 0) && eState != SsStop)
    {
        pHere++;
        switch (eState)
        {
            case SsStart:
                if ( MyIsdigit( cToken ) )
                {
                    eState = SsGetValue;
                    isNumber = sal_True;
                }
                else
                    eState = SsGetString;
                nChars++;
                break;
            case SsGetValue:
                if ( MyIsdigit( cToken ) )
                    nChars++;
                else
                {
                    eState = SsStop;
                    pHere--;
                }
                break;
            case SsGetString:
                if ( !MyIsdigit( cToken ) )
                    nChars++;
                else
                {
                    eState = SsStop;
                    pHere--;
                }
                break;
            default:
                break;
        }   // switch
    }   // while

    if ( nChars )
        rSymbol.assign( pStr, nChars );
    else
        rSymbol.erase();

    pStr = pHere;

    return isNumber;
}


//---------------------------------------------------------------------------
//      SkipThousands

// FIXME: should be grouping; it is only used though in case nAnzStrings is
// near SV_MAX_ANZ_INPUT_STRINGS, in NumberStringDivision().

sal_Bool ImpSvNumberInputScan::SkipThousands(
        const sal_Unicode*& pStr,
        String& rSymbol )
{
    sal_Bool res = sal_False;
    sal_Unicode cToken;
    const String& rThSep = pFormatter->GetNumThousandSep();
    const sal_Unicode* pHere = pStr;
    ScanState eState = SsStart;
    xub_StrLen nCounter = 0;                                // counts 3 digits

    while ( ((cToken = *pHere) != 0) && eState != SsStop)
    {
        pHere++;
        switch (eState)
        {
            case SsStart:
                if ( StringPtrContains( rThSep, pHere-1, 0 ) )
                {
                    nCounter = 0;
                    eState = SsGetValue;
                    pHere += rThSep.size()-1;
                }
                else
                {
                    eState = SsStop;
                    pHere--;
                }
                break;
            case SsGetValue:
                if ( MyIsdigit( cToken ) )
                {
                    rSymbol += cToken;
                    nCounter++;
                    if (nCounter == 3)
                    {
                        eState = SsStart;
                        res = sal_True;                 // .000 combination found
                    }
                }
                else
                {
                    eState = SsStop;
                    pHere--;
                }
                break;
            default:
                break;
        }   // switch
    }   // while

    if (eState == SsGetValue)               // break witth less than 3 digits
    {
        if ( nCounter )
            rSymbol.erase( rSymbol.size() - nCounter, nCounter );
        pHere -= nCounter + rThSep.size();       // put back ThSep also
    }
    pStr = pHere;

    return res;
}


//---------------------------------------------------------------------------
//      NumberStringDivision

void ImpSvNumberInputScan::NumberStringDivision( const String& rString )
{
    const sal_Unicode* pStr = rString.data();
    const sal_Unicode* const pEnd = pStr + rString.size();
    while ( pStr < pEnd && nAnzStrings < SV_MAX_ANZ_INPUT_STRINGS )
    {
        if ( NextNumberStringSymbol( pStr, sStrArray[nAnzStrings] ) )
        {                                               // Zahl
            IsNum[nAnzStrings] = sal_True;
            nNums[nAnzNums] = nAnzStrings;
            nAnzNums++;
            if (nAnzStrings >= SV_MAX_ANZ_INPUT_STRINGS - 7 &&
                nPosThousandString == 0)                // nur einmal
                if ( SkipThousands( pStr, sStrArray[nAnzStrings] ) )
                    nPosThousandString = nAnzStrings;
        }
        else
        {
            IsNum[nAnzStrings] = sal_False;
        }
        nAnzStrings++;
    }
}


//---------------------------------------------------------------------------
// Whether rString contains rWhat at nPos

sal_Bool ImpSvNumberInputScan::StringContainsImpl( const String& rWhat,
            const String& rString, xub_StrLen nPos )
{
    if ( nPos + rWhat.size() <= rString.size() )
        return StringPtrContainsImpl( rWhat, rString.data(), nPos );
    return sal_False;
}


//---------------------------------------------------------------------------
// Whether pString contains rWhat at nPos

sal_Bool ImpSvNumberInputScan::StringPtrContainsImpl( const String& rWhat,
            const sal_Unicode* pString, xub_StrLen nPos )
{
    if ( rWhat.size() == 0 )
        return sal_False;
    const sal_Unicode* pWhat = rWhat.data();
    const sal_Unicode* const pEnd = pWhat + rWhat.size();
    const sal_Unicode* pStr = pString + nPos;
    while ( pWhat < pEnd )
    {
        if ( *pWhat != *pStr )
            return sal_False;
        pWhat++;
        pStr++;
    }
    return sal_True;
}


//---------------------------------------------------------------------------
//      SkipChar
//
// ueberspringt genau das angegebene Zeichen

inline sal_Bool ImpSvNumberInputScan::SkipChar( sal_Unicode c, const String& rString,
        xub_StrLen& nPos )
{
    if ((nPos < rString.size()) && (rString.at(nPos) == c))
    {
        nPos++;
        return sal_True;
    }
    return sal_False;
}


//---------------------------------------------------------------------------
//      SkipBlanks
//
// Ueberspringt Leerzeichen

inline void ImpSvNumberInputScan::SkipBlanks( const String& rString,
        xub_StrLen& nPos )
{
    if ( nPos < rString.size() )
    {
        const sal_Unicode* p = rString.data() + nPos;
        while ( *p == L' ' )
        {
            nPos++;
            p++;
        }
    }
}


//---------------------------------------------------------------------------
//      SkipString
//
// jump over rWhat in rString at nPos

inline sal_Bool ImpSvNumberInputScan::SkipString( const String& rWhat,
        const String& rString, xub_StrLen& nPos )
{
    if ( StringContains( rWhat, rString, nPos ) )
    {
        nPos = nPos + rWhat.size();
        return sal_True;
    }
    return sal_False;
}


//---------------------------------------------------------------------------
//      GetThousandSep
//
// recognizes exactly ,111 in {3} and {3,2} or ,11 in {3,2} grouping

inline sal_Bool ImpSvNumberInputScan::GetThousandSep(
        const String& rString,
        xub_StrLen& nPos,
        sal_uInt16 nStringPos )
{
    const String& rSep = pFormatter->GetNumThousandSep();
    // Is it an ordinary space instead of a non-breaking space?
    bool bSpaceBreak = rSep.at(0) == 0xa0 && rString.at(0) == 0x20 &&
        rSep.size() == 1 && rString.size() == 1;
    if (!( (rString == rSep || bSpaceBreak)             // nothing else
                && nStringPos < nAnzStrings - 1         // safety first!
                && IsNum[nStringPos+1] ))               // number follows
        return sal_False;                                   // no? => out

	DigitGroupingIterator aGrouping(pFormatter->getDigitGrouping());
	// Match ,### in {3} or ,## in {3,2}
    /* FIXME: this could be refined to match ,## in {3,2} only if ,##,## or
     * ,##,### and to match ,### in {3,2} only if it's the last. However,
     * currently there is no track kept where group separators occur. In {3,2}
     * #,###,### and #,##,## would be valid input, which maybe isn't even bad
     * for #,###,###. Other combinations such as #,###,## maybe not. */
	xub_StrLen nLen = sStrArray[nStringPos + 1].size();
	if (nLen == aGrouping.get()                         // with 3 (or so) digits
		|| nLen == aGrouping.advance().get()        // or with 2 (or 3 or so) digits
		|| nPosThousandString == nStringPos + 1       // or concatenated
		)
	{
		nPos = nPos + rSep.size();
		return sal_True;
	}
	return sal_False;
}


//---------------------------------------------------------------------------
//      GetLogical
//
// Conversion of text to logial value
// "sal_True" =>  1:
// "sal_False"=> -1:
// else   =>  0:

short ImpSvNumberInputScan::GetLogical( const String& rString )
{
    short res;

    const ImpSvNumberformatScan* pFS = pFormatter->GetFormatScanner();
    if ( rString == pFS->GetTrueString() )
        res = 1;
    else if ( rString == pFS->GetFalseString() )
        res = -1;
    else
        res = 0;

    return res;
}


//---------------------------------------------------------------------------
//      GetMonth
//
// Converts a string containing a month name (JAN, January) at nPos into the
// month number (negative if abbreviated), returns 0 if nothing found

short ImpSvNumberInputScan::GetMonth( const String& rString, xub_StrLen& nPos )
{
    // #102136# The correct English form of month September abbreviated is
    // SEPT, but almost every data contains SEP instead.
    static const String aSeptCorrect = L"SEPT";
    static const String aSepShortened = L"SEP";

    short res = 0;      // no month found

    if (rString.size() > nPos)                           // only if needed
    {
        if ( !bTextInitialized )
            InitText();
        sal_Int16 nMonths = pFormatter->getMonthsOfYearSize();
        for ( sal_Int16 i = 0; i < nMonths; i++ )
        {
            if ( StringContains( pUpperMonthText[i], rString, nPos ) )
            {                                           // full names first
                nPos = nPos + pUpperMonthText[i].size();
                res = i+1;
                break;  // for
            }
            else if ( StringContains( pUpperAbbrevMonthText[i], rString, nPos ) )
            {                                           // abbreviated
                nPos = nPos + pUpperAbbrevMonthText[i].size();
                res = (short)(-(i+1)); // negative
                break;  // for
            }
            else if ( i == 8 && pUpperAbbrevMonthText[i] == aSeptCorrect &&
                    StringContains( aSepShortened, rString, nPos ) )
            {                                           // #102136# SEPT/SEP
                nPos = nPos + aSepShortened.size();
                res = (short)(-(i+1)); // negative
                break;  // for
            }
        }
    }

    return res;
}


//---------------------------------------------------------------------------
//      GetDayOfWeek
//
// Converts a string containing a DayOfWeek name (Mon, Monday) at nPos into the
// DayOfWeek number + 1 (negative if abbreviated), returns 0 if nothing found

int ImpSvNumberInputScan::GetDayOfWeek( const String& rString, xub_StrLen& nPos )
{
    int res = 0;      // no day found

    if (rString.size() > nPos)                           // only if needed
    {
        if ( !bTextInitialized )
            InitText();
        sal_Int16 nDays = pFormatter->getDayOfWeekSize();
        for ( sal_Int16 i = 0; i < nDays; i++ )
        {
            if ( StringContains( pUpperDayText[i], rString, nPos ) )
            {                                           // full names first
                nPos = nPos + pUpperDayText[i].size();
                res = i + 1;
                break;  // for
            }
            if ( StringContains( pUpperAbbrevDayText[i], rString, nPos ) )
            {                                           // abbreviated
                nPos = nPos + pUpperAbbrevDayText[i].size();
                res = -(i + 1);                         // negative
                break;  // for
            }
        }
    }

    return res;
}


//---------------------------------------------------------------------------
//      GetCurrency
//
// Lesen eines Waehrungssysmbols
// '$'   => sal_True
// sonst => sal_False

sal_Bool ImpSvNumberInputScan::GetCurrency( const String& rString, xub_StrLen& nPos,
            const SvNumberformat* pFormat )
{
    if ( rString.size() > nPos )
    {
        if ( !aUpperCurrSymbol.size() )
        {   // if no format specified the currency of the initialized formatter
            aUpperCurrSymbol = pFormatter->getCurrSymbol();
			ConvertToUpper(aUpperCurrSymbol);
        }
        if ( StringContains( aUpperCurrSymbol, rString, nPos ) )
        {
            nPos = nPos + aUpperCurrSymbol.size();
            return sal_True;
        }
        if ( pFormat )
        {
            String aSymbol, aExtension;
            if ( pFormat->GetNewCurrencySymbol( aSymbol, aExtension ) )
            {
                if ( aSymbol.size() <= rString.size() - nPos )
                {
					ConvertToUpper(aSymbol);
                    if ( StringContains( aSymbol, rString, nPos ) )
                    {
                        nPos = nPos + aSymbol.size();
                        return sal_True;
                    }
                }
            }
        }
    }

    return sal_False;
}


//---------------------------------------------------------------------------
//      GetTimeAmPm
//
// Lesen des Zeitsymbols (AM od. PM) f. kurze Zeitangabe
//
// Rueckgabe:
//  "AM" od. "PM" => sal_True
//  sonst         => sal_False
//
// nAmPos:
//  "AM"  =>  1
//  "PM"  => -1
//  sonst =>  0

sal_Bool ImpSvNumberInputScan::GetTimeAmPm( const String& rString, xub_StrLen& nPos )
{

    if ( rString.size() > nPos )
    {
		String am_str = pFormatter->getTimeAM();
		ConvertToUpper(am_str);
		String pm_str = pFormatter->getTimePM();
		ConvertToUpper(pm_str);
		if (StringContains(am_str, rString, nPos))
        {
            nAmPm = 1;
            nPos = nPos + pFormatter->getTimeAM().size();
            return sal_True;
        }
        else if ( StringContains( pm_str, rString, nPos ) )
        {
            nAmPm = -1;
            nPos = nPos + pFormatter->getTimePM().size();
            return sal_True;
        }
    }

    return sal_False;
}


//---------------------------------------------------------------------------
//      GetDecSep
//
// Lesen eines Dezimaltrenners (',')
// ','   => sal_True
// sonst => sal_False

inline sal_Bool ImpSvNumberInputScan::GetDecSep( const String& rString, xub_StrLen& nPos )
{
    if ( rString.size() > nPos )
    {
        const String& rSep = pFormatter->GetNumDecimalSep();
        if ( rString.substr(nPos) == rSep )
        {
            nPos = nPos + rSep.size();
            return sal_True;
        }
    }
    return sal_False;
}


//---------------------------------------------------------------------------
// read a hundredth seconds separator

inline sal_Bool ImpSvNumberInputScan::GetTime100SecSep( const String& rString, xub_StrLen& nPos )
{
    if ( rString.size() > nPos )
    {
        const String& rSep = pFormatter->getTime100SecSep();
        if ( rString.substr(nPos) == rSep )
        {
            nPos = nPos + rSep.size();
            return sal_True;
        }
    }
    return sal_False;
}


//---------------------------------------------------------------------------
//      GetSign
//
// Lesen eines Vorzeichens, auch Klammer !?!
// '+'   =>  1
// '-'   => -1
// '('   => -1, nNegCheck = 1
// sonst =>  0

int ImpSvNumberInputScan::GetSign( const String& rString, xub_StrLen& nPos )
{
    if (rString.size() > nPos)
        switch (rString.at(nPos))
        {
            case L'+':
                nPos++;
                return 1;
            case L'(':               // '(' aehnlich wie '-' ?!?
                nNegCheck = 1;
                //! fallthru
            case L'-':
                nPos++;
                return -1;
            default:
                break;
        }

    return 0;
}


//---------------------------------------------------------------------------
//      GetESign
//
// Lesen eines Vorzeichens, gedacht fuer Exponent ?!?
// '+'   =>  1
// '-'   => -1
// sonst =>  0

short ImpSvNumberInputScan::GetESign( const String& rString, xub_StrLen& nPos )
{
    if (rString.size() > nPos)
        switch (rString.at(nPos))
        {
            case L'+':
                nPos++;
                return 1;
            case L'-':
                nPos++;
                return -1;
            default:
                break;
        }

    return 0;
}


//---------------------------------------------------------------------------
//      GetNextNumber
//
// i counts string portions, j counts numbers thereof.
// It should had been called SkipNumber instead.

inline sal_Bool ImpSvNumberInputScan::GetNextNumber( sal_uInt16& i, sal_uInt16& j )
{
    if ( i < nAnzStrings && IsNum[i] )
    {
        j++;
        i++;
        return sal_True;
    }
    return sal_False;
}


//---------------------------------------------------------------------------
//      GetTimeRef

void ImpSvNumberInputScan::GetTimeRef(
        double& fOutNumber,
        sal_uInt16 nIndex,          // j-value of the first numeric time part of input, default 0
        sal_uInt16 nAnz )           // count of numeric time parts
{
    sal_uInt16 nHour;
    sal_uInt16 nMinute = 0;
    sal_uInt16 nSecond = 0;
    double fSecond100 = 0.0;
    sal_uInt16 nStartIndex = nIndex;

    if (nTimezonePos)
    {
        // find first timezone number index and adjust count
        for (sal_uInt16 j=0; j<nAnzNums; ++j)
        {
            if (nNums[j] == nTimezonePos)
            {
                // nAnz is not total count, but count of time relevant strings.
                if (nStartIndex < j && j - nStartIndex < nAnz)
                    nAnz = j - nStartIndex;
                break;  // for
            }
        }
    }

    if (nDecPos == 2 && (nAnz == 3 || nAnz == 2))   // 20:45.5 or 45.5
        nHour = 0;
    else if (nIndex - nStartIndex < nAnz)
        nHour   = (sal_uInt16) stoi(sStrArray[nNums[nIndex++]]);
    else
    {
        nHour = 0;
        //DBG_ERRORFILE( "ImpSvNumberInputScan::GetTimeRef: bad number index");
    }
    if (nDecPos == 2 && nAnz == 2)                  // 45.5
        nMinute = 0;
    else if (nIndex - nStartIndex < nAnz)
        nMinute = (sal_uInt16)stoi(sStrArray[nNums[nIndex++]]);
    if (nIndex - nStartIndex < nAnz)
        nSecond = (sal_uInt16)stoi(sStrArray[nNums[nIndex++]]);
    if (nIndex - nStartIndex < nAnz)
        fSecond100 = StringToDouble( sStrArray[nNums[nIndex]], sal_True );
    if (nAmPm == -1 && nHour != 12)             // PM
        nHour += 12;
    else if (nAmPm == 1 && nHour == 12)         // 12 AM
        nHour = 0;

    fOutNumber = ((double)nHour*3600 +
                  (double)nMinute*60 +
                  (double)nSecond +
                  fSecond100)/86400.0;
}


//---------------------------------------------------------------------------
//      ImplGetDay

sal_uInt16 ImpSvNumberInputScan::ImplGetDay( sal_uInt16 nIndex )
{
    sal_uInt16 nRes = 0;

    if (sStrArray[nNums[nIndex]].size() <= 2)
    {
        sal_uInt16 nNum = (sal_uInt16)stoi(sStrArray[nNums[nIndex]]);
        if (nNum <= 31)
            nRes = nNum;
    }

    return nRes;
}


//---------------------------------------------------------------------------
//      ImplGetMonth

sal_uInt16 ImpSvNumberInputScan::ImplGetMonth( sal_uInt16 nIndex )
{
    // preset invalid month number
    sal_uInt16 nRes = pFormatter->getMonthsOfYearSize();

    if (sStrArray[nNums[nIndex]].size() <= 2)
    {
        sal_uInt16 nNum = (sal_uInt16)std::stoi(sStrArray[nNums[nIndex]]);
        if ( 0 < nNum && nNum <= nRes )
            nRes = nNum - 1;        // zero based for CalendarFieldIndex::CFI_MONTH
    }

    return nRes;
}


//---------------------------------------------------------------------------
//      ImplGetYear
//
// 30 -> 1930, 29 -> 2029, oder 56 -> 1756, 55 -> 1855, ...

sal_uInt16 ImpSvNumberInputScan::ImplGetYear( sal_uInt16 nIndex )
{
    sal_uInt16 nYear = 0;

    if (sStrArray[nNums[nIndex]].size() <= 4)
    {
        nYear = (sal_uInt16)std::stoi(sStrArray[nNums[nIndex]]);
		if (nYear < 100)
		{
			if (nYear < (nYear2000 % 100))
				return nYear + (((nYear2000 / 100) + 1) * 100);
			else
				return nYear + ((nYear2000 / 100) * 100);
		}
	}

    return nYear;
}

//---------------------------------------------------------------------------

bool ImpSvNumberInputScan::MayBeIso8601()
{
    if (nMayBeIso8601 == 0)
    {
        if (nAnzNums >= 3 && nNums[0] < nAnzStrings &&
			stoi(sStrArray[nNums[0]]) > 31)
            nMayBeIso8601 = 1;
        else
            nMayBeIso8601 = 2;
    }
    return nMayBeIso8601 == 1;
}

//---------------------------------------------------------------------------
//      GetDateRef

sal_Bool ImpSvNumberInputScan::GetDateRef( double& fDays, sal_uInt16& nCounter,
        const SvNumberformat* pFormat )
{
    //using namespace ::com::sun::star::i18n;
    NfEvalDateFormat eEDF;
    int nFormatOrder;
    if ( pFormat && ((pFormat->GetType() & NUMBERFORMAT_DATE) == NUMBERFORMAT_DATE) )
    {
        eEDF = pFormatter->GetEvalDateFormat();
        switch ( eEDF )
        {
            case NF_EVALDATEFORMAT_INTL :
            case NF_EVALDATEFORMAT_FORMAT :
                nFormatOrder = 1;       // only one loop
            break;
            default:
                nFormatOrder = 2;
                if ( nMatchedAllStrings )
                    eEDF = NF_EVALDATEFORMAT_FORMAT_INTL;
                    // we have a complete match, use it
        }
    }
    else
    {
        eEDF = NF_EVALDATEFORMAT_INTL;
        nFormatOrder = 1;
    }
    sal_Bool res = sal_True;

    Calendar* pCal = pFormatter->GetCalendar();
    for ( int nTryOrder = 1; nTryOrder <= nFormatOrder; nTryOrder++ )
    {
        pCal->setGregorianDateTime( Date() );       // today
        String aOrgCalendar;        // empty => not changed yet
        DateFormat DateFmt;
        sal_Bool bFormatTurn;
        switch ( eEDF )
        {
            case NF_EVALDATEFORMAT_INTL :
                bFormatTurn = sal_False;
                DateFmt = pFormatter->getDateFormat();
            break;
            //case NF_EVALDATEFORMAT_FORMAT :
            //    bFormatTurn = sal_True;
            //    DateFmt = pFormat->GetDateOrder();
            //break;
            //case NF_EVALDATEFORMAT_INTL_FORMAT :
            //    if ( nTryOrder == 1 )
            //    {
            //        bFormatTurn = sal_False;
            //        DateFmt = pFormatter->getDateFormat();
            //    }
            //    else
            //    {
            //        bFormatTurn = sal_True;
            //        DateFmt = pFormat->GetDateOrder();
            //    }
            //break;
            //case NF_EVALDATEFORMAT_FORMAT_INTL :
            //    if ( nTryOrder == 2 )
            //    {
            //        bFormatTurn = sal_False;
            //        DateFmt = pFormatter->getDateFormat();
            //    }
            //    else
            //    {
            //        bFormatTurn = sal_True;
            //        DateFmt = pFormat->GetDateOrder();
            //    }
            //break;
            default:
                //DBG_ERROR( "ImpSvNumberInputScan::GetDateRef: unknown NfEvalDateFormat" );
				DateFmt = DateFormat::YMD;
				bFormatTurn = sal_False;
        }
        if ( bFormatTurn )
        {
#if 0
/* TODO:
We are currently not able to fully support a switch to another calendar during
input for the following reasons:
1. We do have a problem if both (locale's default and format's) calendars
   define the same YMD order and use the same date separator, there is no way
   to distinguish between them if the input results in valid calendar input for
   both calendars. How to solve? Would NfEvalDateFormat be sufficient? Should
   it always be set to NF_EVALDATEFORMAT_FORMAT_INTL and thus the format's
   calendar be preferred? This could be confusing if a Calc cell was formatted
   different to the locale's default and has no content yet, then the user has
   no clue about the format or calendar being set.
2. In Calc cell edit mode a date is always displayed and edited using the
   default edit format of the default calendar (normally being Gregorian). If
   input was ambiguous due to issue #1 we'd need a mechanism to tell that a
   date was edited and not newly entered. Not feasible. Otherwise we'd need a
   mechanism to use a specific edit format with a specific calendar according
   to the format set.
3. For some calendars like Japanese Gengou we'd need era input, which isn't
   implemented at all. Though this is a rare and special case, forcing a
   calendar dependent edit format as suggested in item #2 might require era
   input, if it shouldn't result in a fallback to Gregorian calendar.
4. Last and least: the GetMonth() method currently only matches month names of
   the default calendar. Alternating month names of the actual format's
   calendar would have to be implemented. No problem.

*/
            if ( pFormat->IsOtherCalendar( nStringScanNumFor ) )
                pFormat->SwitchToOtherCalendar( aOrgCalendar, fOrgDateTime );
            else
                pFormat->SwitchToSpecifiedCalendar( aOrgCalendar, fOrgDateTime,
                        nStringScanNumFor );
#endif
        }

        res = sal_True;
        nCounter = 0;
        // For incomplete dates, always assume first day of month if not specified.
        pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, 1 );

        switch (nAnzNums)       // count of numbers in string
        {
            case 0:                 // none
                if (nMonthPos)          // only month (Jan)
                    pCal->setValue( CalendarFieldIndex::CFI_MONTH, abs(nMonth)-1 );
                else
                    res = sal_False;
                break;

            case 1:                 // only one number
                nCounter = 1;
                switch (nMonthPos)  // where is the month
                {
                    case 0:             // not found => only day entered
                        pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(0) );
                        break;
                    case 1:             // month at the beginning (Jan 01)
                        pCal->setValue( CalendarFieldIndex::CFI_MONTH, abs(nMonth)-1 );
                        switch (DateFmt)
                        {
                            case MDY:
                            case YMD:
                                pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(0) );
                                break;
                            case DMY:
                                pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(0) );
                                break;
                            default:
                                res = sal_False;
                                break;
                        }
                        break;
                    case 3:             // month at the end (10 Jan)
                        pCal->setValue( CalendarFieldIndex::CFI_MONTH, abs(nMonth)-1 );
                        switch (DateFmt)
                        {
                            case DMY:
                                pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(0) );
                                break;
                            case YMD:
                                pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(0) );
                                break;
                            default:
                                res = sal_False;
                                break;
                        }
                        break;
                    default:
                        res = sal_False;
                        break;
                }   // switch (nMonthPos)
                break;

            case 2:                 // 2 numbers
                nCounter = 2;
                switch (nMonthPos)  // where is the month
                {
                    case 0:             // not found
                    {
                        bool bHadExact;
                        sal_uInt32 nExactDateOrder = (bFormatTurn ? pFormat->GetExactDateOrder() : 0);
                        if ( 0xff < nExactDateOrder && nExactDateOrder <= 0xffff )
                        {   // formatted as date and exactly 2 parts
                            bHadExact = true;
                            switch ( (nExactDateOrder >> 8) & 0xff )
                            {
                                case L'Y':
                                    pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(0) );
                                break;
                                case L'M':
                                    pCal->setValue( CalendarFieldIndex::CFI_MONTH, ImplGetMonth(0) );
                                break;
                                case L'D':
                                    pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(0) );
                                break;
                                default:
                                    bHadExact = false;
                            }
                            switch ( nExactDateOrder & 0xff )
                            {
                                case L'Y':
                                    pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(1) );
                                break;
                                case L'M':
                                    pCal->setValue( CalendarFieldIndex::CFI_MONTH, ImplGetMonth(1) );
                                break;
                                case L'D':
                                    pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(1) );
                                break;
                                default:
                                    bHadExact = false;
                            }
                        }
                        else
                            bHadExact = false;
                        if ( !bHadExact || !pCal->isValid() )
                        {
                            if ( !bHadExact && nExactDateOrder )
                                pCal->setGregorianDateTime( Date() );   // reset today
                            switch (DateFmt)
                            {
                                case MDY:
                                    // M D
                                    pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(1) );
                                    pCal->setValue( CalendarFieldIndex::CFI_MONTH, ImplGetMonth(0) );
                                    if ( !pCal->isValid() )             // 2nd try
                                    {                                   // M Y
                                        pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, 1 );
                                        pCal->setValue( CalendarFieldIndex::CFI_MONTH, ImplGetMonth(0) );
                                        pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(1) );
                                    }
                                    break;
                                case DMY:
                                    // D M
                                    pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(0) );
                                    pCal->setValue( CalendarFieldIndex::CFI_MONTH, ImplGetMonth(1) );
                                    if ( !pCal->isValid() )             // 2nd try
                                    {                                   // M Y
                                        pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, 1 );
                                        pCal->setValue( CalendarFieldIndex::CFI_MONTH, ImplGetMonth(0) );
                                        pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(1) );
                                    }
                                    break;
                                case YMD:
                                    // M D
                                    pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(1) );
                                    pCal->setValue( CalendarFieldIndex::CFI_MONTH, ImplGetMonth(0) );
                                    if ( !pCal->isValid() )             // 2nd try
                                    {                                   // Y M
                                        pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, 1 );
                                        pCal->setValue( CalendarFieldIndex::CFI_MONTH, ImplGetMonth(1) );
                                        pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(0) );
                                    }
                                    break;
                                default:
                                    res = sal_False;
                                    break;
                            }
                        }
                    }
                    break;
                    case 1:             // month at the beginning (Jan 01 01)
                    {
                        // The input is valid as MDY in almost any
                        // constellation, there is no date order (M)YD except if
                        // set in a format applied.
                        pCal->setValue( CalendarFieldIndex::CFI_MONTH, abs(nMonth)-1 );
                        sal_uInt32 nExactDateOrder = (bFormatTurn ? pFormat->GetExactDateOrder() : 0);
                        if ((((nExactDateOrder >> 8) & 0xff) == L'Y') && ((nExactDateOrder & 0xff) == L'D'))
                        {
                            pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(1) );
                            pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(0) );
                        }
                        else
                        {
                            pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(0) );
                            pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(1) );
                        }
                    }
                    break;
                    case 2:             // month in the middle (10 Jan 94)
                        pCal->setValue( CalendarFieldIndex::CFI_MONTH, abs(nMonth)-1 );
                        switch (DateFmt)
                        {
                            case MDY:   // yes, "10-Jan-94" is valid
                            case DMY:
                                pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(0) );
                                pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(1) );
                                break;
                            case YMD:
                                pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(1) );
                                pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(0) );
                                break;
                            default:
                                res = sal_False;
                                break;
                        }
                        break;
                    default:            // else, e.g. month at the end (94 10 Jan)
                        res = sal_False;
                        break;
                }   // switch (nMonthPos)
                break;

            default:                // more than two numbers (31.12.94 8:23) (31.12. 8:23)
                switch (nMonthPos)  // where is the month
                {
                    case 0:             // not found
                    {
                        nCounter = 3;
                        if ( nTimePos > 1 )
                        {   // find first time number index (should only be 3 or 2 anyway)
                            for ( sal_uInt16 j = 0; j < nAnzNums; j++ )
                            {
                                if ( nNums[j] == nTimePos - 2 )
                                {
                                    nCounter = j;
                                    break;  // for
                                }
                            }
                        }
                        // ISO 8601 yyyy-mm-dd forced recognition
                        DateFormat eDF = (MayBeIso8601() ? YMD : DateFmt);
                        switch (eDF)
                        {
                            case MDY:
                                pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(1) );
                                pCal->setValue( CalendarFieldIndex::CFI_MONTH, ImplGetMonth(0) );
                                if ( nCounter > 2 )
                                    pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(2) );
                                break;
                            case DMY:
                                pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(0) );
                                pCal->setValue( CalendarFieldIndex::CFI_MONTH, ImplGetMonth(1) );
                                if ( nCounter > 2 )
                                    pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(2) );
                                break;
                            case YMD:
                                if ( nCounter > 2 )
                                    pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(2) );
                                pCal->setValue( CalendarFieldIndex::CFI_MONTH, ImplGetMonth(1) );
                                pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(0) );
                                break;
                            default:
                                res = sal_False;
                                break;
                        }
                    }
                        break;
                    case 1:             // month at the beginning (Jan 01 01 8:23)
                        nCounter = 2;
                        switch (DateFmt)
                        {
                            case MDY:
                                pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(0) );
                                pCal->setValue( CalendarFieldIndex::CFI_MONTH, abs(nMonth)-1 );
                                pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(1) );
                                break;
                            default:
                                res = sal_False;
                                break;
                        }
                        break;
                    case 2:             // month in the middle (10 Jan 94 8:23)
                        nCounter = 2;
                        pCal->setValue( CalendarFieldIndex::CFI_MONTH, abs(nMonth)-1 );
                        switch (DateFmt)
                        {
                            case DMY:
                                pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(0) );
                                pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(1) );
                                break;
                            case YMD:
                                pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, ImplGetDay(1) );
                                pCal->setValue( CalendarFieldIndex::CFI_YEAR, ImplGetYear(0) );
                                break;
                            default:
                                res = sal_False;
                                break;
                        }
                        break;
                    default:            // else, e.g. month at the end (94 10 Jan 8:23)
                        nCounter = 2;
                        res = sal_False;
                        break;
                }   // switch (nMonthPos)
                break;
        }   // switch (nAnzNums)

        if ( res && pCal->isValid() )
        {
            //double fDiff = DateTime(*pCal->GetNullDate()) - pCal->getEpochStart();
            fDays = floor( pCal->getLocalDateTime() );
            //fDays -= fDiff;
            nTryOrder = nFormatOrder;   // break for
        }
        else
            res = sal_False;

        //if ( aOrgCalendar.size() )
        //    pCal->loadCalendar( aOrgCalendar, pLoc->getLocale() );  // restore calendar

#if NF_TEST_CALENDAR
{
    using namespace ::com::sun::star;
    struct entry { const char* lan; const char* cou; const char* cal; };
    const entry cals[] = {
        { "en", "US",  "gregorian" },
        { "ar", "TN",      "hijri" },
        { "he", "IL",     "jewish" },
        { "ja", "JP",     "gengou" },
        { "ko", "KR", "hanja_yoil" },
        { "th", "TH",   "buddhist" },
        { "zh", "TW",        "ROC" },
        {0,0,0}
    };
    lang::Locale aLocale;
    sal_Bool bValid;
    sal_Int16 nDay, nMyMonth, nYear, nHour, nMinute, nSecond;
    sal_Int16 nDaySet, nMonthSet, nYearSet, nHourSet, nMinuteSet, nSecondSet;
    sal_Int16 nZO, nDST1, nDST2, nDST, nZOmillis, nDST1millis, nDST2millis, nDSTmillis;
    sal_Int32 nZoneInMillis, nDST1InMillis, nDST2InMillis;
    uno::Reference< lang::XMultiServiceFactory > xSMgr =
        ::comphelper::getProcessServiceFactory();
    uno::Reference< ::com::sun::star::i18n::XExtendedCalendar > xCal(
            xSMgr->createInstance( ::rtl::OUString(
                    RTL_CONSTASCII_USTRINGPARAM(
                        "com.sun.star.i18n.LocaleCalendar" ) ) ),
            uno::UNO_QUERY );
    for ( const entry* p = cals; p->lan; ++p )
    {
        aLocale.Language = ::rtl::OUString::createFromAscii( p->lan );
        aLocale.Country  = ::rtl::OUString::createFromAscii( p->cou );
        xCal->loadCalendar( ::rtl::OUString::createFromAscii( p->cal ),
                aLocale );
        double nDateTime = 0.0;     // 1-Jan-1970 00:00:00
        nZO           = xCal->getValue( i18n::CalendarFieldIndex::ZONE_OFFSET );
        nZOmillis     = xCal->getValue( i18n::CalendarFieldIndex::ZONE_OFFSET_SECOND_MILLIS );
        nZoneInMillis = static_cast<sal_Int32>(nZO) * 60000 +
            (nZO < 0 ? -1 : 1) * static_cast<sal_uInt16>(nZOmillis);
        nDST1         = xCal->getValue( i18n::CalendarFieldIndex::DST_OFFSET );
        nDST1millis   = xCal->getValue( i18n::CalendarFieldIndex::DST_OFFSET_SECOND_MILLIS );
        nDST1InMillis = static_cast<sal_Int32>(nDST1) * 60000 +
            (nDST1 < 0 ? -1 : 1) * static_cast<sal_uInt16>(nDST1millis);
        nDateTime    -= (double)(nZoneInMillis + nDST1InMillis) / 1000.0 / 60.0 / 60.0 / 24.0;
        xCal->setDateTime( nDateTime );
        nDST2         = xCal->getValue( i18n::CalendarFieldIndex::DST_OFFSET );
        nDST2millis   = xCal->getValue( i18n::CalendarFieldIndex::DST_OFFSET_SECOND_MILLIS );
        nDST2InMillis = static_cast<sal_Int32>(nDST2) * 60000 +
            (nDST2 < 0 ? -1 : 1) * static_cast<sal_uInt16>(nDST2millis);
        if ( nDST1InMillis != nDST2InMillis )
        {
            nDateTime = 0.0 - (double)(nZoneInMillis + nDST2InMillis) / 1000.0 / 60.0 / 60.0 / 24.0;
            xCal->setDateTime( nDateTime );
        }
        nDaySet    = xCal->getValue( i18n::CalendarFieldIndex::DAY_OF_MONTH );
        nMonthSet  = xCal->getValue( i18n::CalendarFieldIndex::CFI_MONTH );
        nYearSet   = xCal->getValue( i18n::CalendarFieldIndex::CFI_YEAR );
        nHourSet   = xCal->getValue( i18n::CalendarFieldIndex::CFI_HOUR );
        nMinuteSet = xCal->getValue( i18n::CalendarFieldIndex::CFI_MINUTE );
        nSecondSet = xCal->getValue( i18n::CalendarFieldIndex::CFI_SECOND );
        nZO        = xCal->getValue( i18n::CalendarFieldIndex::ZONE_OFFSET );
        nZOmillis  = xCal->getValue( i18n::CalendarFieldIndex::ZONE_OFFSET_SECOND_MILLIS );
        nDST       = xCal->getValue( i18n::CalendarFieldIndex::DST_OFFSET );
        nDSTmillis = xCal->getValue( i18n::CalendarFieldIndex::DST_OFFSET_SECOND_MILLIS );
        xCal->setValue( i18n::CalendarFieldIndex::DAY_OF_MONTH, nDaySet );
        xCal->setValue( i18n::CalendarFieldIndex::CFI_MONTH, nMonthSet );
        xCal->setValue( i18n::CalendarFieldIndex::CFI_YEAR, nYearSet );
        xCal->setValue( i18n::CalendarFieldIndex::CFI_HOUR, nHourSet );
        xCal->setValue( i18n::CalendarFieldIndex::CFI_MINUTE, nMinuteSet );
        xCal->setValue( i18n::CalendarFieldIndex::CFI_SECOND, nSecondSet );
        bValid  = xCal->isValid();
        nDay    = xCal->getValue( i18n::CalendarFieldIndex::DAY_OF_MONTH );
        nMyMonth= xCal->getValue( i18n::CalendarFieldIndex::CFI_MONTH );
        nYear   = xCal->getValue( i18n::CalendarFieldIndex::CFI_YEAR );
        nHour   = xCal->getValue( i18n::CalendarFieldIndex::CFI_HOUR );
        nMinute = xCal->getValue( i18n::CalendarFieldIndex::CFI_MINUTE );
        nSecond = xCal->getValue( i18n::CalendarFieldIndex::CFI_SECOND );
        bValid = bValid && nDay == nDaySet && nMyMonth == nMonthSet && nYear ==
            nYearSet && nHour == nHourSet && nMinute == nMinuteSet && nSecond
            == nSecondSet;
    }
}
#endif  // NF_TEST_CALENDAR

    }

    return res;
}


//---------------------------------------------------------------------------
//      ScanStartString
//
// ersten String analysieren
// Alles weg => sal_True
// sonst     => sal_False

sal_Bool ImpSvNumberInputScan::ScanStartString( const String& rString,
        const SvNumberformat* pFormat )
{
    xub_StrLen nPos = 0;
    int nDayOfWeek;

    // First of all, eat leading blanks
    SkipBlanks(rString, nPos);

    // Yes, nMatchedAllStrings should know about the sign position
    nSign = GetSign(rString, nPos);
    if ( nSign )           // sign?
        SkipBlanks(rString, nPos);

    // #102371# match against format string only if start string is not a sign character
    if ( nMatchedAllStrings && !(nSign && rString.size() == 1) )
    {   // Match against format in any case, so later on for a "x1-2-3" input
        // we may distinguish between a xy-m-d (or similar) date and a x0-0-0
        // format. No sign detection here!
        if ( ScanStringNumFor( rString, nPos, pFormat, 0, sal_True ) )
            nMatchedAllStrings |= nMatchedStartString;
        else
            nMatchedAllStrings = 0;
    }

    if ( GetDecSep(rString, nPos) )                 // decimal separator in start string
    {
        nDecPos = 1;
        SkipBlanks(rString, nPos);
    }
    else if ( GetCurrency(rString, nPos, pFormat) ) // currency (DM 1)?
    {
        eScannedType = NUMBERFORMAT_CURRENCY;       // !!! it IS currency !!!
        SkipBlanks(rString, nPos);
        if (nSign == 0)                             // no sign yet
        {
            nSign = GetSign(rString, nPos);
            if ( nSign )   // DM -1
                SkipBlanks(rString, nPos);
        }
    }
    else
    {
        nMonth = GetMonth(rString, nPos);
        if ( nMonth )    // month (Jan 1)?
        {
            eScannedType = NUMBERFORMAT_DATE;       // !!! it IS a date !!!
            nMonthPos = 1;                          // month at the beginning
            if ( nMonth < 0 )
                SkipChar( L'.', rString, nPos );     // abbreviated
            SkipBlanks(rString, nPos);
        }
        else
        {
            nDayOfWeek = GetDayOfWeek( rString, nPos );
            if ( nDayOfWeek )
            {   // day of week is just parsed away
                eScannedType = NUMBERFORMAT_DATE;       // !!! it IS a date !!!
                if ( nPos < rString.size() )
                {
                    if ( nDayOfWeek < 0 )
                    {   // abbreviated
                        if ( rString.at( nPos ) == L'.' )
                            ++nPos;
                    }
                    else
                    {   // full long name
                        SkipBlanks(rString, nPos);
                        SkipString( pFormatter->getLongDateDayOfWeekSep(), rString, nPos );
                    }
                    SkipBlanks(rString, nPos);
                    nMonth = GetMonth(rString, nPos);
                    if ( nMonth ) // month (Jan 1)?
                    {
                        nMonthPos = 1;                  // month a the beginning
                        if ( nMonth < 0 )
                            SkipChar( L'.', rString, nPos ); // abbreviated
                        SkipBlanks(rString, nPos);
                    }
                }
            }
        }
    }

    if (nPos < rString.size())                       // not everything consumed
    {
        // Does input StartString equal StartString of format?
        // This time with sign detection!
        if ( !ScanStringNumFor( rString, nPos, pFormat, 0 ) )
            return MatchedReturn();
    }

    return sal_True;
}


//---------------------------------------------------------------------------
//      ScanMidString
//
// String in der Mitte analysieren
// Alles weg => sal_True
// sonst     => sal_False

sal_Bool ImpSvNumberInputScan::ScanMidString( const String& rString,
        sal_uInt16 nStringPos, const SvNumberformat* pFormat )
{
    xub_StrLen nPos = 0;
    short eOldScannedType = eScannedType;

    if ( nMatchedAllStrings )
    {   // Match against format in any case, so later on for a "1-2-3-4" input
        // we may distinguish between a y-m-d (or similar) date and a 0-0-0-0
        // format.
        if ( ScanStringNumFor( rString, 0, pFormat, nStringPos ) )
            nMatchedAllStrings |= nMatchedMidString;
        else
            nMatchedAllStrings = 0;
    }

    SkipBlanks(rString, nPos);
    if (GetDecSep(rString, nPos))                   // decimal separator?
    {
        if (nDecPos == 1 || nDecPos == 3)           // .12.4 or 1.E2.1
            return MatchedReturn();
        else if (nDecPos == 2)                      // . dup: 12.4.
        {
            if (bDecSepInDateSeps)                  // . also date separator
            {
                if (    eScannedType != NUMBERFORMAT_UNDEFINED &&
                        eScannedType != NUMBERFORMAT_DATE &&
                        eScannedType != NUMBERFORMAT_DATETIME)  // already another type
                    return MatchedReturn();
                if (eScannedType == NUMBERFORMAT_UNDEFINED)
                    eScannedType = NUMBERFORMAT_DATE;   // !!! it IS a date
                SkipBlanks(rString, nPos);
            }
            else
                return MatchedReturn();
        }
        else
        {
            nDecPos = 2;                            // . in mid string
            SkipBlanks(rString, nPos);
        }
    }
    else if ( ((eScannedType & NUMBERFORMAT_TIME) == NUMBERFORMAT_TIME)
            && GetTime100SecSep( rString, nPos ) )
    {                                               // hundredth seconds separator
        if ( nDecPos )
            return MatchedReturn();
        nDecPos = 2;                                // . in mid string
        SkipBlanks(rString, nPos);
    }

    if (SkipChar(L'/', rString, nPos))               // fraction?
    {
        if (   eScannedType != NUMBERFORMAT_UNDEFINED   // already another type
            && eScannedType != NUMBERFORMAT_DATE)       // except date
            return MatchedReturn();                               // => jan/31/1994
        else if (    eScannedType != NUMBERFORMAT_DATE      // analyzed date until now
                 && (    eSetType == NUMBERFORMAT_FRACTION  // and preset was fraction
                     || (nAnzNums == 3                      // or 3 numbers
                         && nStringPos > 2) ) )             // and what ???
        {
            SkipBlanks(rString, nPos);
            eScannedType = NUMBERFORMAT_FRACTION;   // !!! it IS a fraction
        }
        else
            nPos--;                                 // put '/' back
    }

    if (GetThousandSep(rString, nPos, nStringPos))  // 1,000
    {
        if (   eScannedType != NUMBERFORMAT_UNDEFINED   // already another type
            && eScannedType != NUMBERFORMAT_CURRENCY)   // except currency
            return MatchedReturn();
        nThousand++;
    }

    const String& rDate = pFormatter->GetDateSep();
    const String& rTime = pFormatter->getTimeSep();
    sal_Unicode cTime = rTime.at(0);
    SkipBlanks(rString, nPos);
    if (                      SkipString(rDate, rString, nPos)  // 10., 10-, 10/
        || ((cTime != L'.') && SkipChar(L'.',   rString, nPos))   // TRICKY:
        || ((cTime != L'/') && SkipChar(L'/',   rString, nPos))   // short boolean
        || ((cTime != L'-') && SkipChar(L'-',   rString, nPos)) ) // evaluation!
    {
        if (   eScannedType != NUMBERFORMAT_UNDEFINED   // already another type
            && eScannedType != NUMBERFORMAT_DATE)       // except date
            return MatchedReturn();
        SkipBlanks(rString, nPos);
        eScannedType = NUMBERFORMAT_DATE;           // !!! it IS a date
        short nTmpMonth = GetMonth(rString, nPos);  // 10. Jan 94
        if (nMonth && nTmpMonth)                    // month dup
            return MatchedReturn();
        if (nTmpMonth)
        {
            nMonth = nTmpMonth;
            nMonthPos = 2;                          // month in the middle
            if ( nMonth < 0 && SkipChar( L'.', rString, nPos ) )
                ;   // short month may be abbreviated Jan.
            else if ( SkipChar( L'-', rString, nPos ) )
                ;   // #79632# recognize 17-Jan-2001 to be a date
                    // #99065# short and long month name
            else
                SkipString(pFormatter->getLongDateMonthSep(), rString, nPos );
            SkipBlanks(rString, nPos);
        }
    }

    short nTempMonth = GetMonth(rString, nPos);     // month in the middle (10 Jan 94)
    if (nTempMonth)
    {
        if (nMonth != 0)                            // month dup
            return MatchedReturn();
        if (   eScannedType != NUMBERFORMAT_UNDEFINED   // already another type
            && eScannedType != NUMBERFORMAT_DATE)       // except date
            return MatchedReturn();
        eScannedType = NUMBERFORMAT_DATE;           // !!! it IS a date
        nMonth = nTempMonth;
        nMonthPos = 2;                              // month in the middle
        if ( nMonth < 0 )
            SkipChar( L'.', rString, nPos );         // abbreviated
        SkipString(pFormatter->getLongDateMonthSep(), rString, nPos );
        SkipBlanks(rString, nPos);
    }

    if (    SkipChar(L'E', rString, nPos)            // 10E, 10e, 10,Ee
         || SkipChar(L'e', rString, nPos) )
    {
        if (eScannedType != NUMBERFORMAT_UNDEFINED) // already another type
            return MatchedReturn();
        else
        {
            SkipBlanks(rString, nPos);
            eScannedType = NUMBERFORMAT_SCIENTIFIC; // !!! it IS scientific
            if (    nThousand+2 == nAnzNums         // special case 1.E2
                 && nDecPos == 2 )
                nDecPos = 3;                        // 1,100.E2 1,100,100.E3
        }
        nESign = GetESign(rString, nPos);           // signed exponent?
        SkipBlanks(rString, nPos);
    }

    if ( SkipString(rTime, rString, nPos) )         // time separator?
    {
        if (nDecPos)                                // already . => maybe error
        {
            if (bDecSepInDateSeps)                  // . also date sep
            {
                if (    eScannedType != NUMBERFORMAT_DATE &&    // already another type than date
                        eScannedType != NUMBERFORMAT_DATETIME)  // or date time
                    return MatchedReturn();
                if (eScannedType == NUMBERFORMAT_DATE)
                    nDecPos = 0;                    // reset for time transition
            }
            else
                return MatchedReturn();
        }
        if (   (   eScannedType == NUMBERFORMAT_DATE        // already date type
                || eScannedType == NUMBERFORMAT_DATETIME)   // or date time
            && nAnzNums > 3)                                // and more than 3 numbers? (31.Dez.94 8:23)
        {
            SkipBlanks(rString, nPos);
            eScannedType = NUMBERFORMAT_DATETIME;   // !!! it IS date with time
        }
        else if (   eScannedType != NUMBERFORMAT_UNDEFINED  // already another type
                 && eScannedType != NUMBERFORMAT_TIME)      // except time
            return MatchedReturn();
        else
        {
            SkipBlanks(rString, nPos);
            eScannedType = NUMBERFORMAT_TIME;       // !!! it IS a time
        }
        if ( !nTimePos )
            nTimePos = nStringPos + 1;
    }

    if (nPos < rString.size())
    {
        switch (eScannedType)
        {
            case NUMBERFORMAT_DATE:
                if (nMonthPos == 1 && pFormatter->getLongDateFormat() == MDY)
                {
                    // #68232# recognize long date separators like ", " in "September 5, 1999"
                    if (SkipString(pFormatter->getLongDateDaySep(), rString, nPos ))
                        SkipBlanks( rString, nPos );
                }
                else if (nStringPos == 5 && nPos == 0 && rString.size() == 1 &&
                        rString.at(0) == L'T' && MayBeIso8601())
                {
                    // ISO 8601 combined date and time, yyyy-mm-ddThh:mm
                    ++nPos;
                }
                break;
#if NF_RECOGNIZE_ISO8601_TIMEZONES
            case NUMBERFORMAT_DATETIME:
                if (nPos == 0 && rString.size() == 1 && nStringPos >= 9 &&
                        MayBeIso8601())
                {
                    // ISO 8601 timezone offset
                    switch (rString.at(0))
                    {
                        case '+':
                        case '-':
                            if (nStringPos == nAnzStrings-2 ||
                                    nStringPos == nAnzStrings-4)
                            {
                                ++nPos;     // yyyy-mm-ddThh:mm[:ss]+xx[[:]yy]
                                // nTimezonePos needed for GetTimeRef()
                                if (!nTimezonePos)
                                    nTimezonePos = nStringPos + 1;
                            }
                            break;
                        case ':':
                            if (nTimezonePos && nStringPos >= 11 &&
                                    nStringPos == nAnzStrings-2)
                                ++nPos;     // yyyy-mm-ddThh:mm[:ss]+xx:yy
                            break;
                    }
                }
                break;
#endif
        }
    }

    if (nPos < rString.size())                       // not everything consumed?
    {
        if ( nMatchedAllStrings & ~nMatchedVirgin )
            eScannedType = eOldScannedType;
        else
            return sal_False;
    }

    return sal_True;
}


//---------------------------------------------------------------------------
//      ScanEndString
//
// Schlussteil analysieren
// Alles weg => sal_True
// sonst     => sal_False

sal_Bool ImpSvNumberInputScan::ScanEndString( const String& rString,
        const SvNumberformat* pFormat )
{
    xub_StrLen nPos = 0;

    if ( nMatchedAllStrings )
    {   // Match against format in any case, so later on for a "1-2-3-4" input
        // we may distinguish between a y-m-d (or similar) date and a 0-0-0-0
        // format.
        if ( ScanStringNumFor( rString, 0, pFormat, 0xFFFF ) )
            nMatchedAllStrings |= nMatchedEndString;
        else
            nMatchedAllStrings = 0;
    }

    SkipBlanks(rString, nPos);
    if (GetDecSep(rString, nPos))                   // decimal separator?
    {
        if (nDecPos == 1 || nDecPos == 3)           // .12.4 or 12.E4.
            return MatchedReturn();
        else if (nDecPos == 2)                      // . dup: 12.4.
        {
            if (bDecSepInDateSeps)                  // . also date sep
            {
                if (    eScannedType != NUMBERFORMAT_UNDEFINED &&
                        eScannedType != NUMBERFORMAT_DATE &&
                        eScannedType != NUMBERFORMAT_DATETIME)  // already another type
                    return MatchedReturn();
                if (eScannedType == NUMBERFORMAT_UNDEFINED)
                    eScannedType = NUMBERFORMAT_DATE;   // !!! it IS a date
                SkipBlanks(rString, nPos);
            }
            else
                return MatchedReturn();
        }
        else
        {
            nDecPos = 3;                            // . in end string
            SkipBlanks(rString, nPos);
        }
    }

    if (   nSign == 0                               // conflict - not signed
        && eScannedType != NUMBERFORMAT_DATE)       // and not date
//!? catch time too?
    {                                               // not signed yet
        nSign = GetSign(rString, nPos);             // 1- DM
        if (nNegCheck)                              // '(' as sign
            return MatchedReturn();
    }

    SkipBlanks(rString, nPos);
    if (nNegCheck && SkipChar(L')', rString, nPos))  // skip ')' if appropriate
    {
        nNegCheck = 0;
        SkipBlanks(rString, nPos);
    }

    if ( GetCurrency(rString, nPos, pFormat) )      // currency symbol?
    {
        if (eScannedType != NUMBERFORMAT_UNDEFINED) // currency dup
            return MatchedReturn();
        else
        {
            SkipBlanks(rString, nPos);
            eScannedType = NUMBERFORMAT_CURRENCY;
        }                                           // behind currency a '-' is allowed
        if (nSign == 0)                             // not signed yet
        {
            nSign = GetSign(rString, nPos);         // DM -
            SkipBlanks(rString, nPos);
            if (nNegCheck)                          // 3 DM (
                return MatchedReturn();
        }
        if ( nNegCheck && eScannedType == NUMBERFORMAT_CURRENCY
                       && SkipChar(L')', rString, nPos) )
        {
            nNegCheck = 0;                          // ')' skipped
            SkipBlanks(rString, nPos);              // only if currency
        }
    }

    if ( SkipChar(L'%', rString, nPos) )             // 1 %
    {
        if (eScannedType != NUMBERFORMAT_UNDEFINED) // already another type
            return MatchedReturn();
        SkipBlanks(rString, nPos);
        eScannedType = NUMBERFORMAT_PERCENT;
    }

    const String& rDate = pFormatter->GetDateSep();
    const String& rTime = pFormatter->getTimeSep();
    if ( SkipString(rTime, rString, nPos) )         // 10:
    {
        if (nDecPos)                                // already , => error
            return MatchedReturn();
        if (eScannedType == NUMBERFORMAT_DATE && nAnzNums > 2) // 31.Dez.94 8:
        {
            SkipBlanks(rString, nPos);
            eScannedType = NUMBERFORMAT_DATETIME;
        }
        else if (eScannedType != NUMBERFORMAT_UNDEFINED &&
                 eScannedType != NUMBERFORMAT_TIME) // already another type
            return MatchedReturn();
        else
        {
            SkipBlanks(rString, nPos);
            eScannedType = NUMBERFORMAT_TIME;
        }
        if ( !nTimePos )
            nTimePos = nAnzStrings;
    }

    sal_Unicode cTime = rTime.at(0);
    if (                      SkipString(rDate, rString, nPos)  // 10., 10-, 10/
        || ((cTime != L'.') && SkipChar(L'.',   rString, nPos))   // TRICKY:
        || ((cTime != L'/') && SkipChar(L'/',   rString, nPos))   // short boolean
        || ((cTime != L'-') && SkipChar(L'-',   rString, nPos)) ) // evaluation!
    {
        if (eScannedType != NUMBERFORMAT_UNDEFINED &&
            eScannedType != NUMBERFORMAT_DATE)          // already another type
            return MatchedReturn();
        else
        {
            SkipBlanks(rString, nPos);
            eScannedType = NUMBERFORMAT_DATE;
        }
        short nTmpMonth = GetMonth(rString, nPos);  // 10. Jan
        if (nMonth && nTmpMonth)                    // month dup
            return MatchedReturn();
        if (nTmpMonth)
        {
            nMonth = nTmpMonth;
            nMonthPos = 3;                          // month at end
            if ( nMonth < 0 )
                SkipChar( L'.', rString, nPos );     // abbreviated
            SkipBlanks(rString, nPos);
        }
    }

    short nTempMonth = GetMonth(rString, nPos);     // 10 Jan
    if (nTempMonth)
    {
        if (nMonth)                                 // month dup
            return MatchedReturn();
        if (eScannedType != NUMBERFORMAT_UNDEFINED &&
            eScannedType != NUMBERFORMAT_DATE)      // already another type
            return MatchedReturn();
        eScannedType = NUMBERFORMAT_DATE;
        nMonth = nTempMonth;
        nMonthPos = 3;                              // month at end
        if ( nMonth < 0 )
            SkipChar( L'.', rString, nPos );         // abbreviated
        SkipBlanks(rString, nPos);
    }

    xub_StrLen nOrigPos = nPos;
    if (GetTimeAmPm(rString, nPos))
    {
        if (eScannedType != NUMBERFORMAT_UNDEFINED &&
            eScannedType != NUMBERFORMAT_TIME &&
            eScannedType != NUMBERFORMAT_DATETIME)  // already another type
            return MatchedReturn();
        else
        {
            // If not already scanned as time, 6.78am does not result in 6
            // seconds and 78 hundredths in the morning. Keep as suffix.
            if (eScannedType != NUMBERFORMAT_TIME && nDecPos == 2 && nAnzNums == 2)
                nPos = nOrigPos;     // rewind am/pm
            else
            {
                SkipBlanks(rString, nPos);
                if ( eScannedType != NUMBERFORMAT_DATETIME )
                    eScannedType = NUMBERFORMAT_TIME;
            }
        }
    }

    if ( nNegCheck && SkipChar(L')', rString, nPos) )
    {
        if (eScannedType == NUMBERFORMAT_CURRENCY)  // only if currency
        {
            nNegCheck = 0;                          // skip ')'
            SkipBlanks(rString, nPos);
        }
        else
            return MatchedReturn();
    }

    if ( nPos < rString.size() &&
            (eScannedType == NUMBERFORMAT_DATE
            || eScannedType == NUMBERFORMAT_DATETIME) )
    {   // day of week is just parsed away
        xub_StrLen nOldPos = nPos;
        const String& rSep = pFormatter->getLongDateDayOfWeekSep();
        if ( StringContains( rSep, rString, nPos ) )
        {
            nPos = nPos + rSep.size();
            SkipBlanks(rString, nPos);
        }
        int nDayOfWeek = GetDayOfWeek( rString, nPos );
        if ( nDayOfWeek )
        {
            if ( nPos < rString.size() )
            {
                if ( nDayOfWeek < 0 )
                {   // short
                    if ( rString.at( nPos ) == L'.' )
                        ++nPos;
                }
                SkipBlanks(rString, nPos);
            }
        }
        else
            nPos = nOldPos;
    }

#if NF_RECOGNIZE_ISO8601_TIMEZONES
    if (nPos == 0 && eScannedType == NUMBERFORMAT_DATETIME &&
            rString.size() == 1 && rString.at(0) == 'Z' && MayBeIso8601())
    {
        // ISO 8601 timezone UTC yyyy-mm-ddThh:mmZ
        ++nPos;
    }
#endif

    if (nPos < rString.size())                       // everything consumed?
    {
        // does input EndString equal EndString in Format?
        if ( !ScanStringNumFor( rString, nPos, pFormat, 0xFFFF ) )
            return sal_False;
    }

    return sal_True;
}


sal_Bool ImpSvNumberInputScan::ScanStringNumFor(
        const String& rString,          // String to scan
        xub_StrLen nPos,                // Position until which was consumed
        const SvNumberformat* pFormat,  // The format to match
        sal_uInt16 nString,                 // Substring of format, 0xFFFF => last
        sal_Bool bDontDetectNegation        // Suppress sign detection
        )
{
    if ( !pFormat )
        return sal_False;
    const String* pStr;
    String aString( rString );
    sal_Bool bFound = sal_False;
    sal_Bool bFirst = sal_True;
    sal_Bool bContinue = sal_True;
    sal_uInt16 nSub;
    do
    {
        // Don't try "lower" subformats ff the very first match was the second
        // or third subformat.
        nSub = nStringScanNumFor;
        do
        {   // Step through subformats, first positive, then negative, then
            // other, but not the last (text) subformat.
            pStr = pFormat->GetNumForString( nSub, nString, sal_True );
            if ( pStr && aString == *pStr )
            {
                bFound = sal_True;
                bContinue = sal_False;
            }
            else if ( nSub < 2 )
                ++nSub;
            else
                bContinue = sal_False;
        } while ( bContinue );
        if ( !bFound && bFirst && nPos )
        {   // try remaining substring
            bFirst = sal_False;
            aString.erase( 0, nPos );
            bContinue = sal_True;
        }
    } while ( bContinue );

    if ( !bFound )
    {
        if ( !bDontDetectNegation && (nString == 0) && !bFirst && (nSign < 0)
                && pFormat->IsNegativeRealNegative() )
        {   // simply negated twice? --1
			EraseAllChars(aString, L' ' );
            if ( (aString.size() == 1) && (aString.at(0) == L'-') )
            {
                bFound = sal_True;
                nStringScanSign = -1;
                nSub = 0;       //! not 1
            }
        }
        if ( !bFound )
            return sal_False;
    }
    else if ( !bDontDetectNegation && (nSub == 1) &&
            pFormat->IsNegativeRealNegative() )
    {   // negative
        if ( nStringScanSign < 0 )
        {
            if ( (nSign < 0) && (nStringScanNumFor != 1) )
                nStringScanSign = 1;        // triple negated --1 yyy
        }
        else if ( nStringScanSign == 0 )
        {
            if ( nSign < 0 )
            {   // nSign and nStringScanSign will be combined later,
                // flip sign if doubly negated
                if ( (nString == 0) && !bFirst
                        && SvNumberformat::HasStringNegativeSign( aString ) )
                    nStringScanSign = -1;   // direct double negation
                else if ( pFormat->IsNegativeWithoutSign() )
                    nStringScanSign = -1;   // indirect double negation
            }
            else
                nStringScanSign = -1;
        }
        else    // > 0
            nStringScanSign = -1;
    }
    nStringScanNumFor = nSub;
    return sal_True;
}


//---------------------------------------------------------------------------
//      IsNumberFormatMain
//
// Recognizes types of number, exponential, fraction, percent, currency, date, time.
// Else text => return sal_False

sal_Bool ImpSvNumberInputScan::IsNumberFormatMain(
        const String& rString,                  // string to be analyzed
        double& ,                               // OUT: result as number, if possible
        const SvNumberformat* pFormat )         // maybe number format set to match against
{
    Reset();
    NumberStringDivision( rString );            // breakdown into strings and numbers
    if (nAnzStrings >= SV_MAX_ANZ_INPUT_STRINGS) // too many elements
        return sal_False;                           // Njet, Nope, ...

    if (nAnzNums == 0)                          // no number in input
    {
        if ( nAnzStrings > 0 )
        {
            // Here we may change the original, we don't need it anymore.
            // This saves copies and ToUpper() in GetLogical() and is faster.
            String& rStrArray = sStrArray[0];
            EraseTrailingChars(rStrArray, L' ');
            EraseLeadingChars(rStrArray, L' ');
            nLogical = GetLogical( rStrArray );
            if ( nLogical )
            {
                eScannedType = NUMBERFORMAT_LOGICAL; // !!! it's a BOOLEAN
                nMatchedAllStrings &= ~nMatchedVirgin;
                return sal_True;
            }
            else
                return sal_False;                   // simple text
        }
        else
            return sal_False;                       // simple text
    }

    sal_uInt16 i = 0;                               // mark any symbol
    sal_uInt16 j = 0;                               // mark only numbers

    switch ( nAnzNums )
    {
        case 1 :                                // Exactly 1 number in input
        {                                       // nAnzStrings >= 1
            if (GetNextNumber(i,j))             // i=1,0
            {                                   // Number at start
                if (eSetType == NUMBERFORMAT_FRACTION)  // Fraction 1 = 1/1
                {
                    if (i >= nAnzStrings ||     // no end string nor decimal separator
                        sStrArray[i] == pFormatter->GetNumDecimalSep())
                    {
                        eScannedType = NUMBERFORMAT_FRACTION;
                        nMatchedAllStrings &= ~nMatchedVirgin;
                        return sal_True;
                    }
                }
            }
            else
            {                                   // Analyze start string
                if (!ScanStartString( sStrArray[i], pFormat ))  // i=0
                    return sal_False;               // already an error
                i++;                            // next symbol, i=1
            }
            GetNextNumber(i,j);                 // i=1,2
            if (eSetType == NUMBERFORMAT_FRACTION)  // Fraction -1 = -1/1
            {
                if (nSign && !nNegCheck &&      // Sign +, -
                    eScannedType == NUMBERFORMAT_UNDEFINED &&   // not date or currency
                    nDecPos == 0 &&             // no previous decimal separator
                    (i >= nAnzStrings ||        // no end string nor decimal separator
                        sStrArray[i] == pFormatter->GetNumDecimalSep())
                )
                {
                    eScannedType = NUMBERFORMAT_FRACTION;
                    nMatchedAllStrings &= ~nMatchedVirgin;
                    return sal_True;
                }
            }
            if (i < nAnzStrings && !ScanEndString( sStrArray[i], pFormat ))
                    return sal_False;
        }
        break;
        case 2 :                                // Exactly 2 numbers in input
        {                                       // nAnzStrings >= 3
            if (!GetNextNumber(i,j))            // i=1,0
            {                                   // Analyze start string
                if (!ScanStartString( sStrArray[i], pFormat ))
                    return sal_False;               // already an error
                i++;                            // i=1
            }
            GetNextNumber(i,j);                 // i=1,2
            if ( !ScanMidString( sStrArray[i], i, pFormat ) )
                return sal_False;
            i++;                                // next symbol, i=2,3
            GetNextNumber(i,j);                 // i=3,4
            if (i < nAnzStrings && !ScanEndString( sStrArray[i], pFormat ))
                return sal_False;
            if (eSetType == NUMBERFORMAT_FRACTION)  // -1,200. as fraction
            {
                if (!nNegCheck  &&                  // no sign '('
                    eScannedType == NUMBERFORMAT_UNDEFINED &&
                    (nDecPos == 0 || nDecPos == 3)  // no decimal separator or at end
                    )
                {
                    eScannedType = NUMBERFORMAT_FRACTION;
                    nMatchedAllStrings &= ~nMatchedVirgin;
                    return sal_True;
                }
            }
        }
        break;
        case 3 :                                // Exactly 3 numbers in input
        {                                       // nAnzStrings >= 5
            if (!GetNextNumber(i,j))            // i=1,0
            {                                   // Analyze start string
                if (!ScanStartString( sStrArray[i], pFormat ))
                    return sal_False;               // already an error
                i++;                            // i=1
                if (nDecPos == 1)               // decimal separator at start => error
                    return sal_False;
            }
            GetNextNumber(i,j);                 // i=1,2
            if ( !ScanMidString( sStrArray[i], i, pFormat ) )
                return sal_False;
            i++;                                // i=2,3
            if (eScannedType == NUMBERFORMAT_SCIENTIFIC)    // E only at end
                return sal_False;
            GetNextNumber(i,j);                 // i=3,4
            if ( !ScanMidString( sStrArray[i], i, pFormat ) )
                return sal_False;
            i++;                                // i=4,5
            GetNextNumber(i,j);                 // i=5,6
            if (i < nAnzStrings && !ScanEndString( sStrArray[i], pFormat ))
                return sal_False;
            if (eSetType == NUMBERFORMAT_FRACTION)  // -1,200,100. as fraction
            {
                if (!nNegCheck  &&                  // no sign '('
                    eScannedType == NUMBERFORMAT_UNDEFINED &&
                    (nDecPos == 0 || nDecPos == 3)  // no decimal separator or at end
                )
                {
                    eScannedType = NUMBERFORMAT_FRACTION;
                    nMatchedAllStrings &= ~nMatchedVirgin;
                    return sal_True;
                }
            }
            if ( eScannedType == NUMBERFORMAT_FRACTION && nDecPos )
                return sal_False;                   // #36857# not a real fraction
        }
        break;
        default:                                // More than 3 numbers in input
        {                                       // nAnzStrings >= 7
            if (!GetNextNumber(i,j))            // i=1,0
            {                                   // Analyze startstring
                if (!ScanStartString( sStrArray[i], pFormat ))
                    return sal_False;               // already an error
                i++;                            // i=1
                if (nDecPos == 1)               // decimal separator at start => error
                    return sal_False;
            }
            GetNextNumber(i,j);                 // i=1,2
            if ( !ScanMidString( sStrArray[i], i, pFormat ) )
                return sal_False;
            i++;                                // i=2,3
            sal_uInt16 nThOld = 10;                 // just not 0 or 1
            while (nThOld != nThousand && j < nAnzNums-1)
                                                // Execute at least one time
                                                // but leave one number.
            {                                   // Loop over group separators
                nThOld = nThousand;
                if (eScannedType == NUMBERFORMAT_SCIENTIFIC)    // E only at end
                    return sal_False;
                GetNextNumber(i,j);
                if ( i < nAnzStrings && !ScanMidString( sStrArray[i], i, pFormat ) )
                    return sal_False;
                i++;
            }
            if (eScannedType == NUMBERFORMAT_DATE ||    // long date or
                eScannedType == NUMBERFORMAT_TIME ||    // long time or
                eScannedType == NUMBERFORMAT_UNDEFINED) // long number
            {
                for (sal_uInt16 k = j; k < nAnzNums-1; k++)
                {
                    if (eScannedType == NUMBERFORMAT_SCIENTIFIC)    // E only at endd
                        return sal_False;
                    GetNextNumber(i,j);
                    if ( i < nAnzStrings && !ScanMidString( sStrArray[i], i, pFormat ) )
                        return sal_False;
                    i++;
                }
            }
            GetNextNumber(i,j);
            if (i < nAnzStrings && !ScanEndString( sStrArray[i], pFormat ))
                return sal_False;
            if (eSetType == NUMBERFORMAT_FRACTION)  // -1,200,100. as fraction
            {
                if (!nNegCheck  &&                  // no sign '('
                    eScannedType == NUMBERFORMAT_UNDEFINED &&
                    (nDecPos == 0 || nDecPos == 3)  // no decimal separator or at end
                )
                {
                    eScannedType = NUMBERFORMAT_FRACTION;
                    nMatchedAllStrings &= ~nMatchedVirgin;
                    return sal_True;
                }
            }
            if ( eScannedType == NUMBERFORMAT_FRACTION && nDecPos )
                return sal_False;                       // #36857# not a real fraction
        }
    }

    if (eScannedType == NUMBERFORMAT_UNDEFINED)
    {
        nMatchedAllStrings &= ~nMatchedVirgin;
        // did match including nMatchedUsedAsReturn
        sal_Bool bDidMatch = (nMatchedAllStrings != 0);
        if ( nMatchedAllStrings )
        {
            sal_Bool bMatch = (pFormat ? pFormat->IsNumForStringElementCountEqual(
                        nStringScanNumFor, nAnzStrings, nAnzNums ) : sal_False);
            if ( !bMatch )
                nMatchedAllStrings = 0;
        }
        if ( nMatchedAllStrings )
            eScannedType = eSetType;
        else if ( bDidMatch )
            return sal_False;
        else
            eScannedType = NUMBERFORMAT_NUMBER;
            // everything else should have been recognized by now
    }
    else if ( eScannedType == NUMBERFORMAT_DATE )
    {   // the very relaxed date input checks may interfere with a preset format
        nMatchedAllStrings &= ~nMatchedVirgin;
        sal_Bool bWasReturn = ((nMatchedAllStrings & nMatchedUsedAsReturn) != 0);
        if ( nMatchedAllStrings )
        {
            sal_Bool bMatch = (pFormat ? pFormat->IsNumForStringElementCountEqual(
                        nStringScanNumFor, nAnzStrings, nAnzNums ) : sal_False);
            if ( !bMatch )
                nMatchedAllStrings = 0;
        }
        if ( nMatchedAllStrings )
            eScannedType = eSetType;
        else if ( bWasReturn )
            return sal_False;
    }
    else
        nMatchedAllStrings = 0;  // reset flag to no substrings matched

    return sal_True;
}


//---------------------------------------------------------------------------
// return sal_True or sal_False depending on the nMatched... state and remember usage
sal_Bool ImpSvNumberInputScan::MatchedReturn()
{
    if ( nMatchedAllStrings & ~nMatchedVirgin )
    {
        nMatchedAllStrings |= nMatchedUsedAsReturn;
        return sal_True;
    }
    return sal_False;
}


//---------------------------------------------------------------------------
// Initialize uppercase months and weekdays

void ImpSvNumberInputScan::InitText()
{
    sal_Int32 j, nElems;
    delete [] pUpperMonthText;
    delete [] pUpperAbbrevMonthText;
    nElems = pFormatter->getMonthsOfYearSize();
    pUpperMonthText = new String[nElems];
    pUpperAbbrevMonthText = new String[nElems];
	for (j = 0; j < nElems; j++)
    {
        pUpperMonthText[j] = pFormatter->getMonthsOfYearFullName(j);
		ConvertToUpper(pUpperMonthText[j]);
		pUpperAbbrevMonthText[j] = pFormatter->getMonthsOfYearAbbrvName(j);
		ConvertToUpper(pUpperAbbrevMonthText[j]);
    }
    delete [] pUpperDayText;
    delete [] pUpperAbbrevDayText;
    nElems = pFormatter->getDayOfWeekSize();
    pUpperDayText = new String[nElems];
    pUpperAbbrevDayText = new String[nElems];
    for (j = 0; j < nElems; j++)
    {
		pUpperDayText[j] = pFormatter->getDayOfWeekFullName(j);
		ConvertToUpper(pUpperDayText[j]);
        pUpperAbbrevDayText[j] = pFormatter->getDayOfWeekAbbrvName(j);
		ConvertToUpper(pUpperAbbrevDayText[j]);
    }
    bTextInitialized = sal_True;
}


//===========================================================================
//          P U B L I C

//---------------------------------------------------------------------------
//      ChangeIntl
//
// MUST be called if International/Locale is changed

void ImpSvNumberInputScan::ChangeIntl()
{
    sal_Unicode cDecSep = pFormatter->GetNumDecimalSep().at(0);
    bDecSepInDateSeps = ( cDecSep == L'-' ||
                          cDecSep == L'/' ||
                          cDecSep == L'.' ||
                          cDecSep == pFormatter->GetDateSep().at(0) );
    bTextInitialized = sal_False;
    aUpperCurrSymbol.erase();
}


//---------------------------------------------------------------------------
//      IsNumberFormat
//
// => does rString represent a number (also date, time et al)

sal_Bool ImpSvNumberInputScan::IsNumberFormat(
        const String& rString,                  // string to be analyzed
        short& F_Type,                          // IN: old type, OUT: new type
        double& fOutNumber,                     // OUT: number if convertable
        const SvNumberformat* pFormat )         // maybe a number format to match against
{
    String sResString;
    String aString;
    sal_Bool res;                                   // return value
    eSetType = F_Type;                          // old type set

    if ( !rString.size() )
        res = sal_False;
    else if (rString.size() > 308)               // arbitrary
        res = sal_False;
    else
    {
        // NoMoreUpperNeeded, all comparisons on UpperCase
        aString = rString;
		ConvertToUpper(aString);
		// convert native number to ASCII if necessary
        TransformInput( aString );
        res = IsNumberFormatMain( aString, fOutNumber, pFormat );
    }

    if (res)
    {
        if ( nNegCheck                              // ')' not found for '('
                || (nSign && (eScannedType == NUMBERFORMAT_DATE
                    || eScannedType == NUMBERFORMAT_DATETIME))
            )                                       // signed date/datetime
            res = sal_False;
        else
        {                                           // check count of partial number strings
            switch (eScannedType)
            {
                case NUMBERFORMAT_PERCENT:
                case NUMBERFORMAT_CURRENCY:
                case NUMBERFORMAT_NUMBER:
                    if (nDecPos == 1)               // .05
                    {
                        // matched MidStrings function like group separators
                        if ( nMatchedAllStrings )
                            nThousand = nAnzNums - 1;
                        else if ( nAnzNums != 1 )
                            res = sal_False;
                    }
                    else if (nDecPos == 2)          // 1.05
                    {
                        // matched MidStrings function like group separators
                        if ( nMatchedAllStrings )
                            nThousand = nAnzNums - 1;
                        else if ( nAnzNums != nThousand+2 )
                            res = sal_False;
                    }
                    else                            // 1,100 or 1,100.
                    {
                        // matched MidStrings function like group separators
                        if ( nMatchedAllStrings )
                            nThousand = nAnzNums - 1;
                        else if ( nAnzNums != nThousand+1 )
                            res = sal_False;
                    }
                    break;

                case NUMBERFORMAT_SCIENTIFIC:       // 1.0e-2
                    if (nDecPos == 1)               // .05
                    {
                        if (nAnzNums != 2)
                            res = sal_False;
                    }
                    else if (nDecPos == 2)          // 1.05
                    {
                        if (nAnzNums != nThousand+3)
                            res = sal_False;
                    }
                    else                            // 1,100 or 1,100.
                    {
                        if (nAnzNums != nThousand+2)
                            res = sal_False;
                    }
                    break;

                case NUMBERFORMAT_DATE:
                    if (nMonth)
                    {                               // month name and numbers
                        if (nAnzNums > 2)
                            res = sal_False;
                    }
                    else
                    {
                        if (nAnzNums > 3)
                            res = sal_False;
                    }
                    break;

                case NUMBERFORMAT_TIME:
                    if (nDecPos)
                    {                               // hundredth seconds included
                        if (nAnzNums > 4)
                            res = sal_False;
                    }
                    else
                    {
                        if (nAnzNums > 3)
                            res = sal_False;
                    }
                    break;

                case NUMBERFORMAT_DATETIME:
                    if (nMonth)
                    {                               // month name and numbers
                        if (nDecPos)
                        {                           // hundredth seconds included
                            if (nAnzNums > 6)
                                res = sal_False;
                        }
                        else
                        {
                            if (nAnzNums > 5)
                                res = sal_False;
                        }
                    }
                    else
                    {
                        if (nDecPos)
                        {                           // hundredth seconds included
                            if (nAnzNums > 7)
                                res = sal_False;
                        }
                        else
                        {
                            if (nAnzNums > 6)
                                res = sal_False;
                        }
                    }
                    break;

                default:
                    break;
            }   // switch
        }   // else
    }   // if (res)

    if (res)
    {                                           // we finally have a number
        switch (eScannedType)
        {
            case NUMBERFORMAT_LOGICAL:
                if      (nLogical ==  1)
                    fOutNumber = 1.0;           // True
                else if (nLogical == -1)
                    fOutNumber = 0.0;           // False
                else
                    res = sal_False;                // Oops
                break;

            case NUMBERFORMAT_PERCENT:
            case NUMBERFORMAT_CURRENCY:
            case NUMBERFORMAT_NUMBER:
            case NUMBERFORMAT_SCIENTIFIC:
            case NUMBERFORMAT_DEFINED:          // if no category detected handle as number
            {
                if ( nDecPos == 1 )                         // . at start
                    sResString = L"0.";
                else
                    sResString.erase();
                sal_uInt16 k;
                for ( k = 0; k <= nThousand; k++)
                    sResString += sStrArray[nNums[k]];  // integer part
                if ( nDecPos == 2 && k < nAnzNums )     // . somewhere
                {
                    sResString += L'.';
                    sal_uInt16 nStop = (eScannedType == NUMBERFORMAT_SCIENTIFIC ?
                            nAnzNums-1 : nAnzNums);
                    for ( ; k < nStop; k++)
                        sResString += sStrArray[nNums[k]];  // fractional part
                }

                if (eScannedType != NUMBERFORMAT_SCIENTIFIC)
                    fOutNumber = StringToDouble(sResString);
                else
                {                                           // append exponent
                    sResString += L'E';
                    if ( nESign == -1 )
                        sResString += L'-';
                    sResString += sStrArray[nNums[nAnzNums-1]];
					try {
						fOutNumber = std::stod(sResString);
					}
					catch(std::exception e)
                    {
                        F_Type = NUMBERFORMAT_TEXT;         // overflow/underflow -> Text
                        if (nESign == -1)
                            fOutNumber = 0.0;
                        else
                            fOutNumber = DBL_MAX;
/*!*/                   return sal_True;
                    }
                }

                if ( nStringScanSign )
                {
                    if ( nSign )
                        nSign *= nStringScanSign;
                    else
                        nSign = nStringScanSign;
                }
                if ( nSign < 0 )
                    fOutNumber = -fOutNumber;

                if (eScannedType == NUMBERFORMAT_PERCENT)
                    fOutNumber/= 100.0;
            }
            break;

            case NUMBERFORMAT_FRACTION:
                if (nAnzNums == 1)
                    fOutNumber = StringToDouble(sStrArray[nNums[0]]);
                else if (nAnzNums == 2)
                {
                    if (nThousand == 1)
                    {
                        sResString = sStrArray[nNums[0]];
                        sResString += sStrArray[nNums[1]];  // integer part
                        fOutNumber = StringToDouble(sResString);
                    }
                    else
                    {
                        double fZaehler = StringToDouble(sStrArray[nNums[0]]);
                        double fNenner = StringToDouble(sStrArray[nNums[1]]);
                        if (fNenner != 0.0)
                            fOutNumber = fZaehler/fNenner;
                        else
                            res = sal_False;
                    }
                }
                else                                        // nAnzNums > 2
                {
                    sal_uInt16 k = 1;
                    sResString = sStrArray[nNums[0]];
                    if (nThousand > 0)
                        for (k = 1; k <= nThousand; k++)
                            sResString += sStrArray[nNums[k]];
                    fOutNumber = StringToDouble(sResString);

                    if (k == nAnzNums-2)
                    {
                        double fZaehler = StringToDouble(sStrArray[nNums[k]]);
                        double fNenner = StringToDouble(sStrArray[nNums[k+1]]);
                        if (fNenner != 0.0)
                            fOutNumber += fZaehler/fNenner;
                        else
                            res = sal_False;
                    }
                }

                if ( nStringScanSign )
                {
                    if ( nSign )
                        nSign *= nStringScanSign;
                    else
                        nSign = nStringScanSign;
                }
                if ( nSign < 0 )
                    fOutNumber = -fOutNumber;
                break;

            case NUMBERFORMAT_TIME:
                GetTimeRef(fOutNumber, 0, nAnzNums);
                if ( nSign < 0 )
                    fOutNumber = -fOutNumber;
                break;

            case NUMBERFORMAT_DATE:
            {
                sal_uInt16 nCounter = 0;                        // dummy here
                res = GetDateRef( fOutNumber, nCounter, pFormat );
            }
            break;

            case NUMBERFORMAT_DATETIME:
            {
                sal_uInt16 nCounter = 0;                        // needed here
                res = GetDateRef( fOutNumber, nCounter, pFormat );
                if ( res )
                {
                    double fTime;
                    GetTimeRef( fTime, nCounter, nAnzNums - nCounter );
                    fOutNumber += fTime;
                }
            }
            break;

            default:
                //DBG_ERRORFILE( "Some number recognized but what's it?" );
                fOutNumber = 0.0;
                break;
        }
    }

    if (res)        // overflow/underflow -> Text
    {
        if      (fOutNumber < -DBL_MAX) // -1.7E308
        {
            F_Type = NUMBERFORMAT_TEXT;
            fOutNumber = -DBL_MAX;
            return sal_True;
        }
        else if (fOutNumber >  DBL_MAX) // 1.7E308
        {
            F_Type = NUMBERFORMAT_TEXT;
            fOutNumber = DBL_MAX;
            return sal_True;
        }
    }

    if (res == sal_False)
    {
        eScannedType = NUMBERFORMAT_TEXT;
        fOutNumber = 0.0;
    }

    F_Type = eScannedType;
    return res;
}
}   // namespace duckdb_numformat
