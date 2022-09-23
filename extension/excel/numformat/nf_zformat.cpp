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

#include <chrono>
#include <ctime> 
#include <ctype.h>
#include <stdlib.h>
#include <float.h>
#include <errno.h>
#include <cmath>
#include <cwctype>
#include "nf_calendar.h"
#include "nf_zformat.h"


namespace duckdb_excel {

// --------------------------- zforfind.cxx ---------------------------------------------

#ifndef DBG_UTIL
#define NF_TEST_CALENDAR 0
#else
#define NF_TEST_CALENDAR 0
#endif

const sal_uInt8 ImpSvNumberInputScan::nMatchedEndString    = 0x01;
const sal_uInt8 ImpSvNumberInputScan::nMatchedMidString    = 0x02;
const sal_uInt8 ImpSvNumberInputScan::nMatchedStartString  = 0x04;
const sal_uInt8 ImpSvNumberInputScan::nMatchedVirgin       = 0x08;
const sal_uInt8 ImpSvNumberInputScan::nMatchedUsedAsReturn = 0x10;

#define NF_RECOGNIZE_ISO8601_TIMEZONES 0


enum ScanState
{
	SsStop,
	SsStart,
	SsGetCon,           // condition
	SsGetString,        // format string
	SsGetPrefix,        // color or NatNumN
	SsGetTime,          // [HH] for time
	SsGetBracketed,     // any [...] not decided yet
	SsGetValue,
	SsGetChar,
	SsGetWord,
	SsGetStar,
	SsGetBlank
};

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


ImpSvNumberInputScan::~ImpSvNumberInputScan()
{
    Reset();
    delete [] pUpperMonthText;
    delete [] pUpperAbbrevMonthText;
    delete [] pUpperDayText;
    delete [] pUpperAbbrevDayText;
}


void ImpSvNumberInputScan::Reset()
{
    for (sal_uInt16 i = 0; i < SV_MAX_ANZ_INPUT_STRINGS; i++)
    {
        sStrArray[i].erase();
        nNums[i] = SV_MAX_ANZ_INPUT_STRINGS-1;
        IsNum[i] = false;
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


inline bool ImpSvNumberInputScan::MyIsdigit( sal_Unicode c )
{
    return c < 128 && isdigit( (unsigned char) c );
}


double ImpSvNumberInputScan::StringToDouble( const String& rStr, bool bForceFraction )
{
    double fNum = 0.0;
    double fFrac = 0.0;
    int nExp = 0;
    uint16_t nPos = 0;
    uint16_t nLen = rStr.size();
    bool bPreSep = !bForceFraction;

    while (nPos < nLen)
    {
        if (rStr.at(nPos) == L'.')
            bPreSep = false;
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


bool ImpSvNumberInputScan::NextNumberStringSymbol(
        const sal_Unicode*& pStr,
        String& rSymbol )
{
    bool isNumber = false;
    sal_Unicode cToken;
    ScanState eState = SsStart;
    const sal_Unicode* pHere = pStr;
    uint16_t nChars = 0;

    while ( ((cToken = *pHere) != 0) && eState != SsStop)
    {
        pHere++;
        switch (eState)
        {
            case SsStart:
                if ( MyIsdigit( cToken ) )
                {
                    eState = SsGetValue;
                    isNumber = true;
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


bool ImpSvNumberInputScan::SkipThousands(
        const sal_Unicode*& pStr,
        String& rSymbol )
{
    bool res = false;
    sal_Unicode cToken;
    const String& rThSep = pFormatter->GetNumThousandSep();
    const sal_Unicode* pHere = pStr;
    ScanState eState = SsStart;
    uint16_t nCounter = 0;                                // counts 3 digits

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
                        res = true;                 // .000 combination found
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


void ImpSvNumberInputScan::NumberStringDivision( const String& rString )
{
    const sal_Unicode* pStr = rString.data();
    const sal_Unicode* const pEnd = pStr + rString.size();
    while ( pStr < pEnd && nAnzStrings < SV_MAX_ANZ_INPUT_STRINGS )
    {
        if ( NextNumberStringSymbol( pStr, sStrArray[nAnzStrings] ) )
        {                                               // Zahl
            IsNum[nAnzStrings] = true;
            nNums[nAnzNums] = nAnzStrings;
            nAnzNums++;
            if (nAnzStrings >= SV_MAX_ANZ_INPUT_STRINGS - 7 &&
                nPosThousandString == 0)                // nur einmal
                if ( SkipThousands( pStr, sStrArray[nAnzStrings] ) )
                    nPosThousandString = nAnzStrings;
        }
        else
        {
            IsNum[nAnzStrings] = false;
        }
        nAnzStrings++;
    }
}


bool ImpSvNumberInputScan::StringContainsImpl( const String& rWhat,
            const String& rString, uint16_t nPos )
{
    if ( nPos + rWhat.size() <= rString.size() )
        return StringPtrContainsImpl( rWhat, rString.data(), nPos );
    return false;
}


bool ImpSvNumberInputScan::StringPtrContainsImpl( const String& rWhat,
            const sal_Unicode* pString, uint16_t nPos )
{
    if ( rWhat.size() == 0 )
        return false;
    const sal_Unicode* pWhat = rWhat.data();
    const sal_Unicode* const pEnd = pWhat + rWhat.size();
    const sal_Unicode* pStr = pString + nPos;
    while ( pWhat < pEnd )
    {
        if ( *pWhat != *pStr )
            return false;
        pWhat++;
        pStr++;
    }
    return true;
}


inline bool ImpSvNumberInputScan::SkipChar( sal_Unicode c, const String& rString,
        uint16_t& nPos )
{
    if ((nPos < rString.size()) && (rString.at(nPos) == c))
    {
        nPos++;
        return true;
    }
    return false;
}


inline void ImpSvNumberInputScan::SkipBlanks( const String& rString,
        uint16_t& nPos )
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


inline bool ImpSvNumberInputScan::SkipString( const String& rWhat,
        const String& rString, uint16_t& nPos )
{
    if ( StringContains( rWhat, rString, nPos ) )
    {
        nPos = nPos + rWhat.size();
        return true;
    }
    return false;
}


inline bool ImpSvNumberInputScan::GetThousandSep(
        const String& rString,
        uint16_t& nPos,
        sal_uInt16 nStringPos )
{
    const String& rSep = pFormatter->GetNumThousandSep();
    // Is it an ordinary space instead of a non-breaking space?
    bool bSpaceBreak = rSep.at(0) == 0xa0 && rString.at(0) == 0x20 &&
        rSep.size() == 1 && rString.size() == 1;
    if (!( (rString == rSep || bSpaceBreak)             // nothing else
                && nStringPos < nAnzStrings - 1         // safety first!
                && IsNum[nStringPos+1] ))               // number follows
        return false;                                   // no? => out

	DigitGroupingIterator aGrouping(pFormatter->getDigitGrouping());
	// Match ,### in {3} or ,## in {3,2}
    /* FIXME: this could be refined to match ,## in {3,2} only if ,##,## or
     * ,##,### and to match ,### in {3,2} only if it's the last. However,
     * currently there is no track kept where group separators occur. In {3,2}
     * #,###,### and #,##,## would be valid input, which maybe isn't even bad
     * for #,###,###. Other combinations such as #,###,## maybe not. */
	uint16_t nLen = sStrArray[nStringPos + 1].size();
	if (nLen == aGrouping.get()                         // with 3 (or so) digits
		|| nLen == aGrouping.advance().get()        // or with 2 (or 3 or so) digits
		|| nPosThousandString == nStringPos + 1       // or concatenated
		)
	{
		nPos = nPos + rSep.size();
		return true;
	}
	return false;
}


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


short ImpSvNumberInputScan::GetMonth( const String& rString, uint16_t& nPos )
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


// Converts a string containing a DayOfWeek name (Mon, Monday) at nPos into the
// DayOfWeek number + 1 (negative if abbreviated), returns 0 if nothing found
int ImpSvNumberInputScan::GetDayOfWeek( const String& rString, uint16_t& nPos )
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


bool ImpSvNumberInputScan::GetCurrency( const String& rString, uint16_t& nPos,
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
            return true;
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
                        return true;
                    }
                }
            }
        }
    }

    return false;
}


bool ImpSvNumberInputScan::GetTimeAmPm( const String& rString, uint16_t& nPos )
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
            return true;
        }
        else if ( StringContains( pm_str, rString, nPos ) )
        {
            nAmPm = -1;
            nPos = nPos + pFormatter->getTimePM().size();
            return true;
        }
    }

    return false;
}


inline bool ImpSvNumberInputScan::GetDecSep( const String& rString, uint16_t& nPos )
{
    if ( rString.size() > nPos )
    {
        const String& rSep = pFormatter->GetNumDecimalSep();
        if ( rString.substr(nPos) == rSep )
        {
            nPos = nPos + rSep.size();
            return true;
        }
    }
    return false;
}


// read a hundredth seconds separator

inline bool ImpSvNumberInputScan::GetTime100SecSep( const String& rString, uint16_t& nPos )
{
    if ( rString.size() > nPos )
    {
        const String& rSep = pFormatter->getTime100SecSep();
        if ( rString.substr(nPos) == rSep )
        {
            nPos = nPos + rSep.size();
            return true;
        }
    }
    return false;
}


int ImpSvNumberInputScan::GetSign( const String& rString, uint16_t& nPos )
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


short ImpSvNumberInputScan::GetESign( const String& rString, uint16_t& nPos )
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


// i counts string portions, j counts numbers thereof.
// It should had been called SkipNumber instead.

inline bool ImpSvNumberInputScan::GetNextNumber( sal_uInt16& i, sal_uInt16& j )
{
    if ( i < nAnzStrings && IsNum[i] )
    {
        j++;
        i++;
        return true;
    }
    return false;
}


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
        fSecond100 = StringToDouble( sStrArray[nNums[nIndex]], true );
    if (nAmPm == -1 && nHour != 12)             // PM
        nHour += 12;
    else if (nAmPm == 1 && nHour == 12)         // 12 AM
        nHour = 0;

    fOutNumber = ((double)nHour*3600 +
                  (double)nMinute*60 +
                  (double)nSecond +
                  fSecond100)/86400.0;
}


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

bool ImpSvNumberInputScan::GetDateRef( double& fDays, sal_uInt16& nCounter,
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
    bool res = true;

    Calendar* pCal = pFormatter->GetCalendar();
    for ( int nTryOrder = 1; nTryOrder <= nFormatOrder; nTryOrder++ )
    {
        pCal->setGregorianDateTime( Date() );       // today
        String aOrgCalendar;        // empty => not changed yet
        DateFormat DateFmt;
        bool bFormatTurn;
        switch ( eEDF )
        {
            case NF_EVALDATEFORMAT_INTL :
                bFormatTurn = false;
                DateFmt = pFormatter->getDateFormat();
            break;
            default:
				DateFmt = DateFormat::YMD;
				bFormatTurn = false;
        }

        res = true;
        nCounter = 0;
        // For incomplete dates, always assume first day of month if not specified.
        pCal->setValue( CalendarFieldIndex::DAY_OF_MONTH, 1 );

        switch (nAnzNums)       // count of numbers in string
        {
            case 0:                 // none
                if (nMonthPos)          // only month (Jan)
                    pCal->setValue( CalendarFieldIndex::CFI_MONTH, abs(nMonth)-1 );
                else
                    res = false;
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
                                res = false;
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
                                res = false;
                                break;
                        }
                        break;
                    default:
                        res = false;
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
                                    res = false;
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
                                res = false;
                                break;
                        }
                        break;
                    default:            // else, e.g. month at the end (94 10 Jan)
                        res = false;
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
                                res = false;
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
                                res = false;
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
                                res = false;
                                break;
                        }
                        break;
                    default:            // else, e.g. month at the end (94 10 Jan 8:23)
                        nCounter = 2;
                        res = false;
                        break;
                }   // switch (nMonthPos)
                break;
        }   // switch (nAnzNums)

        if ( res && pCal->isValid() )
        {
            fDays = floor( pCal->getLocalDateTime() );
            nTryOrder = nFormatOrder;   // break for
        }
        else
            res = false;
    }

    return res;
}


bool ImpSvNumberInputScan::ScanStartString( const String& rString,
        const SvNumberformat* pFormat )
{
    uint16_t nPos = 0;
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
        if ( ScanStringNumFor( rString, nPos, pFormat, 0, true ) )
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

    return true;
}


bool ImpSvNumberInputScan::ScanMidString( const String& rString,
        sal_uInt16 nStringPos, const SvNumberformat* pFormat )
{
    uint16_t nPos = 0;
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
        }
    }

    if (nPos < rString.size())                       // not everything consumed?
    {
        if ( nMatchedAllStrings & ~nMatchedVirgin )
            eScannedType = eOldScannedType;
        else
            return false;
    }

    return true;
}


bool ImpSvNumberInputScan::ScanEndString( const String& rString,
        const SvNumberformat* pFormat )
{
    uint16_t nPos = 0;

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

    uint16_t nOrigPos = nPos;
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
        uint16_t nOldPos = nPos;
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

    if (nPos < rString.size())                       // everything consumed?
    {
        // does input EndString equal EndString in Format?
        if ( !ScanStringNumFor( rString, nPos, pFormat, 0xFFFF ) )
            return false;
    }

    return true;
}


bool ImpSvNumberInputScan::ScanStringNumFor(
        const String& rString,          // String to scan
        uint16_t nPos,                // Position until which was consumed
        const SvNumberformat* pFormat,  // The format to match
        sal_uInt16 nString,                 // Substring of format, 0xFFFF => last
        bool bDontDetectNegation        // Suppress sign detection
        )
{
    if ( !pFormat )
        return false;
    const String* pStr;
    String aString( rString );
    bool bFound = false;
    bool bFirst = true;
    bool bContinue = true;
    sal_uInt16 nSub;
    do
    {
        // Don't try "lower" subformats ff the very first match was the second
        // or third subformat.
        nSub = nStringScanNumFor;
        do
        {   // Step through subformats, first positive, then negative, then
            // other, but not the last (text) subformat.
            pStr = pFormat->GetNumForString( nSub, nString, true );
            if ( pStr && aString == *pStr )
            {
                bFound = true;
                bContinue = false;
            }
            else if ( nSub < 2 )
                ++nSub;
            else
                bContinue = false;
        } while ( bContinue );
        if ( !bFound && bFirst && nPos )
        {   // try remaining substring
            bFirst = false;
            aString.erase( 0, nPos );
            bContinue = true;
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
                bFound = true;
                nStringScanSign = -1;
                nSub = 0;       //! not 1
            }
        }
        if ( !bFound )
            return false;
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
    return true;
}


bool ImpSvNumberInputScan::IsNumberFormatMain(
        const String& rString,                  // string to be analyzed
        double& ,                               // OUT: result as number, if possible
        const SvNumberformat* pFormat )         // maybe number format set to match against
{
    Reset();
    NumberStringDivision( rString );            // breakdown into strings and numbers
    if (nAnzStrings >= SV_MAX_ANZ_INPUT_STRINGS) // too many elements
        return false;                           // Njet, Nope, ...

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
                return true;
            }
            else
                return false;                   // simple text
        }
        else
            return false;                       // simple text
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
                        return true;
                    }
                }
            }
            else
            {                                   // Analyze start string
                if (!ScanStartString( sStrArray[i], pFormat ))  // i=0
                    return false;               // already an error
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
                    return true;
                }
            }
            if (i < nAnzStrings && !ScanEndString( sStrArray[i], pFormat ))
                    return false;
        }
        break;
        case 2 :                                // Exactly 2 numbers in input
        {                                       // nAnzStrings >= 3
            if (!GetNextNumber(i,j))            // i=1,0
            {                                   // Analyze start string
                if (!ScanStartString( sStrArray[i], pFormat ))
                    return false;               // already an error
                i++;                            // i=1
            }
            GetNextNumber(i,j);                 // i=1,2
            if ( !ScanMidString( sStrArray[i], i, pFormat ) )
                return false;
            i++;                                // next symbol, i=2,3
            GetNextNumber(i,j);                 // i=3,4
            if (i < nAnzStrings && !ScanEndString( sStrArray[i], pFormat ))
                return false;
            if (eSetType == NUMBERFORMAT_FRACTION)  // -1,200. as fraction
            {
                if (!nNegCheck  &&                  // no sign '('
                    eScannedType == NUMBERFORMAT_UNDEFINED &&
                    (nDecPos == 0 || nDecPos == 3)  // no decimal separator or at end
                    )
                {
                    eScannedType = NUMBERFORMAT_FRACTION;
                    nMatchedAllStrings &= ~nMatchedVirgin;
                    return true;
                }
            }
        }
        break;
        case 3 :                                // Exactly 3 numbers in input
        {                                       // nAnzStrings >= 5
            if (!GetNextNumber(i,j))            // i=1,0
            {                                   // Analyze start string
                if (!ScanStartString( sStrArray[i], pFormat ))
                    return false;               // already an error
                i++;                            // i=1
                if (nDecPos == 1)               // decimal separator at start => error
                    return false;
            }
            GetNextNumber(i,j);                 // i=1,2
            if ( !ScanMidString( sStrArray[i], i, pFormat ) )
                return false;
            i++;                                // i=2,3
            if (eScannedType == NUMBERFORMAT_SCIENTIFIC)    // E only at end
                return false;
            GetNextNumber(i,j);                 // i=3,4
            if ( !ScanMidString( sStrArray[i], i, pFormat ) )
                return false;
            i++;                                // i=4,5
            GetNextNumber(i,j);                 // i=5,6
            if (i < nAnzStrings && !ScanEndString( sStrArray[i], pFormat ))
                return false;
            if (eSetType == NUMBERFORMAT_FRACTION)  // -1,200,100. as fraction
            {
                if (!nNegCheck  &&                  // no sign '('
                    eScannedType == NUMBERFORMAT_UNDEFINED &&
                    (nDecPos == 0 || nDecPos == 3)  // no decimal separator or at end
                )
                {
                    eScannedType = NUMBERFORMAT_FRACTION;
                    nMatchedAllStrings &= ~nMatchedVirgin;
                    return true;
                }
            }
            if ( eScannedType == NUMBERFORMAT_FRACTION && nDecPos )
                return false;                   // #36857# not a real fraction
        }
        break;
        default:                                // More than 3 numbers in input
        {                                       // nAnzStrings >= 7
            if (!GetNextNumber(i,j))            // i=1,0
            {                                   // Analyze startstring
                if (!ScanStartString( sStrArray[i], pFormat ))
                    return false;               // already an error
                i++;                            // i=1
                if (nDecPos == 1)               // decimal separator at start => error
                    return false;
            }
            GetNextNumber(i,j);                 // i=1,2
            if ( !ScanMidString( sStrArray[i], i, pFormat ) )
                return false;
            i++;                                // i=2,3
            sal_uInt16 nThOld = 10;                 // just not 0 or 1
            while (nThOld != nThousand && j < nAnzNums-1)
                                                // Execute at least one time
                                                // but leave one number.
            {                                   // Loop over group separators
                nThOld = nThousand;
                if (eScannedType == NUMBERFORMAT_SCIENTIFIC)    // E only at end
                    return false;
                GetNextNumber(i,j);
                if ( i < nAnzStrings && !ScanMidString( sStrArray[i], i, pFormat ) )
                    return false;
                i++;
            }
            if (eScannedType == NUMBERFORMAT_DATE ||    // long date or
                eScannedType == NUMBERFORMAT_TIME ||    // long time or
                eScannedType == NUMBERFORMAT_UNDEFINED) // long number
            {
                for (sal_uInt16 k = j; k < nAnzNums-1; k++)
                {
                    if (eScannedType == NUMBERFORMAT_SCIENTIFIC)    // E only at endd
                        return false;
                    GetNextNumber(i,j);
                    if ( i < nAnzStrings && !ScanMidString( sStrArray[i], i, pFormat ) )
                        return false;
                    i++;
                }
            }
            GetNextNumber(i,j);
            if (i < nAnzStrings && !ScanEndString( sStrArray[i], pFormat ))
                return false;
            if (eSetType == NUMBERFORMAT_FRACTION)  // -1,200,100. as fraction
            {
                if (!nNegCheck  &&                  // no sign '('
                    eScannedType == NUMBERFORMAT_UNDEFINED &&
                    (nDecPos == 0 || nDecPos == 3)  // no decimal separator or at end
                )
                {
                    eScannedType = NUMBERFORMAT_FRACTION;
                    nMatchedAllStrings &= ~nMatchedVirgin;
                    return true;
                }
            }
            if ( eScannedType == NUMBERFORMAT_FRACTION && nDecPos )
                return false;                       // #36857# not a real fraction
        }
    }

    if (eScannedType == NUMBERFORMAT_UNDEFINED)
    {
        nMatchedAllStrings &= ~nMatchedVirgin;
        // did match including nMatchedUsedAsReturn
        bool bDidMatch = (nMatchedAllStrings != 0);
        if ( nMatchedAllStrings )
        {
            bool bMatch = (pFormat ? pFormat->IsNumForStringElementCountEqual(
                        nStringScanNumFor, nAnzStrings, nAnzNums ) : false);
            if ( !bMatch )
                nMatchedAllStrings = 0;
        }
        if ( nMatchedAllStrings )
            eScannedType = eSetType;
        else if ( bDidMatch )
            return false;
        else
            eScannedType = NUMBERFORMAT_NUMBER;
            // everything else should have been recognized by now
    }
    else if ( eScannedType == NUMBERFORMAT_DATE )
    {   // the very relaxed date input checks may interfere with a preset format
        nMatchedAllStrings &= ~nMatchedVirgin;
        bool bWasReturn = ((nMatchedAllStrings & nMatchedUsedAsReturn) != 0);
        if ( nMatchedAllStrings )
        {
            bool bMatch = (pFormat ? pFormat->IsNumForStringElementCountEqual(
                        nStringScanNumFor, nAnzStrings, nAnzNums ) : false);
            if ( !bMatch )
                nMatchedAllStrings = 0;
        }
        if ( nMatchedAllStrings )
            eScannedType = eSetType;
        else if ( bWasReturn )
            return false;
    }
    else
        nMatchedAllStrings = 0;  // reset flag to no substrings matched

    return true;
}


// return true or false depending on the nMatched... state and remember usage
bool ImpSvNumberInputScan::MatchedReturn()
{
    if ( nMatchedAllStrings & ~nMatchedVirgin )
    {
        nMatchedAllStrings |= nMatchedUsedAsReturn;
        return true;
    }
    return false;
}


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
    bTextInitialized = true;
}


// MUST be called if International/Locale is changed

void ImpSvNumberInputScan::ChangeIntl()
{
    sal_Unicode cDecSep = pFormatter->GetNumDecimalSep().at(0);
    bDecSepInDateSeps = ( cDecSep == L'-' ||
                          cDecSep == L'/' ||
                          cDecSep == L'.' ||
                          cDecSep == pFormatter->GetDateSep().at(0) );
    bTextInitialized = false;
    aUpperCurrSymbol.erase();
}


// => does rString represent a number (also date, time et al)

bool ImpSvNumberInputScan::IsNumberFormat(
        const String& rString,                  // string to be analyzed
        short& F_Type,                          // IN: old type, OUT: new type
        double& fOutNumber,                     // OUT: number if convertable
        const SvNumberformat* pFormat )         // maybe a number format to match against
{
    String sResString;
    String aString;
    bool res;                                   // return value
    eSetType = F_Type;                          // old type set

    if ( !rString.size() )
        res = false;
    else if (rString.size() > 308)               // arbitrary
        res = false;
    else
    {
        // NoMoreUpperNeeded, all comparisons on UpperCase
        aString = rString;
		ConvertToUpper(aString);
		// convert native number to ASCII if necessary
        res = IsNumberFormatMain( aString, fOutNumber, pFormat );
    }

    if (res)
    {
        if ( nNegCheck                              // ')' not found for '('
                || (nSign && (eScannedType == NUMBERFORMAT_DATE
                    || eScannedType == NUMBERFORMAT_DATETIME))
            )                                       // signed date/datetime
            res = false;
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
                            res = false;
                    }
                    else if (nDecPos == 2)          // 1.05
                    {
                        // matched MidStrings function like group separators
                        if ( nMatchedAllStrings )
                            nThousand = nAnzNums - 1;
                        else if ( nAnzNums != nThousand+2 )
                            res = false;
                    }
                    else                            // 1,100 or 1,100.
                    {
                        // matched MidStrings function like group separators
                        if ( nMatchedAllStrings )
                            nThousand = nAnzNums - 1;
                        else if ( nAnzNums != nThousand+1 )
                            res = false;
                    }
                    break;

                case NUMBERFORMAT_SCIENTIFIC:       // 1.0e-2
                    if (nDecPos == 1)               // .05
                    {
                        if (nAnzNums != 2)
                            res = false;
                    }
                    else if (nDecPos == 2)          // 1.05
                    {
                        if (nAnzNums != nThousand+3)
                            res = false;
                    }
                    else                            // 1,100 or 1,100.
                    {
                        if (nAnzNums != nThousand+2)
                            res = false;
                    }
                    break;

                case NUMBERFORMAT_DATE:
                    if (nMonth)
                    {                               // month name and numbers
                        if (nAnzNums > 2)
                            res = false;
                    }
                    else
                    {
                        if (nAnzNums > 3)
                            res = false;
                    }
                    break;

                case NUMBERFORMAT_TIME:
                    if (nDecPos)
                    {                               // hundredth seconds included
                        if (nAnzNums > 4)
                            res = false;
                    }
                    else
                    {
                        if (nAnzNums > 3)
                            res = false;
                    }
                    break;

                case NUMBERFORMAT_DATETIME:
                    if (nMonth)
                    {                               // month name and numbers
                        if (nDecPos)
                        {                           // hundredth seconds included
                            if (nAnzNums > 6)
                                res = false;
                        }
                        else
                        {
                            if (nAnzNums > 5)
                                res = false;
                        }
                    }
                    else
                    {
                        if (nDecPos)
                        {                           // hundredth seconds included
                            if (nAnzNums > 7)
                                res = false;
                        }
                        else
                        {
                            if (nAnzNums > 6)
                                res = false;
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
                    res = false;                // Oops
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
/*!*/                   return true;
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
                            res = false;
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
                            res = false;
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
            return true;
        }
        else if (fOutNumber >  DBL_MAX) // 1.7E308
        {
            F_Type = NUMBERFORMAT_TEXT;
            fOutNumber = DBL_MAX;
            return true;
        }
    }

    if (res == false)
    {
        eScannedType = NUMBERFORMAT_TEXT;
        fOutNumber = 0.0;
    }

    F_Type = eScannedType;
    return res;
}


// ------------------------------------- zforscan.cxx -----------------------------------------------

const sal_Unicode cNonBreakingSpace = 0xA0;

bool isLetter(const String& rStr, uint16_t nPos)
{
	if (nPos < 0 || nPos >= rStr.size()) {
		return false;
	}
	sal_Unicode c = rStr.at(nPos);
	if (c >= L'A' && c <= L'Z' || c >= L'a' && c <= L'z') {
		return true;
	}
	return false;
}


ImpSvNumberformatScan::ImpSvNumberformatScan( LocaleData* pFormatterP )
{
	pFormatter = pFormatterP;
	bConvertMode = false;
	//! All keywords MUST be UPPERCASE!
	sKeyword[NF_KEY_E] = L"E";		// Exponent
	sKeyword[NF_KEY_AMPM] = L"AM/PM";	// AM/PM
	sKeyword[NF_KEY_AP] = L"A/P";		// AM/PM short
	sKeyword[NF_KEY_MI] = L"M";		// Minute
	sKeyword[NF_KEY_MMI] = L"MM";		// Minute 02
	sKeyword[NF_KEY_S] = L"S";		// Second
	sKeyword[NF_KEY_SS] = L"SS";		// Second 02
	sKeyword[NF_KEY_Q] = L"Q";		// Quarter short 'Q'
	sKeyword[NF_KEY_QQ] = L"QQ";		// Quarter long
	sKeyword[NF_KEY_NN] = L"NN";		// Day of week short
	sKeyword[NF_KEY_NNN] = L"NNN";		// Day of week long
	sKeyword[NF_KEY_NNNN] = L"NNNN";		// Day of week long incl. separator
	sKeyword[NF_KEY_WW] = L"WW";		// Week of year
	sKeyword[NF_KEY_CCC] = L"CCC";		// Currency abbreviation
    bKeywordsNeedInit = true;   // locale dependent keywords
    bCompatCurNeedInit = true;  // locale dependent compatibility currency strings

	nStandardPrec = 2;

	sErrStr = L"###";
	Reset();
}

ImpSvNumberformatScan::~ImpSvNumberformatScan()
{
	Reset();
}


void ImpSvNumberformatScan::ChangeIntl()
{
    bKeywordsNeedInit = true;
    bCompatCurNeedInit = true;
    // may be initialized by InitSpecialKeyword()
    sKeyword[NF_KEY_TRUE].erase();
    sKeyword[NF_KEY_FALSE].erase();
}


void ImpSvNumberformatScan::InitSpecialKeyword( NfKeywordIndex eIdx ) const
{
	String str;
    switch ( eIdx )
    {
        case NF_KEY_TRUE :
			str = pFormatter->getReservedWord(reservedWords::TRUE_WORD);
			ConvertToUpper(str);
            ((ImpSvNumberformatScan*)this)->sKeyword[NF_KEY_TRUE] = str;
            if ( !sKeyword[NF_KEY_TRUE].size() )
            {
                ((ImpSvNumberformatScan*)this)->sKeyword[NF_KEY_TRUE] = L"true";
            }
        break;
        case NF_KEY_FALSE :
			str = pFormatter->getReservedWord(reservedWords::FALSE_WORD);
			ConvertToUpper(str);
			((ImpSvNumberformatScan*)this)->sKeyword[NF_KEY_FALSE] = str;
            if ( !sKeyword[NF_KEY_FALSE].size() )
            {
                ((ImpSvNumberformatScan*)this)->sKeyword[NF_KEY_FALSE] = L"false";
            }
        break;
        default:
			break;
    }
}


void ImpSvNumberformatScan::InitCompatCur() const
{
    ImpSvNumberformatScan* pThis = (ImpSvNumberformatScan*)this;
    // currency symbol for old style ("automatic") compatibility format codes
    //pFormatter->GetCompatibilityCurrency( pThis->sCurSymbol, pThis->sCurAbbrev );
	pThis->sCurAbbrev = pFormatter->getCurrBankSymbol();
	pThis->sCurSymbol = pFormatter->getCurrSymbol();

	// currency symbol upper case
	String str = sCurSymbol;
	ConvertToUpper(str);
	pThis->sCurString = str;

	bCompatCurNeedInit = false;
}


void ImpSvNumberformatScan::InitKeywords() const
{
    if ( !bKeywordsNeedInit )
        return ;
    ((ImpSvNumberformatScan*)this)->SetDependentKeywords();
    bKeywordsNeedInit = false;
}


/** Extract the name of General, Standard, Whatever, ignoring leading modifiers
    such as [NatNum1]. */
static String lcl_extractStandardGeneralName( const String& rCode )
{
    String aStr;
    const sal_Unicode* p = rCode.data();
    const sal_Unicode* const pStop = p + rCode.size();
    const sal_Unicode* pBeg = p;    // name begins here
    bool bMod = false;
    bool bDone = false;
    while (p < pStop && !bDone)
    {
        switch (*p)
        {
            case L'[':
                bMod = true;
                break;
            case L']':
                if (bMod)
                {
                    bMod = false;
                    pBeg = p+1;
                }
                // else: would be a locale data error, easily to be spotted in
                // UI dialog
                break;
            case L';':
                if (!bMod)
                {
                    bDone = true;
                    --p;    // put back, increment by one follows
                }
                break;
        }
        ++p;
        if (bMod)
            pBeg = p;
    }
    if (pBeg < p)
        aStr = rCode.substr(pBeg - rCode.data(), p - pBeg);
    return aStr;
}


void ImpSvNumberformatScan::SetDependentKeywords()
{
	// #80023# be sure to generate keywords for the loaded Locale, not for the
	// requested Locale, otherwise number format codes might not match
	LocaleIndentifier eLang = pFormatter->GetLocaleId();

    String aFormat = pFormatter->getFormatCodeNumberStandard();
	sNameStandardFormat = lcl_extractStandardGeneralName( aFormat);
	ConvertToUpper(sNameStandardFormat);
	sKeyword[NF_KEY_GENERAL] = sNameStandardFormat;

	// preset new calendar keywords
	sKeyword[NF_KEY_AAA] = L"AAA";
	sKeyword[NF_KEY_AAAA] = L"AAAA";
	sKeyword[NF_KEY_EC] = L"E";
	sKeyword[NF_KEY_EEC] = L"EE";
	sKeyword[NF_KEY_G] = L"G";
	sKeyword[NF_KEY_GG] = L"GG";
	sKeyword[NF_KEY_GGG] = L"GGG";
	sKeyword[NF_KEY_R] = L"R";
	sKeyword[NF_KEY_RR] = L"RR";

    // Thai T NatNum special. Other locale's small letter 't' results in upper
    // case comparison not matching but length does in conversion mode. Ugly.
    //if (eLang == LANGUAGE_THAI)
    //    sKeyword[NF_KEY_THAI_T] = L"T";
    //else
        sKeyword[NF_KEY_THAI_T] = L"t";

	switch ( eLang )
	{
		default:
		{
			// day
			switch ( eLang )
			{
				case LocaleId_fr_FR            :
					sKeyword[NF_KEY_D] = L"J";
					sKeyword[NF_KEY_DD] = L"JJ";
					sKeyword[NF_KEY_DDD] = L"JJJ";
					sKeyword[NF_KEY_DDDD] = L"JJJJ";
				break;
				default:
					sKeyword[NF_KEY_D] = L"D";
					sKeyword[NF_KEY_DD] = L"DD";
					sKeyword[NF_KEY_DDD] = L"DDD";
					sKeyword[NF_KEY_DDDD] = L"DDDD";
			}
			// month
			switch ( eLang )
			{
				default:
					sKeyword[NF_KEY_M] = L"M";
					sKeyword[NF_KEY_MM] = L"MM";
					sKeyword[NF_KEY_MMM] = L"MMM";
					sKeyword[NF_KEY_MMMM] = L"MMMM";
					sKeyword[NF_KEY_MMMMM] = L"MMMMM";
			}
			// year
			switch ( eLang )
			{
				case LocaleId_fr_FR:
					sKeyword[NF_KEY_YY] = L"AA";
					sKeyword[NF_KEY_YYYY] = L"AAAA";
					// must exchange the day of week name code, same as Xcl
					sKeyword[NF_KEY_AAA] = L"OOO";
					sKeyword[NF_KEY_AAAA] = L"OOOO";
				break;
				default:
					sKeyword[NF_KEY_YY] = L"YY";
					sKeyword[NF_KEY_YYYY] = L"YYYY";
			}
			// hour
			switch ( eLang )
			{
				default:
					sKeyword[NF_KEY_H] = L"H";
					sKeyword[NF_KEY_HH] = L"HH";
			}
			// boolean
			sKeyword[NF_KEY_BOOLEAN] = L"BOOLEAN";
			// colours
			sKeyword[NF_KEY_COLOR] = L"COLOR";
			sKeyword[NF_KEY_BLACK] = L"BLACK";
			sKeyword[NF_KEY_BLUE] = L"BLUE";
			sKeyword[NF_KEY_GREEN] = L"GREEN";
			sKeyword[NF_KEY_CYAN] = L"CYAN";
			sKeyword[NF_KEY_RED] = L"RED";
			sKeyword[NF_KEY_MAGENTA] = L"MAGENTA";
			sKeyword[NF_KEY_BROWN] = L"BROWN";
			sKeyword[NF_KEY_GREY] = L"GREY";
			sKeyword[NF_KEY_YELLOW] = L"YELLOW";
			sKeyword[NF_KEY_WHITE] = L"WHITE";
		}
		break;
	}

	// boolean keyords
    InitSpecialKeyword( NF_KEY_TRUE );
    InitSpecialKeyword( NF_KEY_FALSE );

    // compatibility currency strings
    InitCompatCur();
}


void ImpSvNumberformatScan::ChangeStandardPrec(sal_uInt16 nPrec)
{
	nStandardPrec = nPrec;
}

short ImpSvNumberformatScan::GetKeyWord( const String& sSymbol, uint16_t nPos )
{
	String sString = sSymbol.substr(nPos);
	ConvertToUpper(sString);
    const NfKeywordTable & rKeyword = GetKeywords();
	// #77026# for the Xcl perverts: the GENERAL keyword is recognized anywhere
    if ( sString.find( rKeyword[NF_KEY_GENERAL] ) == 0 )
		return NF_KEY_GENERAL;
	//! MUST be a reverse search to find longer strings first
	short i = NF_KEYWORD_ENTRIES_COUNT-1;
	bool bFound = false;
    for ( ; i > NF_KEY_LASTKEYWORD_SO5; --i )
    {
        bFound = sString.find(rKeyword[i]) == 0;
        if ( bFound )
        {
            break;
        }
    }
	// new keywords take precedence over old keywords
	if ( !bFound )
	{	// skip the gap of colors et al between new and old keywords and search on
		i = NF_KEY_LASTKEYWORD;
        while ( i > 0 && sString.find(rKeyword[i]) != 0)
			i--;
        if ( i > NF_KEY_LASTOLDKEYWORD && sString != rKeyword[i] )
		{	// found something, but maybe it's something else?
			// e.g. new NNN is found in NNNN, for NNNN we must search on
			short j = i - 1;
            while ( j > 0 && sString.find(rKeyword[j]) != 0)
				j--;
            if ( j && rKeyword[j].size() > rKeyword[i].size() )
				return j;
		}
	}
	return i;		// 0 => not found
}

short ImpSvNumberformatScan::Next_Symbol( const String& rStr,
			uint16_t& nPos, String& sSymbol )
{
    if ( bKeywordsNeedInit )
        InitKeywords();
	const uint16_t nStart = nPos;
	short eType = 0;
	ScanState eState = SsStart;
	sSymbol.erase();
	while ( nPos < rStr.size() && eState != SsStop )
	{
		sal_Unicode cToken = rStr.at( nPos++ );
		switch (eState)
		{
			case SsStart:
			{
				// Fetch any currency longer than one character and don't get
				// confused later on by "E/" or other combinations of letters
                // and meaningful symbols. Necessary for old automatic currency.
                // #96158# But don't do it if we're starting a "[...]" section,
                // for example a "[$...]" new currency symbol to not parse away
                // "$U" (symbol) of "[$UYU]" (abbreviation).
                if ( nCurrPos != STRING_NOTFOUND && sCurString.size() > 1 &&
                        nPos-1 + sCurString.size() <= rStr.size() &&
                        !(nPos > 1 && rStr.at( nPos-2 ) == L'[') )
				{
                    String aTest( rStr.substr( nPos-1, sCurString.size() ) );
					ConvertToUpper(aTest);
                    if ( aTest == sCurString )
					{
                        sSymbol = rStr.substr( --nPos, sCurString.size() );
						nPos = nPos + sSymbol.size();
						eState = SsStop;
						eType = NF_SYMBOLTYPE_STRING;
						return eType;
					}
				}
				switch (cToken)
				{
					case L'#':
					case L'0':
					case L'?':
					case L'%':
					case L'@':
					case L'[':
					case L']':
					case L',':
					case L'.':
					case L'/':
					case L'\'':
					case L' ':
					case L':':
					case L'-':
					{
						eType = NF_SYMBOLTYPE_DEL;
						sSymbol += cToken;
						eState = SsStop;
					}
					break;
					case L'*':
					{
						eType = NF_SYMBOLTYPE_STAR;
						sSymbol += cToken;
						eState = SsGetStar;
					}
					break;
					case L'_':
					{
						eType = NF_SYMBOLTYPE_BLANK;
						sSymbol += cToken;
						eState = SsGetBlank;
					}
					break;
					case L'"':
						eType = NF_SYMBOLTYPE_STRING;
						eState = SsGetString;
						sSymbol += cToken;
					break;
					case L'\\':
						eType = NF_SYMBOLTYPE_STRING;
						eState = SsGetChar;
						sSymbol += cToken;
					break;
					case L'$':
					case L'+':
					case L'(':
					case L')':
						eType = NF_SYMBOLTYPE_STRING;
						eState = SsStop;
						sSymbol += cToken;
					break;
					default :
					{
                        if (StringEqualsChar( pFormatter->GetNumDecimalSep(), cToken) ||
                                StringEqualsChar( pFormatter->GetNumThousandSep(), cToken) ||
                                StringEqualsChar( pFormatter->GetDateSep(), cToken) ||
                                StringEqualsChar(pFormatter->getTimeSep(), cToken) ||
                                StringEqualsChar(pFormatter->getTime100SecSep(), cToken))
                        {
                            // Another separator than pre-known ASCII
                            eType = NF_SYMBOLTYPE_DEL;
                            sSymbol += cToken;
                            eState = SsStop;
                        }
                        else if ( isLetter( rStr, nPos-1 ) )
						{
							short nTmpType = GetKeyWord( rStr, nPos-1 );
							if ( nTmpType )
							{
								bool bCurrency = false;
								// "Automatic" currency may start with keyword,
								// like "R" (Rand) and 'R' (era)
								if ( nCurrPos != STRING_NOTFOUND &&
                                    nPos-1 + sCurString.size() <= rStr.size() &&
                                    sCurString.find( sKeyword[nTmpType] ) == 0 )
								{
                                    String aTest( rStr.substr( nPos-1, sCurString.size() ) );
									ConvertToUpper(aTest);
                                    if ( aTest == sCurString )
										bCurrency = true;
								}
								if ( bCurrency )
								{
									eState = SsGetWord;
									sSymbol += cToken;
								}
								else
								{
									eType = nTmpType;
									uint16_t nLen = sKeyword[eType].size();
									sSymbol = rStr.substr( nPos-1, nLen );
									if ( eType == NF_KEY_E || IsAmbiguousE( eType ) )
									{
										sal_Unicode cNext = rStr.at(nPos);
										switch ( cNext )
										{
											case L'+' :
											case L'-' :	// E+ E- combine to one symbol
												sSymbol += cNext;
												eType = NF_KEY_E;
												nPos++;
											break;
											case L'0' :
											case L'#' :	// scientific E without sign
												eType = NF_KEY_E;
											break;
										}
									}
									nPos--;
									nPos = nPos + nLen;
									eState = SsStop;
								}
							}
							else
							{
								eState = SsGetWord;
								sSymbol += cToken;
							}
						}
						else if (std::iswdigit(rStr.at(nPos-1)))
						{
							eType = NF_SYMBOLTYPE_DIGIT;
							eState = SsStop;
							sSymbol += cToken;
						}
						else
						{
							eType = NF_SYMBOLTYPE_STRING;
							eState = SsStop;
							sSymbol += cToken;
						}
					}
					break;
				}
			}
			break;
			case SsGetChar:
			{
				sSymbol += cToken;
				eState = SsStop;
			}
			break;
			case SsGetString:
			{
				if (cToken == L'"')
					eState = SsStop;
				sSymbol += cToken;
			}
			break;
			case SsGetWord:
			{
				if ( isLetter( rStr, nPos-1 ) )
				{
					short nTmpType = GetKeyWord( rStr, nPos-1 );
					if ( nTmpType )
					{	// beginning of keyword, stop scan and put back
						eType = NF_SYMBOLTYPE_STRING;
						eState = SsStop;
						nPos--;
					}
					else
						sSymbol += cToken;
				}
				else
				{
					bool bDontStop = false;
					switch (cToken)
					{
						case L'/':						// AM/PM, A/P
						{
							sal_Unicode cNext = rStr.at(nPos);
							if ( cNext == L'P' || cNext == L'p' )
							{
								uint16_t nLen = sSymbol.size();
								if ( 1 <= nLen
										&& (sSymbol.at(0) == L'A' || sSymbol.at(0) == L'a')
										&& (nLen == 1 || (nLen == 2
											&& (sSymbol.at(1) == L'M' || sSymbol.at(1) == L'm')
											&& (rStr.at(nPos+1) == L'M' || rStr.at(nPos+1) == L'm'))) )
								{
									sSymbol += cToken;
									bDontStop = true;
								}
							}
						}
						break;
					}
					// anything not recognized will stop the scan
					if ( eState != SsStop && !bDontStop )
					{
						eState = SsStop;
						nPos--;
						eType = NF_SYMBOLTYPE_STRING;
					}
				}
			}
			break;
			case SsGetStar:
			{
				eState = SsStop;
				sSymbol += cToken;
				nRepPos = (nPos - nStart) - 1;	// every time > 0!!
			}
			break;
			case SsGetBlank:
			{
				eState = SsStop;
				sSymbol += cToken;
			}
			break;
			default:
			break;
		}									// of switch
	} 										// of while
	if (eState == SsGetWord)
		eType = NF_SYMBOLTYPE_STRING;
	return eType;
}

uint16_t ImpSvNumberformatScan::Symbol_Division(const String& rString)
{
	nCurrPos = STRING_NOTFOUND;
													// Is currency involved?
	String sString = rString;
	ConvertToUpper(sString);
	uint16_t nCPos = 0;
	while (nCPos != STRING_NOTFOUND)
	{
        nCPos = sString.find(GetCurString(), nCPos);
		if (nCPos != STRING_NOTFOUND)
		{
			// in Quotes?
			uint16_t nQ = SvNumberformat::GetQuoteEnd( sString, nCPos );
			if ( nQ == STRING_NOTFOUND )
			{
				sal_Unicode c;
				if ( nCPos == 0 ||
					((c = sString.at(uint16_t(nCPos-1))) != '"'
							&& c != L'\\') )			// dm can be replaced by "dm
				{                   				// \d to be protected
					nCurrPos = nCPos;
					nCPos = STRING_NOTFOUND;		// cancellation
				}
				else
					nCPos++;						// keep searching
			}
			else
				nCPos = nQ + 1;						// keep searching
		}
	}
	nAnzStrings = 0;
	bool bStar = false;					// is set with '*'detection
	Reset();

	uint16_t nPos = 0;
	const uint16_t nLen = rString.size();
	while (nPos < nLen && nAnzStrings < NF_MAX_FORMAT_SYMBOLS)
	{
		nTypeArray[nAnzStrings] = Next_Symbol(rString, nPos, sStrArray[nAnzStrings]);
		if (nTypeArray[nAnzStrings] == NF_SYMBOLTYPE_STAR)
		{								// monitoring of '*'
			if (bStar)
				return nPos;		// error: double '*'
			else
				bStar = true;
		}
		nAnzStrings++;
	}

	return 0;						// 0 => ok
}

void ImpSvNumberformatScan::SkipStrings(sal_uInt16& i, uint16_t& nPos)
{
	while (i < nAnzStrings && (   nTypeArray[i] == NF_SYMBOLTYPE_STRING
							   || nTypeArray[i] == NF_SYMBOLTYPE_BLANK
							   || nTypeArray[i] == NF_SYMBOLTYPE_STAR) )
	{
		nPos = nPos + sStrArray[i].size();
		i++;
	}
}


sal_uInt16 ImpSvNumberformatScan::PreviousKeyword(sal_uInt16 i)
{
	short res = 0;
	if (i > 0 && i < nAnzStrings)
	{
		i--;
		while (i > 0 && nTypeArray[i] <= 0)
			i--;
		if (nTypeArray[i] > 0)
			res = nTypeArray[i];
	}
	return res;
}

sal_uInt16 ImpSvNumberformatScan::NextKeyword(sal_uInt16 i)
{
	short res = 0;
	if (i < nAnzStrings-1)
	{
		i++;
		while (i < nAnzStrings-1 && nTypeArray[i] <= 0)
			i++;
		if (nTypeArray[i] > 0)
			res = nTypeArray[i];
	}
	return res;
}

short ImpSvNumberformatScan::PreviousType( sal_uInt16 i )
{
	if ( i > 0 && i < nAnzStrings )
	{
		do
		{
			i--;
		} while ( i > 0 && nTypeArray[i] == NF_SYMBOLTYPE_EMPTY );
		return nTypeArray[i];
	}
	return 0;
}

sal_Unicode ImpSvNumberformatScan::PreviousChar(sal_uInt16 i)
{
	sal_Unicode res = L' ';
	if (i > 0 && i < nAnzStrings)
	{
		i--;
		while (i > 0 && ( 	nTypeArray[i] == NF_SYMBOLTYPE_EMPTY
						 || nTypeArray[i] == NF_SYMBOLTYPE_STRING
						 || nTypeArray[i] == NF_SYMBOLTYPE_STAR
						 || nTypeArray[i] == NF_SYMBOLTYPE_BLANK ) )
			i--;
		if (sStrArray[i].size() > 0)
			res = sStrArray[i].at(uint16_t(sStrArray[i].size()-1));
	}
	return res;
}

sal_Unicode ImpSvNumberformatScan::NextChar(sal_uInt16 i)
{
	sal_Unicode res = L' ';
	if (i < nAnzStrings-1)
	{
		i++;
		while (i < nAnzStrings-1 &&
			   (   nTypeArray[i] == NF_SYMBOLTYPE_EMPTY
				|| nTypeArray[i] == NF_SYMBOLTYPE_STRING
				|| nTypeArray[i] == NF_SYMBOLTYPE_STAR
				|| nTypeArray[i] == NF_SYMBOLTYPE_BLANK))
			i++;
		if (sStrArray[i].size() > 0)
			res = sStrArray[i].at(0);
	}
	return res;
}

bool ImpSvNumberformatScan::IsLastBlankBeforeFrac(sal_uInt16 i)
{
	bool res = true;
	if (i < nAnzStrings-1)
	{
		bool bStop = false;
		i++;
		while (i < nAnzStrings-1 && !bStop)
		{
			i++;
			if ( nTypeArray[i] == NF_SYMBOLTYPE_DEL &&
					sStrArray[i].at(0) == L'/')
				bStop = true;
			else if ( nTypeArray[i] == NF_SYMBOLTYPE_DEL &&
					sStrArray[i].at(0) == L' ')
				res = false;
		}
		if (!bStop)									// kein '/'
			res = false;
	}
	else
		res = false;								// kein '/' mehr

	return res;
}

void ImpSvNumberformatScan::Reset()
{
	nAnzStrings = 0;
	nAnzResStrings = 0;

	for (size_t i = 0; i < NF_MAX_FORMAT_SYMBOLS; i++)
	{
		sStrArray[i].erase();
		nTypeArray[i] = 0;
	}

	eScannedType = NUMBERFORMAT_UNDEFINED;
	nRepPos = 0;
	bExp = false;
	bThousand = false;
	nThousand = 0;
	bDecSep = false;
	nDecPos =  -1;
	nExpPos = (sal_uInt16) -1;
	nBlankPos = (sal_uInt16) -1;
	nCntPre = 0;
	nCntPost = 0;
	nCntExp = 0;
	nExpVal = 0;
	bFrac = false;
	bBlank = false;
    nNatNumModifier = 0;
}


bool ImpSvNumberformatScan::Is100SecZero( sal_uInt16 i, bool bHadDecSep )
{
    sal_uInt16 nIndexPre = PreviousKeyword( i );
    return (nIndexPre == NF_KEY_S || nIndexPre == NF_KEY_SS)
            && (bHadDecSep                 // S, SS ','
            || (i>0 && nTypeArray[i-1] == NF_SYMBOLTYPE_STRING));
                // SS"any"00  take "any" as a valid decimal separator
}


uint16_t ImpSvNumberformatScan::ScanType(const String&)
{
	uint16_t nPos = 0;
    sal_uInt16 i = 0;
    short eNewType;
    bool bMatchBracket = false;
    bool bHaveGeneral = false;      // if General/Standard encountered

    SkipStrings(i, nPos);
	while (i < nAnzStrings)
	{
        if (nTypeArray[i] > 0)
        {                                       // keyword
			switch (nTypeArray[i])
			{
				case NF_KEY_E:			 				// E
					eNewType = NUMBERFORMAT_SCIENTIFIC;
				break;
				case NF_KEY_AMPM:		 				// AM,A,PM,P
				case NF_KEY_AP:
				case NF_KEY_H:							// H
				case NF_KEY_HH:							// HH
				case NF_KEY_S:							// S
				case NF_KEY_SS:							// SS
					eNewType = NUMBERFORMAT_TIME;
				break;
				case NF_KEY_M:			 				// M
				case NF_KEY_MM:			 				// MM
                {                                       // minute or month
					sal_uInt16 nIndexPre = PreviousKeyword(i);
					sal_uInt16 nIndexNex = NextKeyword(i);
					sal_Unicode cChar = PreviousChar(i);
					if (nIndexPre == NF_KEY_H	|| 	// H
						nIndexPre == NF_KEY_HH	|| 	// HH
						nIndexNex == NF_KEY_S	|| 	// S
						nIndexNex == NF_KEY_SS	||  // SS
						cChar == L'['  )     // [M
					{
						eNewType = NUMBERFORMAT_TIME;
						nTypeArray[i] -= 2;			// 6 -> 4, 7 -> 5
					}
					else
						eNewType = NUMBERFORMAT_DATE;
				}
				break;
				case NF_KEY_MMM:				// MMM
				case NF_KEY_MMMM:				// MMMM
				case NF_KEY_MMMMM:				// MMMMM
				case NF_KEY_Q:					// Q
				case NF_KEY_QQ:					// QQ
				case NF_KEY_D:					// D
				case NF_KEY_DD:					// DD
				case NF_KEY_DDD:				// DDD
				case NF_KEY_DDDD:				// DDDD
				case NF_KEY_YY:					// YY
				case NF_KEY_YYYY:				// YYYY
				case NF_KEY_NN:					// NN
				case NF_KEY_NNN:				// NNN
				case NF_KEY_NNNN:				// NNNN
				case NF_KEY_WW :				// WW
				case NF_KEY_AAA :				// AAA
				case NF_KEY_AAAA :				// AAAA
				case NF_KEY_EC :				// E
				case NF_KEY_EEC :				// EE
				case NF_KEY_G :					// G
				case NF_KEY_GG :				// GG
				case NF_KEY_GGG :				// GGG
				case NF_KEY_R :					// R
				case NF_KEY_RR :				// RR
					eNewType = NUMBERFORMAT_DATE;
				break;
				case NF_KEY_CCC:				// CCC
					eNewType = NUMBERFORMAT_CURRENCY;
				break;
				case NF_KEY_GENERAL:			// Standard
					eNewType = NUMBERFORMAT_NUMBER;
                    bHaveGeneral = true;
				break;
				default:
					eNewType = NUMBERFORMAT_UNDEFINED;
				break;
			}
		}
        else
        {                                       // control character
			switch ( sStrArray[i].at(0) )
			{
				case L'#':
				case L'?':
					eNewType = NUMBERFORMAT_NUMBER;
				break;
				case L'0':
				{
                    if ( (eScannedType & NUMBERFORMAT_TIME) == NUMBERFORMAT_TIME )
					{
                        if ( Is100SecZero( i, bDecSep ) )
                        {
                            bDecSep = true;                 // subsequent 0's
							eNewType = NUMBERFORMAT_TIME;
                        }
						else
                            return nPos;                    // Error
					}
					else
						eNewType = NUMBERFORMAT_NUMBER;
				}
				break;
				case L'%':
					eNewType = NUMBERFORMAT_PERCENT;
				break;
				case L'/':
					eNewType = NUMBERFORMAT_FRACTION;
				break;
				case L'[':
				{
					if ( i < nAnzStrings-1 &&
							nTypeArray[i+1] == NF_SYMBOLTYPE_STRING &&
							sStrArray[i+1].at(0) == L'$' )
					{	// as of SV_NUMBERFORMATTER_VERSION_NEW_CURR
						eNewType = NUMBERFORMAT_CURRENCY;
                        bMatchBracket = true;
					}
					else if ( i < nAnzStrings-1 &&
							nTypeArray[i+1] == NF_SYMBOLTYPE_STRING &&
							sStrArray[i+1].at(0) == L'~' )
					{	// as of SV_NUMBERFORMATTER_VERSION_CALENDAR
						eNewType = NUMBERFORMAT_DATE;
                        bMatchBracket = true;
					}
					else
					{
						sal_uInt16 nIndexNex = NextKeyword(i);
						if (nIndexNex == NF_KEY_H	|| 	// H
							nIndexNex == NF_KEY_HH	|| 	// HH
							nIndexNex == NF_KEY_M	|| 	// M
							nIndexNex == NF_KEY_MM	|| 	// MM
							nIndexNex == NF_KEY_S	|| 	// S
							nIndexNex == NF_KEY_SS   )	// SS
							eNewType = NUMBERFORMAT_TIME;
						else
                            return nPos;                // Error
					}
				}
				break;
				case L'@':
					eNewType = NUMBERFORMAT_TEXT;
				break;
				default:
                    if ( sStrArray[i] == pFormatter->getTime100SecSep() )
                        bDecSep = true;                     // for SS,0
                    eNewType = NUMBERFORMAT_UNDEFINED;
				break;
			}
		}
		if (eScannedType == NUMBERFORMAT_UNDEFINED)
			eScannedType = eNewType;
		else if (eScannedType == NUMBERFORMAT_TEXT || eNewType == NUMBERFORMAT_TEXT)
			eScannedType = NUMBERFORMAT_TEXT;				// Text is always text
		else if (eNewType == NUMBERFORMAT_UNDEFINED)
		{											// remains as before
		}
		else if (eScannedType != eNewType)
		{
			switch (eScannedType)
			{
				case NUMBERFORMAT_DATE:
				{
					switch (eNewType)
					{
						case NUMBERFORMAT_TIME:
							eScannedType = NUMBERFORMAT_DATETIME;
						break;
						case NUMBERFORMAT_FRACTION: 		// DD/MM
						break;
						default:
						{
							if (nCurrPos != STRING_NOTFOUND)
								eScannedType = NUMBERFORMAT_UNDEFINED;
                            else if ( sStrArray[i] != pFormatter->GetDateSep() )
								return nPos;
						}
					}
				}
				break;
				case NUMBERFORMAT_TIME:
				{
					switch (eNewType)
					{
						case NUMBERFORMAT_DATE:
							eScannedType = NUMBERFORMAT_DATETIME;
						break;
						case NUMBERFORMAT_FRACTION: 		// MM/SS
						break;
						default:
						{
							if (nCurrPos != STRING_NOTFOUND)
								eScannedType = NUMBERFORMAT_UNDEFINED;
							else if ( sStrArray[i] != pFormatter->getTimeSep() )
								return nPos;
						}
					}
				}
				break;
				case NUMBERFORMAT_DATETIME:
				{
					switch (eNewType)
					{
						case NUMBERFORMAT_TIME:
						case NUMBERFORMAT_DATE:
						break;
						case NUMBERFORMAT_FRACTION: 		// DD/MM
						break;
						default:
						{
							if (nCurrPos != STRING_NOTFOUND)
								eScannedType = NUMBERFORMAT_UNDEFINED;
                            else if ( sStrArray[i] != pFormatter->GetDateSep()
								   && sStrArray[i] != pFormatter->getTimeSep() )
								return nPos;
						}
					}
				}
				break;
				case NUMBERFORMAT_PERCENT:
				{
					switch (eNewType)
					{
						case NUMBERFORMAT_NUMBER:	// only number by percent
						break;
						default:
							return nPos;
					}
				}
				break;
				case NUMBERFORMAT_SCIENTIFIC:
				{
					switch (eNewType)
					{
						case NUMBERFORMAT_NUMBER:	// only number after E
						break;
						default:
							return nPos;
					}
				}
				break;
				case NUMBERFORMAT_NUMBER:
				{
					switch (eNewType)
					{
						case NUMBERFORMAT_SCIENTIFIC:
						case NUMBERFORMAT_PERCENT:
						case NUMBERFORMAT_FRACTION:
						case NUMBERFORMAT_CURRENCY:
							eScannedType = eNewType;
						break;
						default:
							if (nCurrPos != STRING_NOTFOUND)
								eScannedType = NUMBERFORMAT_UNDEFINED;
							else
								return nPos;
					}
				}
				break;
				case NUMBERFORMAT_FRACTION:
				{
					switch (eNewType)
					{
						case NUMBERFORMAT_NUMBER:			// only number after fraction
						break;
						default:
							return nPos;
					}
				}
				break;
				default:
				break;
			}
		}
		nPos = nPos + sStrArray[i].size();			// Proofreading position
		i++;
        if ( bMatchBracket )
        {   // no type detection inside of matching brackets if [$...], [~...]
            while ( bMatchBracket && i < nAnzStrings )
            {
                if ( nTypeArray[i] == NF_SYMBOLTYPE_DEL
                        && sStrArray[i].at(0) == L']' )
                    bMatchBracket = false;
                else
                    nTypeArray[i] = NF_SYMBOLTYPE_STRING;
                nPos = nPos + sStrArray[i].size();
                i++;
            }
            if ( bMatchBracket )
                return nPos;    // missing closing bracket at end of code
        }
		SkipStrings(i, nPos);
	}

	if ((eScannedType == NUMBERFORMAT_NUMBER || eScannedType == NUMBERFORMAT_UNDEFINED)
		 && nCurrPos != STRING_NOTFOUND && !bHaveGeneral)
		eScannedType = NUMBERFORMAT_CURRENCY;	// old "automatic" currency
	if (eScannedType == NUMBERFORMAT_UNDEFINED)
		eScannedType = NUMBERFORMAT_DEFINED;
	return 0;								// Alles ok
}


bool ImpSvNumberformatScan::InsertSymbol( sal_uInt16 & nPos, NfSymbolType eType, const String& rStr )
{
    if (nAnzStrings >= NF_MAX_FORMAT_SYMBOLS || nPos > nAnzStrings)
        return false;
    ++nAnzResStrings;
    if (nPos > 0 && nTypeArray[nPos-1] == NF_SYMBOLTYPE_EMPTY)
        --nPos;     // reuse position
    else
    {
        ++nAnzStrings;
        for (size_t i = nAnzStrings; i > nPos; --i)
        {
            nTypeArray[i] = nTypeArray[i-1];
            sStrArray[i] = sStrArray[i-1];
        }
    }
    nTypeArray[nPos] = static_cast<short>(eType);
    sStrArray[nPos] = rStr;
    return true;
}


int ImpSvNumberformatScan::FinalScanGetCalendar( uint16_t& nPos, sal_uInt16& i,
			sal_uInt16& rAnzResStrings )
{
	return 0;
}

uint16_t ImpSvNumberformatScan::FinalScan( String& rString, String& rComment )
{
	// save values for convert mode
    String sOldDecSep       = pFormatter->GetNumDecimalSep();
    String sOldThousandSep  = pFormatter->GetNumThousandSep();
    String sOldDateSep      = pFormatter->GetDateSep();
	String sOldTimeSep		= pFormatter->getTimeSep();
    String sOldTime100SecSep= pFormatter->getTime100SecSep();
    String sOldCurSymbol    = GetCurSymbol();
    String sOldCurString    = GetCurString();
    sal_Unicode cOldKeyH    = sKeyword[NF_KEY_H].at(0);
    sal_Unicode cOldKeyMI   = sKeyword[NF_KEY_MI].at(0);
    sal_Unicode cOldKeyS    = sKeyword[NF_KEY_S].at(0);

	// If the group separator is a Non-Breaking Space (French) continue with a
	// normal space instead so queries on space work correctly.
	// The format string is adjusted to allow both.
	// For output of the format code string the LocaleData characters are used.
	if ( sOldThousandSep.at(0) == cNonBreakingSpace && sOldThousandSep.size() == 1 )
		sOldThousandSep = L' ';

	// change locale data et al
	if (bConvertMode)
    {
		pFormatter->SetLocaleId(eNewLnge);
        //! init new keywords
        InitKeywords();
    }

    uint16_t nPos = 0;                    // error correction position
    sal_uInt16 i = 0;                           // symbol loop counter
    sal_uInt16 nCounter = 0;                    // counts digits
    nAnzResStrings = nAnzStrings;           // counts remaining symbols
    bDecSep = false;                        // reset in case already used in TypeCheck
    bool bThaiT = false;                    // Thai T NatNum modifier present

	switch (eScannedType)
	{
		case NUMBERFORMAT_TEXT:
		case NUMBERFORMAT_DEFINED:
		{
			while (i < nAnzStrings)
			{
				switch (nTypeArray[i])
				{
					case NF_SYMBOLTYPE_BLANK:
					case NF_SYMBOLTYPE_STAR:
					break;
					case NF_SYMBOLTYPE_COMMENT:
					{
						String& rStr = sStrArray[i];
						nPos = nPos + rStr.size();
						SvNumberformat::EraseCommentBraces( rStr );
						rComment += rStr;
						nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
						nAnzResStrings--;
					}
					break;
					case NF_KEY_GENERAL :	// #77026# "General" is the same as "@"
					break;
					default:
					{
						if ( nTypeArray[i] != NF_SYMBOLTYPE_DEL ||
								sStrArray[i].at(0) != L'@' )
							nTypeArray[i] = NF_SYMBOLTYPE_STRING;
					}
					break;
				}
				nPos = nPos + sStrArray[i].size();
				i++;
			}										// of while
		}
		break;
		case NUMBERFORMAT_NUMBER:
		case NUMBERFORMAT_PERCENT:
		case NUMBERFORMAT_CURRENCY:
		case NUMBERFORMAT_SCIENTIFIC:
		case NUMBERFORMAT_FRACTION:
		{
			sal_Unicode cThousandFill = L' ';
			while (i < nAnzStrings)
			{
				if (eScannedType == NUMBERFORMAT_FRACTION &&  	// special case
					nTypeArray[i] == NF_SYMBOLTYPE_DEL && 			// # ### #/#
					StringEqualsChar( sOldThousandSep, L' ' ) && // e.g. France or Sweden
					StringEqualsChar( sStrArray[i], L' ' ) &&
					!bFrac                          &&
					IsLastBlankBeforeFrac(i) )
				{
					nTypeArray[i] = NF_SYMBOLTYPE_STRING;			// del->string
				}                                               // no dew.p.


				if (nTypeArray[i] == NF_SYMBOLTYPE_BLANK	||
					nTypeArray[i] == NF_SYMBOLTYPE_STAR	||
					nTypeArray[i] == NF_KEY_CCC			||	// CCC
					nTypeArray[i] == NF_KEY_GENERAL )		// Standard
				{
					if (nTypeArray[i] == NF_KEY_GENERAL)
					{
						nThousand = FLAG_STANDARD_IN_FORMAT;
						if ( bConvertMode )
							sStrArray[i] = sNameStandardFormat;
					}
					nPos = nPos + sStrArray[i].size();
					i++;
				}
				else if (nTypeArray[i] == NF_SYMBOLTYPE_STRING ||  // Strings oder
						 nTypeArray[i] > 0) 					// Keywords
				{
					if (eScannedType == NUMBERFORMAT_SCIENTIFIC &&
							 nTypeArray[i] == NF_KEY_E) 		// E+
					{
						if (bExp) 								// doppelt
							return nPos;
						bExp = true;
						nExpPos = i;
						if (bDecSep)
							nCntPost = nCounter;
						else
							nCntPre = nCounter;
						nCounter = 0;
						nTypeArray[i] = NF_SYMBOLTYPE_EXP;
					}
					else if (eScannedType == NUMBERFORMAT_FRACTION &&
							 sStrArray[i].at(0) == L' ')
					{
						if (!bBlank && !bFrac)	// not double or behind /
						{
							if (bDecSep && nCounter > 0)	// decimal places
								return nPos;				// error
							bBlank = true;
							nBlankPos = i;
							nCntPre = nCounter;
							nCounter = 0;
						}
						nTypeArray[i] = NF_SYMBOLTYPE_FRACBLANK;
					}
                    else if (nTypeArray[i] == NF_KEY_THAI_T)
                    {
                        bThaiT = true;
                        sStrArray[i] = sKeyword[nTypeArray[i]];
                    }
					else
						nTypeArray[i] = NF_SYMBOLTYPE_STRING;
					nPos = nPos + sStrArray[i].size();
					i++;
				}
				else if (nTypeArray[i] == NF_SYMBOLTYPE_DEL)
				{
					sal_Unicode cHere = sStrArray[i].at(0);
                    // Handle not pre-known separators in switch.
                    sal_Unicode cSimplified;
                    if (StringEqualsChar( pFormatter->GetNumThousandSep(), cHere))
                        cSimplified = L',';
                    else if (StringEqualsChar( pFormatter->GetNumDecimalSep(), cHere))
                        cSimplified = L'.';
                    else
                        cSimplified = cHere;
					switch ( cSimplified )
					{
						case L'#':
						case L'0':
						case L'?':
						{
							if (nThousand > 0)					// #... #
								return nPos;					// error
							//else if (bFrac && cHere == L'0')	// modified for "0 0/0"
							//	return nPos;					// 0 in the denominator
							nTypeArray[i] = NF_SYMBOLTYPE_DIGIT;
							String& rStr = sStrArray[i];
							nPos = nPos + rStr.size();
							i++;
							nCounter++;
							while (i < nAnzStrings &&
								(sStrArray[i].at(0) == L'#' ||
									sStrArray[i].at(0) == L'0' ||
									sStrArray[i].at(0) == L'?')
								)
							{
								nTypeArray[i] = NF_SYMBOLTYPE_DIGIT;
								nPos = nPos + sStrArray[i].size();
								nCounter++;
								i++;
							}
						}
						break;
						case L'-':
						{
                            if ( bDecSep && nDecPos+1 == i &&
									nTypeArray[nDecPos] == NF_SYMBOLTYPE_DECSEP )
                            {   // "0.--"
								nTypeArray[i] = NF_SYMBOLTYPE_DIGIT;
								String& rStr = sStrArray[i];
								nPos = nPos + rStr.size();
								i++;
								nCounter++;
								while (i < nAnzStrings &&
										(sStrArray[i].at(0) == L'-') )
								{
                                    // If more than two dashes are present in
                                    // currency formats the last dash will be
                                    // interpreted literally as a minus sign.
                                    // Has to be this ugly. Period.
                                    if ( eScannedType == NUMBERFORMAT_CURRENCY
                                            && rStr.size() >= 2 &&
                                            (i == nAnzStrings-1 ||
                                            sStrArray[i+1].at(0) != L'-') )
                                        break;
									rStr += sStrArray[i];
									nPos = nPos + sStrArray[i].size();
									nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
									nAnzResStrings--;
									nCounter++;
									i++;
								}
							}
							else
							{
								nTypeArray[i] = NF_SYMBOLTYPE_STRING;
								nPos = nPos + sStrArray[i].size();
								i++;
							}
						}
						break;
						case L'.':
						case L',':
						case L'\'':
						case L' ':
						{
							sal_Unicode cSep = cHere;	// remember
							if ( StringEqualsChar( sOldThousandSep, cSep ) )
							{
								// previous char with skip empty
								sal_Unicode cPre = PreviousChar(i);
								sal_Unicode cNext;
								if (bExp || bBlank || bFrac)
								{	// after E, / or ' '
									if ( !StringEqualsChar( sOldThousandSep, L' ' ) )
									{
										nPos = nPos + sStrArray[i].size();
										nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
										nAnzResStrings--;
										i++; 				// eat it
									}
									else
										nTypeArray[i] = NF_SYMBOLTYPE_STRING;
								}
								else if (i > 0 && i < nAnzStrings-1   &&
									(cPre == L'#' || cPre == L'0')      &&
									((cNext = NextChar(i)) == L'#' || cNext == L'0')
									)					// #,#
								{
									nPos = nPos + sStrArray[i].size();
									if (!bThousand)					// only once
                                    {
										bThousand = true;
										cThousandFill = sStrArray[i+1].at(0);
									}
                                    // Eat it, will be reinserted at proper
                                    // grouping positions further down.
                                    nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
                                    nAnzResStrings--;
									i++;
								}
								else if (i > 0 && (cPre == L'#' || cPre == L'0')
									&& PreviousType(i) == NF_SYMBOLTYPE_DIGIT
									&& nThousand < FLAG_STANDARD_IN_FORMAT )
								{									// #,,,,
									if ( StringEqualsChar( sOldThousandSep, L' ' ) )
									{	// strange, those French..
										bool bFirst = true;
										String& rStr = sStrArray[i];
                                        //  set a hard Non-Breaking Space or ConvertMode
                                        const String& rSepF = pFormatter->GetNumThousandSep();
										while ( i < nAnzStrings
											&& sStrArray[i] == sOldThousandSep
											&& StringEqualsChar( sOldThousandSep, NextChar(i) ) )
										{	// last was a space or another space
											// is following => separator
											nPos = nPos + sStrArray[i].size();
											if ( bFirst )
											{
												bFirst = false;
												rStr = rSepF;
												nTypeArray[i] = NF_SYMBOLTYPE_THSEP;
											}
											else
											{
												rStr += rSepF;
												nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
												nAnzResStrings--;
											}
											nThousand++;
											i++;
										}
										if ( i < nAnzStrings-1
											&& sStrArray[i] == sOldThousandSep )
										{	// something following last space
											// => space if currency contained,
											// else separator
											nPos = nPos + sStrArray[i].size();
											if ( (nPos <= nCurrPos &&
													nCurrPos < nPos + sStrArray[i+1].size())
												|| nTypeArray[i+1] == NF_KEY_CCC
												|| (i < nAnzStrings-2 &&
												sStrArray[i+1].at(0) == L'[' &&
												sStrArray[i+2].at(0) == L'$') )
											{
												nTypeArray[i] = NF_SYMBOLTYPE_STRING;
											}
											else
											{
												if ( bFirst )
												{
													bFirst = false;
													rStr = rSepF;
													nTypeArray[i] = NF_SYMBOLTYPE_THSEP;
												}
												else
												{
													rStr += rSepF;
													nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
													nAnzResStrings--;
												}
												nThousand++;
											}
											i++;
										}
									}
									else
									{
                                        do
                                        {
                                            nThousand++;
                                            nTypeArray[i] = NF_SYMBOLTYPE_THSEP;
                                            nPos = nPos + sStrArray[i].size();
                                            sStrArray[i] = pFormatter->GetNumThousandSep();
                                            i++;
                                        } while (i < nAnzStrings &&
                                                sStrArray[i] == sOldThousandSep);
									}
								}
								else 					// any grsep
								{
									nTypeArray[i] = NF_SYMBOLTYPE_STRING;
									String& rStr = sStrArray[i];
									nPos = nPos + rStr.size();
									i++;
									while ( i < nAnzStrings &&
										sStrArray[i] == sOldThousandSep )
									{
										rStr += sStrArray[i];
										nPos = nPos + sStrArray[i].size();
										nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
										nAnzResStrings--;
										i++;
									}
								}
							}
							else if ( StringEqualsChar( sOldDecSep, cSep ) )
							{
								if (bBlank || bFrac)    // . behind / or ' '
									return nPos;		// error
								else if (bExp)			// behind E
								{
									nPos = nPos + sStrArray[i].size();
									nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
									nAnzResStrings--;
									i++; 				// eat it
								}
								else if (bDecSep)		// any .
								{
									nTypeArray[i] = NF_SYMBOLTYPE_STRING;
									String& rStr = sStrArray[i];
									nPos = nPos + rStr.size();
									i++;
									while ( i < nAnzStrings &&
										sStrArray[i] == sOldDecSep )
									{
										rStr += sStrArray[i];
										nPos = nPos + sStrArray[i].size();
										nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
										nAnzResStrings--;
										i++;
									}
								}
								else
								{
									nPos = nPos + sStrArray[i].size();
									nTypeArray[i] = NF_SYMBOLTYPE_DECSEP;
                                    sStrArray[i] = pFormatter->GetNumDecimalSep();
									bDecSep = true;
									nDecPos = i;
									nCntPre = nCounter;
									nCounter = 0;

									i++;
								}
							} 							// of else = DecSep
							else						// . without meaning
							{
								if (cSep == L' ' &&
									eScannedType == NUMBERFORMAT_FRACTION &&
									StringEqualsChar( sStrArray[i], L' ' ) )
								{
									if (!bBlank && !bFrac)	// no dups
									{	                    // or behind /
										if (bDecSep && nCounter > 0)// dec.
											return nPos;			// error
										bBlank = true;
										nBlankPos = i;
										nCntPre = nCounter;
										nCounter = 0;
									}
									nTypeArray[i] = NF_SYMBOLTYPE_STRING;
									nPos = nPos + sStrArray[i].size();
								}
								else
								{
									nTypeArray[i] = NF_SYMBOLTYPE_STRING;
									String& rStr = sStrArray[i];
									nPos = nPos + rStr.size();
									i++;
									while (i < nAnzStrings &&
										StringEqualsChar( sStrArray[i], cSep ) )
									{
										rStr += sStrArray[i];
										nPos = nPos + sStrArray[i].size();
										nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
										nAnzResStrings--;
										i++;
									}
								}
							}
						}
						break;
						case L'/':
						{
							if (eScannedType == NUMBERFORMAT_FRACTION)
							{
								if ( i == 0 ||
										(nTypeArray[i-1] != NF_SYMBOLTYPE_DIGIT &&
									 	nTypeArray[i-1] != NF_SYMBOLTYPE_EMPTY) )
									return nPos ? nPos : 1;	// /? not allowed
								else if (!bFrac || (bDecSep && nCounter > 0))
								{
									bFrac = true;
									nCntPost = nCounter;
									nCounter = 0;
									nTypeArray[i] = NF_SYMBOLTYPE_FRAC;
									nPos = nPos + sStrArray[i].size();
									i++;
								}
								else 				// / double or , in count
									return nPos;	// error
							}
							else
							{
								nTypeArray[i] = NF_SYMBOLTYPE_STRING;
								nPos = nPos + sStrArray[i].size();
								i++;
							}
						}
						break;
						case L'[' :
						{
							if ( eScannedType == NUMBERFORMAT_CURRENCY &&
									i < nAnzStrings-1 &&
									nTypeArray[i+1] == NF_SYMBOLTYPE_STRING &&
									sStrArray[i+1].at(0) == L'$' )
							{	// [$DM-xxx]
								// ab SV_NUMBERFORMATTER_VERSION_NEW_CURR
								nPos = nPos + sStrArray[i].size();			// [
								nTypeArray[i] = NF_SYMBOLTYPE_CURRDEL;
								nPos = nPos + sStrArray[++i].size();		// $
								sStrArray[i-1] += sStrArray[i];		// [$
								nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
								nAnzResStrings--;
								if ( ++i >= nAnzStrings )
									return nPos;		// error
								nPos = nPos + sStrArray[i].size();			// DM
								String& rStr = sStrArray[i];
								String* pStr = &sStrArray[i];
								nTypeArray[i] = NF_SYMBOLTYPE_CURRENCY;	// wandeln
								bool bHadDash = false;
								i++;
								while ( i < nAnzStrings &&
										sStrArray[i].at(0) != L']' )
								{
									nPos = nPos + sStrArray[i].size();
									if ( bHadDash )
									{
										*pStr += sStrArray[i];
										nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
										nAnzResStrings--;
									}
									else
									{
										if ( sStrArray[i].at(0) == L'-' )
										{
											bHadDash = true;
											pStr = &sStrArray[i];
											nTypeArray[i] = NF_SYMBOLTYPE_CURREXT;
										}
										else
										{
											*pStr += sStrArray[i];
											nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
											nAnzResStrings--;
										}
									}
									i++;
								}
								if ( rStr.size() && i < nAnzStrings &&
										sStrArray[i].at(0) == L']' )
								{
									nTypeArray[i] = NF_SYMBOLTYPE_CURRDEL;
									nPos = nPos + sStrArray[i].size();
									i++;
								}
								else
									return nPos;		// error
							}
							else
							{
								nTypeArray[i] = NF_SYMBOLTYPE_STRING;
								nPos = nPos + sStrArray[i].size();
								i++;
							}
						}
						break;
						default:					// Others Dels
						{
                            if (eScannedType == NUMBERFORMAT_PERCENT &&
                                    cHere == L'%')
                                nTypeArray[i] = NF_SYMBOLTYPE_PERCENT;
                            else
                                nTypeArray[i] = NF_SYMBOLTYPE_STRING;
							nPos = nPos + sStrArray[i].size();
							i++;
						}
						break;
					}								// of switch (Del)
				}									// of else Del
				else if ( nTypeArray[i] == NF_SYMBOLTYPE_COMMENT )
				{
					String& rStr = sStrArray[i];
					nPos = nPos + rStr.size();
					SvNumberformat::EraseCommentBraces( rStr );
					rComment += rStr;
					nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
					nAnzResStrings--;
					i++;
				}
				else if (eScannedType == NUMBERFORMAT_FRACTION && nTypeArray[i] == NF_SYMBOLTYPE_DIGIT)	// added part
				{
					String& rStr = sStrArray[i];
					String digit_str = rStr;
					nPos = nPos + rStr.size();
					i++;
					nCounter++;
					while (i < nAnzStrings && (nTypeArray[i] == NF_SYMBOLTYPE_DIGIT || 
						sStrArray[i].at(0) == L'0'))
					{
						nTypeArray[i] = NF_SYMBOLTYPE_DIGIT;
						digit_str += sStrArray[i];
						nPos = nPos + sStrArray[i].size();
						nCounter++;
						i++;
					}
					if (digit_str.size() > 0) nExpVal = std::stoi(digit_str);
				}
				else
				{
					//DBG_ERRORFILE( "unknown NF_SYMBOLTYPE_..." );
					nPos = nPos + sStrArray[i].size();
					i++;
				}
			}                                  		// of while
			if (eScannedType == NUMBERFORMAT_FRACTION)
			{
				if (bFrac)
					nCntExp = nCounter;
				else if (bBlank)
					nCntPost = nCounter;
				else
					nCntPre = nCounter;
			}
			else
			{
				if (bExp)
					nCntExp = nCounter;
				else if (bDecSep)
					nCntPost = nCounter;
				else
					nCntPre = nCounter;
			}
			if (bThousand)                          // Expansion of grouping separators
			{
				sal_uInt16 nMaxPos;
				if (bFrac)
				{
					if (bBlank)
						nMaxPos = nBlankPos;
					else
						nMaxPos = 0;				// no grouping
				}
				else if (bDecSep)					// decimal separator present
					nMaxPos = nDecPos;
				else if (bExp)						// 'E' exponent present
					nMaxPos = nExpPos;
				else								// up to end
					nMaxPos = i;
                // Insert separators at proper positions.
                uint16_t nCount = 0;
				DigitGroupingIterator aGrouping(pFormatter->getDigitGrouping());
                size_t nFirstDigitSymbol = nMaxPos;
                size_t nFirstGroupingSymbol = nMaxPos;
                i = nMaxPos;
                while (i-- > 0)
                {
                    if (nTypeArray[i] == NF_SYMBOLTYPE_DIGIT)
                    {
                        nFirstDigitSymbol = i;
                        nCount = nCount + sStrArray[i].size();   // MSC converts += to int and then warns, so ...
                        // Insert separator only if not leftmost symbol.
                        if (i > 0 && nCount >= aGrouping.getPos())
                        {
                            if (!InsertSymbol( i, NF_SYMBOLTYPE_THSEP, pFormatter->GetNumThousandSep()))
                                // nPos isn't correct here, but signals error
                                return nPos;
                            // i may have been decremented by 1
                            nFirstDigitSymbol = i + 1;
                            nFirstGroupingSymbol = i;
							aGrouping.advance();
                        }
                    }
                }
                // Generated something like "string",000; remove separator again.
                if (nFirstGroupingSymbol < nFirstDigitSymbol)
                {
                    nTypeArray[nFirstGroupingSymbol] = NF_SYMBOLTYPE_EMPTY;
                    nAnzResStrings--;
                }
            }
            // Combine digits into groups to save memory (Info will be copied
            // later, taking only non-empty symbols).
            for (i = 0; i < nAnzStrings; ++i)
            {
                if (nTypeArray[i] == NF_SYMBOLTYPE_DIGIT)
                {
                    String& rStr = sStrArray[i];
                    while (++i < nAnzStrings &&
                            nTypeArray[i] == NF_SYMBOLTYPE_DIGIT)
                    {
                        rStr += sStrArray[i];
                        nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
                        nAnzResStrings--;
                    }
                }
            }
		}
		break;										// of NUMBERFORMAT_NUMBER
		case NUMBERFORMAT_DATE:
		{
			while (i < nAnzStrings)
			{
				switch (nTypeArray[i])
				{
					case NF_SYMBOLTYPE_BLANK:
					case NF_SYMBOLTYPE_STAR:
					case NF_SYMBOLTYPE_STRING:
						nPos = nPos + sStrArray[i].size();
						i++;
					break;
					case NF_SYMBOLTYPE_COMMENT:
					{
						String& rStr = sStrArray[i];
						nPos = nPos + rStr.size();
						SvNumberformat::EraseCommentBraces( rStr );
						rComment += rStr;
						nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
						nAnzResStrings--;
						i++;
					}
					break;
					case NF_SYMBOLTYPE_DEL:
					{
						int nCalRet;
						if (sStrArray[i] == sOldDateSep)
						{
							nTypeArray[i] = NF_SYMBOLTYPE_DATESEP;
							nPos = nPos + sStrArray[i].size();
                            if (bConvertMode)
                                sStrArray[i] = pFormatter->GetDateSep();
							i++;
						}
						else if ( (nCalRet = FinalScanGetCalendar( nPos, i, nAnzResStrings )) != 0 )
						{
							if ( nCalRet < 0  )
								return nPos;		// error
						}
						else
						{
							nTypeArray[i] = NF_SYMBOLTYPE_STRING;
							nPos = nPos + sStrArray[i].size();
							i++;
						}
					}
					break;
                    case NF_KEY_THAI_T :
                        bThaiT = true;
                        // fall thru
					case NF_KEY_M:							// M
					case NF_KEY_MM:							// MM
					case NF_KEY_MMM:						// MMM
					case NF_KEY_MMMM:						// MMMM
					case NF_KEY_MMMMM:						// MMMMM
					case NF_KEY_Q:							// Q
					case NF_KEY_QQ:							// QQ
					case NF_KEY_D:							// D
					case NF_KEY_DD:							// DD
					case NF_KEY_DDD:						// DDD
					case NF_KEY_DDDD:						// DDDD
					case NF_KEY_YY:							// YY
					case NF_KEY_YYYY:						// YYYY
					case NF_KEY_NN:							// NN
					case NF_KEY_NNN:						// NNN
					case NF_KEY_NNNN:						// NNNN
					case NF_KEY_WW :						// WW
					case NF_KEY_AAA :						// AAA
					case NF_KEY_AAAA :						// AAAA
					case NF_KEY_EC :						// E
					case NF_KEY_EEC :						// EE
					case NF_KEY_G :							// G
					case NF_KEY_GG :						// GG
					case NF_KEY_GGG :						// GGG
					case NF_KEY_R :							// R
					case NF_KEY_RR :						// RR
						sStrArray[i] = sKeyword[nTypeArray[i]];	// tTtT -> TTTT
						nPos = nPos + sStrArray[i].size();
						i++;
					break;
					default:							// andere Keywords
						nTypeArray[i] = NF_SYMBOLTYPE_STRING;
						nPos = nPos + sStrArray[i].size();
						i++;
					break;
				}
			}										// of while
		}
		break;										// of NUMBERFORMAT_DATE
		case NUMBERFORMAT_TIME:
		{
			while (i < nAnzStrings)
			{
				switch (nTypeArray[i])
				{
					case NF_SYMBOLTYPE_BLANK:
					case NF_SYMBOLTYPE_STAR:
					{
						nPos = nPos + sStrArray[i].size();
						i++;
					}
					break;
					case NF_SYMBOLTYPE_DEL:
					{
						switch( sStrArray[i].at(0) )
						{
							case L'0':
							{
                                if ( Is100SecZero( i, bDecSep ) )
								{
                                    bDecSep = true;
									nTypeArray[i] = NF_SYMBOLTYPE_DIGIT;
									String& rStr = sStrArray[i];
									i++;
									nPos = nPos + sStrArray[i].size();
									nCounter++;
									while (i < nAnzStrings &&
										   sStrArray[i].at(0) == L'0')
									{
										rStr += sStrArray[i];
										nPos = nPos + sStrArray[i].size();
										nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
										nAnzResStrings--;
										nCounter++;
										i++;
									}
								}
								else
									return nPos;
							}
							break;
							case L'#':
							case L'?':
								return nPos;
							case L'[':
							{
								if (bThousand)				// double
									return nPos;
								bThousand = true;			// at time free
								sal_Unicode cChar = std::towupper(NextChar(i));
								if ( cChar == cOldKeyH )
									nThousand = 1;		// H
								else if ( cChar == cOldKeyMI )
									nThousand = 2;		// M
								else if ( cChar == cOldKeyS )
									nThousand = 3;		// S
								else
									return nPos;
								nPos = nPos + sStrArray[i].size();
								i++;
							}
                            break;
							case L']':
							{
								if (!bThousand)				// no [ before
									return nPos;
								nPos = nPos + sStrArray[i].size();
								i++;
							}
							break;
							default:
							{
								nPos = nPos + sStrArray[i].size();
                                if ( sStrArray[i] == sOldTimeSep )
                                {
                                    nTypeArray[i] = NF_SYMBOLTYPE_TIMESEP;
                                    if ( bConvertMode )
                                        sStrArray[i] = pFormatter->getTimeSep();
                                }
                                else if ( sStrArray[i] == sOldTime100SecSep )
                                {
                                    bDecSep = true;
                                    nTypeArray[i] = NF_SYMBOLTYPE_TIME100SECSEP;
                                    if ( bConvertMode )
                                        sStrArray[i] = pFormatter->getTime100SecSep();
                                }
                                else
                                    nTypeArray[i] = NF_SYMBOLTYPE_STRING;
								i++;
							}
							break;
						}
					}
					break;
					case NF_SYMBOLTYPE_STRING:
					{
						nPos = nPos + sStrArray[i].size();
						i++;
					}
					break;
					case NF_SYMBOLTYPE_COMMENT:
					{
						String& rStr = sStrArray[i];
						nPos = nPos + rStr.size();
						SvNumberformat::EraseCommentBraces( rStr );
						rComment += rStr;
						nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
						nAnzResStrings--;
						i++;
					}
					break;
					case NF_KEY_AMPM:						// AM/PM
					case NF_KEY_AP:							// A/P
					{
						bExp = true;					// abused for A/P
						sStrArray[i] = sKeyword[nTypeArray[i]];	// tTtT -> TTTT
						nPos = nPos + sStrArray[i].size();
						i++;
					}
					break;
                    case NF_KEY_THAI_T :
                        bThaiT = true;
                        // fall thru
					case NF_KEY_MI:							// M
					case NF_KEY_MMI:						// MM
					case NF_KEY_H:							// H
					case NF_KEY_HH:							// HH
					case NF_KEY_S:							// S
					case NF_KEY_SS:							// SS
					{
						sStrArray[i] = sKeyword[nTypeArray[i]];	// tTtT -> TTTT
						nPos = nPos + sStrArray[i].size();
						i++;
					}
					break;
					default:							// andere Keywords
					{
						nTypeArray[i] = NF_SYMBOLTYPE_STRING;
						nPos = nPos + sStrArray[i].size();
						i++;
					}
					break;
				}
			}                   					// of while
			nCntPost = nCounter;					// counter of zeros
			if (bExp)
				nCntExp = 1;						// notes AM/PM
		}
		break;										// of NUMBERFORMAT_TIME
		case NUMBERFORMAT_DATETIME:
		{
            bool bTimePart = false;
			while (i < nAnzStrings)
			{
				switch (nTypeArray[i])
				{
					case NF_SYMBOLTYPE_BLANK:
					case NF_SYMBOLTYPE_STAR:
					case NF_SYMBOLTYPE_STRING:
						nPos = nPos + sStrArray[i].size();
						i++;
					break;
					case NF_SYMBOLTYPE_COMMENT:
					{
						String& rStr = sStrArray[i];
						nPos = nPos + rStr.size();
						SvNumberformat::EraseCommentBraces( rStr );
						rComment += rStr;
						nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
						nAnzResStrings--;
						i++;
					}
					break;
					case NF_SYMBOLTYPE_DEL:
					{
						int nCalRet;
                        if ( (nCalRet = FinalScanGetCalendar( nPos, i, nAnzResStrings )) != 0 )
						{
							if ( nCalRet < 0  )
								return nPos;		// error
						}
						else
						{
                            switch( sStrArray[i].at(0) )
                            {
                                case L'0':
                                {
                                    if ( bTimePart && Is100SecZero( i, bDecSep ) )
                                    {
                                        bDecSep = true;
                                        nTypeArray[i] = NF_SYMBOLTYPE_DIGIT;
                                        String& rStr = sStrArray[i];
                                        i++;
                                        nPos = nPos + sStrArray[i].size();
                                        nCounter++;
                                        while (i < nAnzStrings &&
                                            sStrArray[i].at(0) == L'0')
                                        {
                                            rStr += sStrArray[i];
                                            nPos = nPos + sStrArray[i].size();
                                            nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
                                            nAnzResStrings--;
                                            nCounter++;
                                            i++;
                                        }
                                    }
                                    else
                                        return nPos;
                                }
                                break;
                                case L'#':
                                case L'?':
                                    return nPos;
                                default:
                                {
                                    nPos = nPos + sStrArray[i].size();
                                    if (bTimePart)
                                    {
                                        if ( sStrArray[i] == sOldTimeSep )
                                        {
                                            nTypeArray[i] = NF_SYMBOLTYPE_TIMESEP;
                                            if ( bConvertMode )
                                                sStrArray[i] = pFormatter->getTimeSep();
                                        }
                                        else if ( sStrArray[i] == sOldTime100SecSep )
                                        {
                                            bDecSep = true;
                                            nTypeArray[i] = NF_SYMBOLTYPE_TIME100SECSEP;
                                            if ( bConvertMode )
                                                sStrArray[i] = pFormatter->getTime100SecSep();
                                        }
                                        else
                                            nTypeArray[i] = NF_SYMBOLTYPE_STRING;
                                    }
                                    else
                                    {
                                        if ( sStrArray[i] == sOldDateSep )
                                        {
                                            nTypeArray[i] = NF_SYMBOLTYPE_DATESEP;
                                            if (bConvertMode)
                                                sStrArray[i] = pFormatter->GetDateSep();
                                        }
                                        else
                                            nTypeArray[i] = NF_SYMBOLTYPE_STRING;
                                    }
                                    i++;
                                }
                            }
						}
					}
					break;
					case NF_KEY_AMPM:						// AM/PM
					case NF_KEY_AP:							// A/P
					{
                        bTimePart = true;
						bExp = true;					// abused for A/P
						sStrArray[i] = sKeyword[nTypeArray[i]];	// tTtT -> TTTT
						nPos = nPos + sStrArray[i].size();
						i++;
					}
					break;
					case NF_KEY_MI:							// M
					case NF_KEY_MMI:						// MM
					case NF_KEY_H:							// H
					case NF_KEY_HH:							// HH
					case NF_KEY_S:							// S
					case NF_KEY_SS:							// SS
                        bTimePart = true;
						sStrArray[i] = sKeyword[nTypeArray[i]];	// tTtT -> TTTT
						nPos = nPos + sStrArray[i].size();
						i++;
					break;
					case NF_KEY_M:							// M
					case NF_KEY_MM:							// MM
					case NF_KEY_MMM:						// MMM
					case NF_KEY_MMMM:						// MMMM
					case NF_KEY_MMMMM:						// MMMMM
					case NF_KEY_Q:							// Q
					case NF_KEY_QQ:							// QQ
					case NF_KEY_D:							// D
					case NF_KEY_DD:							// DD
					case NF_KEY_DDD:						// DDD
					case NF_KEY_DDDD:						// DDDD
					case NF_KEY_YY:							// YY
					case NF_KEY_YYYY:						// YYYY
					case NF_KEY_NN:							// NN
					case NF_KEY_NNN:						// NNN
					case NF_KEY_NNNN:						// NNNN
					case NF_KEY_WW :						// WW
					case NF_KEY_AAA :						// AAA
					case NF_KEY_AAAA :						// AAAA
					case NF_KEY_EC :						// E
					case NF_KEY_EEC :						// EE
					case NF_KEY_G :							// G
					case NF_KEY_GG :						// GG
					case NF_KEY_GGG :						// GGG
					case NF_KEY_R :							// R
					case NF_KEY_RR :						// RR
                        bTimePart = false;
						sStrArray[i] = sKeyword[nTypeArray[i]];	// tTtT -> TTTT
						nPos = nPos + sStrArray[i].size();
						i++;
					break;
                    case NF_KEY_THAI_T :
                        bThaiT = true;
						sStrArray[i] = sKeyword[nTypeArray[i]];
						nPos = nPos + sStrArray[i].size();
						i++;
					break;
					default:							// andere Keywords
						nTypeArray[i] = NF_SYMBOLTYPE_STRING;
						nPos = nPos + sStrArray[i].size();
						i++;
					break;
				}
			}										// of while
            nCntPost = nCounter;                    // decimals (100th seconds)
			if (bExp)
				nCntExp = 1;						// notes AM/PM
		}
		break;										// of NUMBERFORMAT_DATETIME
		default:
		break;
	}
	if (eScannedType == NUMBERFORMAT_SCIENTIFIC &&
		(nCntPre + nCntPost == 0 || nCntExp == 0))
		return nPos;
	else if (eScannedType == NUMBERFORMAT_FRACTION && (nCntExp > 8 || nCntExp == 0))
		return nPos;

    if (bThaiT && !GetNatNumModifier())
        SetNatNumModifier(1);

	if ( bConvertMode )
	{	// strings containing keywords of the target locale must be quoted, so
		// the user sees the difference and is able to edit the format string
		for ( i=0; i < nAnzStrings; i++ )
		{
			if ( nTypeArray[i] == NF_SYMBOLTYPE_STRING &&
					sStrArray[i].at(0) != L'\"' )
			{
				if ( bConvertSystemToSystem && eScannedType == NUMBERFORMAT_CURRENCY )
				{	// don't stringize automatic currency, will be converted
                    if ( sStrArray[i] == sOldCurSymbol )
						continue;	// for
					// DM might be splitted into D and M
                    if ( sStrArray[i].size() < sOldCurSymbol.size() && 
						std::towupper(sStrArray[i].at(0)) == sOldCurString.at(0) )
					{
						String aTmp( sStrArray[i] );
						sal_uInt16 j = i + 1;
                        while ( aTmp.size() < sOldCurSymbol.size() &&
								j < nAnzStrings &&
								nTypeArray[j] == NF_SYMBOLTYPE_STRING )
						{
							aTmp += sStrArray[j++];
						}
						String str = aTmp; ConvertToUpper(str);
						if (str == sOldCurString )
						{
							sStrArray[i++] = aTmp;
							for ( ; i<j; i++ )
							{
								nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
								nAnzResStrings--;
							}
							i = j - 1;
							continue;	// for
						}
					}
				}
				String& rStr = sStrArray[i];
				uint16_t nLen = rStr.size();
				for ( uint16_t j=0; j<nLen; j++ )
				{
					if ( (j == 0 || rStr.at(j-1) != L'\\') && GetKeyWord( rStr, j ) )
					{
						rStr.insert( 0, L"\"" );
						rStr += L'\"';
						break;	// for
					}
				}
			}
		}
	}
	// concatenate strings, remove quotes for output, and rebuild the format string
	rString.erase();
	i = 0;
	while (i < nAnzStrings)
	{
		switch ( nTypeArray[i] )
		{
			case NF_SYMBOLTYPE_STRING :
			{
				uint16_t nStringPos = rString.size();
				uint16_t nArrPos = 0;
				sal_uInt16 iPos = i;
				do
				{
                    if (sStrArray[i].size() == 2 &&
                            sStrArray[i].at(0) == L'\\')
                    {
                        // Unescape some simple forms of symbols even in the UI
                        // visible string to prevent duplicates that differ
                        // only in notation, originating from import.
                        // e.g. YYYY-MM-DD and YYYY\-MM\-DD are identical,
                        // but 0\ 000 0 and 0 000 0 in a French locale are not.
                        sal_Unicode c = sStrArray[i].at(1);
                        switch (c)
                        {
                            case L'+':
                            case L'-':
                                rString += c;
                                break;
                            case L' ':
                            case L'.':
                            case L'/':
                                if (((eScannedType & NUMBERFORMAT_DATE) == 0)
                                        && (StringEqualsChar(
                                                pFormatter->GetNumThousandSep(),
                                                c) || StringEqualsChar(
                                                    pFormatter->GetNumDecimalSep(),
                                                    c) || (c == L' ' &&
                                                        StringEqualsChar(
                                                            pFormatter->GetNumThousandSep(),
                                                            cNonBreakingSpace))))
                                    rString += sStrArray[i];
                                else if ((eScannedType & NUMBERFORMAT_DATE) &&
                                        StringEqualsChar(
                                            pFormatter->GetDateSep(), c))
                                    rString += sStrArray[i];
                                else if ((eScannedType & NUMBERFORMAT_TIME) &&
                                        (StringEqualsChar(pFormatter->getTimeSep(),
                                                           c) ||
                                         StringEqualsChar(
											 pFormatter->getTime100SecSep(), c)))
                                    rString += sStrArray[i];
                                else if (eScannedType & NUMBERFORMAT_FRACTION)
                                    rString += sStrArray[i];
                                else
                                    rString += c;
                                break;
                            default:
                                rString += sStrArray[i];
                        }
                    }
                    else
                        rString += sStrArray[i];
					if ( RemoveQuotes( sStrArray[i] ) > 0 )
					{	// update currency up to quoted string
						if ( eScannedType == NUMBERFORMAT_CURRENCY )
						{	// dM -> DM  or  DM -> $  in old automatic
							// currency formats, oh my ..., why did we ever
							// introduce them?
							String aTmp = sStrArray[iPos].substr(nArrPos);
							ConvertToUpper(aTmp);
							uint16_t nCPos = aTmp.find( sOldCurString );
							if ( nCPos != STRING_NOTFOUND )
							{
								const String& rCur =
									bConvertMode && bConvertSystemToSystem ?
                                    GetCurSymbol() : sOldCurSymbol;
								sStrArray[iPos].replace( nArrPos+nCPos,	sOldCurString.size(), rCur );
								rString.replace( nStringPos+nCPos, sOldCurString.size(), rCur );
							}
							nStringPos = rString.size();
							if ( iPos == i )
								nArrPos = sStrArray[iPos].size();
							else
								nArrPos = sStrArray[iPos].size() + sStrArray[i].size();
						}
					}
					if ( iPos != i )
					{
						sStrArray[iPos] += sStrArray[i];
						nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
						nAnzResStrings--;
					}
					i++;
				} while ( i < nAnzStrings && nTypeArray[i] == NF_SYMBOLTYPE_STRING );
				if ( i < nAnzStrings )
					i--;	// enter switch on next symbol again
				if ( eScannedType == NUMBERFORMAT_CURRENCY && nStringPos < rString.size() )
				{	// same as above, since last RemoveQuotes
					String aTmp = sStrArray[iPos].substr(nArrPos);
					ConvertToUpper(aTmp);
					uint16_t nCPos = aTmp.find( sOldCurString );
					if ( nCPos != STRING_NOTFOUND )
					{
						const String& rCur =
							bConvertMode && bConvertSystemToSystem ?
                            GetCurSymbol() : sOldCurSymbol;
						sStrArray[iPos].replace( nArrPos+nCPos,	sOldCurString.size(), rCur );
						rString.replace( nStringPos+nCPos, sOldCurString.size(), rCur );
					}
				}
			}
			break;
			case NF_SYMBOLTYPE_CURRENCY :
			{
				rString += sStrArray[i];
				RemoveQuotes( sStrArray[i] );
			}
			break;
            case NF_KEY_THAI_T:
                if (bThaiT && GetNatNumModifier() == 1)
                {   // Remove T from format code, will be replaced with a [NatNum1] prefix.
                    nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
                    nAnzResStrings--;
                }
                else
                    rString += sStrArray[i];
            break;
			case NF_SYMBOLTYPE_EMPTY :
				// nothing
			break;
			default:
				rString += sStrArray[i];
		}
		i++;
	}
	return 0;
}


uint16_t ImpSvNumberformatScan::RemoveQuotes( String& rStr )
{
	if ( rStr.size() > 1 )
	{
		sal_Unicode c = rStr.at(0);
		uint16_t n;
		if ( c == L'"' && rStr.at( (n = uint16_t(rStr.size()-1)) ) == L'"' )
		{
			rStr.erase(n,1);
			rStr.erase(0,1);
			return 2;
		}
		else if ( c == L'\\' )
		{
			rStr.erase(0,1);
			return 1;
		}
	}
	return 0;
}


uint16_t ImpSvNumberformatScan::ScanFormat( String& rString, String& rComment )
{
	uint16_t res = Symbol_Division(rString);	//lexical analysis
	if (!res)
		res = ScanType(rString);            // Format type detection
	if (!res)
		res = FinalScan( rString, rComment );	// Type-dependent End Analysis
	return res;								// res = Control position
											// res = 0 => Format ok
}

void ImpSvNumberformatScan::CopyInfo(ImpSvNumberformatInfo* pInfo, sal_uInt16 nAnz)
{
	size_t i,j;
	j = 0;
	i = 0;
	while (i < nAnz && j < NF_MAX_FORMAT_SYMBOLS)
	{
		if (nTypeArray[j] != NF_SYMBOLTYPE_EMPTY)
		{
			pInfo->sStrArray[i]  = sStrArray[j];
			pInfo->nTypeArray[i] = nTypeArray[j];
			i++;
		}
		j++;
	}
	pInfo->eScannedType = eScannedType;
	pInfo->bThousand    = bThousand;
	pInfo->nThousand    = nThousand;
	pInfo->nCntPre      = nCntPre;
	pInfo->nCntPost     = nCntPost;
	pInfo->nCntExp      = nCntExp;
	pInfo->nExpVal      = nExpVal;
}


// ----------------------------------------------- zformat.cxx ----------------------------------------------------------

const sal_uInt16 UPPER_PRECISION = 300; // entirely arbitrary...
const double EXP_LOWER_BOUND = 1.0E-4; // prefer scientific notation below this value.

const double _D_MAX_U_LONG_ = (double) 0xffffffff;      // 4294967295.0
const double _D_MAX_LONG_   = (double) 0x7fffffff;      // 2147483647.0
const sal_uInt16 _MAX_FRACTION_PREC = 3;
const double D_EPS = 1.0E-2;

const double _D_MAX_D_BY_100  = 1.7E306;
const double _D_MIN_M_BY_1000 = 2.3E-305;

const sal_uInt16 UNLIMITED_PRECISION = (sal_uInt16)0xffff;
const sal_uInt16 INPUTSTRING_PRECISION = (sal_uInt16)0xfffe;

static sal_uInt8 cCharWidths[ 128-32 ] = {
    1,1,1,2,2,3,2,1,1,1,1,2,1,1,1,1,
    2,2,2,2,2,2,2,2,2,2,1,1,2,2,2,2,
    3,2,2,2,2,2,2,3,2,1,2,2,2,3,3,3,
    2,3,2,2,2,2,2,3,2,2,2,1,1,1,2,2,
    1,2,2,2,2,2,1,2,2,1,1,2,1,3,2,2,
    2,2,1,2,1,2,2,2,2,2,2,1,1,1,2,1
};

static int const n10Count = 16;
static double const n10s[2][n10Count] = {
	{ 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8,
	  1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16 },
	{ 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8,
	  1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16 }
};
double const nKorrVal[] = {
	0, 9e-1, 9e-2, 9e-3, 9e-4, 9e-5, 9e-6, 9e-7, 9e-8,
	9e-9, 9e-10, 9e-11, 9e-12, 9e-13, 9e-14, 9e-15
};

// return pow(10.0,nExp) optimized for exponents in the interval [-16,16]
static double getN10Exp(int nExp)
{
	if (nExp < 0)
	{
		if (-nExp <= n10Count)
			return n10s[1][-nExp - 1];
		else
			return pow(10.0, static_cast<double>(nExp));
	}
	else if (nExp > 0)
	{
		if (nExp <= n10Count)
			return n10s[0][nExp - 1];
		else
			return pow(10.0, static_cast<double>(nExp));
	}
	else // ( nExp == 0 )
		return 1.0;
}

// Solaris C++ 5.2 compiler has problems when "StringT ** pResult" is
// "typename T::String ** pResult" instead:
void doubleToString(String& pResult,
	sal_Int32 * pResultCapacity, sal_Int32 nResultOffset,
	double fValue, rtl_math_StringFormat eFormat,
	sal_Int32 nDecPlaces, sal_Unicode cDecSeparator,
	sal_Int32 const * pGroups,
	sal_Unicode cGroupSeparator,
	bool bEraseTrailingDecZeros)
{
	static double const nRoundVal[] = {
		5.0e+0, 0.5e+0, 0.5e-1, 0.5e-2, 0.5e-3, 0.5e-4, 0.5e-5, 0.5e-6,
		0.5e-7, 0.5e-8, 0.5e-9, 0.5e-10,0.5e-11,0.5e-12,0.5e-13,0.5e-14
	};

	// sign adjustment, instead of testing for fValue<0.0 this will also fetch
	// -0.0
	bool bSign = fabs(fValue) < 0.0;
	if (bSign)
		fValue = -fValue;

	if (std::isnan(fValue))
	{
		pResult = L"NaN";
		return;
	}

	if (std::isinf(fValue))
	{
		if (bSign)
			pResult = L"-INF";
		else
			pResult = L"INF";
		return;
	}

	// find the exponent
	int nExp = 0;
	if (fValue > 0.0)
	{
		nExp = static_cast<int>(floor(log10(fValue)));
		fValue /= getN10Exp(nExp);
	}

	switch (eFormat)
	{
	case rtl_math_StringFormat_Automatic:
	{   // E or F depending on exponent magnitude
		int nPrec;
		if (nExp <= -15 || nExp >= 15)        // #58531# was <-16, >16
		{
			nPrec = 14;
			eFormat = rtl_math_StringFormat_E;
		}
		else
		{
			if (nExp < 14)
			{
				nPrec = 15 - nExp - 1;
				eFormat = rtl_math_StringFormat_F;
			}
			else
			{
				nPrec = 15;
				eFormat = rtl_math_StringFormat_F;
			}
		}
		if (nDecPlaces == rtl_math_DecimalPlaces_Max)
			nDecPlaces = nPrec;
	}
	break;
	case rtl_math_StringFormat_G:
	{   // G-Point, similar to sprintf %G
		if (nDecPlaces == rtl_math_DecimalPlaces_DefaultSignificance)
			nDecPlaces = 6;
		if (nExp < -4 || nExp >= nDecPlaces)
		{
			nDecPlaces = std::max< sal_Int32 >(1, nDecPlaces - 1);
			eFormat = rtl_math_StringFormat_E;
		}
		else
		{
			nDecPlaces = std::max< sal_Int32 >(0, nDecPlaces - nExp - 1);
			eFormat = rtl_math_StringFormat_F;
		}
	}
	break;
	default:
		break;
	}

	sal_Int32 nDigits = nDecPlaces + 1;

	if (eFormat == rtl_math_StringFormat_F)
		nDigits += nExp;

	// Round the number
	if (nDigits >= 0)
	{
		if ((fValue += nRoundVal[nDigits > 15 ? 15 : nDigits]) >= 10)
		{
			fValue = 1.0;
			nExp++;
			if (eFormat == rtl_math_StringFormat_F)
				nDigits++;
		}
	}

	static sal_Int32 const nBufMax = 256;
	sal_Unicode aBuf[nBufMax];
	sal_Unicode * pBuf = aBuf;
	sal_Unicode * p = pBuf;
	if (bSign)
		*p++ = static_cast<sal_Unicode>(L'-');

	bool bHasDec = false;

	int nDecPos;
	// Check for F format and number < 1
	if (eFormat == rtl_math_StringFormat_F)
	{
		if (nExp < 0)
		{
			*p++ = static_cast<sal_Unicode>(L'0');
			if (nDecPlaces > 0)
			{
				*p++ = cDecSeparator;
				bHasDec = true;
			}
			sal_Int32 i = (nDigits <= 0 ? nDecPlaces : -nExp - 1);
			while ((i--) > 0)
				*p++ = static_cast<sal_Unicode>(L'0');
			nDecPos = 0;
		}
		else
			nDecPos = nExp + 1;
	}
	else
		nDecPos = 1;

	int nGrouping = 0, nGroupSelector = 0, nGroupExceed = 0;
	if (nDecPos > 1 && pGroups && pGroups[0] && cGroupSeparator)
	{
		while (nGrouping + pGroups[nGroupSelector] < nDecPos)
		{
			nGrouping += pGroups[nGroupSelector];
			if (pGroups[nGroupSelector + 1])
			{
				if (nGrouping + pGroups[nGroupSelector + 1] >= nDecPos)
					break;  // while
				++nGroupSelector;
			}
			else if (!nGroupExceed)
				nGroupExceed = nGrouping;
		}
	}

	// print the number
	if (nDigits > 0)
	{
		for (int i = 0; ; i++)
		{
			if (i < 15)
			{
				int nDigit;
				if (nDigits - 1 == 0 && i > 0 && i < 14)
					nDigit = static_cast<int>(floor(fValue
						+ nKorrVal[15 - i]));
				else
					nDigit = static_cast<int>(fValue + 1E-15);
				if (nDigit >= 10)
				{   // after-treatment of up-rounding to the next decade
					sal_Int32 sLen = static_cast<long>(p - pBuf) - 1;
					if (sLen == -1)
					{
						p = pBuf;
						if (eFormat == rtl_math_StringFormat_F)
						{
							*p++ = static_cast<sal_Unicode>(L'1');
							*p++ = static_cast<sal_Unicode>(L'0');
						}
						else
						{
							*p++ = static_cast<sal_Unicode>(L'1');
							*p++ = cDecSeparator;
							*p++ = static_cast<sal_Unicode>(L'0');
							nExp++;
							bHasDec = true;
						}
					}
					else
					{
						for (sal_Int32 j = sLen; j >= 0; j--)
						{
							sal_Unicode cS = pBuf[j];
							if (cS != cDecSeparator)
							{
								if (cS != static_cast<sal_Unicode>(L'9'))
								{
									pBuf[j] = ++cS;
									j = -1;                 // break loop
								}
								else
								{
									pBuf[j]
										= static_cast<sal_Unicode>('0');
									if (j == 0)
									{
										if (eFormat == rtl_math_StringFormat_F)
										{   // insert '1'
											sal_Unicode * px = p++;
											while (pBuf < px)
											{
												*px = *(px - 1);
												px--;
											}
											pBuf[0] = static_cast<
												sal_Unicode>(L'1');
										}
										else
										{
											pBuf[j] = static_cast<
												sal_Unicode>(L'1');
											nExp++;
										}
									}
								}
							}
						}
						*p++ = static_cast<sal_Unicode>(L'0');
					}
					fValue = 0.0;
				}
				else
				{
					*p++ = static_cast<sal_Unicode>(
						nDigit + static_cast<sal_Unicode>(L'0'));
					fValue = (fValue - nDigit) * 10.0;
				}
			}
			else
				*p++ = static_cast<sal_Unicode>(L'0');
			if (!--nDigits)
				break;  // for
			if (nDecPos)
			{
				if (!--nDecPos)
				{
					*p++ = cDecSeparator;
					bHasDec = true;
				}
				else if (nDecPos == nGrouping)
				{
					*p++ = cGroupSeparator;
					nGrouping -= pGroups[nGroupSelector];
					if (nGroupSelector && nGrouping < nGroupExceed)
						--nGroupSelector;
				}
			}
		}
	}

	if (!bHasDec && eFormat == rtl_math_StringFormat_F)
	{   // nDecPlaces < 0 did round the value
		while (--nDecPos > 0)
		{   // fill before decimal point
			if (nDecPos == nGrouping)
			{
				*p++ = cGroupSeparator;
				nGrouping -= pGroups[nGroupSelector];
				if (nGroupSelector && nGrouping < nGroupExceed)
					--nGroupSelector;
			}
			*p++ = static_cast<sal_Unicode>(L'0');
		}
	}

	if (bEraseTrailingDecZeros && bHasDec && p > pBuf)
	{
		while (*(p - 1) == static_cast<sal_Unicode>(L'0'))
			p--;
		if (*(p - 1) == cDecSeparator)
			p--;
	}

	// Print the exponent ('E', followed by '+' or '-', followed by exactly
	// three digits).  The code in rtl_[u]str_valueOf{Float|Double} relies on
	// this format.
	if (eFormat == rtl_math_StringFormat_E)
	{
		if (p == pBuf)
			*p++ = static_cast<sal_Unicode>(L'1');
		// maybe no nDigits if nDecPlaces < 0
		*p++ = static_cast<sal_Unicode>(L'E');
		if (nExp < 0)
		{
			nExp = -nExp;
			*p++ = static_cast<sal_Unicode>(L'-');
		}
		else
			*p++ = static_cast<sal_Unicode>(L'+');
		//      if (nExp >= 100 )
		*p++ = static_cast<sal_Unicode>(
			nExp / 100 + static_cast<sal_Unicode>(L'0'));
		nExp %= 100;
		*p++ = static_cast<sal_Unicode>(
			nExp / 10 + static_cast<sal_Unicode>(L'0'));
		*p++ = static_cast<sal_Unicode>(
			nExp % 10 + static_cast<sal_Unicode>(L'0'));
	}

	if (pResultCapacity == NULL) {
		pBuf[p - pBuf] = L'\0';
		pResult = pBuf;
	}
	else if (*pResultCapacity >= 0) {
		int32_t cnt = p - pBuf;
		if (cnt > *pResultCapacity) cnt = *pResultCapacity;
		pBuf[cnt] = L'\0';
		pResult = pBuf;
	}
	else {
		pResult = L"";
	}
}

void rtl_math_doubleToUString(String& pResult,
	sal_Int32 * pResultCapacity,
	sal_Int32 nResultOffset, double fValue,
	rtl_math_StringFormat eFormat,
	sal_Int32 nDecPlaces,
	sal_Unicode cDecSeparator,
	sal_Int32 const * pGroups,
	sal_Unicode cGroupSeparator,
	bool bEraseTrailingDecZeros)
{
	doubleToString(
		pResult, pResultCapacity, nResultOffset, fValue, eFormat, nDecPlaces,
		cDecSeparator, pGroups, cGroupSeparator, bEraseTrailingDecZeros);
}

/** A wrapper around rtl_math_doubleToUString, with no grouping.
 */
inline String doubleToUString(double fValue,
	rtl_math_StringFormat eFormat,
	sal_Int32 nDecPlaces,
	sal_Unicode cDecSeparator,
	bool bEraseTrailingDecZeros = false)
{
	String aResult;
	rtl_math_doubleToUString(aResult, 0, 0, fValue, eFormat, nDecPlaces,
		cDecSeparator, 0, 0, bEraseTrailingDecZeros);
	return aResult;
}

double rtl_math_round(double fValue, int nDecPlaces,
	enum rtl_math_RoundingMode eMode)
{
	//OSL_ASSERT(nDecPlaces >= -20 && nDecPlaces <= 20);

	if (fValue == 0.0)
		return fValue;

	// sign adjustment
	bool bSign = fabs(fValue) < 0.0;
	if (bSign)
		fValue = -fValue;

	double fFac = 0;
	if (nDecPlaces != 0)
	{
		// max 20 decimals, we don't have unlimited precision
		// #38810# and no overflow on fValue*=fFac
		if (nDecPlaces < -20 || 20 < nDecPlaces || fValue > (DBL_MAX / 1e20))
			return bSign ? -fValue : fValue;

		fFac = getN10Exp(nDecPlaces);
		fValue *= fFac;
	}
	//else  //! uninitialized fFac, not needed

	switch (eMode)
	{
	case rtl_math_RoundingMode_Corrected:
	{
		int nExp;       // exponent for correction
		if (fValue > 0.0)
			nExp = static_cast<int>(floor(log10(fValue)));
		else
			nExp = 0;
		int nIndex = 15 - nExp;
		if (nIndex > 15)
			nIndex = 15;
		else if (nIndex <= 1)
			nIndex = 0;
		fValue = floor(fValue + 0.5 + nKorrVal[nIndex]);
	}
	break;
	case rtl_math_RoundingMode_Down:
		fValue = floor(fValue);
		break;
	case rtl_math_RoundingMode_Up:
		fValue = ceil(fValue);
		break;
	case rtl_math_RoundingMode_Floor:
		fValue = bSign ? ceil(fValue)
			: floor(fValue);
		break;
	case rtl_math_RoundingMode_Ceiling:
		fValue = bSign ? floor(fValue)
			: ceil(fValue);
		break;
	case rtl_math_RoundingMode_HalfDown:
	{
		double f = floor(fValue);
		fValue = ((fValue - f) <= 0.5) ? f : ceil(fValue);
	}
	break;
	case rtl_math_RoundingMode_HalfUp:
	{
		double f = floor(fValue);
		fValue = ((fValue - f) < 0.5) ? f : ceil(fValue);
	}
	break;
	case rtl_math_RoundingMode_HalfEven:
#if defined FLT_ROUNDS
		/*
			Use fast version. FLT_ROUNDS may be defined to a function by some compilers!

			DBL_EPSILON is the smallest fractional number which can be represented,
			its reciprocal is therefore the smallest number that cannot have a
			fractional part. Once you add this reciprocal to `x', its fractional part
			is stripped off. Simply subtracting the reciprocal back out returns `x'
			without its fractional component.
			Simple, clever, and elegant - thanks to Ross Cottrell, the original author,
			who placed it into public domain.

			volatile: prevent compiler from being too smart
		*/
		if (FLT_ROUNDS == 1)
		{
			volatile double x = fValue + 1.0 / DBL_EPSILON;
			fValue = x - 1.0 / DBL_EPSILON;
		}
		else
#endif // FLT_ROUNDS
		{
			double f = floor(fValue);
			if ((fValue - f) != 0.5)
				fValue = floor(fValue + 0.5);
			else
			{
				double g = f / 2.0;
				fValue = (g == floor(g)) ? f : (f + 1.0);
			}
		}
		break;
	default:
		//OSL_ASSERT(false);
		break;
	}

	if (nDecPlaces != 0)
		fValue /= fFac;

	return bSign ? -fValue : fValue;
}

inline double math_round(
	double fValue, int nDecPlaces = 0,
	rtl_math_RoundingMode eMode = rtl_math_RoundingMode_Corrected)
{
	return rtl_math_round(fValue, nDecPlaces, eMode);
}

// static
uint16_t SvNumberformat::InsertBlanks( String& r, uint16_t nPos, sal_Unicode c )
{
    if( c >= 32 )
    {
        sal_uInt16 n = 2;   // Default for characters > 128 (HACK!)
		if (c <= 127)
			n = 1;// cCharWidths[c - 32];
        while( n-- )
            r.insert( nPos++, L" " );
    }
    return nPos;
}

static long GetPrecExp( double fAbsVal )
{
    if ( fAbsVal < 1e-7 || fAbsVal > 1e7 )
    {   // the gap, whether it's faster or not, is between 1e6 and 1e7
        return (long) floor( log10( fAbsVal ) ) + 1;
    }
    else
    {
        long nPrecExp = 1;
        while( fAbsVal < 1 )
        {
            fAbsVal *= 10;
            nPrecExp--;
        }
        while( fAbsVal >= 10 )
        {
            fAbsVal /= 10;
            nPrecExp++;
        }
        return nPrecExp;
    }
}

const sal_uInt16 nNewCurrencyVersionId = 0x434E;    // "NC"
const sal_Unicode cNewCurrencyMagic = 0x01;     // Magic for format code in comment
const sal_uInt16 nNewStandardFlagVersionId = 0x4653;    // "SF"

void ImpSvNumberformatInfo::Copy( const ImpSvNumberformatInfo& rNumFor, sal_uInt16 nAnz )
{
    for (sal_uInt16 i = 0; i < nAnz; i++)
    {
        sStrArray[i]  = rNumFor.sStrArray[i];
        nTypeArray[i] = rNumFor.nTypeArray[i];
    }
    eScannedType = rNumFor.eScannedType;
    bThousand    = rNumFor.bThousand;
    nThousand    = rNumFor.nThousand;
    nCntPre      = rNumFor.nCntPre;
    nCntPost     = rNumFor.nCntPost;
    nCntExp      = rNumFor.nCntExp;
	nExpVal		 = rNumFor.nExpVal;
}

ImpSvNumFor::ImpSvNumFor()
{
    nAnzStrings = 0;
    aI.nTypeArray = NULL;
    aI.sStrArray = NULL;
    aI.eScannedType = NUMBERFORMAT_UNDEFINED;
    aI.bThousand = false;
    aI.nThousand = 0;
    aI.nCntPre = 0;
    aI.nCntPost = 0;
    aI.nCntExp = 0;
    pColor = NULL;
}

ImpSvNumFor::~ImpSvNumFor()
{
    for (sal_uInt16 i = 0; i < nAnzStrings; i++)
        aI.sStrArray[i].erase();
    delete [] aI.sStrArray;
    delete [] aI.nTypeArray;
}

void ImpSvNumFor::Enlarge(sal_uInt16 nAnz)
{
    if ( nAnzStrings != nAnz )
    {
        if ( aI.nTypeArray )
            delete [] aI.nTypeArray;
        if ( aI.sStrArray )
            delete [] aI.sStrArray;
        nAnzStrings = nAnz;
        if ( nAnz )
        {
            aI.nTypeArray = new short[nAnz];
            aI.sStrArray  = new String[nAnz];
        }
        else
        {
            aI.nTypeArray = NULL;
            aI.sStrArray  = NULL;
        }
    }
}

void ImpSvNumFor::Copy( const ImpSvNumFor& rNumFor, ImpSvNumberformatScan* pSc )
{
    Enlarge( rNumFor.nAnzStrings );
    aI.Copy( rNumFor.aI, nAnzStrings );
    sColorName = rNumFor.sColorName;
    aNatNum = rNumFor.aNatNum;
}

bool ImpSvNumFor::HasNewCurrency() const
{
    for ( sal_uInt16 j=0; j<nAnzStrings; j++ )
    {
        if ( aI.nTypeArray[j] == NF_SYMBOLTYPE_CURRENCY )
            return true;
    }
    return false;
}

bool ImpSvNumFor::HasTextFormatCode() const
{
    return aI.eScannedType == NUMBERFORMAT_TEXT;
}

bool ImpSvNumFor::GetNewCurrencySymbol( String& rSymbol,
            String& rExtension ) const
{
    for ( sal_uInt16 j=0; j<nAnzStrings; j++ )
    {
        if ( aI.nTypeArray[j] == NF_SYMBOLTYPE_CURRENCY )
        {
            rSymbol = aI.sStrArray[j];
            if ( j < nAnzStrings-1 && aI.nTypeArray[j+1] == NF_SYMBOLTYPE_CURREXT )
                rExtension = aI.sStrArray[j+1];
            else
                rExtension.erase();
            return true;
        }
    }
    return false;
}


enum BracketFormatSymbolType
{
    BRACKET_SYMBOLTYPE_FORMAT   = -1,   // subformat string
    BRACKET_SYMBOLTYPE_COLOR    = -2,   // color
    BRACKET_SYMBOLTYPE_ERROR    = -3,   // error
    BRACKET_SYMBOLTYPE_DBNUM1   = -4,   // DoubleByteNumber, represent numbers
    BRACKET_SYMBOLTYPE_DBNUM2   = -5,   // using CJK characters, Excel compatible.
    BRACKET_SYMBOLTYPE_DBNUM3   = -6,
    BRACKET_SYMBOLTYPE_DBNUM4   = -7,
    BRACKET_SYMBOLTYPE_DBNUM5   = -8,
    BRACKET_SYMBOLTYPE_DBNUM6   = -9,
    BRACKET_SYMBOLTYPE_DBNUM7   = -10,
    BRACKET_SYMBOLTYPE_DBNUM8   = -11,
    BRACKET_SYMBOLTYPE_DBNUM9   = -12,
    BRACKET_SYMBOLTYPE_LOCALE   = -13,
    BRACKET_SYMBOLTYPE_NATNUM0  = -14,  // Our NativeNumber support, ASCII
    BRACKET_SYMBOLTYPE_NATNUM1  = -15,  // Our NativeNumber support, represent
    BRACKET_SYMBOLTYPE_NATNUM2  = -16,  //  numbers using CJK, CTL, ...
    BRACKET_SYMBOLTYPE_NATNUM3  = -17,
    BRACKET_SYMBOLTYPE_NATNUM4  = -18,
    BRACKET_SYMBOLTYPE_NATNUM5  = -19,
    BRACKET_SYMBOLTYPE_NATNUM6  = -20,
    BRACKET_SYMBOLTYPE_NATNUM7  = -21,
    BRACKET_SYMBOLTYPE_NATNUM8  = -22,
    BRACKET_SYMBOLTYPE_NATNUM9  = -23,
    BRACKET_SYMBOLTYPE_NATNUM10 = -24,
    BRACKET_SYMBOLTYPE_NATNUM11 = -25,
    BRACKET_SYMBOLTYPE_NATNUM12 = -26,
    BRACKET_SYMBOLTYPE_NATNUM13 = -27,
    BRACKET_SYMBOLTYPE_NATNUM14 = -28,
    BRACKET_SYMBOLTYPE_NATNUM15 = -29,
    BRACKET_SYMBOLTYPE_NATNUM16 = -30,
    BRACKET_SYMBOLTYPE_NATNUM17 = -31,
    BRACKET_SYMBOLTYPE_NATNUM18 = -32,
    BRACKET_SYMBOLTYPE_NATNUM19 = -33
};

bool lcl_SvNumberformat_IsBracketedPrefix( short nSymbolType )
{
    if ( nSymbolType > 0  )
        return true;        // conditions
    switch ( nSymbolType )
    {
        case BRACKET_SYMBOLTYPE_COLOR :
        case BRACKET_SYMBOLTYPE_DBNUM1 :
        case BRACKET_SYMBOLTYPE_DBNUM2 :
        case BRACKET_SYMBOLTYPE_DBNUM3 :
        case BRACKET_SYMBOLTYPE_DBNUM4 :
        case BRACKET_SYMBOLTYPE_DBNUM5 :
        case BRACKET_SYMBOLTYPE_DBNUM6 :
        case BRACKET_SYMBOLTYPE_DBNUM7 :
        case BRACKET_SYMBOLTYPE_DBNUM8 :
        case BRACKET_SYMBOLTYPE_DBNUM9 :
        case BRACKET_SYMBOLTYPE_LOCALE :
        case BRACKET_SYMBOLTYPE_NATNUM0 :
        case BRACKET_SYMBOLTYPE_NATNUM1 :
        case BRACKET_SYMBOLTYPE_NATNUM2 :
        case BRACKET_SYMBOLTYPE_NATNUM3 :
        case BRACKET_SYMBOLTYPE_NATNUM4 :
        case BRACKET_SYMBOLTYPE_NATNUM5 :
        case BRACKET_SYMBOLTYPE_NATNUM6 :
        case BRACKET_SYMBOLTYPE_NATNUM7 :
        case BRACKET_SYMBOLTYPE_NATNUM8 :
        case BRACKET_SYMBOLTYPE_NATNUM9 :
        case BRACKET_SYMBOLTYPE_NATNUM10 :
        case BRACKET_SYMBOLTYPE_NATNUM11 :
        case BRACKET_SYMBOLTYPE_NATNUM12 :
        case BRACKET_SYMBOLTYPE_NATNUM13 :
        case BRACKET_SYMBOLTYPE_NATNUM14 :
        case BRACKET_SYMBOLTYPE_NATNUM15 :
        case BRACKET_SYMBOLTYPE_NATNUM16 :
        case BRACKET_SYMBOLTYPE_NATNUM17 :
        case BRACKET_SYMBOLTYPE_NATNUM18 :
        case BRACKET_SYMBOLTYPE_NATNUM19 :
            return true;
    }
    return false;
}


SvNumberformat::SvNumberformat(String& rString,
                               LocaleData* pFormatterP,
                               ImpSvNumberInputScan* pISc,
                               uint16_t& nCheckPos,
                               LanguageType eLan,
                               bool bStan)
{
	InitFormat(rString, pFormatterP, pISc, nCheckPos, eLan, bStan);
}

SvNumberformat::SvNumberformat(std::string& rString,
	LocaleData* pFormatterP,
	ImpSvNumberInputScan* pISc,
	uint16_t& nCheckPos,
	LanguageType eLan,
	bool bStan)
{
	std::wstring in_str(rString.length(), L' ');
	std::copy(rString.begin(), rString.end(), in_str.begin());

	InitFormat(in_str, pFormatterP, pISc, nCheckPos, eLan, bStan);
}

void SvNumberformat::InitFormat(String& rString,
	LocaleData* pFormatterP,
	ImpSvNumberInputScan* pISc,
	uint16_t& nCheckPos,
	LanguageType eLan,
	bool bStan)
{
	pFormatter = pFormatterP;
	nNewStandardDefined = 0;
	bStarFlag = false;

	if (!pFormatter || !pISc)
	{
		return;
	}
	rScanPtr = pFormatter->GetFormatScanner();
	bStandard = bStan;
	bIsUsed = false;
	fLimit1 = 0.0;
	fLimit2 = 0.0;
	eOp1 = NUMBERFORMAT_OP_NO;
	eOp2 = NUMBERFORMAT_OP_NO;
	eType = NUMBERFORMAT_DEFINED;

	bool bCancel = false;
	bool bCondition = false;
	short eSymbolType;
	uint16_t nPos = 0;
	uint16_t nPosOld;
	nCheckPos = 0;
	String aComment;

	// Split into 4 sub formats
	sal_uInt16 nIndex;
	for (nIndex = 0; nIndex < 4 && !bCancel; nIndex++)
	{
		// Original language/country may have to be reestablished

		String sStr;
		nPosOld = nPos;                         // Start position of substring
		// first get bracketed prefixes; e.g. conditions, color
		do
		{
			eSymbolType = ImpNextSymbol(rString, nPos, sStr);
			if (eSymbolType > 0)                    // condition
			{
				if (nIndex == 0 && !bCondition)
				{
					bCondition = true;
					eOp1 = (SvNumberformatLimitOps)eSymbolType;
				}
				else if (nIndex == 1 && bCondition)
					eOp2 = (SvNumberformatLimitOps)eSymbolType;
				else                                // error
				{
					bCancel = true;                 // break for
					nCheckPos = nPosOld;
				}
				if (!bCancel)
				{
					double fNumber;
					uint16_t nAnzChars = ImpGetNumber(rString, nPos, sStr);
					if (nAnzChars > 0)
					{
						short F_Type = NUMBERFORMAT_UNDEFINED;
						if (!pISc->IsNumberFormat(sStr, F_Type, fNumber) ||
							(F_Type != NUMBERFORMAT_NUMBER &&
								F_Type != NUMBERFORMAT_SCIENTIFIC))
						{
							fNumber = 0.0;
							nPos = nPos - nAnzChars;
							rString.erase(nPos, nAnzChars);
							rString.insert(nPos, L"0");
							nPos++;
						}
					}
					else
					{
						fNumber = 0.0;
						rString.insert(nPos++, L"0");
					}
					if (nIndex == 0)
						fLimit1 = fNumber;
					else
						fLimit2 = fNumber;
					if (rString.at(nPos) == L']')
						nPos++;
					else
					{
						bCancel = true;             // break for
						nCheckPos = nPos;
					}
				}
				nPosOld = nPos;                     // position before string
			}
			else if (lcl_SvNumberformat_IsBracketedPrefix(eSymbolType))
			{
				if (!bCancel)
				{
					rString.erase(nPosOld, nPos - nPosOld);
					rString.insert(nPosOld, sStr);
					nPos = nPosOld + sStr.size();
					rString.insert(nPos, L"]");
					rString.insert(nPosOld, L"[");
					nPos += 2;
					nPosOld = nPos;     // position before string
				}
			}
		} while (!bCancel && lcl_SvNumberformat_IsBracketedPrefix(eSymbolType));

		// The remaining format code string
		if (!bCancel)
		{
			if (eSymbolType == BRACKET_SYMBOLTYPE_FORMAT)
			{
				if (nIndex == 1 && eOp1 == NUMBERFORMAT_OP_NO)
					eOp1 = NUMBERFORMAT_OP_GT;  // undefined condition, default: > 0
				else if (nIndex == 2 && eOp2 == NUMBERFORMAT_OP_NO)
					eOp2 = NUMBERFORMAT_OP_LT;  // undefined condition, default: < 0
				if (sStr.size() == 0)
				{   // empty sub format
				}
				else
				{
					uint16_t nStrPos = rScanPtr->ScanFormat(sStr, aComment);
					sal_uInt16 nAnz = rScanPtr->GetAnzResStrings();
					if (nAnz == 0)              // error
						nStrPos = 1;
					if (nStrPos == 0)               // ok
					{
						rString.erase(nPosOld, nPos - nPosOld);
						rString.insert(nPosOld, sStr);
						nPos = nPosOld + sStr.size();
						if (nPos < rString.size())
						{
							rString.insert(nPos, L";");
							nPos++;
						}
						NumFor[nIndex].Enlarge(nAnz);
						rScanPtr->CopyInfo(&(NumFor[nIndex].Info()), nAnz);
						// type check
						if (nIndex == 0)
							eType = (short)NumFor[nIndex].Info().eScannedType;
						else if (nIndex == 3)
						{
							NumFor[nIndex].Info().eScannedType = NUMBERFORMAT_TEXT;
						}
						else if ((short)NumFor[nIndex].Info().eScannedType != eType)
							eType = NUMBERFORMAT_DEFINED;
					}
					else
					{
						nCheckPos = nPosOld + nStrPos;  // error in string
						bCancel = true;                 // break for
					}
				}
			}
			else if (eSymbolType == BRACKET_SYMBOLTYPE_ERROR)   // error
			{
				nCheckPos = nPosOld;
				bCancel = true;
			}
			else if (lcl_SvNumberformat_IsBracketedPrefix(eSymbolType))
			{
				nCheckPos = nPosOld + 1;                  // error, prefix in string
				bCancel = true;                         // break for
			}
		}
		if (bCancel && !nCheckPos)
			nCheckPos = 1;      // nCheckPos is used as an error condition
		if (!bCancel)
		{
			if (NumFor[nIndex].GetNatNum().IsSet() &&
				NumFor[nIndex].GetNatNum().GetLang() == LocaleId_en_US)
				NumFor[nIndex].SetNatNumLang(eLan);
		}
		if (rString.size() == nPos)
		{
			if (nIndex == 2 && eSymbolType == BRACKET_SYMBOLTYPE_FORMAT &&
				rString.at(nPos - 1) == L';')
			{   // #83510# A 4th subformat explicitly specified to be empty
				// hides any text. Need the type here for HasTextFormat()
				NumFor[3].Info().eScannedType = NUMBERFORMAT_TEXT;
			}
			bCancel = true;
		}
		if (NumFor[nIndex].GetNatNum().IsSet())
			NumFor[nIndex].SetNatNumDate(
			(NumFor[nIndex].Info().eScannedType & NUMBERFORMAT_DATE) != 0);
	}

	if (bCondition && !nCheckPos)
	{
		if (nIndex == 1 && NumFor[0].GetnAnz() == 0 &&
			rString.at(rString.size() - 1) != L';')
		{   // No format code => GENERAL   but not if specified empty
			String aAdd(rScanPtr->GetStandardName());
			String aTmp;
			if (!rScanPtr->ScanFormat(aAdd, aTmp))
			{
				sal_uInt16 nAnz = rScanPtr->GetAnzResStrings();
				if (nAnz)
				{
					NumFor[0].Enlarge(nAnz);
					rScanPtr->CopyInfo(&(NumFor[0].Info()), nAnz);
					rString += aAdd;
				}
			}
		}
		else if (nIndex == 1 && NumFor[nIndex].GetnAnz() == 0 &&
			rString.at(rString.size() - 1) != L';' &&
			(NumFor[0].GetnAnz() > 1 || (NumFor[0].GetnAnz() == 1 &&
				NumFor[0].Info().nTypeArray[0] != NF_KEY_GENERAL)))
		{   // No trailing second subformat => GENERAL   but not if specified empty
			// and not if first subformat is GENERAL
			String aAdd(rScanPtr->GetStandardName());
			String aTmp;
			if (!rScanPtr->ScanFormat(aAdd, aTmp))
			{
				sal_uInt16 nAnz = rScanPtr->GetAnzResStrings();
				if (nAnz)
				{
					NumFor[nIndex].Enlarge(nAnz);
					rScanPtr->CopyInfo(&(NumFor[nIndex].Info()), nAnz);
					rString += L';';
					rString += aAdd;
				}
			}
		}
		else if (nIndex == 2 && NumFor[nIndex].GetnAnz() == 0 &&
			rString.at(rString.size() - 1) != L';' &&
			eOp2 != NUMBERFORMAT_OP_NO)
		{   // No trailing third subformat => GENERAL   but not if specified empty
			String aAdd(rScanPtr->GetStandardName());
			String aTmp;
			if (!rScanPtr->ScanFormat(aAdd, aTmp))
			{
				sal_uInt16 nAnz = rScanPtr->GetAnzResStrings();
				if (nAnz)
				{
					NumFor[nIndex].Enlarge(nAnz);
					rScanPtr->CopyInfo(&(NumFor[nIndex].Info()), nAnz);
					rString += L';';
					rString += aAdd;
				}
			}
		}
	}
	sFormatstring = rString;
	if (aComment.size())
	{
		SetComment(aComment);     // sets sComment and sFormatstring
		rString = sFormatstring;    // 
	}
	if (NumFor[2].GetnAnz() == 0 &&                 // no 3rd substring
		eOp1 == NUMBERFORMAT_OP_GT && eOp2 == NUMBERFORMAT_OP_NO &&
		fLimit1 == 0.0 && fLimit2 == 0.0)
		eOp1 = NUMBERFORMAT_OP_GE;                  // 0 to the first format
}

SvNumberformat::~SvNumberformat()
{
}

uint16_t SvNumberformat::ImpGetNumber(String& rString,
                                 uint16_t& nPos,
                                 String& sSymbol)
{
    uint16_t nStartPos = nPos;
    sal_Unicode cToken;
    uint16_t nLen = rString.size();
    sSymbol.erase();
    while ( nPos < nLen && ((cToken = rString.at(nPos)) != ']') )
    {
        if (cToken == L' ')
        {                                               // delete spaces
            rString.erase(nPos,1);
            nLen--;
        }
        else
        {
            nPos++;
            sSymbol += cToken;
        }
    }
    return nPos - nStartPos;
}


// static
LanguageType SvNumberformat::ImpGetLanguageType( const String& rString,
        uint16_t& nPos )
{
    sal_Int32 nNum = 0;
    sal_Unicode cToken = 0;
    uint16_t nLen = rString.size();
    while ( nPos < nLen && ((cToken = rString.at(nPos)) != L']') )
    {
        if (L'0' <= cToken && cToken <= L'9' )
        {
            nNum *= 16;
            nNum += cToken - L'0';
        }
        else if ( L'a' <= cToken && cToken <= L'f' )
        {
            nNum *= 16;
            nNum += cToken - L'a' + 10;
        }
        else if ( L'A' <= cToken && cToken <= L'F' )
        {
            nNum *= 16;
            nNum += cToken - L'A' + 10;
        }
        else
            return LocaleId_en_US;
        ++nPos;
    }
    return (nNum && (cToken == ']' || nPos == nLen)) ? (LanguageType)nNum :
		LocaleId_en_US;
}

bool IsSingleSymbol(String& rString, uint16_t nPos){
	bool ret = false;
	while(nPos > 0){
		if(rString.at(nPos) == '*' || rString.at(nPos) == '\\' || rString.at(nPos) == L'_'){
			ret = !ret;
			nPos--;
		}
		else
			return ret;
	}
	return ret;
}

short SvNumberformat::ImpNextSymbol(String& rString,
                                 uint16_t& nPos,
                                 String& sSymbol)
{
    short eSymbolType = BRACKET_SYMBOLTYPE_FORMAT;
    sal_Unicode cToken;
    sal_Unicode cLetter = L' ';                               // intermediate result
    uint16_t nLen = rString.size();
    ScanState eState = SsStart;
    sSymbol.erase();
    const NfKeywordTable & rKeywords = rScanPtr->GetKeywords();
    while (nPos < nLen && eState != SsStop)
    {
        cToken = rString.at(nPos);
        nPos++;
        switch (eState)
        {
            case SsStart:
            {
                if (cToken == L'[')
                {
                    eState = SsGetBracketed;
                    sSymbol += cToken;
                }
                else if (cToken == L';')
                {
                    eState = SsGetString;
                    nPos--;
                    eSymbolType = BRACKET_SYMBOLTYPE_FORMAT;
                }
                else if (cToken == L']')
                {
                    eState = SsStop;
                    eSymbolType = BRACKET_SYMBOLTYPE_ERROR;
                }
                else if (cToken == L' ')             // Skip Blanks
                {
                    rString.erase(nPos-1,1);
                    nPos--;
                    nLen--;
                }
                else
                {
                    sSymbol += cToken;
                    eState = SsGetString;
                    eSymbolType = BRACKET_SYMBOLTYPE_FORMAT;
                }
            }
            break;
            case SsGetBracketed:
            {
                switch (cToken)
                {
                    case L'<':
                    case L'>':
                    case L'=':
                    {
                        EraseAllChars(sSymbol, L'[');
                        sSymbol += cToken;
                        cLetter = cToken;
                        eState = SsGetCon;
                        switch (cToken)
                        {
                            case L'<': eSymbolType = NUMBERFORMAT_OP_LT; break;
                            case L'>': eSymbolType = NUMBERFORMAT_OP_GT; break;
                            case L'=': eSymbolType = NUMBERFORMAT_OP_EQ; break;
                            default: break;
                        }
                    }
                    break;
                    case L' ':
                    {
                        rString.erase(nPos-1,1);
                        nPos--;
                        nLen--;
                    }
                    break;
                    case L'$' :
                    {
                        if ( rString.at(nPos) == L'-' )
                        {   // [$-xxx] locale
                            EraseAllChars(sSymbol, L'[');
                            eSymbolType = BRACKET_SYMBOLTYPE_LOCALE;
                            eState = SsGetPrefix;
                        }
                        else
                        {   // currency as of SV_NUMBERFORMATTER_VERSION_NEW_CURR
                            eSymbolType = BRACKET_SYMBOLTYPE_FORMAT;
                            eState = SsGetString;
                        }
                        sSymbol += cToken;
                    }
                    break;
                    case L'~' :
                    {   // calendarID as of SV_NUMBERFORMATTER_VERSION_CALENDAR
                        eSymbolType = BRACKET_SYMBOLTYPE_FORMAT;
                        sSymbol += cToken;
                        eState = SsGetString;
                    }
                    break;
                    default:
                    {
                        static const String aNatNum = L"NATNUM";
                        static const String aDBNum = L"DBNUM";
                        String aUpperNatNum = rString.substr(nPos-1, aNatNum.size());
						ConvertToUpper(aUpperNatNum);
                        String aUpperDBNum = rString.substr(nPos-1, aDBNum.size());
						ConvertToUpper(aUpperDBNum);
                        sal_Unicode cUpper = aUpperNatNum.at(0);
                        if (cUpper == rKeywords[NF_KEY_H].at(0)   ||  // H
                            cUpper == rKeywords[NF_KEY_MI].at(0)   ||  // M
                            cUpper == rKeywords[NF_KEY_S].at(0)    )   // S
                        {
                            sSymbol += cToken;
                            eState = SsGetTime;
                            cLetter = cToken;
                        }
                        else
                        {
                            EraseAllChars(sSymbol, L'[');
                            sSymbol += cToken;
                            eSymbolType = BRACKET_SYMBOLTYPE_COLOR;
                            eState = SsGetPrefix;
                        }
                    }
                    break;
                }
            }
            break;
            case SsGetString:
            {
                if (cToken == L';' && (nPos>=2) &&!IsSingleSymbol(rString, nPos-2))
				{
                    eState = SsStop;
				}
                else
                    sSymbol += cToken;
            }
            break;
            case SsGetTime:
            {
                if (cToken == L']')
                {
                    sSymbol += cToken;
                    eState = SsGetString;
                    eSymbolType = BRACKET_SYMBOLTYPE_FORMAT;
                }
                else
                {
                    sal_Unicode cUpper = std::towupper(rString.at(nPos-1));
                    if (cUpper == rKeywords[NF_KEY_H].at(0)    ||  // H
                        cUpper == rKeywords[NF_KEY_MI].at(0)   ||  // M
                        cUpper == rKeywords[NF_KEY_S].at(0)    )   // S
                    {
                        if (cLetter == cToken)
                        {
                            sSymbol += cToken;
                            cLetter = L' ';
                        }
                        else
                        {
                            EraseAllChars(sSymbol, L'[');
                            sSymbol += cToken;
                            eState = SsGetPrefix;
                        }
                    }
                    else
                    {
                        EraseAllChars(sSymbol, L'[');
                        sSymbol += cToken;
                        eSymbolType = BRACKET_SYMBOLTYPE_COLOR;
                        eState = SsGetPrefix;
                    }
                }
            }
            break;
            case SsGetCon:
            {
                switch (cToken)
                {
                    case L'<':
                    {
                        eState = SsStop;
                        eSymbolType = BRACKET_SYMBOLTYPE_ERROR;
                    }
                    break;
                    case L'>':
                    {
                        if (cLetter == L'<')
                        {
                            sSymbol += cToken;
                            cLetter = L' ';
                            eState = SsStop;
                            eSymbolType = NUMBERFORMAT_OP_NE;
                        }
                        else
                        {
                            eState = SsStop;
                            eSymbolType = BRACKET_SYMBOLTYPE_ERROR;
                        }
                    }
                    break;
                    case L'=':
                    {
                        if (cLetter == L'<')
                        {
                            sSymbol += cToken;
                            cLetter = L' ';
                            eSymbolType = NUMBERFORMAT_OP_LE;
                        }
                        else if (cLetter == L'>')
                        {
                            sSymbol += cToken;
                            cLetter = L' ';
                            eSymbolType = NUMBERFORMAT_OP_GE;
                        }
                        else
                        {
                            eState = SsStop;
                            eSymbolType = BRACKET_SYMBOLTYPE_ERROR;
                        }
                    }
                    break;
                    case L' ':
                    {
                        rString.erase(nPos-1,1);
                        nPos--;
                        nLen--;
                    }
                    break;
                    default:
                    {
                        eState = SsStop;
                        nPos--;
                    }
                    break;
                }
            }
            break;
            case SsGetPrefix:
            {
                if (cToken == L']')
                    eState = SsStop;
                else
                    sSymbol += cToken;
            }
            break;
            default:
            break;
        }                                   // of switch
    }                                       // of while

    return eSymbolType;
}

bool SvNumberformat::HasNewCurrency() const
{
    for ( sal_uInt16 j=0; j<4; j++ )
    {
        if ( NumFor[j].HasNewCurrency() )
            return true;
    }
    return false;
}

bool SvNumberformat::HasTextFormatCode() const
{
    for ( sal_uInt16 j=0; j<4; j++ )
    {
        if ( NumFor[j].HasTextFormatCode() )
            return true;
    }
    return false;
}

bool SvNumberformat::GetNewCurrencySymbol( String& rSymbol,
            String& rExtension ) const
{
    for ( sal_uInt16 j=0; j<4; j++ )
    {
        if ( NumFor[j].GetNewCurrencySymbol( rSymbol, rExtension ) )
            return true;
    }
    rSymbol.erase();
    rExtension.erase();
    return false;
}


// static
String SvNumberformat::StripNewCurrencyDelimiters( const String& rStr,
            bool bQuoteSymbol )
{
    String aTmp;
	size_t find_pos;
    uint16_t nStartPos, nPos, nLen;
    nLen = rStr.size();
    nStartPos = 0;
    while ( (find_pos = rStr.find( L"[$", nStartPos )) != std::string::npos )
    {
		nPos = (uint16_t)find_pos;
        uint16_t nEnd;
        if ( (nEnd = GetQuoteEnd( rStr, nPos )) < nLen )
        {
            aTmp += rStr.substr( nStartPos, ++nEnd - nStartPos );
            nStartPos = nEnd;
        }
        else
        {
            aTmp += rStr.substr( nStartPos, nPos - nStartPos );
            nStartPos = nPos + 2;
            uint16_t nDash;
            nEnd = nStartPos - 1;
            do
            {
                nDash = rStr.find( L"-", ++nEnd );
            } while ( (nEnd = GetQuoteEnd( rStr, nDash )) < nLen );
            uint16_t nClose;
            nEnd = nStartPos - 1;
            do
            {
                nClose = rStr.find( L"]", ++nEnd );
            } while ( (nEnd = GetQuoteEnd( rStr, nClose )) < nLen );
            nPos = ( nDash < nClose ? nDash : nClose );
            if ( !bQuoteSymbol || rStr.at( nStartPos ) == L'"' )
                aTmp += rStr.substr( nStartPos, nPos - nStartPos );
            else
            {
                aTmp += L'"';
                aTmp += rStr.substr( nStartPos, nPos - nStartPos );
                aTmp += L'"';
            }
            nStartPos = nClose + 1;
        }
    }
    if ( nLen > nStartPos )
        aTmp += rStr.substr( nStartPos, nLen - nStartPos );
    return aTmp;
}


void SvNumberformat::Build50Formatstring( String& rStr ) const
{
    rStr = StripNewCurrencyDelimiters( sFormatstring, true );
}


void SvNumberformat::ImpGetOutputStandard(double& fNumber, String& OutString)
{
    sal_uInt16 nStandardPrec = rScanPtr->GetStandardPrec();

    if ( fabs(fNumber) > 1.0E15 )       // #58531# war E16
    {
        nStandardPrec = ::std::min(nStandardPrec, static_cast<sal_uInt16>(14)); // limits to 14 decimals
        OutString = doubleToUString( fNumber,
                rtl_math_StringFormat_E, nStandardPrec /*2*/,
                pFormatter->GetNumDecimalSep().at(0));
    }
    else
        ImpGetOutputStdToPrecision(fNumber, OutString, nStandardPrec);
}

void SvNumberformat::ImpGetOutputStdToPrecision(double& rNumber, String& rOutString, sal_uInt16 nPrecision) const
{
    nPrecision = ::std::min(UPPER_PRECISION, nPrecision);
    
    rOutString = doubleToUString( rNumber,
            rtl_math_StringFormat_F, nPrecision /*2*/,
            pFormatter->GetNumDecimalSep().at(0), true );
    if (rOutString.at(0) == L'-' &&
        std::count(rOutString.begin(), rOutString.end(), L'0') == rOutString.size())
        EraseLeadingChars(rOutString, L'-');            // nicht -0

    ImpTransliterate( rOutString, NumFor[0].GetNatNum() );
}

void SvNumberformat::ImpGetOutputInputLine(double fNumber, String& OutString)
{
    bool bModified = false;
    if ( (eType & NUMBERFORMAT_PERCENT) && (fabs(fNumber) < _D_MAX_D_BY_100))
    {
        if (fNumber == 0.0)
        {
            OutString = L"0%";
            return;
        }
        fNumber *= 100;
        bModified = true;
    }

    if (fNumber == 0.0)
    {
        OutString = L'0';
        return;
    }

    OutString = doubleToUString( fNumber,
            rtl_math_StringFormat_Automatic, rtl_math_DecimalPlaces_Max,
            pFormatter->GetNumDecimalSep().at(0), true );

    if ( eType & NUMBERFORMAT_PERCENT && bModified)
        OutString += L'%';
    return;
}

short SvNumberformat::ImpCheckCondition(double& fNumber,
                                     double& fLimit,
                                     SvNumberformatLimitOps eOp)
{
    switch(eOp)
    {
        case NUMBERFORMAT_OP_NO: return -1;
        case NUMBERFORMAT_OP_EQ: return (short) (fNumber == fLimit);
        case NUMBERFORMAT_OP_NE: return (short) (fNumber != fLimit);
        case NUMBERFORMAT_OP_LT: return (short) (fNumber <  fLimit);
        case NUMBERFORMAT_OP_LE: return (short) (fNumber <= fLimit);
        case NUMBERFORMAT_OP_GT: return (short) (fNumber >  fLimit);
        case NUMBERFORMAT_OP_GE: return (short) (fNumber >= fLimit);
        default: return -1;
    }
}

bool SvNumberformat::GetOutputString(String& sString,
                                     String& OutString,
                                     Color** ppColor)
{
    OutString.erase();
    sal_uInt16 nIx;
    if (eType & NUMBERFORMAT_TEXT)
        nIx = 0;
    else if (NumFor[3].GetnAnz() > 0)
        nIx = 3;
    else
    {
        *ppColor = NULL;        // no change of color
        return false;
    }
	*ppColor = NULL; // NumFor[nIx].GetColor();
    const ImpSvNumberformatInfo& rInfo = NumFor[nIx].Info();
    if (rInfo.eScannedType == NUMBERFORMAT_TEXT)
    {
        bool bRes = false;
        const sal_uInt16 nAnz = NumFor[nIx].GetnAnz();
        for (sal_uInt16 i = 0; i < nAnz; i++)
        {
            switch (rInfo.nTypeArray[i])
            {
                case NF_SYMBOLTYPE_STAR:
                    if( bStarFlag )
                    {
                        OutString += (sal_Unicode) 0x1B;
                        OutString += rInfo.sStrArray[i].at(1);
                        bRes = true;
                    }
                break;
                case NF_SYMBOLTYPE_BLANK:
                    InsertBlanks( OutString, OutString.size(),
                        rInfo.sStrArray[i].at(0) );
                break;
                case NF_KEY_GENERAL :   // #77026# "General" is the same as "@"
                case NF_SYMBOLTYPE_DEL :
                    OutString += sString;
                break;
                default:
                    OutString += rInfo.sStrArray[i];
            }
        }
        return bRes;
    }
    return false;
}

sal_uLong SvNumberformat::ImpGGT(sal_uLong x, sal_uLong y)
{
    if (y == 0)
        return x;
    else
    {
        sal_uLong z = x%y;
        while (z)
        {
            x = y;
            y = z;
            z = x%y;
        }
        return y;
    }
}

sal_uLong SvNumberformat::ImpGGTRound(sal_uLong x, sal_uLong y)
{
    if (y == 0)
        return x;
    else
    {
        sal_uLong z = x%y;
        while ((double)z/(double)y > D_EPS)
        {
            x = y;
            y = z;
            z = x%y;
        }
        return y;
    }
}

namespace {

void lcl_GetOutputStringScientific(
    double fNumber, sal_uInt16 nCharCount, LocaleData* pFormatter, String& rOutString)
{
    bool bSign = fabs(fNumber) < 0.0;

    // 1.000E+015 (one digit and the decimal point, and the five chars for the exponential part, totalling 7).
    sal_uInt16 nPrec = nCharCount > 7 ? nCharCount - 7 : 0;
    if (nPrec && bSign)
        // Make room for the negative sign.
        --nPrec;

    nPrec = ::std::min(nPrec, static_cast<sal_uInt16>(14)); // limit to 14 decimals.

    rOutString = doubleToUString(
        fNumber, rtl_math_StringFormat_E, nPrec, pFormatter->GetNumDecimalSep().at(0));
}

}

bool SvNumberformat::GetOutputString(double fNumber, sal_uInt16 nCharCount, String& rOutString) const
{
    using namespace std;

    if (eType != NUMBERFORMAT_NUMBER)
        return false;

    double fTestNum = fabs(fNumber);

    if (fTestNum < EXP_LOWER_BOUND)
    {
        lcl_GetOutputStringScientific(fNumber, nCharCount, pFormatter, rOutString);
        return true;
    }

    double fExp = log10(fTestNum);
    // Values < 1.0 always have one digit before the decimal point.
    sal_uInt16 nDigitPre = fExp >= 0.0 ? static_cast<sal_uInt16>(ceil(fExp)) : 1;

    if (nDigitPre > 15)
    {
        lcl_GetOutputStringScientific(fNumber, nCharCount, pFormatter, rOutString);
        return true;
    }

    sal_uInt16 nPrec = nCharCount >= nDigitPre ? nCharCount - nDigitPre : 0;
    if (nPrec && fNumber < 0.0)
        // Subtract the negative sign.
        --nPrec;
    if (nPrec)
        // Subtract the decimal point.
        --nPrec;

    ImpGetOutputStdToPrecision(fNumber, rOutString, nPrec);
    if (rOutString.size() > nCharCount)
        // String still wider than desired.  Switch to scientific notation.
        lcl_GetOutputStringScientific(fNumber, nCharCount, pFormatter, rOutString);

    return true;
}

bool SvNumberformat::GetOutputString(double fNumber,
                                     String& OutString,
                                     Color** ppColor)
{
    bool bRes = false;
    OutString.erase();                          // delete everything
    *ppColor = NULL;                            // no color change
    if (eType & NUMBERFORMAT_LOGICAL)
    {
        if (fNumber)
            OutString = rScanPtr->GetTrueString();
        else
            OutString = rScanPtr->GetFalseString();
        return false;
    }
    if (eType & NUMBERFORMAT_TEXT)
    {
        ImpGetOutputStandard(fNumber, OutString);
        return false;
    }
    bool bHadStandard = false;
    if (bStandard)                              // individual standard formats
    {
        if (rScanPtr->GetStandardPrec() == INPUTSTRING_PRECISION)     // all number formats InputLine
        {
            ImpGetOutputInputLine(fNumber, OutString);
            return false;
        }
        switch (eType)
        {
            case NUMBERFORMAT_NUMBER:                   // 
            {
                if (rScanPtr->GetStandardPrec() == UNLIMITED_PRECISION)
                {
                    bool bSign = fNumber < 0.0;
                    if (bSign)
                        fNumber = -fNumber;
                    ImpGetOutputStdToPrecision(fNumber, OutString, 10); // Use 10 decimals for general 'unlimited' format.
                    if (fNumber < EXP_LOWER_BOUND)
                    {
                        uint16_t nLen = OutString.size();
                        if (!nLen)
                            return false;
        
                        if (nLen > 11 || (OutString == L"0" && fNumber != 0.0))
                        {
                            sal_uInt16 nStandardPrec = rScanPtr->GetStandardPrec();
                            nStandardPrec = ::std::min(nStandardPrec, static_cast<sal_uInt16>(14)); // limits to 14 decimals
                            OutString = doubleToUString( fNumber,
                                    rtl_math_StringFormat_E, nStandardPrec /*2*/,
                                    pFormatter->GetNumDecimalSep().at(0), true);
                        }
                    }
                    if (bSign)
                        OutString.insert(0, L"-");
                    return false;
                }
                ImpGetOutputStandard(fNumber, OutString);
                bHadStandard = true;
            }
            break;
            case NUMBERFORMAT_DATE:
                bRes |= ImpGetDateOutput(fNumber, 0, OutString);
                bHadStandard = true;
            break;
            case NUMBERFORMAT_TIME:
                bRes |= ImpGetTimeOutput(fNumber, 0, OutString);
                bHadStandard = true;
            break;
            case NUMBERFORMAT_DATETIME:
                bRes |= ImpGetDateTimeOutput(fNumber, 0, OutString);
                bHadStandard = true;
            break;
        }
    }
    if ( !bHadStandard )
    {
        sal_uInt16 nIx;                             // Index des Teilformats
        short nCheck = ImpCheckCondition(fNumber, fLimit1, eOp1);
        if (nCheck == -1 || nCheck == 1)            // nur 1 String oder True
            nIx = 0;
        else
        {
            nCheck = ImpCheckCondition(fNumber, fLimit2, eOp2);
            if (nCheck == -1 || nCheck == 1)
                nIx = 1;
            else
                nIx = 2;
        }
        if (nIx == 1 &&          // negatives Format
                IsNegativeRealNegative() && fNumber < 0.0)      // unsigned
            fNumber = -fNumber;                 // Eliminate signs
		if (nIx == 0 && IsNegativeRealNegative2() && fNumber < 0.0)
			fNumber = -fNumber;
		*ppColor = NULL; // NumFor[nIx].GetColor();
        const ImpSvNumberformatInfo& rInfo = NumFor[nIx].Info();
        const sal_uInt16 nAnz = NumFor[nIx].GetnAnz();
        if (nAnz == 0 && rInfo.eScannedType == NUMBERFORMAT_UNDEFINED)
            return false;                       // leer => nichts
        else if (nAnz == 0)                     // sonst Standard-Format
        {
            ImpGetOutputStandard(fNumber, OutString);
            return false;
        }
        switch (rInfo.eScannedType)
        {
            case NUMBERFORMAT_TEXT:
            case NUMBERFORMAT_DEFINED:
            {
                for (sal_uInt16 i = 0; i < nAnz; i++)
                {
                    switch (rInfo.nTypeArray[i])
                    {
                        case NF_SYMBOLTYPE_STAR:
                            if( bStarFlag )
                            {
                                OutString += (sal_Unicode) 0x1B;
                                OutString += rInfo.sStrArray[i].at(1);
                                bRes = true;
                            }
                            break;
                        case NF_SYMBOLTYPE_BLANK:
                            InsertBlanks( OutString, OutString.size(),
                                rInfo.sStrArray[i].at(0) );
                            break;
                        case NF_SYMBOLTYPE_STRING:
                        case NF_SYMBOLTYPE_CURRENCY:
                            OutString += rInfo.sStrArray[i];
                            break;
                        case NF_SYMBOLTYPE_THSEP:
                            if (rInfo.nThousand == 0)
                                OutString += rInfo.sStrArray[i];
                        break;
                        default:
                        break;
                    }
                }
            }
            break;
            case NUMBERFORMAT_DATE:
                bRes |= ImpGetDateOutput(fNumber, nIx, OutString);
            break;
            case NUMBERFORMAT_TIME:
                bRes |= ImpGetTimeOutput(fNumber, nIx, OutString);
            break;
            case NUMBERFORMAT_DATETIME:
                bRes |= ImpGetDateTimeOutput(fNumber, nIx, OutString);
            break;
            case NUMBERFORMAT_NUMBER:
            case NUMBERFORMAT_PERCENT:
            case NUMBERFORMAT_CURRENCY:
                bRes |= ImpGetNumberOutput(fNumber, nIx, OutString);
            break;
            case NUMBERFORMAT_FRACTION:
            {
                String sStr, sFrac, sDiv;               // Strings, value for
                sal_uLong nFrac, nDiv;                      // pre-decimal point
                                                        // numerator and denominator
                bool bSign = false;
                if (fNumber < 0)
                {
                    if (nIx == 0)                       // not in rear
                        bSign = true;                   // Formats
                    fNumber = -fNumber;
                }
                double fNum = floor(fNumber);           // integer part
                fNumber -= fNum;                        // decimal part
                if (fNum > _D_MAX_U_LONG_ || rInfo.nCntExp > 9)
                                                        // too large
                {
                    OutString = rScanPtr->GetErrorString();
                    return false;
                }
                if (rInfo.nCntExp == 0)
                {
                    return false;
                }
                sal_uLong nBasis = ((sal_uLong)floor(           // 9, 99, 999 ,...
                                    pow(10.0,rInfo.nCntExp))) - 1;
                sal_uLong x0, y0, x1, y1;

				if (rInfo.nExpVal <= 0)
				{
					if (rInfo.nCntExp <= _MAX_FRACTION_PREC)
					{
						bool bUpperHalf;
						if (fNumber > 0.5)
						{
							bUpperHalf = true;
							fNumber -= (fNumber - 0.5) * 2.0;
						}
						else
							bUpperHalf = false;
						// Entry into Farey series
						// Find:
						x0 = (sal_uLong)floor(fNumber*nBasis); // z.B. 2/9 <= x < 3/9
						if (x0 == 0)                        //      => x0 = 2
						{
							y0 = 1;
							x1 = 1;
							y1 = nBasis;
						}
						else if (x0 == (nBasis - 1) / 2)    // (b-1)/2, 1/2
						{                               // goes (nbase odd)
							y0 = nBasis;
							x1 = 1;
							y1 = 2;
						}
						else if (x0 == 1)
						{
							y0 = nBasis;                    //  1/n; 1/(n-1)
							x1 = 1;
							y1 = nBasis - 1;
						}
						else
						{
							y0 = nBasis;                    // z.B. 2/9   2/8
							x1 = x0;
							y1 = nBasis - 1;
							double fUg = (double)x0 / (double)y0;
							double fOg = (double)x1 / (double)y1;
							sal_uLong nGgt = ImpGGT(y0, x0);       // x0/y0 short
							x0 /= nGgt;
							y0 /= nGgt;                     // boxing:
							sal_uLong x2 = 0;
							sal_uLong y2 = 0;
							bool bStop = false;
							while (!bStop)
							{
                                double fTest = (double)x1 / (double)y1;
								while (!bStop)
								{
									while (fTest > fOg)
									{
										x1--;
										fTest = (double)x1 / (double)y1;
									}
									while (fTest < fUg && y1 > 1)
									{
										y1--;
										fTest = (double)x1 / (double)y1;
									}
									if (fTest <= fOg)
									{
										fOg = fTest;
										bStop = true;
									}
									else if (y1 == 1)
										bStop = true;
								}                               // of while
								nGgt = ImpGGT(y1, x1);             // x1/y1 short
								x2 = x1 / nGgt;
								y2 = y1 / nGgt;
								if (x2*y0 - x0 * y2 == 1 || y1 <= 1)  // Test, ob x2/y2
									bStop = true;               // next Farey number
								else
								{
									y1--;
									bStop = false;
								}
							}                                   // of while
							x1 = x2;
							y1 = y2;
						}                                       // of else
						double fup, flow;
						flow = (double)x0 / (double)y0;
						fup = (double)x1 / (double)y1;
						while (fNumber > fup)
						{
							sal_uLong x2 = ((y0 + nBasis) / y1)*x1 - x0; // next Farey number
							sal_uLong y2 = ((y0 + nBasis) / y1)*y1 - y0;
							//                      GetNextFareyNumber(nBasis, x0, x1, y0, y1, x2, y2);
							x0 = x1;
							y0 = y1;
							x1 = x2;
							y1 = y2;
							flow = fup;
							fup = (double)x1 / (double)y1;
						}
						if (fNumber - flow < fup - fNumber)
						{
							nFrac = x0;
							nDiv = y0;
						}
						else
						{
							nFrac = x1;
							nDiv = y1;
						}
						if (bUpperHalf)                     // Original restaur.
						{
							if (nFrac == 0 && nDiv == 1)    // 1/1
								fNum += 1.0;
							else
								nFrac = nDiv - nFrac;
						}
					}
					else                                    // big denominators
					{                                       // 0,1234->123/1000
						sal_uLong nGgt;
						nDiv = 10000000;
						nFrac = ((sal_uLong)floor(0.5 + fNumber * 10000000.0));
						nGgt = ImpGGT(nDiv, nFrac);
						if (nGgt > 1)
						{
							nDiv /= nGgt;
							nFrac /= nGgt;
						}
						if (nDiv > nBasis)
						{
							nGgt = ImpGGTRound(nDiv, nFrac);
							if (nGgt > 1)
							{
								nDiv /= nGgt;
								nFrac /= nGgt;
							}
						}
						if (nDiv > nBasis)
						{
							nDiv = nBasis;
							nFrac = ((sal_uLong)floor(0.5 + fNumber *
								pow(10.0, rInfo.nCntExp)));
							nGgt = ImpGGTRound(nDiv, nFrac);
							if (nGgt > 1)
							{
								nDiv /= nGgt;
								nFrac /= nGgt;
							}
						}
					}
				}
				else
				{
					nDiv = rInfo.nExpVal;
					double temp_val = fNumber * nDiv;
					nFrac = floor(temp_val);
					if (temp_val - nFrac >= 0.5)
						nFrac++;
					if (nFrac == nDiv)
					{
						fNum += 1.0;
						nDiv = 1;
						nFrac = 0;
					}
				}

                if (rInfo.nCntPre == 0)    // spurious break
                {
                    double fNum1 = fNum * (double)nDiv + (double)nFrac;
                    if (fNum1 > _D_MAX_U_LONG_)
                    {
                        OutString = rScanPtr->GetErrorString();
                        return false;
                    }
                    nFrac = (sal_uLong) floor(fNum1);
                    sStr.erase();
                }
                else if (fNum == 0.0 && nFrac != 0)
                    sStr.erase();
                else
                {
                    wchar_t aBuf[100];
                    swprintf( aBuf, sizeof(aBuf) / sizeof(wchar_t), L"%.f", fNum );   // simple rounded integer (#100211# - checked)
                    sStr = aBuf;
                    ImpTransliterate( sStr, NumFor[nIx].GetNatNum() );
                }
                if (rInfo.nCntPre > 0 && nFrac == 0)
                {
                    sFrac.erase();
                    sDiv.erase();
                }
                else
                {
                    sFrac = ImpIntToString( nIx, nFrac );
                    sDiv = ImpIntToString( nIx, nDiv );
                }

                sal_uInt16 j = nAnz-1;                  // last symbol->backward.
                uint16_t k;                       // denominator:
                bRes |= ImpNumberFill(sDiv, fNumber, k, j, nIx, NF_SYMBOLTYPE_FRAC);
                bool bCont = true;
                if (rInfo.nTypeArray[j] == NF_SYMBOLTYPE_FRAC)
                {
                    if (rInfo.nCntPre > 0 && nFrac == 0)
                        sDiv.insert(0, 1, L' ');
                    else
                        sDiv.insert( 0, 1, rInfo.sStrArray[j].at(0) );
                    if ( j )
                        j--;
                    else
                        bCont = false;
                }
                                                    // next counter:
                if ( !bCont )
                    sFrac.erase();
                else
                {
                    bRes |= ImpNumberFill(sFrac, fNumber, k, j, nIx, NF_SYMBOLTYPE_FRACBLANK);
                    if (rInfo.nTypeArray[j] == NF_SYMBOLTYPE_FRACBLANK)
                    {
                        sFrac.insert(0, rInfo.sStrArray[j]);
                        if ( j )
                            j--;
                        else
                            bCont = false;
                    }
                }
                                                    // further main number
                if ( !bCont )
                    sStr.erase();
                else
                {
                    k = sStr.size();                 // after last digit
					if (k > 0)
					{
						bRes |= ImpNumberFillWithThousands(sStr, fNumber, k, j, nIx,
							rInfo.nCntPre);
					}
                }
                if (bSign && !(nFrac == 0 && fNum == 0.0))
                    OutString.insert(0, 1, L'-');        // not -0
                OutString += sStr;
                OutString += sFrac;
                OutString += sDiv;
            }
            break;
            case NUMBERFORMAT_SCIENTIFIC:
            {
                bool bSign = false;
                if (fNumber < 0)
                {
                    if (nIx == 0)                       // not in rear
                        bSign = true;                   // Formaten
                    fNumber = -fNumber;
                }
                String sStr( doubleToUString( fNumber,
                            rtl_math_StringFormat_E,
                            rInfo.nCntPre + rInfo.nCntPost - 1, L'.' ));

                String ExpStr;
                short nExpSign = 1;
                uint16_t nExPos = sStr.find('E');
                if ( nExPos != STRING_NOTFOUND )
                {
                    // split into mantisse and exponent and get rid of "E+" or "E-"
                    uint16_t nExpStart = nExPos + 1;
                    switch ( sStr.at( nExpStart ) )
                    {
                        case L'-' :
                            nExpSign = -1;
                            // fallthru
                        case L'+' :
                            ++nExpStart;
                        break;
                    }
                    ExpStr = sStr.substr( nExpStart );    // part following the "E+"
                    sStr.erase( nExPos );
                    EraseAllChars(sStr, L'.');        // cut any decimal delimiter
                    if ( rInfo.nCntPre != 1 )       // rescale Exp
                    {
                        sal_Int32 nExp = std::stoi(ExpStr) * nExpSign;
                        nExp -= sal_Int32(rInfo.nCntPre)-1;
                        if ( nExp < 0 )
                        {
                            nExpSign = -1;
                            nExp = -nExp;
                        }
                        else
                            nExpSign = 1;
                        ExpStr = std::to_wstring( nExp );
                    }
                }
                sal_uInt16 j = nAnz-1;                  // last symbol
                uint16_t k;                       // position in ExpStr
                bRes |= ImpNumberFill(ExpStr, fNumber, k, j, nIx, NF_SYMBOLTYPE_EXP);

                uint16_t nZeros = 0;              // erase leading zeros
                while (nZeros < k && ExpStr.at(nZeros) == L'0')
                    ++nZeros;
                if (nZeros)
                    ExpStr.erase( 0, nZeros);

                bool bCont = true;
                if (rInfo.nTypeArray[j] == NF_SYMBOLTYPE_EXP)
                {
                    const String& rStr = rInfo.sStrArray[j];
                    if (nExpSign == -1)
                        ExpStr.insert(0, 1, L'-');
                    else if (rStr.size() > 1 && rStr.at(1) == L'+')
                        ExpStr.insert(0, 1, L'+');
                    ExpStr.insert(0, 1, rStr.at(0));
                    if ( j )
                        j--;
                    else
                        bCont = false;
                }
                                                    // further main number:
                if ( !bCont )
                    sStr.erase();
                else
                {
                    k = sStr.size();                 // after last digit
                    bRes |= ImpNumberFillWithThousands(sStr,fNumber, k,j,nIx,
                                            rInfo.nCntPre +
                                            rInfo.nCntPost);
                }
                if (bSign)
                    sStr.insert(0, 1, L'-');
                OutString = sStr;
                OutString += ExpStr;
            }
            break;
        }
    }
    return bRes;
}

bool SvNumberformat::GetOutputString(double fNumber, std::string& OutString)
{
	String out_str;
    duckdb_excel::Color *pColor = nullptr;
	bool result = GetOutputString(fNumber, out_str, &pColor);

	std::string temp(out_str.length(), ' ');
	std::copy(out_str.begin(), out_str.end(), temp.begin());
	OutString = temp;

	return result;
}

bool SvNumberformat::ImpGetTimeOutput(double fNumber,
                                   sal_uInt16 nIx,
                                   String& OutString)
{
    //bool bCalendarSet = false;
    double fNumberOrig = fNumber;
    bool bRes = false;
    bool bSign = false;
    if (fNumber < 0.0)
    {
        fNumber = -fNumber;
        if (nIx == 0)
            bSign = true;
    }
    const ImpSvNumberformatInfo& rInfo = NumFor[nIx].Info();
    if (rInfo.bThousand)       // []-Format
    {
        if (fNumber > 1.0E10)               // too large
        {
            OutString = rScanPtr->GetErrorString();
            return false;
        }
    }
    else
        fNumber -= floor(fNumber);          // otherwise cut off date
    bool bInputLine;
    uint16_t nCntPost;
    if ( rScanPtr->GetStandardPrec() == 300 &&
            0 < rInfo.nCntPost && rInfo.nCntPost < 7 )
    {   // round at 7 decimals (+5 of 86400 == 12 significant digits)
        bInputLine = true;
        nCntPost = 7;
    }
    else
    {
        bInputLine = false;
        nCntPost = uint16_t(rInfo.nCntPost);
    }
    if (bSign && !rInfo.bThousand)     // no []-Format
        fNumber = 1.0 - fNumber;        // "reciprocal"
    double fTime = fNumber * 86400.0;
    fTime = math_round( fTime, int(nCntPost) );
    if (bSign && fTime == 0.0)
        bSign = false;                      // not -00:00:00

    if( floor( fTime ) > _D_MAX_U_LONG_ )
    {
        OutString = rScanPtr->GetErrorString();
        return false;
    }
    sal_uLong nSeconds = (sal_uLong)floor( fTime );

    String sSecStr( doubleToUString( fTime-nSeconds,
                rtl_math_StringFormat_F, int(nCntPost), L'.'));
    EraseLeadingChars(sSecStr, L'0');
    EraseLeadingChars(sSecStr, L'.');
    if ( bInputLine )
    {
        EraseTrailingChars(sSecStr, L'0');
        if ( sSecStr.size() < uint16_t(rInfo.nCntPost) )
            sSecStr.insert( sSecStr.size(), rInfo.nCntPost - sSecStr.size(), L'0' );
        ImpTransliterate( sSecStr, NumFor[nIx].GetNatNum() );
        nCntPost = sSecStr.size();
    }
    else
        ImpTransliterate( sSecStr, NumFor[nIx].GetNatNum() );

    uint16_t nSecPos = 0;                 // For digits
                                            // process
    sal_uLong nHour, nMin, nSec;
    if (!rInfo.bThousand)      // no [] Format
    {
        nHour = (nSeconds/3600) % 24;
        nMin = (nSeconds%3600) / 60;
        nSec = nSeconds%60;
    }
    else if (rInfo.nThousand == 3) // [ss]
    {
        nHour = 0;
        nMin = 0;
        nSec = nSeconds;
    }
    else if (rInfo.nThousand == 2) // [mm]:ss
    {
        nHour = 0;
        nMin = nSeconds / 60;
        nSec = nSeconds % 60;
    }
    else if (rInfo.nThousand == 1) // [hh]:mm:ss
    {
        nHour = nSeconds / 3600;
        nMin = (nSeconds%3600) / 60;
        nSec = nSeconds%60;
    }
	else {
		// TODO  What should these be set to?
		nHour = 0;
		nMin  = 0;
		nSec  = 0;
	}

    sal_Unicode cAmPm = L' ';                   // a or p
    if (rInfo.nCntExp)     // AM/PM
    {
        if (nHour == 0)
        {
            nHour = 12;
            cAmPm = L'a';
        }
        else if (nHour < 12)
            cAmPm = L'a';
        else
        {
            cAmPm = L'p';
            if (nHour > 12)
                nHour -= 12;
        }
    }
    const sal_uInt16 nAnz = NumFor[nIx].GetnAnz();
    for (sal_uInt16 i = 0; i < nAnz; i++)
    {
        switch (rInfo.nTypeArray[i])
        {
            case NF_SYMBOLTYPE_STAR:
                if( bStarFlag )
                {
                    OutString += (sal_Unicode) 0x1B;
                    OutString += rInfo.sStrArray[i].at(1);
                    bRes = true;
                }
                break;
            case NF_SYMBOLTYPE_BLANK:
                InsertBlanks( OutString, OutString.size(),
                    rInfo.sStrArray[i].at(0) );
                break;
            case NF_SYMBOLTYPE_STRING:
            case NF_SYMBOLTYPE_CURRENCY:
            case NF_SYMBOLTYPE_DATESEP:
            case NF_SYMBOLTYPE_TIMESEP:
            case NF_SYMBOLTYPE_TIME100SECSEP:
                OutString += rInfo.sStrArray[i];
                break;
            case NF_SYMBOLTYPE_DIGIT:
            {
                uint16_t nLen = ( bInputLine && i > 0 &&
                    (rInfo.nTypeArray[i-1] == NF_SYMBOLTYPE_STRING ||
                     rInfo.nTypeArray[i-1] == NF_SYMBOLTYPE_TIME100SECSEP) ?
                    nCntPost : rInfo.sStrArray[i].size() );
                for (uint16_t j = 0; j < nLen && nSecPos < nCntPost; j++)
                {
                    OutString += sSecStr.at(nSecPos);
                    nSecPos++;
                }
            }
            break;
            case NF_KEY_AMPM:               // AM/PM
            {
                if (cAmPm == L'a')
                    OutString += pFormatter->getTimeAM();
                else
                    OutString += pFormatter->getTimePM();
            }
            break;
            case NF_KEY_AP:                 // A/P
            {
                if (cAmPm == L'a')
                    OutString += L'a';
                else
                    OutString += L'p';
            }
            break;
            case NF_KEY_MI:                 // M
                OutString += ImpIntToString( nIx, nMin );
            break;
            case NF_KEY_MMI:                // MM
                OutString += ImpIntToString( nIx, nMin, 2 );
            break;
            case NF_KEY_H:                  // H
                OutString += ImpIntToString( nIx, nHour );
            break;
            case NF_KEY_HH:                 // HH
                OutString += ImpIntToString( nIx, nHour, 2 );
            break;
            case NF_KEY_S:                  // S
                OutString += ImpIntToString( nIx, nSec );
            break;
            case NF_KEY_SS:                 // SS
                OutString += ImpIntToString( nIx, nSec, 2 );
            break;
            default:
            break;
        }
    }
    if (bSign && rInfo.bThousand)
        OutString.insert(0, L"-");
    return bRes;
}


bool SvNumberformat::ImpIsOtherCalendar( const ImpSvNumFor& rNumFor ) const
{
    return false;
}


void SvNumberformat::SwitchToOtherCalendar( String& rOrgCalendar,
        double& fOrgDateTime ) const
{
}


void SvNumberformat::SwitchToGregorianCalendar( const String& rOrgCalendar,
        double fOrgDateTime ) const
{
}


bool SvNumberformat::ImpFallBackToGregorianCalendar( String& rOrgCalendar, double& fOrgDateTime )
{
    return false;
}


bool SvNumberformat::ImpSwitchToSpecifiedCalendar( String& rOrgCalendar,
        double& fOrgDateTime, const ImpSvNumFor& rNumFor ) const
{
    return false;
}


// static
void SvNumberformat::ImpAppendEraG( String& OutString, sal_Int16 nNatNum )
{
    OutString += pFormatter->GetCalendar()->getDisplayString(CalendarDisplayCode::SHORT_ERA, nNatNum);
}


bool SvNumberformat::ImpGetDateOutput(double fNumber,
                                   sal_uInt16 nIx,
                                   String& OutString)
{
    bool bRes = false;
    Calendar* rCal = pFormatter->GetCalendar();
    double fDiff = DateTime(*(rCal->GetNullDate())) - rCal->getEpochStart();
    fNumber += fDiff;
    rCal->setLocalDateTime( fNumber );
    String aOrgCalendar;        // empty => not changed yet
    double fOrgDateTime;
    bool bOtherCalendar = ImpIsOtherCalendar( NumFor[nIx] );
    if ( bOtherCalendar )
        SwitchToOtherCalendar( aOrgCalendar, fOrgDateTime );
    if ( ImpFallBackToGregorianCalendar( aOrgCalendar, fOrgDateTime ) )
        bOtherCalendar = false;
    const ImpSvNumberformatInfo& rInfo = NumFor[nIx].Info();
    const sal_uInt16 nAnz = NumFor[nIx].GetnAnz();
    sal_Int16 nNatNum = NumFor[nIx].GetNatNum().GetNatNum();
    for (sal_uInt16 i = 0; i < nAnz; i++)
    {
        switch (rInfo.nTypeArray[i])
        {
            case NF_SYMBOLTYPE_CALENDAR :
            break;
            case NF_SYMBOLTYPE_STAR:
                if( bStarFlag )
                {
                    OutString += (sal_Unicode) 0x1B;
                    OutString += rInfo.sStrArray[i].at(1);
                    bRes = true;
                }
            break;
            case NF_SYMBOLTYPE_BLANK:
                InsertBlanks( OutString, OutString.size(),
                    rInfo.sStrArray[i].at(0) );
            break;
            case NF_SYMBOLTYPE_STRING:
            case NF_SYMBOLTYPE_CURRENCY:
            case NF_SYMBOLTYPE_DATESEP:
            case NF_SYMBOLTYPE_TIMESEP:
            case NF_SYMBOLTYPE_TIME100SECSEP:
                OutString += rInfo.sStrArray[i];
            break;
            case NF_KEY_M:                  // M
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_MONTH, nNatNum );
            break;
            case NF_KEY_MM:                 // MM
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_MONTH, nNatNum );
            break;
            case NF_KEY_MMM:                // MMM
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_MONTH_NAME, nNatNum );
            break;
            case NF_KEY_MMMM:               // MMMM
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_MONTH_NAME, nNatNum );
            break;
            case NF_KEY_MMMMM:              // MMMMM
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_MONTH_NAME, nNatNum ).at(0);
            break;
            case NF_KEY_Q:                  // Q
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_QUARTER, nNatNum );
            break;
            case NF_KEY_QQ:                 // QQ
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_QUARTER, nNatNum );
            break;
            case NF_KEY_D:                  // D
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_DAY, nNatNum );
            break;
            case NF_KEY_DD:                 // DD
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_DAY, nNatNum );
            break;
            case NF_KEY_DDD:                // DDD
            {
                if ( bOtherCalendar )
                    SwitchToGregorianCalendar( aOrgCalendar, fOrgDateTime );
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_DAY_NAME, nNatNum );
                if ( bOtherCalendar )
                    SwitchToOtherCalendar( aOrgCalendar, fOrgDateTime );
            }
            break;
            case NF_KEY_DDDD:               // DDDD
            {
                if ( bOtherCalendar )
                    SwitchToGregorianCalendar( aOrgCalendar, fOrgDateTime );
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_DAY_NAME, nNatNum );
                if ( bOtherCalendar )
                    SwitchToOtherCalendar( aOrgCalendar, fOrgDateTime );
            }
            break;
            case NF_KEY_YY:                 // YY
            {
                if ( bOtherCalendar )
                    SwitchToGregorianCalendar( aOrgCalendar, fOrgDateTime );
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_YEAR, nNatNum );
                if ( bOtherCalendar )
                    SwitchToOtherCalendar( aOrgCalendar, fOrgDateTime );
            }
            break;
            case NF_KEY_YYYY:               // YYYY
            {
                if ( bOtherCalendar )
                    SwitchToGregorianCalendar( aOrgCalendar, fOrgDateTime );
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_YEAR, nNatNum );
                if ( bOtherCalendar )
                    SwitchToOtherCalendar( aOrgCalendar, fOrgDateTime );
            }
            break;
            case NF_KEY_EC:                 // E
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_YEAR, nNatNum );
            break;
            case NF_KEY_EEC:                // EE
            case NF_KEY_R:                  // R
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_YEAR, nNatNum );
            break;
            case NF_KEY_NN:                 // NN
            case NF_KEY_AAA:                // AAA
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_DAY_NAME, nNatNum );
            break;
            case NF_KEY_NNN:                // NNN
            case NF_KEY_AAAA:               // AAAA
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_DAY_NAME, nNatNum );
            break;
            case NF_KEY_NNNN:               // NNNN
            {
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_DAY_NAME, nNatNum );
                OutString += pFormatter->getLongDateDayOfWeekSep();
            }
            break;
            case NF_KEY_WW :                // WW
            {
                sal_Int16 nVal = rCal->getValue( CalendarFieldIndex::WEEK_OF_YEAR );
                OutString += ImpIntToString( nIx, nVal );
            }
            break;
            case NF_KEY_G:                  // G
                ImpAppendEraG( OutString, nNatNum );
            break;
            case NF_KEY_GG:                 // GG
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_ERA, nNatNum );
            break;
            case NF_KEY_GGG:                // GGG
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_ERA, nNatNum );
            break;
            case NF_KEY_RR:                 // RR => GGGEE
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_YEAR_AND_ERA, nNatNum );
            break;
        }
    }
    return bRes;
}

bool SvNumberformat::ImpGetDateTimeOutput(double fNumber,
                                       sal_uInt16 nIx,
                                       String& OutString)
{
    bool bRes = false;

    Calendar* rCal = pFormatter->GetCalendar();
    double fDiff = DateTime(*(rCal->GetNullDate())) - rCal->getEpochStart();
    fNumber += fDiff;

    const ImpSvNumberformatInfo& rInfo = NumFor[nIx].Info();
    bool bInputLine;
    uint16_t nCntPost;
    if ( rScanPtr->GetStandardPrec() == 300 &&
            0 < rInfo.nCntPost && rInfo.nCntPost < 7 )
    {   // round at 7 decimals (+5 of 86400 == 12 significant digits)
        bInputLine = true;
        nCntPost = 7;
    }
    else
    {
        bInputLine = false;
        nCntPost = uint16_t(rInfo.nCntPost);
    }
    double fTime = (fNumber - floor( fNumber )) * 86400.0;
    fTime = math_round( fTime, int(nCntPost) );
    if (fTime >= 86400.0)
    {
        // result of fNumber==x.999999999... rounded up, use correct date/time
        fTime -= 86400.0;
        fNumber = floor( fNumber + 0.5) + fTime;
    }
    rCal->setLocalDateTime( fNumber );

    String aOrgCalendar;        // empty => not changed yet
    double fOrgDateTime;
    bool bOtherCalendar = ImpIsOtherCalendar( NumFor[nIx] );
    if ( bOtherCalendar )
        SwitchToOtherCalendar( aOrgCalendar, fOrgDateTime );
    if ( ImpFallBackToGregorianCalendar( aOrgCalendar, fOrgDateTime ) )
        bOtherCalendar = false;
    sal_Int16 nNatNum = NumFor[nIx].GetNatNum().GetNatNum();

    sal_uLong nSeconds = (sal_uLong)floor( fTime );
    String sSecStr( doubleToUString( fTime-nSeconds,
                rtl_math_StringFormat_F, int(nCntPost), L'.'));
    EraseLeadingChars(sSecStr, L'0');
    EraseLeadingChars(sSecStr, L'.');
    if ( bInputLine )
    {
        EraseTrailingChars(sSecStr, '0');
        if ( sSecStr.size() < uint16_t(rInfo.nCntPost) )
            sSecStr.insert(sSecStr.size(), uint16_t(rInfo.nCntPost) - sSecStr.size(), L'0' );
        ImpTransliterate( sSecStr, NumFor[nIx].GetNatNum() );
        nCntPost = sSecStr.size();
    }
    else
        ImpTransliterate( sSecStr, NumFor[nIx].GetNatNum() );

    uint16_t nSecPos = 0;                     // Zum Ziffernweisen
                                            // abarbeiten
    sal_uLong nHour, nMin, nSec;
    if (!rInfo.bThousand)      // [] Format
    {
        nHour = (nSeconds/3600) % 24;
        nMin = (nSeconds%3600) / 60;
        nSec = nSeconds%60;
    }
    else if (rInfo.nThousand == 3) // [ss]
    {
        nHour = 0;
        nMin = 0;
        nSec = nSeconds;
    }
    else if (rInfo.nThousand == 2) // [mm]:ss
    {
        nHour = 0;
        nMin = nSeconds / 60;
        nSec = nSeconds % 60;
    }
    else if (rInfo.nThousand == 1) // [hh]:mm:ss
    {
        nHour = nSeconds / 3600;
        nMin = (nSeconds%3600) / 60;
        nSec = nSeconds%60;
    }
    else {
        nHour = 0;  // TODO What should these values be?
        nMin  = 0;
        nSec  = 0;
    }
    sal_Unicode cAmPm = L' ';                   // a oder p
    if (rInfo.nCntExp)     // AM/PM
    {
        if (nHour == 0)
        {
            nHour = 12;
            cAmPm = L'a';
        }
        else if (nHour < 12)
            cAmPm = L'a';
        else
        {
            cAmPm = L'p';
            if (nHour > 12)
                nHour -= 12;
        }
    }
    const sal_uInt16 nAnz = NumFor[nIx].GetnAnz();
    for (sal_uInt16 i = 0; i < nAnz; i++)
    {
        switch (rInfo.nTypeArray[i])
        {
            case NF_SYMBOLTYPE_CALENDAR :
                break;
            case NF_SYMBOLTYPE_STAR:
                if( bStarFlag )
                {
                    OutString += (sal_Unicode) 0x1B;
                    OutString += rInfo.sStrArray[i].at(1);
                    bRes = true;
                }
                break;
            case NF_SYMBOLTYPE_BLANK:
                InsertBlanks( OutString, OutString.size(),
                    rInfo.sStrArray[i].at(0) );
                break;
            case NF_SYMBOLTYPE_STRING:
            case NF_SYMBOLTYPE_CURRENCY:
            case NF_SYMBOLTYPE_DATESEP:
            case NF_SYMBOLTYPE_TIMESEP:
            case NF_SYMBOLTYPE_TIME100SECSEP:
                OutString += rInfo.sStrArray[i];
                break;
            case NF_SYMBOLTYPE_DIGIT:
            {
                uint16_t nLen = ( bInputLine && i > 0 &&
                    (rInfo.nTypeArray[i-1] == NF_SYMBOLTYPE_STRING ||
                     rInfo.nTypeArray[i-1] == NF_SYMBOLTYPE_TIME100SECSEP) ?
                    nCntPost : rInfo.sStrArray[i].size() );
                for (uint16_t j = 0; j < nLen && nSecPos < nCntPost; j++)
                {
                    OutString += sSecStr.at(nSecPos);
                    nSecPos++;
                }
            }
            break;
            case NF_KEY_AMPM:               // AM/PM
            {
                if (cAmPm == L'a')
                    OutString += pFormatter->getTimeAM();
                else
                    OutString += pFormatter->getTimePM();
            }
            break;
            case NF_KEY_AP:                 // A/P
            {
                if (cAmPm == L'a')
                    OutString += L'a';
                else
                    OutString += L'p';
            }
            break;
            case NF_KEY_MI:                 // M
                OutString += ImpIntToString( nIx, nMin );
            break;
            case NF_KEY_MMI:                // MM
                OutString += ImpIntToString( nIx, nMin, 2 );
            break;
            case NF_KEY_H:                  // H
                OutString += ImpIntToString( nIx, nHour );
            break;
            case NF_KEY_HH:                 // HH
                OutString += ImpIntToString( nIx, nHour, 2 );
            break;
            case NF_KEY_S:                  // S
                OutString += ImpIntToString( nIx, nSec );
            break;
            case NF_KEY_SS:                 // SS
                OutString += ImpIntToString( nIx, nSec, 2 );
            break;
            case NF_KEY_M:                  // M
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_MONTH, nNatNum );
            break;
            case NF_KEY_MM:                 // MM
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_MONTH, nNatNum );
            break;
            case NF_KEY_MMM:                // MMM
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_MONTH_NAME, nNatNum );
            break;
            case NF_KEY_MMMM:               // MMMM
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_MONTH_NAME, nNatNum );
            break;
            case NF_KEY_MMMMM:              // MMMMM
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_MONTH_NAME, nNatNum ).at(0);
            break;
            case NF_KEY_Q:                  // Q
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_QUARTER, nNatNum );
            break;
            case NF_KEY_QQ:                 // QQ
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_QUARTER, nNatNum );
            break;
            case NF_KEY_D:                  // D
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_DAY, nNatNum );
            break;
            case NF_KEY_DD:                 // DD
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_DAY, nNatNum );
            break;
            case NF_KEY_DDD:                // DDD
            {
                if ( bOtherCalendar )
                    SwitchToGregorianCalendar( aOrgCalendar, fOrgDateTime );
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_DAY_NAME, nNatNum );
                if ( bOtherCalendar )
                    SwitchToOtherCalendar( aOrgCalendar, fOrgDateTime );
            }
            break;
            case NF_KEY_DDDD:               // DDDD
            {
                if ( bOtherCalendar )
                    SwitchToGregorianCalendar( aOrgCalendar, fOrgDateTime );
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_DAY_NAME, nNatNum );
                if ( bOtherCalendar )
                    SwitchToOtherCalendar( aOrgCalendar, fOrgDateTime );
            }
            break;
            case NF_KEY_YY:                 // YY
            {
                if ( bOtherCalendar )
                    SwitchToGregorianCalendar( aOrgCalendar, fOrgDateTime );
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_YEAR, nNatNum );
                if ( bOtherCalendar )
                    SwitchToOtherCalendar( aOrgCalendar, fOrgDateTime );
            }
            break;
            case NF_KEY_YYYY:               // YYYY
            {
                if ( bOtherCalendar )
                    SwitchToGregorianCalendar( aOrgCalendar, fOrgDateTime );
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_YEAR, nNatNum );
                if ( bOtherCalendar )
                    SwitchToOtherCalendar( aOrgCalendar, fOrgDateTime );
            }
            break;
            case NF_KEY_EC:                 // E
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_YEAR, nNatNum );
            break;
            case NF_KEY_EEC:                // EE
            case NF_KEY_R:                  // R
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_YEAR, nNatNum );
            break;
            case NF_KEY_NN:                 // NN
            case NF_KEY_AAA:                // AAA
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_DAY_NAME, nNatNum );
            break;
            case NF_KEY_NNN:                // NNN
            case NF_KEY_AAAA:               // AAAA
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_DAY_NAME, nNatNum );
            break;
            case NF_KEY_NNNN:               // NNNN
            {
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_DAY_NAME, nNatNum );
                OutString += pFormatter->getLongDateDayOfWeekSep();
            }
            break;
            case NF_KEY_WW :                // WW
            {
                sal_Int16 nVal = rCal->getValue( CalendarFieldIndex::WEEK_OF_YEAR );
                OutString += ImpIntToString( nIx, nVal );
            }
            break;
            case NF_KEY_G:                  // G
                ImpAppendEraG( OutString, nNatNum );
            break;
            case NF_KEY_GG:                 // GG
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::SHORT_ERA, nNatNum );
            break;
            case NF_KEY_GGG:                // GGG
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_ERA, nNatNum );
            break;
            case NF_KEY_RR:                 // RR => GGGEE
                OutString += rCal->getDisplayString(
                        CalendarDisplayCode::LONG_YEAR_AND_ERA, nNatNum );
            break;
        }
    }
    return bRes;
}

bool SvNumberformat::ImpGetNumberOutput(double fNumber,
                                     sal_uInt16 nIx,
                                     String& OutString)
{
    bool bRes = false;
    bool bSign;
    if (fNumber < 0.0)
    {
        if (nIx == 0)                       // nicht in hinteren
            bSign = true;                   // Formaten
        else
            bSign = false;
        fNumber = -fNumber;
    }
    else
    {
        bSign = false;
        if ( fabs( fNumber ) < 0.0 )
            fNumber = -fNumber;     // yes, -0.0 is possible, eliminate '-'
    }
    const ImpSvNumberformatInfo& rInfo = NumFor[nIx].Info();
    if (rInfo.eScannedType == NUMBERFORMAT_PERCENT)
    {
        if (fNumber < _D_MAX_D_BY_100)
            fNumber *= 100.0;
        else
        {
            OutString = rScanPtr->GetErrorString();
            return false;
        }
    }
    sal_uInt16 i, j;
    uint16_t k;
    String sStr;
    long nPrecExp;
    bool bInteger = false;
    if ( rInfo.nThousand != FLAG_STANDARD_IN_FORMAT )
    {   // special formatting only if no GENERAL keyword in format code
        const sal_uInt16 nThousand = rInfo.nThousand;
        for (i = 0; i < nThousand; i++)
        {
           if (fNumber > _D_MIN_M_BY_1000)
               fNumber /= 1000.0;
           else
               fNumber = 0.0;
        }
        if (fNumber > 0.0)
            nPrecExp = GetPrecExp( fNumber );
        else
            nPrecExp = 0;
        if (rInfo.nCntPost)    // decimal places
        {
            if (rInfo.nCntPost + nPrecExp > 15 && nPrecExp < 15)
            {
                sStr = doubleToUString( fNumber,
                        rtl_math_StringFormat_F, 15-nPrecExp, L'.');
                for (long l = 15-nPrecExp; l < (long) rInfo.nCntPost; l++)
                    sStr += L'0';
            }
            else
                sStr = doubleToUString( fNumber,
                        rtl_math_StringFormat_F, rInfo.nCntPost, L'.' );
            EraseLeadingChars(sStr, L'0');        // leading zeros removed
        }
        else if (fNumber == 0.0)            // Null
        {
        }
        else                                // Integer
        {
            sStr = doubleToUString( fNumber,
                    rtl_math_StringFormat_F, 0, L'.');
            EraseLeadingChars(sStr, L'0');        // leading zeros removed
        }
        uint16_t nPoint = sStr.find( L'.' );
        if ( nPoint != STRING_NOTFOUND )
        {
            const sal_Unicode* p = sStr.data() + nPoint;
            while ( *++p == L'0' )
                ;
            if ( !*p )
                bInteger = true;
            sStr.erase( nPoint, 1 );            //  . remove
        }
        if (bSign &&
            (sStr.size() == 0 || std::count(sStr.begin(), sStr.end(), L'0') == sStr.size()+1))   // nur 00000
            bSign = false;              // not -0.00
    }                                   // End of != FLAG_STANDARD_IN_FORMAT

                                        // from back to front
                                        // edit:
    k = sStr.size();                     // after last digit
    j = NumFor[nIx].GetnAnz()-1;        // last Symbol
                                        // decimal places:
    if (rInfo.nCntPost > 0)
    {
        bool bTrailing = true;          // whether end zeros?
        bool bFilled = false;           // if refilled ?
        short nType;
        while (j > 0 &&                 // 
           (nType = rInfo.nTypeArray[j]) != NF_SYMBOLTYPE_DECSEP)
        {
            switch ( nType )
            {
                case NF_SYMBOLTYPE_STAR:
                    if( bStarFlag )
                    {
                        sStr.insert( k /*++*/, 1, (sal_Unicode) 0x1B );
                        sStr.insert(k, 1, rInfo.sStrArray[j].at(1));
                        bRes = true;
                    }
                    break;
                case NF_SYMBOLTYPE_BLANK:
                    /*k = */ InsertBlanks( sStr,k,rInfo.sStrArray[j].at(0) );
                    break;
                case NF_SYMBOLTYPE_STRING:
                case NF_SYMBOLTYPE_CURRENCY:
                case NF_SYMBOLTYPE_PERCENT:
                    sStr.insert(k, rInfo.sStrArray[j]);
                    break;
                case NF_SYMBOLTYPE_THSEP:
                    if (rInfo.nThousand == 0)
                        sStr.insert(k, rInfo.sStrArray[j]);
                break;
                case NF_SYMBOLTYPE_DIGIT:
                {
                    const String& rStr = rInfo.sStrArray[j];
                    const sal_Unicode* p1 = rStr.data();
                    const sal_Unicode* p = p1 + rStr.size();
                    while ( p1 < p-- )
                    {
                        const sal_Unicode c = *p;
                        k--;
                        if ( sStr.at(k) != L'0' )
                            bTrailing = false;
                        if (bTrailing)
                        {
                            if ( c == L'0' )
                                bFilled = true;
                            else if ( c == L'-' )
                            {
                                if ( bInteger )
                                    sStr[k] = L'-';
                                bFilled = true;
                            }
                            else if ( c == L'?' )
                            {
                                sStr[k] = L' ';
                                bFilled = true;
                            }
                            else if ( !bFilled )    // #
                                sStr.erase(k,1);
                        }
                    }                           // of for
                }                               // of case digi
                break;
                case NF_KEY_CCC:                // CCC-Currency
                    sStr.insert(k, rScanPtr->GetCurAbbrev());
                break;
                case NF_KEY_GENERAL:            // Standard im String
                {
                    String sNum;
                    ImpGetOutputStandard(fNumber, sNum);
                    EraseLeadingChars(sNum, L'-');
                    sStr.insert(k, sNum);
                }
                break;
                default:
                break;
            }                                   // of switch
            j--;
        }                                       // of while
    }                                           // of decimal point

    bRes |= ImpNumberFillWithThousands(sStr, fNumber, k, j, nIx, // fill up with if necessary .
                            rInfo.nCntPre);
    if ( rInfo.nCntPost > 0 )
    {
        const String& rDecSep = pFormatter->GetNumDecimalSep();
        uint16_t nLen = rDecSep.size();
        if ( sStr.size() > nLen && sStr.substr( sStr.size() - nLen, nLen ) == rDecSep )
            sStr.erase( sStr.size() - nLen );        // no decimals => strip DecSep
    }
    if (bSign)
        sStr.insert(0, 1, L'-');
    ImpTransliterate( sStr, NumFor[nIx].GetNatNum() );
    OutString = sStr;
    return bRes;
}

bool SvNumberformat::ImpNumberFillWithThousands(
                                String& sStr,       // number string
                                double& rNumber,    // number
                                uint16_t k,       // position within string
                                sal_uInt16 j,           // symbol index within format code
                                sal_uInt16 nIx,         // subformat index
                                sal_uInt16 nDigCnt)     // count of integer digits in format
{
    bool bRes = false;
    uint16_t nLeadingStringChars = 0; // inserted StringChars before number
    uint16_t nDigitCount = 0;         // count of integer digits from the right
    bool bStop = false;
    const ImpSvNumberformatInfo& rInfo = NumFor[nIx].Info();
    // no normal thousands separators if number divided by thousands
    bool bDoThousands = (rInfo.nThousand == 0);
	DigitGroupingIterator aGrouping(pFormatter->getDigitGrouping());
    while (!bStop)                                      // backwards
    {
        if (j == 0)
            bStop = true;
        switch (rInfo.nTypeArray[j])
        {
            case NF_SYMBOLTYPE_DECSEP:
				aGrouping.reset();
                // fall thru
            case NF_SYMBOLTYPE_STRING:
            case NF_SYMBOLTYPE_CURRENCY:
            case NF_SYMBOLTYPE_PERCENT:
                sStr.insert(k, rInfo.sStrArray[j]);
                if ( k == 0 )
                    nLeadingStringChars =
                        nLeadingStringChars + rInfo.sStrArray[j].size();
            break;
            case NF_SYMBOLTYPE_STAR:
                if( bStarFlag )
                {
                    sStr.insert( k/*++*/, 1, (sal_Unicode) 0x1B );
                    sStr.insert(k, 1, rInfo.sStrArray[j].at(1));
                    bRes = true;
                }
                break;
            case NF_SYMBOLTYPE_BLANK:
                /*k = */ InsertBlanks( sStr,k,rInfo.sStrArray[j].at(0) );
                break;
            case NF_SYMBOLTYPE_THSEP:
            {
                // #i7284# #102685# Insert separator also if number is divided
                // by thousands and the separator is specified somewhere in
                // between and not only at the end.
                // #i12596# But do not insert if it's a parenthesized negative
                // format like (#,)
                // In fact, do not insert if divided and regex [0#,],[^0#] and
                // no other digit symbol follows (which was already detected
                // during scan of format code, otherwise there would be no
                // division), else do insert. Same in ImpNumberFill() below.
                if ( !bDoThousands && j < NumFor[nIx].GetnAnz()-1 )
                    bDoThousands = ((j == 0) ||
                            (rInfo.nTypeArray[j-1] != NF_SYMBOLTYPE_DIGIT &&
                             rInfo.nTypeArray[j-1] != NF_SYMBOLTYPE_THSEP) ||
                            (rInfo.nTypeArray[j+1] == NF_SYMBOLTYPE_DIGIT));
                if ( bDoThousands )
                {
                    if (k > 0)
                        sStr.insert(k, rInfo.sStrArray[j]);
                    else if (nDigitCount < nDigCnt)
                    {
                        // Leading '#' displays nothing (e.g. no leading
                        // separator for numbers <1000 with #,##0 format).
                        // Leading '?' displays blank.
                        // Everything else, including nothing, displays the
                        // separator.
                        sal_Unicode cLeader = 0;
                        if (j > 0 && rInfo.nTypeArray[j-1] == NF_SYMBOLTYPE_DIGIT)
                        {
                            const String& rStr = rInfo.sStrArray[j-1];
                            uint16_t nLen = rStr.size();
                            if (nLen)
                                cLeader = rStr.at(nLen-1);
                        }
                        switch (cLeader)
                        {
                            case L'#':
                                ;   // nothing
                                break;
                            case L'?':
                                // erAck: 2008-04-03T16:24+0200
                                // Actually this currently isn't executed
                                // because the format scanner in the context of
                                // "?," doesn't generate a group separator but
                                // a literal ',' character instead that is
                                // inserted unconditionally. Should be changed
                                // on some occasion.
                                sStr.insert(k, 1, L' ');
                                break;
                            default:
                                sStr.insert(k, rInfo.sStrArray[j]);
                        }
                    }
					aGrouping.advance();
                }
            }
            break;
            case NF_SYMBOLTYPE_DIGIT:
            {
                const String& rStr = rInfo.sStrArray[j];
                const sal_Unicode* p1 = rStr.data();
                const sal_Unicode* p = p1 + rStr.size();
                while ( p1 < p-- )
                {
                    nDigitCount++;
                    if (k > 0)
                        k--;
                    else
                    {
                        switch (*p)
                        {
                            case L'0':
                                sStr.insert(0, 1, L'0');
                                break;
                            case L'?':
                                sStr.insert(0, 1, L' ');
                                break;
                        }
                    }
                    if (nDigitCount == nDigCnt && k > 0)
                    {   // more digits than specified
                        ImpDigitFill(sStr, 0, k, nIx, nDigitCount, aGrouping);
                    }
                }
            }
            break;
            case NF_KEY_CCC:                        // CCC currency
                sStr.insert(k, rScanPtr->GetCurAbbrev());
            break;
            case NF_KEY_GENERAL:                    // "General" in string
            {
                String sNum;
                ImpGetOutputStandard(rNumber, sNum);
                EraseLeadingChars(sNum, L'-');
                sStr.insert(k, sNum);
            }
            break;

            default:
            break;
        } // switch
        j--;        // next format code string
    } // while
    k = k + nLeadingStringChars;    // MSC converts += to int and then warns, so ...
    if (k > nLeadingStringChars)
        ImpDigitFill(sStr, nLeadingStringChars, k, nIx, nDigitCount, aGrouping);
    return bRes;
}

void SvNumberformat::ImpDigitFill(
        String& sStr,                   // number string
        uint16_t nStart,              // start of digits
        uint16_t& k,                  // position within string
        sal_uInt16 nIx,                     // subformat index
        uint16_t & nDigitCount,       // count of integer digits from the right so far
        DigitGroupingIterator & rGrouping )    // current grouping
{
	if (NumFor[nIx].Info().bThousand)               // only if grouping
	{                                               // fill in separators
		const String& rThousandSep = pFormatter->GetNumThousandSep();
		while (k > nStart)
		{
			if (nDigitCount == rGrouping.getPos())
			{
				sStr.insert(k, rThousandSep);
				rGrouping.advance();
			}
			nDigitCount++;
			k--;
		}
	}
	else                                            // simply skip
		k = nStart;
}

bool SvNumberformat::ImpNumberFill( String& sStr,       // number string
                                double& rNumber,        // number for "General" format
                                uint16_t& k,          // position within string
                                sal_uInt16& j,              // symbol index within format code
                                sal_uInt16 nIx,             // subformat index
                                short eSymbolType )     // type of stop condition
{
    bool bRes = false;
    k = sStr.size();                         // behind last digit
    const ImpSvNumberformatInfo& rInfo = NumFor[nIx].Info();
    // no normal thousands separators if number divided by thousands
    bool bDoThousands = (rInfo.nThousand == 0);
    short nType;
    while (j > 0 && (nType = rInfo.nTypeArray[j]) != eSymbolType )
    {                                       // rueckwaerts:
        switch ( nType )
        {
            case NF_SYMBOLTYPE_STAR:
                if( bStarFlag )
                {
                    sStr.insert( k++, 1, sal_Unicode(0x1B) );
                    sStr.insert(k, 1, rInfo.sStrArray[j].at(1));
                    bRes = true;
                }
                break;
            case NF_SYMBOLTYPE_BLANK:
                k = InsertBlanks( sStr,k,rInfo.sStrArray[j].at(0) );
                break;
            case NF_SYMBOLTYPE_THSEP:
            {
                // Same as in ImpNumberFillWithThousands() above, do not insert
                // if divided and regex [0#,],[^0#] and no other digit symbol
                // follows (which was already detected during scan of format
                // code, otherwise there would be no division), else do insert.
                if ( !bDoThousands && j < NumFor[nIx].GetnAnz()-1 )
                    bDoThousands = ((j == 0) ||
                            (rInfo.nTypeArray[j-1] != NF_SYMBOLTYPE_DIGIT &&
                             rInfo.nTypeArray[j-1] != NF_SYMBOLTYPE_THSEP) ||
                            (rInfo.nTypeArray[j+1] == NF_SYMBOLTYPE_DIGIT));
                if ( bDoThousands && k > 0 )
                {
                    sStr.insert(k, rInfo.sStrArray[j]);
                }
            }
            break;
            case NF_SYMBOLTYPE_DIGIT:
            {
                const String& rStr = rInfo.sStrArray[j];
                const sal_Unicode* p1 = rStr.data();
                const sal_Unicode* p = p1 + rStr.size();
                while ( p1 < p-- )
                {
                    if (k > 0)
                        k--;
                    else
                    {
                        switch (*p)
                        {
                            case L'0':
                                sStr.insert(0, 1, L'0');
                                break;
                            case L'?':
                                sStr.insert(0, 1, L' ');
                                break;
                        }
                    }
                }
            }
            break;
            case NF_KEY_CCC:                // CCC-Waehrung
                sStr.insert(k, rScanPtr->GetCurAbbrev());
            break;
            case NF_KEY_GENERAL:            // Standard im String
            {
                String sNum;
                ImpGetOutputStandard(rNumber, sNum);
                EraseLeadingChars(sNum, L'-');    // Vorzeichen weg!!
                sStr.insert(k, sNum);
            }
            break;

            default:
                sStr.insert(k, rInfo.sStrArray[j]);
            break;
        }                                       // of switch
        j--;                                    // naechster String
    }                                           // of while
    return bRes;
}

void SvNumberformat::GetFormatSpecialInfo(bool& bThousand,
                                          bool& IsRed,
                                          sal_uInt16& nPrecision,
                                          sal_uInt16& nAnzLeading) const
{
    // as before: take info from nNumFor=0 for whole format (for dialog etc.)

    short nDummyType;
    GetNumForInfo( 0, nDummyType, bThousand, nPrecision, nAnzLeading );

    IsRed = false;
}

void SvNumberformat::GetNumForInfo( sal_uInt16 nNumFor, short& rScannedType,
                    bool& bThousand, sal_uInt16& nPrecision, sal_uInt16& nAnzLeading ) const
{
    // take info from a specified sub-format (for XML export)

    if ( nNumFor > 3 )
        return;             // invalid

    const ImpSvNumberformatInfo& rInfo = NumFor[nNumFor].Info();
    rScannedType = rInfo.eScannedType;
    bThousand = rInfo.bThousand;
    nPrecision = rInfo.nCntPost;
    if (bStandard && rInfo.eScannedType == NUMBERFORMAT_NUMBER)
                                                        // StandardFormat
        nAnzLeading = 1;
    else
    {
        nAnzLeading = 0;
        bool bStop = false;
        sal_uInt16 i = 0;
        const sal_uInt16 nAnz = NumFor[nNumFor].GetnAnz();
        while (!bStop && i < nAnz)
        {
            short nType = rInfo.nTypeArray[i];
            if ( nType == NF_SYMBOLTYPE_DIGIT)
            {
                const sal_Unicode* p = rInfo.sStrArray[i].data();
                while ( *p == L'#' )
                    p++;
                while ( *p++ == L'0' )
                    nAnzLeading++;
            }
            else if (nType == NF_SYMBOLTYPE_DECSEP || nType == NF_SYMBOLTYPE_EXP)
                bStop = true;
            i++;
        }
    }
}

const String* SvNumberformat::GetNumForString( sal_uInt16 nNumFor, sal_uInt16 nPos,
            bool bString /* = false */ ) const
{
    if ( nNumFor > 3 )
        return NULL;
    sal_uInt16 nAnz = NumFor[nNumFor].GetnAnz();
    if ( !nAnz )
        return NULL;
    if ( nPos == 0xFFFF )
    {
        nPos = nAnz - 1;
        if ( bString )
        {   // rueckwaerts
            short* pType = NumFor[nNumFor].Info().nTypeArray + nPos;
            while ( nPos > 0 && (*pType != NF_SYMBOLTYPE_STRING) &&
                    (*pType != NF_SYMBOLTYPE_CURRENCY) )
            {
                pType--;
                nPos--;
            }
            if ( (*pType != NF_SYMBOLTYPE_STRING) && (*pType != NF_SYMBOLTYPE_CURRENCY) )
                return NULL;
        }
    }
    else if ( nPos > nAnz - 1 )
        return NULL;
    else if ( bString )
    {   // vorwaerts
        short* pType = NumFor[nNumFor].Info().nTypeArray + nPos;
        while ( nPos < nAnz && (*pType != NF_SYMBOLTYPE_STRING) &&
                (*pType != NF_SYMBOLTYPE_CURRENCY) )
        {
            pType++;
            nPos++;
        }
        if ( nPos >= nAnz || ((*pType != NF_SYMBOLTYPE_STRING) &&
                    (*pType != NF_SYMBOLTYPE_CURRENCY)) )
            return NULL;
    }
    return &NumFor[nNumFor].Info().sStrArray[nPos];
}


short SvNumberformat::GetNumForType( sal_uInt16 nNumFor, sal_uInt16 nPos,
            bool bString /* = false */ ) const
{
    if ( nNumFor > 3 )
        return 0;
    sal_uInt16 nAnz = NumFor[nNumFor].GetnAnz();
    if ( !nAnz )
        return 0;
    if ( nPos == 0xFFFF )
    {
        nPos = nAnz - 1;
        if ( bString )
        {   // rueckwaerts
            short* pType = NumFor[nNumFor].Info().nTypeArray + nPos;
            while ( nPos > 0 && (*pType != NF_SYMBOLTYPE_STRING) &&
                    (*pType != NF_SYMBOLTYPE_CURRENCY) )
            {
                pType--;
                nPos--;
            }
            if ( (*pType != NF_SYMBOLTYPE_STRING) && (*pType != NF_SYMBOLTYPE_CURRENCY) )
                return 0;
        }
    }
    else if ( nPos > nAnz - 1 )
        return 0;
    else if ( bString )
    {   // vorwaerts
        short* pType = NumFor[nNumFor].Info().nTypeArray + nPos;
        while ( nPos < nAnz && (*pType != NF_SYMBOLTYPE_STRING) &&
                (*pType != NF_SYMBOLTYPE_CURRENCY) )
        {
            pType++;
            nPos++;
        }
        if ( (*pType != NF_SYMBOLTYPE_STRING) && (*pType != NF_SYMBOLTYPE_CURRENCY) )
            return 0;
    }
    return NumFor[nNumFor].Info().nTypeArray[nPos];
}


bool SvNumberformat::IsNegativeWithoutSign() const
{
    if ( IsNegativeRealNegative() )
    {
        const String* pStr = GetNumForString( 1, 0, true );
        if ( pStr )
            return !HasStringNegativeSign( *pStr );
    }
    return false;
}


DateFormat SvNumberformat::GetDateOrder() const
{
    if ( (eType & NUMBERFORMAT_DATE) == NUMBERFORMAT_DATE )
    {
        short const * const pType = NumFor[0].Info().nTypeArray;
        sal_uInt16 nAnz = NumFor[0].GetnAnz();
        for ( sal_uInt16 j=0; j<nAnz; j++ )
        {
            switch ( pType[j] )
            {
                case NF_KEY_D :
                case NF_KEY_DD :
                    return DMY;
                case NF_KEY_M :
                case NF_KEY_MM :
                case NF_KEY_MMM :
                case NF_KEY_MMMM :
                case NF_KEY_MMMMM :
                    return MDY;
                case NF_KEY_YY :
                case NF_KEY_YYYY :
                case NF_KEY_EC :
                case NF_KEY_EEC :
                case NF_KEY_R :
                case NF_KEY_RR :
                    return YMD;
            }
        }
    }
    return pFormatter->getDateFormat();
}


sal_uInt32 SvNumberformat::GetExactDateOrder() const
{
    sal_uInt32 nRet = 0;
    if ( (eType & NUMBERFORMAT_DATE) != NUMBERFORMAT_DATE )
    {
        return nRet;
    }
    short const * const pType = NumFor[0].Info().nTypeArray;
    sal_uInt16 nAnz = NumFor[0].GetnAnz();
    int nShift = 0;
    for ( sal_uInt16 j=0; j<nAnz && nShift < 3; j++ )
    {
        switch ( pType[j] )
        {
            case NF_KEY_D :
            case NF_KEY_DD :
                nRet = (nRet << 8) | L'D';
                ++nShift;
            break;
            case NF_KEY_M :
            case NF_KEY_MM :
            case NF_KEY_MMM :
            case NF_KEY_MMMM :
            case NF_KEY_MMMMM :
                nRet = (nRet << 8) | L'M';
                ++nShift;
            break;
            case NF_KEY_YY :
            case NF_KEY_YYYY :
            case NF_KEY_EC :
            case NF_KEY_EEC :
            case NF_KEY_R :
            case NF_KEY_RR :
                nRet = (nRet << 8) | L'Y';
                ++nShift;
            break;
        }
    }
    return nRet;
}


void SvNumberformat::GetConditions( SvNumberformatLimitOps& rOper1, double& rVal1,
                          SvNumberformatLimitOps& rOper2, double& rVal2 ) const
{
    rOper1 = eOp1;
    rOper2 = eOp2;
    rVal1  = fLimit1;
    rVal2  = fLimit2;
}


Color* SvNumberformat::GetColor( sal_uInt16 nNumFor ) const
{
	return NULL;
}


void lcl_SvNumberformat_AddLimitStringImpl( String& rStr,
            SvNumberformatLimitOps eOp, double fLimit, const String& rDecSep )
{
    if ( eOp != NUMBERFORMAT_OP_NO )
    {
        switch ( eOp )
        {
            case NUMBERFORMAT_OP_EQ :
                rStr = L"[=";
            break;
            case NUMBERFORMAT_OP_NE :
                rStr = L"[<>";
            break;
            case NUMBERFORMAT_OP_LT :
                rStr = L"[<";
            break;
            case NUMBERFORMAT_OP_LE :
                rStr = L"[<=";
            break;
            case NUMBERFORMAT_OP_GT :
                rStr = L"[>";
            break;
            case NUMBERFORMAT_OP_GE :
                rStr = L"[>=";
            break;
            default:
                break;
        }
        rStr += String( doubleToUString( fLimit,
                rtl_math_StringFormat_Automatic, rtl_math_DecimalPlaces_Max,
                rDecSep.at(0), true));
        rStr += L']';
    }
}


String SvNumberformat::ImpGetNatNumString( const SvNumberNatNum& rNum,
        sal_Int32 nVal, sal_uInt16 nMinDigits ) const
{
    String aStr;
    if ( nMinDigits )
    {
        if ( nMinDigits == 2 )
        {   // speed up the most common case
            if ( 0 <= nVal && nVal < 10 )
            {
				wchar_t str[3];
				swprintf(str, sizeof(str) / sizeof(wchar_t), L"0%d", nVal);
				aStr = str;
            }
            else
                aStr = std::to_wstring( nVal );
        }
        else
        {
            String aValStr( std::to_wstring( nVal ) );
            if ( aValStr.size() >= nMinDigits )
                aStr = aValStr;
            else
            {
				aStr = L"";
				aStr.insert(0, nMinDigits - aValStr.size(), L'0');
                aStr += aValStr;
            }
        }
    }
    else
        aStr = std::to_wstring( nVal );
    ImpTransliterate( aStr, rNum );
    return aStr;
}


bool SvNumberformat::HasStringNegativeSign( const String& rStr )
{
    // for Sign must be '-' at the beginning or at the end of the substring (blanks ignored)
    uint16_t nLen = rStr.size();
    if ( !nLen )
        return false;
    const sal_Unicode* const pBeg = rStr.data();
    const sal_Unicode* const pEnd = pBeg + nLen;
    const sal_Unicode* p = pBeg;
    do
    {   // beginning
        if ( *p == L'-' )
            return true;
    } while ( *p == L' ' && ++p < pEnd );
    p = pEnd - 1;
    do
    {   // end
        if ( *p == L'-' )
            return true;
    } while ( *p == L' ' && pBeg < --p );
    return false;
}


// static
void SvNumberformat::SetComment( const String& rStr, String& rFormat,
        String& rComment )
{
    if ( rComment.size() )
    {   // Delete old comment from format string
        //! not via EraseComment, the comment must match
        String aTmp( L"{" );
        aTmp += L' ';
        aTmp += rComment;
        aTmp += L' ';
        aTmp += L'}';
        uint16_t nCom = 0;
        do
        {
            nCom = rFormat.find( aTmp, nCom );
        } while ( (nCom != STRING_NOTFOUND) && (nCom + aTmp.size() != rFormat.size()) );
        if ( nCom != STRING_NOTFOUND )
            rFormat.erase( nCom );
    }
    if ( rStr.size() )
    {   // put new comment
        rFormat += L'{';
        rFormat += L' ';
        rFormat += rStr;
        rFormat += L' ';
        rFormat += L'}';
        rComment = rStr;
    }
}


// static
void SvNumberformat::EraseCommentBraces( String& rStr )
{
    uint16_t nLen = rStr.size();
    if ( nLen && rStr.at(0) == L'{' )
    {
        rStr.erase( 0, 1 );
        --nLen;
    }
    if ( nLen && rStr.at(0) == L' ' )
    {
        rStr.erase( 0, 1 );
        --nLen;
    }
    if ( nLen && rStr.at( nLen-1 ) == L'}' )
        rStr.erase( --nLen, 1 );
    if ( nLen && rStr.at( nLen-1 ) == L' ' )
        rStr.erase( --nLen, 1 );
}


// static
void SvNumberformat::EraseComment( String& rStr )
{
    const sal_Unicode* p = rStr.data();
    bool bInString = false;
    bool bEscaped = false;
    bool bFound = false;
    uint16_t nPos = 0;
    while ( !bFound && *p )
    {
        switch ( *p )
        {
            case L'\\' :
                bEscaped = !bEscaped;
            break;
            case L'\"' :
                if ( !bEscaped )
                    bInString = !bInString;
            break;
            case L'{' :
                if ( !bEscaped && !bInString )
                {
                    bFound = true;
                    nPos = (uint16_t)(p - rStr.data());
                }
            break;
        }
        if ( bEscaped && *p != L'\\' )
            bEscaped = false;
        ++p;
    }
    if ( bFound )
        rStr.erase( nPos );
}


// static
bool SvNumberformat::IsInQuote( const String& rStr, uint16_t nPos,
            sal_Unicode cQuote, sal_Unicode cEscIn, sal_Unicode cEscOut )
{
    uint16_t nLen = rStr.size();
    if ( nPos >= nLen )
        return false;
    const sal_Unicode* p0 = rStr.data();
    const sal_Unicode* p = p0;
    const sal_Unicode* p1 = p0 + nPos;
    bool bQuoted = false;
    while ( p <= p1 )
    {
        if ( *p == cQuote )
        {
            if ( p == p0 )
                bQuoted = true;
            else if ( bQuoted )
            {
                if ( *(p-1) != cEscIn )
                    bQuoted = false;
            }
            else
            {
                if ( *(p-1) != cEscOut )
                    bQuoted = true;
            }
        }
        p++;
    }
    return bQuoted;
}


// static
uint16_t SvNumberformat::GetQuoteEnd( const String& rStr, uint16_t nPos,
            sal_Unicode cQuote, sal_Unicode cEscIn, sal_Unicode cEscOut )
{
    uint16_t nLen = rStr.size();
    if ( nPos >= nLen )
        return STRING_NOTFOUND;
    if ( !IsInQuote( rStr, nPos, cQuote, cEscIn, cEscOut ) )
    {
        if ( rStr.at( nPos ) == cQuote )
            return nPos;        // closing quote
        return STRING_NOTFOUND;
    }
    const sal_Unicode* p0 = rStr.data();
    const sal_Unicode* p = p0 + nPos;
    const sal_Unicode* p1 = p0 + nLen;
    while ( p < p1 )
    {
        if ( *p == cQuote && p > p0 && *(p-1) != cEscIn )
            return (uint16_t)(p - p0);
        p++;
    }
    return nLen;        // String end
}


sal_uInt16 SvNumberformat::ImpGetNumForStringElementCount( sal_uInt16 nNumFor ) const
{
    sal_uInt16 nCnt = 0;
    sal_uInt16 nAnz = NumFor[nNumFor].GetnAnz();
    short const * const pType = NumFor[nNumFor].Info().nTypeArray;
    for ( sal_uInt16 j=0; j<nAnz; ++j )
    {
        switch ( pType[j] )
        {
            case NF_SYMBOLTYPE_STRING:
            case NF_SYMBOLTYPE_CURRENCY:
            case NF_SYMBOLTYPE_DATESEP:
            case NF_SYMBOLTYPE_TIMESEP:
            case NF_SYMBOLTYPE_TIME100SECSEP:
            case NF_SYMBOLTYPE_PERCENT:
                ++nCnt;
            break;
        }
    }
    return nCnt;
}

}   // namespace duckdb_excel