// MARKER(update_precomp.py): autogen include statement, do not remove
#ifndef GCC
#endif

#include <stdlib.h>
#include <cwctype>
#include <clocale>
#include "date.hxx"
#include "time.hxx"
#include "datetime.hxx"
#include "zformat.hxx"
#include "nfkeytab.hxx"
#include "digitgroupingiterator.hxx"
#include "zforscan.hxx"
#include "localedata.h"

//using namespace svt;

const sal_Unicode cNonBreakingSpace = 0xA0;

//namespace 
//{
//    struct ImplEnglishColors
//    {
//        const String* operator()()
//        {
//            static const String aEnglishColors[NF_MAX_DEFAULT_COLORS] =
//            {
//                String( RTL_CONSTASCII_USTRINGPARAM( "BLACK" ) ),
//                String( RTL_CONSTASCII_USTRINGPARAM( "BLUE" ) ),
//                String( RTL_CONSTASCII_USTRINGPARAM( "GREEN" ) ),
//                String( RTL_CONSTASCII_USTRINGPARAM( "CYAN" ) ),
//                String( RTL_CONSTASCII_USTRINGPARAM( "RED" ) ),
//                String( RTL_CONSTASCII_USTRINGPARAM( "MAGENTA" ) ),
//                String( RTL_CONSTASCII_USTRINGPARAM( "BROWN" ) ),
//                String( RTL_CONSTASCII_USTRINGPARAM( "GREY" ) ),
//                String( RTL_CONSTASCII_USTRINGPARAM( "YELLOW" ) ),
//                String( RTL_CONSTASCII_USTRINGPARAM( "WHITE" ) )
//            };
//            return &aEnglishColors[0];
//        }
//    };
//
//    struct theEnglishColors
//            : public rtl::StaticAggregate< const String, ImplEnglishColors> {};
//
//}

//inline bool isLetter(const String& str, sal_uInt16 count)
//{
//	for (sal_uInt32 i = 0; i < str.size(); i++)
//	{
//		sal_Unicode c = str.at(i);
//		if (!(
//			(c >= L'A' && c <= L'Z') || 
//			(c >= L'a' && c <= L'z') || 
//			c == L'+' || 
//			c == L'-' || 
//			c == L'0' || 
//			c == L'#' || 
//			c == L'/' || 
//			c == L' ' ||
//			c == L':' ||
//			c == L'[' ||
//			c == L']' ||
//			c == L'.' ||
//			false))
//		{
//			return false;
//		}
//	}
//	return true;
//}
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

	//StandardColor[0]  =  Color(COL_BLACK);
	//StandardColor[1]  =  Color(COL_LIGHTBLUE);
	//StandardColor[2]  =  Color(COL_LIGHTGREEN);
	//StandardColor[3]  =  Color(COL_LIGHTCYAN);
	//StandardColor[4]  =  Color(COL_LIGHTRED);
	//StandardColor[5]  =  Color(COL_LIGHTMAGENTA);
	//StandardColor[6]  =  Color(COL_BROWN);
	//StandardColor[7]  =  Color(COL_GRAY);
	//StandardColor[8]  =  Color(COL_YELLOW);
	//StandardColor[9]  =  Color(COL_WHITE);

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
                //DBG_ERRORFILE( "InitSpecialKeyword: TRUE_WORD?" );
                ((ImpSvNumberformatScan*)this)->sKeyword[NF_KEY_TRUE] = L"true";
            }
        break;
        case NF_KEY_FALSE :
			str = pFormatter->getReservedWord(reservedWords::FALSE_WORD);
			ConvertToUpper(str);
			((ImpSvNumberformatScan*)this)->sKeyword[NF_KEY_FALSE] = str;
            if ( !sKeyword[NF_KEY_FALSE].size() )
            {
                //DBG_ERRORFILE( "InitSpecialKeyword: FALSE_WORD?" );
                ((ImpSvNumberformatScan*)this)->sKeyword[NF_KEY_FALSE] = L"false";
            }
        break;
        default:
            //DBG_ERRORFILE( "InitSpecialKeyword: unknown request" );
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

#if (0)
Color* ImpSvNumberformatScan::GetColor(String& sStr)
{
	String sString = pFormatter->GetCharClass()->upper(sStr);
    const NfKeywordTable & rKeyword = GetKeywords();
	size_t i = 0;
	while (i < NF_MAX_DEFAULT_COLORS &&
           sString != rKeyword[NF_KEY_FIRSTCOLOR+i] )
		i++;
    if ( i >= NF_MAX_DEFAULT_COLORS )
    {
        const String* pEnglishColors = theEnglishColors::get();
        size_t j = 0;
        while ( j < NF_MAX_DEFAULT_COLORS &&
                sString != pEnglishColors[j] )
            ++j;
        if ( j < NF_MAX_DEFAULT_COLORS )
            i = j;
    }
    
    Color* pResult = NULL;
	if (i >= NF_MAX_DEFAULT_COLORS)
	{
        const String& rColorWord = rKeyword[NF_KEY_COLOR];
		uint16_t nPos = sString.Match(rColorWord);
		if (nPos > 0)
		{
			sStr.erase(0, nPos);
			EraseLeadingChars(sStr, L' ');
			EraseTrailingChars(sStr, L' ');
			if (bConvertMode)
			{
				pFormatter->ChangeIntl(eNewLnge);
                sStr.insert( 0, GetKeywords()[NF_KEY_COLOR] );  // Color -> FARBE
				pFormatter->ChangeIntl(eTmpLnge);
			}
			else
				sStr.insert(0, rColorWord);
			sString.erase(0, nPos);
			EraseLeadingChars(sString, L' ');
			EraseTrailingChars(sString, L' ');

			if ( CharClass::isAsciiNumeric( sString ) )
			{
				long nIndex = _wtoi(sString.data());
				if (nIndex > 0 && nIndex <= 64)
					pResult = pFormatter->GetUserDefColor((sal_uInt16)nIndex-1);
			}
		}
	}
	else
	{
		sStr.erase();
		if (bConvertMode)
		{
			pFormatter->ChangeIntl(eNewLnge);
            sStr = GetKeywords()[NF_KEY_FIRSTCOLOR+i];           // red -> rot
			pFormatter->ChangeIntl(eTmpLnge);
		}
		else
            sStr = rKeyword[NF_KEY_FIRSTCOLOR+i];

		pResult = &(StandardColor[i]);
	}
    return pResult;
}
#endif


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
    // The Thai T NatNum modifier during Xcl import.
    //if (i == 0 && bConvertMode && sString.GetChar(0) == 'T' && eTmpLnge ==
    //        LANGUAGE_ENGLISH_US && MsLangId::getRealLanguage( eNewLnge) ==
    //        LANGUAGE_THAI)
    //    i = NF_KEY_THAI_T;
	return i;		// 0 => not found
}

//---------------------------------------------------------------------------
// Next_Symbol
//---------------------------------------------------------------------------
// Breaks the input down into symbols for the rest
// Processing (Turing machine).
//---------------------------------------------------------------------------
// Output State = SsStart
//---------------+-------------------+-----------------------+---------------
// former state  |  read character   | action                | New condition
//---------------+-------------------+-----------------------+---------------
// SsStart       | Letter            | Symbol=sign           | SsGetWord
//               |    "              | Typ = String          | SsGetString
//               |    \              | Typ = String          | SsGetChar
//               |    *              | Typ = Star            | SsGetStar
//               |    _              | Typ = Blank           | SsGetBlank
//               | @ # 0 ? / . , % [ | Symbol = sign;        |
//               | ] ' Blank         | Typ = control characters   | SsStop
//               | $ - + ( ) :       | Typ    = String;      |
//               | |                 | Typ    = Comment      | SsStop
//               | Otherwise         | Symbol = sign         | SsStop
//---------------|-------------------+-----------------------+---------------
// SsGetChar     | Otherwise         | Symbol=sign           | SsStop
//---------------+-------------------+-----------------------+---------------
// GetString     | "                 |                       | SsStop
//               | Otherwise         | Symbol+=sign          | GetString
//---------------+-------------------+-----------------------+---------------
// SsGetWord     | Letter            | Symbol += sign        |
//               | + -        (E+ E-)| Symbol += sign        | SsStop
//               | /          (AM/PM)| Symbol += sign        |
//               | Otherwise         | Pos--, if Key Typ=Word| SsStop
//---------------+-------------------+-----------------------+---------------
// SsGetStar     | Otherwise         | Symbol+=sign          | SsStop
//               |                   | mark special case *   |
//---------------+-------------------+-----------------------+---------------
// SsGetBlank    | Otherwise         | Symbol+=sign          | SsStop
//               |                   | mark special case _   |
//---------------+-------------------+-----------------------+---------------
// Was a keyword recognized in the SsGetWord state
// (also as the initial subword of the symbol)
// so the remaining letters are written back !!

enum ScanState
{
	SsStop      = 0,
	SsStart     = 1,
	SsGetChar   = 2,
	SsGetString = 3,
	SsGetWord   = 4,
	SsGetStar   = 5,
	SsGetBlank  = 6
};

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
#if NF_COMMENT_IN_FORMATSTRING
					case '{':
						eType = NF_SYMBOLTYPE_COMMENT;
						eState = SsStop;
						sSymbol.Append( rStr.GetBuffer() + (nPos-1), rStr.size() - (nPos-1) );
						nPos = rStr.size();
					break;
#endif
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

// ER 20.06.97 14:05   nicht noetig, wenn nAnzStrings beachtet wird
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
			eScannedType = NUMBERFORMAT_TEXT;				// Text bleibt immer Text
		else if (eNewType == NUMBERFORMAT_UNDEFINED)
		{											// bleibt wie bisher
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
						case NUMBERFORMAT_NUMBER:	// nur Zahl nach Prozent
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
						case NUMBERFORMAT_NUMBER:	// nur Zahl nach E
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
						case NUMBERFORMAT_NUMBER:			// nur Zahl nach Bruch
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
		nPos = nPos + sStrArray[i].size();			// Korrekturposition
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
#if (0)
	if ( sStrArray[i].at(0) == L'[' &&
			i < nAnzStrings-1 &&
			nTypeArray[i+1] == NF_SYMBOLTYPE_STRING &&
			sStrArray[i+1].at(0) == L'~' )
	{	// [~calendarID]
		// as of SV_NUMBERFORMATTER_VERSION_CALENDAR
		nPos = nPos + sStrArray[i].size();			// [
		nTypeArray[i] = NF_SYMBOLTYPE_CALDEL;
		nPos = nPos + sStrArray[++i].size();		// ~
		sStrArray[i-1] += sStrArray[i];		// [~
		nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
		rAnzResStrings--;
		if ( ++i >= nAnzStrings )
			return -1;		// error
		nPos = nPos + sStrArray[i].size();			// calendarID
		String& rStr = sStrArray[i];
		nTypeArray[i] = NF_SYMBOLTYPE_CALENDAR;	// convert
		i++;
		while ( i < nAnzStrings &&
				sStrArray[i].at(0) != L']' )
		{
			nPos = nPos + sStrArray[i].size();
			rStr += sStrArray[i];
			nTypeArray[i] = NF_SYMBOLTYPE_EMPTY;
			rAnzResStrings--;
			i++;
		}
		if ( rStr.size() && i < nAnzStrings &&
				sStrArray[i].at(0) == L']' )
		{
			nTypeArray[i] = NF_SYMBOLTYPE_CALDEL;
			nPos = nPos + sStrArray[i].size();
			i++;
		}
		else
			return -1;		// error
		return 1;
	}
#endif
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
				}                                               // kein Taus.p.


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
						if (!bBlank && !bFrac)	// nicht doppelt oder hinter /
						{
							if (bDecSep && nCounter > 0)	// Nachkommastellen
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
                            //DBG_ASSERT( sStrArray[i].size() == 1, "ImpSvNumberformatScan::FinalScan: combined digits in group separator insertion");
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
								if (bThousand)				// doppelt
									return nPos;
								bThousand = true;			// bei Time frei
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
								if (!bThousand)				// kein [ vorher
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
						bExp = true;					// missbraucht fuer A/P
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
			nCntPost = nCounter;					// Zaehler der Nullen
			if (bExp)
				nCntExp = 1;						// merkt AM/PM
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
				nCntExp = 1;						// merkt AM/PM
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
	uint16_t res = Symbol_Division(rString);	//lexikalische Analyse
	if (!res)
		res = ScanType(rString);            // Erkennung des Formattyps
	if (!res)
		res = FinalScan( rString, rComment );	// Typabhaengige Endanalyse
	return res;								// res = Kontrollposition
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
