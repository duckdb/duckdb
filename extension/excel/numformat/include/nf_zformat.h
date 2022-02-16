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

#ifndef _NF_ZFORMAT_H
#define _NF_ZFORMAT_H

#include <string>
#include "nf_calendar.h"
#include "nf_localedata.h"


namespace duckdb_excel {

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
    bool IsNumberFormat(
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
    bool    bTextInitialized;                   // Whether days and months are initialized
                                                // Variables for provisional results:
    String sStrArray[SV_MAX_ANZ_INPUT_STRINGS]; // Array of scanned substrings
    bool   IsNum[SV_MAX_ANZ_INPUT_STRINGS];     // Whether a substring is numeric
    sal_uInt16 nNums[SV_MAX_ANZ_INPUT_STRINGS];     // Sequence of offsets to numeric strings
    sal_uInt16 nAnzStrings;                         // Total count of scanned substrings
    sal_uInt16 nAnzNums;                            // Count of numeric substrings
    bool   bDecSepInDateSeps;                   // True <=> DecSep in {.,-,/,DateSep}
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

    void Reset();                               // Reset all variables before start of analysis

    void InitText();                            // Init of months and days of week

    double StringToDouble(
            const String& rStr,
            bool bForceFraction = false );

    bool NextNumberStringSymbol(                // Next number/string symbol
            const sal_Unicode*& pStr,
            String& rSymbol );

    bool SkipThousands(                         // Concatenate ,000,23 blocks
            const sal_Unicode*& pStr,           // in input to 000123
            String& rSymbol );

    void NumberStringDivision(                  // Divide numbers/strings into
            const String& rString );            // arrays and variables above.
                                                // Leading blanks and blanks
                                                // after numbers are thrown away


                                                // optimized substring versions

    static inline bool StringContains(          // Whether rString contains rWhat at nPos
            const String& rWhat,
            const String& rString,
            uint16_t nPos )
                {   // mostly used with one character
                    if ( rWhat.size() <= 0 || nPos >= rString.size() || rWhat.at(0) != rString.at(nPos) )
                        return false;
                    return StringContainsImpl( rWhat, rString, nPos );
                }
    static inline bool StringPtrContains(       // Whether pString contains rWhat at nPos
            const String& rWhat,
            const sal_Unicode* pString,
            uint16_t nPos )                   // nPos MUST be a valid offset from pString
                {   // mostly used with one character
                    if ( rWhat.at(0) != *(pString+nPos) )
                        return false;
                    return StringPtrContainsImpl( rWhat, pString, nPos );
                }
    static bool StringContainsImpl(             //! DO NOT use directly
            const String& rWhat,
            const String& rString,
            uint16_t nPos );
    static bool StringPtrContainsImpl(          //! DO NOT use directly
            const String& rWhat,
            const sal_Unicode* pString,
            uint16_t nPos );


    static inline bool SkipChar(                // Skip a special character
            sal_Unicode c,
            const String& rString,
            uint16_t& nPos );
    static inline void SkipBlanks(              // Skip blank
            const String& rString,
            uint16_t& nPos );
    static inline bool SkipString(              // Jump over rWhat in rString at nPos
            const String& rWhat,
            const String& rString,
            uint16_t& nPos );

    inline bool GetThousandSep(                 // Recognizes exactly ,111 as group separator
            const String& rString,
            uint16_t& nPos,
            sal_uInt16 nStringPos );
    short GetLogical(                           // Get boolean value
            const String& rString );
    short GetMonth(                             // Get month and advance string position
            const String& rString,
            uint16_t& nPos );
    int GetDayOfWeek(                           // Get day of week and advance string position
            const String& rString,
            uint16_t& nPos );
    bool GetCurrency(                           // Get currency symbol and advance string position
            const String& rString,
            uint16_t& nPos,
            const SvNumberformat* pFormat = NULL ); // optional number format to match against
    bool GetTimeAmPm(                           // Get symbol AM or PM and advance string position
            const String& rString,
            uint16_t& nPos );
    inline bool GetDecSep(                      // Get decimal separator and advance string position
            const String& rString,
            uint16_t& nPos );
    inline bool GetTime100SecSep(               // Get hundredth seconds separator and advance string position
            const String& rString,
            uint16_t& nPos );
    int GetSign(                                // Get sign  and advance string position
            const String& rString,              // Including special case '('
            uint16_t& nPos );
    short GetESign(                             // Get sign of exponent and advance string position
            const String& rString,
            uint16_t& nPos );

    inline bool GetNextNumber(                  // Get next number as array offset
            sal_uInt16& i,
            sal_uInt16& j );

    void GetTimeRef(                            // Converts time -> double (only decimals)
            double& fOutNumber,                 // result as double
            sal_uInt16 nIndex,                      // Index of hour in input
            sal_uInt16 nAnz );                      // Count of time substrings in input
    sal_uInt16 ImplGetDay  ( sal_uInt16 nIndex );       // Day input, 0 if no match
    sal_uInt16 ImplGetMonth( sal_uInt16 nIndex );       // Month input, zero based return, NumberOfMonths if no match
    sal_uInt16 ImplGetYear ( sal_uInt16 nIndex );       // Year input, 0 if no match
    bool GetDateRef(                            // Conversion of date to number
            double& fDays,                      // OUT: days diff to null date
            sal_uInt16& nCounter,                   // Count of date substrings
            const SvNumberformat* pFormat = NULL ); // optional number format to match against

    bool ScanStartString(                       // Analyze start of string
            const String& rString,
            const SvNumberformat* pFormat = NULL );
    bool ScanMidString(                         // Analyze middle substring
            const String& rString,
            sal_uInt16 nStringPos,
            const SvNumberformat* pFormat = NULL );
    bool ScanEndString(                         // Analyze end of string
            const String& rString,
            const SvNumberformat* pFormat = NULL );

    // Whether input may be a ISO 8601 date format, yyyy-mm-dd...
    // checks if at least 3 numbers and first number>31
    bool MayBeIso8601();

    // Compare rString to substring of array indexed by nString
    // nString == 0xFFFF => last substring
    bool ScanStringNumFor(
            const String& rString,
            uint16_t nPos,
            const SvNumberformat* pFormat,
            sal_uInt16 nString,
            bool bDontDetectNegation = false );

    // if nMatchedAllStrings set nMatchedUsedAsReturn and return true,
    // else do nothing and return false
    bool MatchedReturn();

    //! Be sure that the string to be analyzed is already converted to upper
    //! case and if it contained native humber digits that they are already
    //! converted to ASCII.
    bool IsNumberFormatMain(                    // Main anlyzing function
            const String& rString,
            double& fOutNumber,                 // return value if string is numeric
            const SvNumberformat* pFormat = NULL    // optional number format to match against
            );

    static inline bool MyIsdigit( sal_Unicode c );

};

class LocaleData;
struct ImpSvNumberformatInfo;


const size_t NF_MAX_FORMAT_SYMBOLS   = 100;
const size_t NF_MAX_DEFAULT_COLORS   = 10;

// Hack: nThousand==1000 => "Default" occurs in format string
const sal_uInt16 FLAG_STANDARD_IN_FORMAT = 1000;

class LocaleData;

class ImpSvNumberformatScan
{
public:

	ImpSvNumberformatScan( LocaleData* pFormatter );
	~ImpSvNumberformatScan();
	void ChangeIntl();							// swap keywords

    void ChangeStandardPrec(sal_uInt16 nPrec);  // exchanges standard precision

	uint16_t ScanFormat( String& rString, String& rComment );	// Calling up the scan analysis

	void CopyInfo(ImpSvNumberformatInfo* pInfo,
					 sal_uInt16 nAnz);				// Copies the FormatInfo
	sal_uInt16 GetAnzResStrings() const				{ return nAnzResStrings; }

    const NfKeywordTable & GetKeywords() const
        {
            if ( bKeywordsNeedInit )
                InitKeywords();
            return sKeyword;
        }
    // Keywords used in output like true and false
    const String& GetSpecialKeyword( NfKeywordIndex eIdx ) const
        {
            if ( !sKeyword[eIdx].size() )
                InitSpecialKeyword( eIdx );
            return sKeyword[eIdx];
        }
    const String& GetTrueString() const     { return GetSpecialKeyword( NF_KEY_TRUE ); }
    const String& GetFalseString() const    { return GetSpecialKeyword( NF_KEY_FALSE ); }
    const String& GetColorString() const    { return GetKeywords()[NF_KEY_COLOR]; }
    const String& GetRedString() const      { return GetKeywords()[NF_KEY_RED]; }
    const String& GetBooleanString() const  { return GetKeywords()[NF_KEY_BOOLEAN]; }
	const String& GetErrorString() const  	{ return sErrStr; }

    const String& GetStandardName() const
        {
            if ( bKeywordsNeedInit )
                InitKeywords();
            return sNameStandardFormat;
        }
    sal_uInt16 GetStandardPrec() const          { return nStandardPrec; }
												// definierte Farben

    // the compatibility currency symbol for old automatic currency formats
    const String& GetCurSymbol() const
        {
            if ( bCompatCurNeedInit )
                InitCompatCur();
            return sCurSymbol;
        }

    // the compatibility currency abbreviation for CCC format code
    const String& GetCurAbbrev() const
        {
            if ( bCompatCurNeedInit )
                InitCompatCur();
            return sCurAbbrev;
        }

    // the compatibility currency symbol upper case for old automatic currency formats
    const String& GetCurString() const
        {
            if ( bCompatCurNeedInit )
                InitCompatCur();
            return sCurString;
        }

	void SetConvertMode(LanguageType eTmpLge, LanguageType eNewLge,
			bool bSystemToSystem = false )
	{
		bConvertMode = true;
		eNewLnge = eNewLge;
		eTmpLnge = eTmpLge;
		bConvertSystemToSystem = bSystemToSystem;
	}
	void SetConvertMode(bool bMode) { bConvertMode = bMode; }
	bool GetConvertMode() const     { return bConvertMode; }
	LanguageType GetNewLnge() const { return eNewLnge; }
	LanguageType GetTmpLnge() const { return eTmpLnge; }
    sal_uInt8 GetNatNumModifier() const      { return nNatNumModifier; }
    void SetNatNumModifier( sal_uInt8 n )    { nNatNumModifier = n; }

	LocaleData* GetNumberformatter() { return pFormatter; }


private:
	NfKeywordTable sKeyword;
	String sNameStandardFormat;
    sal_uInt16 nStandardPrec;
	LocaleData* pFormatter;

	String sStrArray[NF_MAX_FORMAT_SYMBOLS];    // array of symbols
	short nTypeArray[NF_MAX_FORMAT_SYMBOLS];    // array of info
												// External Info:
	sal_uInt16 nAnzResStrings;						// Number of result symbols
#if !(defined SOLARIS && defined X86)
	short eScannedType;							// Typ gemaess Scan
#else
	int eScannedType;							// according to optimization
#endif
	bool bThousand;								// With a thousand point
	sal_uInt16 nThousand;							// Counts .... episodes
	sal_uInt16 nCntPre;								// Counts digits before the decimal point
	sal_uInt16 nCntPost;							// Counts decimal places
	sal_uInt16 nCntExp;								// Counts Exp.Jobs, AM/PM
	sal_uInt32 nExpVal;
												// Internal Info:
	sal_uInt16 nAnzStrings;							// number of symbols
	sal_uInt16 nRepPos;								// position of one '*'
	sal_uInt16 nExpPos;								// internal Position of E
	sal_uInt16 nBlankPos;							// internal position of the blank
	short nDecPos;								// internal Pos. from ,
	bool bExp;									// is set when the E is read
	bool bFrac;									// is set when reading the /
	bool bBlank;								// becomes sat with ' '(Fraction).
	bool bDecSep;								// Is set at the first
    mutable bool bKeywordsNeedInit;             // Locale dependent keywords need to be initialized
    mutable bool bCompatCurNeedInit;            // Locale dependent compatibility currency need to be initialized
    String sCurSymbol;                          // Currency symbol for compatibility format codes
    String sCurString;                          // Currency symbol in upper case
    String sCurAbbrev;                          // Currency abbreviation
    String sErrStr;                             // String for error output

	bool bConvertMode;							// Is set in convert mode
												// Country/language in which the
	LanguageType eNewLnge;						// scanned string converted
												// will (for Excel filters)
												// Country/language from which the
	LanguageType eTmpLnge;						// scanned string converted
												// will (for Excel filters)
	bool bConvertSystemToSystem;				// Whether the conversion is
												// from one system locale to
												// another system locale (in
												// this case the automatic
												// currency symbol is converted
												// too).

	uint16_t nCurrPos;						// Position of the Waehrungssymbols

    sal_uInt8 nNatNumModifier;                       // Thai T speciality

    void InitKeywords() const;
    void InitSpecialKeyword( NfKeywordIndex eIdx ) const;
    void InitCompatCur() const;

	void SetDependentKeywords();
												// Sets the language depend. keyword.
	void SkipStrings(sal_uInt16& i,uint16_t& nPos);// Skips string symbols
	sal_uInt16 PreviousKeyword(sal_uInt16 i);			// Returns index of prev.
												// Key words or 0
	sal_uInt16 NextKeyword(sal_uInt16 i);				// Returns index of next
												// Key words or 0
	sal_Unicode PreviousChar(sal_uInt16 i);				// Gives last letter
												// before the position,
												// skip EMPTY, STRING, STAR, BLANK
	sal_Unicode NextChar(sal_uInt16 i);					// Gives first letter after
	short PreviousType( sal_uInt16 i );				// gives type before position,
												// skip EMPTY
	bool IsLastBlankBeforeFrac(sal_uInt16 i);		// True <=> there is none ' '
												// more up to the '/'
	void Reset();								// Reset all variables
												// before start of analysis
	short GetKeyWord( const String& sSymbol,	// determine keyword at nPos
		uint16_t nPos );                      // return 0 <=> not found

	inline bool IsAmbiguousE( short nKey )		// whether nKey is ambiguous E of NF_KEY_E/NF_KEY_EC
		{
			return (nKey == NF_KEY_EC || nKey == NF_KEY_E) &&
                (GetKeywords()[NF_KEY_EC] == GetKeywords()[NF_KEY_E]);
		}

    bool Is100SecZero( sal_uInt16 i, bool bHadDecSep );

	short Next_Symbol(const String& rStr,
						uint16_t& nPos,
					  String& sSymbol);       // next icon
	uint16_t Symbol_Division(const String& rString);// lexical pre-analysis
	uint16_t ScanType(const String& rString);	// Formattyp Analysis
	uint16_t FinalScan( String& rString, String& rComment );	// Final analysis with default
												// of the type
	int FinalScanGetCalendar( uint16_t& nPos, sal_uInt16& i, sal_uInt16& nAnzResStrings );

    bool InsertSymbol( sal_uInt16 & nPos, NfSymbolType eType, const String& rStr );

	static inline bool StringEqualsChar( const String& rStr, sal_Unicode ch )
		{ return rStr.at(0) == ch && rStr.size() == 1; }

	static uint16_t RemoveQuotes( String& rStr );
};

#define NF_COMMENT_IN_FORMATSTRING 0

namespace utl {
    class DigitGroupingIterator;
}

class SvStream;
class Color;

class ImpSvNumberformatScan;            // format code string scanner
class ImpSvNumberInputScan;             // input string scanner
class ImpSvNumMultipleWriteHeader;      // compatible file format
class ImpSvNumMultipleReadHeader;       // compatible file format

enum SvNumberformatLimitOps
{
    NUMBERFORMAT_OP_NO  = 0,            // Undefined, no OP
    NUMBERFORMAT_OP_EQ  = 1,            // Operator =
    NUMBERFORMAT_OP_NE  = 2,            // Operator <>
    NUMBERFORMAT_OP_LT  = 3,            // Operator <
    NUMBERFORMAT_OP_LE  = 4,            // Operator <=
    NUMBERFORMAT_OP_GT  = 5,            // Operator >
    NUMBERFORMAT_OP_GE  = 6             // Operator >=
};

// SYSTEM-german to SYSTEM-xxx and vice versa conversion hack onLoad
enum NfHackConversion
{
    NF_CONVERT_NONE,
    NF_CONVERT_GERMAN_ENGLISH,
    NF_CONVERT_ENGLISH_GERMAN
};

struct ImpSvNumberformatInfo            // Struct for FormatInfo
{
    String* sStrArray;                  // Array of symbols
    short* nTypeArray;                  // Array of infos
    sal_uInt16 nThousand;                   // Count of group separator sequences
    sal_uInt16 nCntPre;                     // Count of digits before decimal point
    sal_uInt16 nCntPost;                    // Count of digits after decimal point
    sal_uInt16 nCntExp;                     // Count of exponent digits, or AM/PM
	sal_uInt32 nExpVal;                     // 
	short eScannedType;                 // Type determined by scan
    bool bThousand;                     // Has group (AKA thousand) separator

    void Copy( const ImpSvNumberformatInfo& rNumFor, sal_uInt16 nAnz );
};

class SvNumberNatNum
{
	LocaleIndentifier    eLang;
    sal_uInt8            nNum;
    bool            bDBNum  :1;     // DBNum, to be converted to NatNum
    bool            bDate   :1;     // Used in date? (needed for DBNum/NatNum mapping)
    bool            bSet    :1;     // If set, since NatNum0 is possible

public:

                    SvNumberNatNum() : eLang( LocaleId_en_US ), nNum(0),
                                        bDBNum(0), bDate(0), bSet(0) {}
    bool            IsComplete() const  { return bSet && eLang != LocaleId_en_US; }
    sal_uInt8            GetRawNum() const   { return nNum; }
	sal_uInt8            GetNatNum() const { return 0; }
    LanguageType    GetLang() const     { return eLang; }
    void            SetLang( LanguageType e ) { eLang = e; }
    void            SetNum( sal_uInt8 nNumber, bool bDBNumber )
                        {
                            nNum = nNumber;
                            bDBNum = bDBNumber;
                            bSet = true;
                        }
    bool            IsSet() const       { return bSet; }
    void            SetDate( bool bDateP )   { bDate = (bDateP != 0); }
};

class CharClass;

class ImpSvNumFor                       // One of four subformats of the format code string
{
public:
    ImpSvNumFor();                      // Ctor without filling the Info
    ~ImpSvNumFor();

    void Enlarge(sal_uInt16 nAnz);          // Init of arrays to the right size

    void Copy( const ImpSvNumFor& rNumFor, ImpSvNumberformatScan* pSc );

    ImpSvNumberformatInfo& Info() { return aI;}
    const ImpSvNumberformatInfo& Info() const { return aI; }

    // Get count of substrings (symbols)
    sal_uInt16 GetnAnz() const { return nAnzStrings;}

    Color* GetColor() const { return pColor; }
    void SetColor( Color* pCol, String& rName )
     { pColor = pCol; sColorName = rName; }
    const String& GetColorName() const { return sColorName; }

    // new SYMBOLTYPE_CURRENCY in subformat?
    bool HasNewCurrency() const;
    bool GetNewCurrencySymbol( String& rSymbol, String& rExtension ) const;

    void SetNatNumNum( sal_uInt8 nNum, bool bDBNum ) { aNatNum.SetNum( nNum, bDBNum ); }
    void SetNatNumLang( LanguageType eLang ) { aNatNum.SetLang( eLang ); }
    void SetNatNumDate( bool bDate ) { aNatNum.SetDate( bDate ); }
    const SvNumberNatNum& GetNatNum() const { return aNatNum; }

    // check, if the format code contains a subformat for text
    bool HasTextFormatCode() const;

private:
    ImpSvNumberformatInfo aI;           
    String sColorName;                  
    Color* pColor;                      
    sal_uInt16 nAnzStrings;             
    SvNumberNatNum aNatNum;             

};

class LocaleData;

class  SvNumberformat
{
public:
    // Normal ctor
    SvNumberformat( String& rString,
                   LocaleData* pFormatter,
                   ImpSvNumberInputScan* pISc,
                   uint16_t& nCheckPos,
                   LanguageType eLan = LocaleIndentifier::LocaleId_en_US,
                   bool bStand = false );

	// Ascii version of constructor
	SvNumberformat(std::string& rString,
		LocaleData* pFormatter,
		ImpSvNumberInputScan* pISc,
		uint16_t& nCheckPos,
		LanguageType eLan = LocaleIndentifier::LocaleId_en_US,
		bool bStand = false);

	void InitFormat(String& rString,
		LocaleData* pFormatter,
		ImpSvNumberInputScan* pISc,
		uint16_t& nCheckPos,
		LanguageType eLan,
		bool bStand);
	
    ~SvNumberformat();

    /// Get type of format, may include NUMBERFORMAT_DEFINED bit
    short GetType() const
        { return (nNewStandardDefined &&
            (nNewStandardDefined <= SV_NUMBERFORMATTER_VERSION)) ?
            (eType & ~NUMBERFORMAT_DEFINED) : eType; }

    void SetType(const short eSetType)          { eType = eSetType; }
    // Standard means the I18N defined standard format of this type
    void SetStandard()                          { bStandard = true; }
    bool IsStandard() const                     { return bStandard; }

    // For versions before version nVer it is UserDefined, for newer versions
    // it is builtin. nVer of SV_NUMBERFORMATTER_VERSION_...
    void SetNewStandardDefined( sal_uInt16 nVer )
        { nNewStandardDefined = nVer; eType |= NUMBERFORMAT_DEFINED; }

    sal_uInt16 GetNewStandardDefined() const        { return nNewStandardDefined; }
    bool IsAdditionalStandardDefined() const
        { return nNewStandardDefined == SV_NUMBERFORMATTER_VERSION_ADDITIONAL_I18N_FORMATS; }

    LanguageType GetLanguage() const            { return eLnge;}

    const String& GetFormatstring() const   { return sFormatstring; }

    void SetUsed(const bool b)                  { bIsUsed = b; }
    bool GetUsed() const                        { return bIsUsed; }
    bool IsStarFormatSupported() const          { return bStarFlag; }
    void SetStarFormatSupport( bool b )         { bStarFlag = b; }

    bool GetOutputString( double fNumber, sal_uInt16 nCharCount, String& rOutString ) const;

    bool GetOutputString( double fNumber, String& OutString, Color** ppColor );
	bool GetOutputString( double fNumber, std::string& OutString );
	bool GetOutputString( String& sString, String& OutString, Color** ppColor );

    // True if type text
    bool IsTextFormat() const { return (eType & NUMBERFORMAT_TEXT) != 0; }
    // True if 4th subformat present
    bool HasTextFormat() const
        {
            return (NumFor[3].GetnAnz() > 0) ||
                (NumFor[3].Info().eScannedType == NUMBERFORMAT_TEXT);
        }

    void GetFormatSpecialInfo(bool& bThousand,
                              bool& IsRed,
                              sal_uInt16& nPrecision,
                              sal_uInt16& nAnzLeading) const;

    /// Count of decimal precision
    sal_uInt16 GetFormatPrecision() const   { return NumFor[0].Info().nCntPost; }

    //! Read/write access on a special sal_uInt16 component, may only be used on the
    //! standard format 0, 5000, ... and only by the number formatter!
    sal_uInt16 GetLastInsertKey() const
        { return NumFor[0].Info().nThousand; }
    void SetLastInsertKey(sal_uInt16 nKey)
        { NumFor[0].Info().nThousand = nKey; }

    //! Only onLoad: convert from stored to current system language/country
    void ConvertLanguage( LocaleData& rConverter,
        LanguageType eConvertFrom, LanguageType eConvertTo, bool bSystem = false );

    // Substring of a subformat code nNumFor (0..3)
    // nPos == 0xFFFF => last substring
    // bString==true: first/last SYMBOLTYPE_STRING or SYMBOLTYPE_CURRENCY
    const String* GetNumForString( sal_uInt16 nNumFor, sal_uInt16 nPos,
            bool bString = false ) const;

    // Subtype of a subformat code nNumFor (0..3)
    // nPos == 0xFFFF => last substring
    // bString==true: first/last SYMBOLTYPE_STRING or SYMBOLTYPE_CURRENCY
    short GetNumForType( sal_uInt16 nNumFor, sal_uInt16 nPos, bool bString = false ) const;

    /** If the count of string elements (substrings, ignoring [modifiers] and
        so on) in a subformat code nNumFor (0..3) is equal to the given number.
        Used by ImpSvNumberInputScan::IsNumberFormatMain() to detect a matched
        format.  */
    bool IsNumForStringElementCountEqual( sal_uInt16 nNumFor, sal_uInt16 nAllCount,
            sal_uInt16 nNumCount ) const
        {
            if ( nNumFor < 4 )
            {
                // First try a simple approach. Note that this is called only
                // if all MidStrings did match so far, to verify that all
                // strings of the format were matched and not just the starting
                // sequence, so we don't have to check if GetnAnz() includes
                // [modifiers] or anything else if both counts are equal.
                sal_uInt16 nCnt = NumFor[nNumFor].GetnAnz();
                if ( nAllCount == nCnt )
                    return true;
                if ( nAllCount < nCnt ) // check ignoring [modifiers] and so on
                    return ImpGetNumForStringElementCount( nNumFor ) ==
                        (nAllCount - nNumCount);
            }
            return false;
        }

    // Whether the second subformat code is really for negative numbers
    // or another limit set.
    bool IsNegativeRealNegative() const
        {
            return fLimit1 == 0.0 && fLimit2 == 0.0 &&
            ( (eOp1 == NUMBERFORMAT_OP_GE && eOp2 == NUMBERFORMAT_OP_NO) ||
              (eOp1 == NUMBERFORMAT_OP_GT && eOp2 == NUMBERFORMAT_OP_LT) ||
              (eOp1 == NUMBERFORMAT_OP_NO && eOp2 == NUMBERFORMAT_OP_NO) );
        }
	// Whether the first subformat code is really for negative numbers
    // or another limit set.
    bool IsNegativeRealNegative2() const
        {
            return fLimit1 == 0.0 && fLimit2 == 0.0 &&
            ( (eOp2 == NUMBERFORMAT_OP_GT && eOp1 == NUMBERFORMAT_OP_LT) ||
			  (eOp2 == NUMBERFORMAT_OP_EQ && eOp1 == NUMBERFORMAT_OP_LT) ||
			  (eOp2 == NUMBERFORMAT_OP_GE && eOp1 == NUMBERFORMAT_OP_LT) ||
			  (eOp2 == NUMBERFORMAT_OP_NO && eOp1 == NUMBERFORMAT_OP_LT) ||
			  (eOp2 == NUMBERFORMAT_OP_NO && eOp1 == NUMBERFORMAT_OP_LE) ||
			  (eOp2 == NUMBERFORMAT_OP_GT && eOp1 == NUMBERFORMAT_OP_LE));
        }

    // Whether the negative format is without a sign or not
    bool IsNegativeWithoutSign() const;

    // Whether a new SYMBOLTYPE_CURRENCY is contained in the format
    bool HasNewCurrency() const;

    // check, if the format code contains a subformat for text
    bool HasTextFormatCode() const;

    // Build string from NewCurrency for saving it SO50 compatible
    void Build50Formatstring( String& rStr ) const;

    // strip [$-yyy] from all [$xxx-yyy] leaving only xxx's,
    // if bQuoteSymbol==true the xxx will become "xxx"
    static String StripNewCurrencyDelimiters( const String& rStr,
        bool bQuoteSymbol );

    // If a new SYMBOLTYPE_CURRENCY is contained if the format is of type
    // NUMBERFORMAT_CURRENCY, and if so the symbol xxx and the extension nnn
    // of [$xxx-nnn] are returned
    bool GetNewCurrencySymbol( String& rSymbol, String& rExtension ) const;

    static bool HasStringNegativeSign( const String& rStr );

    /**
        Whether a character at position nPos is somewhere between two matching
        cQuote or not.
        If nPos points to a cQuote, a true is returned on an opening cQuote,
        a false is returned on a closing cQuote.
        A cQuote between quotes may be escaped by a cEscIn, a cQuote outside of
        quotes may be escaped by a cEscOut.
        The default '\0' results in no escapement possible.
        Defaults are set right according to the "unlogic" of the Numberformatter
     */
    static bool IsInQuote( const String& rString, uint16_t nPos,
            sal_Unicode cQuote = '"',
            sal_Unicode cEscIn = '\0', sal_Unicode cEscOut = '\\' );

    /**
        Return the position of a matching closing cQuote if the character at
        position nPos is between two matching cQuote, otherwise return
        STRING_NOTFOUND.
        If nPos points to an opening cQuote the position of the matching
        closing cQuote is returned.
        If nPos points to a closing cQuote nPos is returned.
        If nPos points into a part which starts with an opening cQuote but has
        no closing cQuote, rString.size() is returned.
        Uses <method>IsInQuote</method> internally, so you don't have to call
        that prior to a call of this method.
     */
    static uint16_t GetQuoteEnd( const String& rString, uint16_t nPos,
                sal_Unicode cQuote = '"',
                sal_Unicode cEscIn = '\0', sal_Unicode cEscOut = '\\' );

    void SetComment( const String& rStr ) { sComment = rStr; }
    const String& GetComment() const { return sComment; }

    // Erase "{ "..." }" from format subcode string to get the pure comment (old version)
    static void EraseCommentBraces( String& rStr );
    // Set comment rStr in format string rFormat and in rComment (old version)
    static void SetComment( const String& rStr, String& rFormat, String& rComment );
    // Erase comment at end of rStr to get pure format code string (old version)
    static void EraseComment( String& rStr );

    /** Insert the number of blanks into the string that is needed to simulate
        the width of character c for underscore formats */
    static uint16_t InsertBlanks( String& r, uint16_t nPos, sal_Unicode c );

    /// One of YMD,DMY,MDY if date format
    DateFormat GetDateOrder() const;

    /** A coded value of the exact YMD combination used, if date format.
        For example: YYYY-MM-DD => ('Y' << 16) | ('M' << 8) | 'D'
        or: MM/YY => ('M' << 8) | 'Y'  */
    sal_uInt32 GetExactDateOrder() const;

    ImpSvNumberformatScan* ImpGetScan() const { return rScanPtr; }

    // used in XML export
    void GetConditions( SvNumberformatLimitOps& rOper1, double& rVal1,
                        SvNumberformatLimitOps& rOper2, double& rVal2 ) const;
    Color* GetColor( sal_uInt16 nNumFor ) const;
    void GetNumForInfo( sal_uInt16 nNumFor, short& rScannedType,
                    bool& bThousand, sal_uInt16& nPrecision, sal_uInt16& nAnzLeading ) const;

    // rAttr.Number not empty if NatNum attributes are to be stored
    //void GetNatNumXml(
    //        ::com::sun::star::i18n::NativeNumberXmlAttributes& rAttr,
    //        sal_uInt16 nNumFor ) const;

    /** @returns <TRUE/> if E,EE,R,RR,AAA,AAAA in format code of subformat
        nNumFor (0..3) and <b>no</b> preceding calendar was specified and the
        currently loaded calendar is "gregorian". */
    bool IsOtherCalendar( sal_uInt16 nNumFor ) const
        {
            if ( nNumFor < 4 )
                return ImpIsOtherCalendar( NumFor[nNumFor] );
            return false;
        }

    /** Switches to the first non-"gregorian" calendar, but only if the current
        calendar is "gregorian"; original calendar name and date/time returned,
        but only if calendar switched and rOrgCalendar was empty. */
    void SwitchToOtherCalendar( String& rOrgCalendar, double& fOrgDateTime ) const;

    /** Switches to the "gregorian" calendar, but only if the current calendar
        is non-"gregorian" and rOrgCalendar is not empty. Thus a preceding
        ImpSwitchToOtherCalendar() call should have been placed prior to
        calling this method. */
    void SwitchToGregorianCalendar( const String& rOrgCalendar, double fOrgDateTime ) const;

    /** Switches to the first specified calendar, if any, in subformat nNumFor
        (0..3). Original calendar name and date/time returned, but only if
        calendar switched and rOrgCalendar was empty.

        @return
            <TRUE/> if a calendar was specified and switched to,
            <FALSE/> else.
     */
    bool SwitchToSpecifiedCalendar( String& rOrgCalendar, double& fOrgDateTime,
            sal_uInt16 nNumFor ) const
        {
            if ( nNumFor < 4 )
                return ImpSwitchToSpecifiedCalendar( rOrgCalendar,
                        fOrgDateTime, NumFor[nNumFor] );
            return false;
        }

private:
    ImpSvNumFor NumFor[4];          // Array for the 4 subformats
    String sFormatstring;           // The format code string
    String sComment;                // Comment, since number formatter version 6
    double fLimit1;                 // Value for first condition
    double fLimit2;                 // Value for second condition
	LocaleData* pFormatter;
	ImpSvNumberformatScan* rScanPtr;
    LanguageType eLnge;             // Language/country of the format
    SvNumberformatLimitOps eOp1;    // Operator for first condition
    SvNumberformatLimitOps eOp2;    // Operator for second condition
    sal_uInt16 nNewStandardDefined;     // new builtin formats as of version 6
    short eType;                    // Type of format
    bool bStarFlag;                 // Take *n format as ESC n
    bool bStandard;                 // If this is a default standard format
    bool bIsUsed;                   // Flag as used for storing

     sal_uInt16 ImpGetNumForStringElementCount( sal_uInt16 nNumFor ) const;

     bool ImpIsOtherCalendar( const ImpSvNumFor& rNumFor ) const;

     bool ImpSwitchToSpecifiedCalendar( String& rOrgCalendar,
            double& fOrgDateTime, const ImpSvNumFor& rNumFor ) const;

    //const LocaleDataWrapper& rLoc() const   { return rScan.GetLoc(); }
    //CalendarWrapper& GetCal() const         { return rScan.GetCal(); }

    // divide in substrings and color conditions
     short ImpNextSymbol( String& rString,
                     uint16_t& nPos,
                     String& sSymbol );

    // read string until ']' and strip blanks (after condition)
     static uint16_t ImpGetNumber( String& rString,
                   uint16_t& nPos,
                   String& sSymbol );

    // get xxx of "[$-xxx]" as LanguageType, starting at and advancing position nPos
     static LanguageType ImpGetLanguageType( const String& rString, uint16_t& nPos );

    // standard number output
     void ImpGetOutputStandard( double& fNumber, String& OutString );
     void ImpGetOutputStdToPrecision( double& rNumber, String& rOutString, sal_uInt16 nPrecision ) const;
    // numbers in input line
     void ImpGetOutputInputLine( double fNumber, String& OutString );

    // check subcondition
    // OP undefined => -1
    // else 0 or 1
     short ImpCheckCondition(double& fNumber,
                         double& fLimit,
                         SvNumberformatLimitOps eOp);

     sal_uLong ImpGGT(sal_uLong x, sal_uLong y);
     sal_uLong ImpGGTRound(sal_uLong x, sal_uLong y);

    // Helper function for number strings
    // append string symbols, insert leading 0 or ' ', or ...
     bool ImpNumberFill( String& sStr,
                    double& rNumber,
                    uint16_t& k,
                    sal_uInt16& j,
                    sal_uInt16 nIx,
                    short eSymbolType );

    // Helper function to fill in the integer part and the group (AKA thousand) separators
     bool ImpNumberFillWithThousands( String& sStr,
                                 double& rNumber,
                                 uint16_t k,
                                 sal_uInt16 j,
                                 sal_uInt16 nIx,
                                 sal_uInt16 nDigCnt );
                                    // Hilfsfunktion zum Auffuellen der Vor-
                                    // kommazahl auch mit Tausenderpunkt

    // Helper function to fill in the group (AKA thousand) separators
    // or to skip additional digits
     void ImpDigitFill( String& sStr,
                    uint16_t nStart,
                    uint16_t& k,
                    sal_uInt16 nIx,
                    uint16_t & nDigitCount,
                    DigitGroupingIterator & rGrouping );

     bool ImpGetDateOutput( double fNumber,
                       sal_uInt16 nIx,
                       String& OutString );
     bool ImpGetTimeOutput( double fNumber,
                       sal_uInt16 nIx,
                       String& OutString );
     bool ImpGetDateTimeOutput( double fNumber,
                           sal_uInt16 nIx,
                           String& OutString );

    // Switches to the "gregorian" calendar if the current calendar is
    // non-"gregorian" and the era is a "Dummy" era of a calendar which doesn't
    // know a "before" era (like zh_TW ROC or ja_JP Gengou). If switched and
    // rOrgCalendar was "gregorian" the string is emptied. If rOrgCalendar was
    // empty the previous calendar name and date/time are returned.
     bool ImpFallBackToGregorianCalendar( String& rOrgCalendar, double& fOrgDateTime );

    // Append a "G" short era string of the given calendar. In the case of a
    // Gengou calendar this is a one character abbreviation, for other
    // calendars the XExtendedCalendar::getDisplayString() method is called.
     void ImpAppendEraG( String& OutString, sal_Int16 nNatNum );

     bool ImpGetNumberOutput( double fNumber,
                         sal_uInt16 nIx,
                         String& OutString );

     //void ImpCopyNumberformat( const SvNumberformat& rFormat );

    // normal digits or other digits, depending on ImpSvNumFor.aNatNum,
    // [NatNum1], [NatNum2], ...
     String ImpGetNatNumString( const SvNumberNatNum& rNum, sal_Int32 nVal,
            sal_uInt16 nMinDigits = 0  ) const;

    String ImpIntToString( sal_uInt16 nIx, sal_Int32 nVal, sal_uInt16 nMinDigits = 0 ) const
        {
            const SvNumberNatNum& rNum = NumFor[nIx].GetNatNum();
            if ( nMinDigits || rNum.IsComplete() )
                return ImpGetNatNumString( rNum, nVal, nMinDigits );
            return std::to_wstring(nVal);
        }

    // transliterate according to NativeNumber
     //void ImpTransliterateImpl( String& rStr, const SvNumberNatNum& rNum ) const;

    void ImpTransliterate( String& rStr, const SvNumberNatNum& rNum ) const
        {
            //if ( rNum.IsComplete() )
            //    ImpTransliterateImpl( rStr, rNum );
        }
};

}   // namespace duckdb_excel

#endif  // _NF_ZFORMAT_H
