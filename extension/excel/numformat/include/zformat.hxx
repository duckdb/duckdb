#ifndef _ZFORMAT_HXX
#define _ZFORMAT_HXX

#include <string>
#include "define.h"
#include "nfkeytab.hxx"
#include "digitgroupingiterator.hxx"

// We need ImpSvNumberformatScan for the private SvNumberformat definitions.
#include "zforscan.hxx"
#include "localedata.h"

// If comment field is also in format code string, was used for SUPD versions 371-372
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
    //void Load(SvStream& rStream, sal_uInt16 nAnz);
    //void Save(SvStream& rStream, sal_uInt16 nAnz) const;
};

// NativeNumber, represent numbers using CJK or other digits if nNum>0,
// eLang specifies the Locale to use.
class SvNumberNatNum
{
	LocaleIndentifier    eLang;
    sal_uInt8            nNum;
    bool            bDBNum  :1;     // DBNum, to be converted to NatNum
    bool            bDate   :1;     // Used in date? (needed for DBNum/NatNum mapping)
    bool            bSet    :1;     // If set, since NatNum0 is possible

public:

    //static  sal_uInt8    MapDBNumToNatNum( sal_uInt8 nDBNum, LanguageType eLang, bool bDate );
    //static  sal_uInt8    MapNatNumToDBNum( sal_uInt8 nNatNum, LanguageType eLang, bool bDate );

                    SvNumberNatNum() : eLang( LocaleId_en_US ), nNum(0),
                                        bDBNum(0), bDate(0), bSet(0) {}
    bool            IsComplete() const  { return bSet && eLang != LocaleId_en_US; }
    sal_uInt8            GetRawNum() const   { return nNum; }
	sal_uInt8            GetNatNum() const { return 0; }// { return bDBNum ? MapDBNumToNatNum(nNum, eLang, bDate) : nNum; }
    //sal_uInt8            GetDBNum() const    { return bDBNum ? nNum : MapNatNumToDBNum( nNum, eLang, bDate ); }
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
    //void Load( SvStream& rStream, ImpSvNumberformatScan& rSc, String& rLoadedColorName);
    //void Save( SvStream& rStream ) const;

    // if pSc is set, it is used to get the Color pointer
    void Copy( const ImpSvNumFor& rNumFor, ImpSvNumberformatScan* pSc );

    // Access to Info; call Enlarge before!
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
    //void SaveNewCurrencyMap( SvStream& rStream ) const;
    //void LoadNewCurrencyMap( SvStream& rStream );

    // [NatNum1], [NatNum2], ...
    void SetNatNumNum( sal_uInt8 nNum, bool bDBNum ) { aNatNum.SetNum( nNum, bDBNum ); }
    void SetNatNumLang( LanguageType eLang ) { aNatNum.SetLang( eLang ); }
    void SetNatNumDate( bool bDate ) { aNatNum.SetDate( bDate ); }
    const SvNumberNatNum& GetNatNum() const { return aNatNum; }

    // check, if the format code contains a subformat for text
    bool HasTextFormatCode() const;

private:
    ImpSvNumberformatInfo aI;           // Hilfsstruct fuer die restlichen Infos
    String sColorName;                  // color name
    Color* pColor;                      // pointer to color of subformat
    sal_uInt16 nAnzStrings;                 // count of symbols
    SvNumberNatNum aNatNum;             // DoubleByteNumber

};

class LocaleData;

class  SvNumberformat
{
public:
    // Ctor for Load
    //SvNumberformat( ImpSvNumberformatScan& rSc, LanguageType eLge );

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
	
	// Copy ctor
    //SvNumberformat( SvNumberformat& rFormat );

    // Copy ctor with exchange of format code string scanner (used in merge)
    //SvNumberformat( SvNumberformat& rFormat, ImpSvNumberformatScan& rSc );

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

    // Build a format string of application defined keywords
    //String GetMappedFormatstring( const NfKeywordTable& rKeywords,
    //                                const LocaleDataWrapper& rLoc,
    //                                bool bDontQuote = false ) const;

    void SetUsed(const bool b)                  { bIsUsed = b; }
    bool GetUsed() const                        { return bIsUsed; }
    bool IsStarFormatSupported() const          { return bStarFlag; }
    void SetStarFormatSupport( bool b )         { bStarFlag = b; }

    //NfHackConversion Load( SvStream& rStream, ImpSvNumMultipleReadHeader& rHdr, MyLocaleData* pConverter, ImpSvNumberInputScan& rISc );
    //void Save( SvStream& rStream, ImpSvNumMultipleWriteHeader& rHdr  ) const;

    // Load a string which might contain an Euro symbol,
    // in fact that could be any string used in number formats.
    //static void LoadString( SvStream& rStream, String& rStr );

    /** 
     * Get output string from a numeric value that fits the number of 
     * characters specified.
     */
    bool GetOutputString( double fNumber, sal_uInt16 nCharCount, String& rOutString ) const;

    bool GetOutputString( double fNumber, String& OutString, Color** ppColor );
	bool GetOutputString( double fNumber, std::string& OutString, Color** ppColor );
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

    void SetComment( const String& rStr )
#if NF_COMMENT_IN_FORMATSTRING
        { SetComment( rStr, sFormatstring, sComment ); }
#else
        { sComment = rStr; }
#endif
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

std::string GetNumberFormatString(std::string& format, double num_value);

#endif  // _ZFORMAT_HXX
