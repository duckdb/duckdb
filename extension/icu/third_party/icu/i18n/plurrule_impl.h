// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
*******************************************************************************
* Copyright (C) 2007-2016, International Business Machines Corporation and
* others. All Rights Reserved.
*******************************************************************************
*
* File PLURRULE_IMPL.H
*
*******************************************************************************
*/


#ifndef PLURRULE_IMPL_H
#define PLURRULE_IMPL_H

// Internal definitions for the PluralRules implementation.

#include "unicode/utypes.h"

#if !UCONFIG_NO_FORMATTING

#include "unicode/format.h"
#include "unicode/locid.h"
#include "unicode/parseerr.h"
#include "unicode/strenum.h"
#include "unicode/ures.h"
#include "uvector.h"
#include "hash.h"
#include "uassert.h"

class PluralRulesTest;

U_NAMESPACE_BEGIN

class AndConstraint;
class RuleChain;
class Digiplurrule_token_tInterval;
class PluralRules;
class VisibleDigits;

// namespace pluralimpl {

// TODO: Remove this and replace with u"" literals. Was for EBCDIC compatibility.

static const UChar PLURRULE_DOT = ((UChar) 0x002E);
// static const UChar PLURRULE_SINGLE_QUOTE = ((UChar) 0x0027);
// static const UChar PLURRULE_SLASH = ((UChar) 0x002F);
// static const UChar PLURRULE_BACKSLASH = ((UChar) 0x005C);
static const UChar PLURRULE_SPACE = ((UChar) 0x0020);
static const UChar PLURRULE_EXCLAMATION = ((UChar) 0x0021);
// static const UChar PLURRULE_QUOTATION_MARK = ((UChar) 0x0022);
// static const UChar PLURRULE_NUMBER_SIGN = ((UChar) 0x0023);
static const UChar PLURRULE_PERCENT_SIGN = ((UChar) 0x0025);
// static const UChar PLURRULE_ASTERISK = ((UChar) 0x002A);
static const UChar PLURRULE_COMMA = ((UChar) 0x002C);
// static const UChar PLURRULE_HYPHEN = ((UChar) 0x002D);
static const UChar PLURRULE_U_ZERO = ((UChar) 0x0030);
// static const UChar PLURRULE_U_ONE = ((UChar) 0x0031);
// static const UChar PLURRULE_U_TWO = ((UChar) 0x0032);
// static const UChar PLURRULE_U_THREE = ((UChar) 0x0033);
// static const UChar PLURRULE_U_FOUR = ((UChar) 0x0034);
// static const UChar PLURRULE_U_FIVE = ((UChar) 0x0035);
// static const UChar PLURRULE_U_SIX = ((UChar) 0x0036);
// static const UChar PLURRULE_U_SEVEN = ((UChar) 0x0037);
// static const UChar PLURRULE_U_EIGHT = ((UChar) 0x0038);
static const UChar PLURRULE_U_NINE = ((UChar) 0x0039);
static const UChar PLURRULE_COLON = ((UChar) 0x003A);
static const UChar PLURRULE_SEMI_COLON = ((UChar) 0x003B);
static const UChar PLURRULE_EQUALS = ((UChar) 0x003D);
static const UChar PLURRULE_AT = ((UChar) 0x0040);
// static const UChar PLURRULE_CAP_A = ((UChar) 0x0041);
// static const UChar PLURRULE_CAP_B = ((UChar) 0x0042);
// static const UChar PLURRULE_CAP_R = ((UChar) 0x0052);
// static const UChar PLURRULE_CAP_Z = ((UChar) 0x005A);
// static const UChar PLURRULE_LOWLINE = ((UChar) 0x005F);
// static const UChar PLURRULE_LEFTBRACE = ((UChar) 0x007B);
// static const UChar PLURRULE_RIGHTBRACE = ((UChar) 0x007D);
static const UChar PLURRULE_TILDE = ((UChar) 0x007E);
static const UChar PLURRULE_ELLIPSIS = ((UChar) 0x2026);

static const UChar PLURRULE_LOW_A = ((UChar) 0x0061);
// static const UChar PLURRULE_LOW_B = ((UChar) 0x0062);
static const UChar PLURRULE_LOW_C = ((UChar) 0x0063);
static const UChar PLURRULE_LOW_D = ((UChar) 0x0064);
static const UChar PLURRULE_LOW_E = ((UChar) 0x0065);
static const UChar PLURRULE_LOW_F = ((UChar) 0x0066);
static const UChar PLURRULE_LOW_G = ((UChar) 0x0067);
static const UChar PLURRULE_LOW_H = ((UChar) 0x0068);
static const UChar PLURRULE_LOW_I = ((UChar) 0x0069);
// static const UChar PLURRULE_LOW_J = ((UChar) 0x006a);
// static const UChar PLURRULE_LOW_K = ((UChar) 0x006B);
static const UChar PLURRULE_LOW_L = ((UChar) 0x006C);
static const UChar PLURRULE_LOW_M = ((UChar) 0x006D);
static const UChar PLURRULE_LOW_N = ((UChar) 0x006E);
static const UChar PLURRULE_LOW_O = ((UChar) 0x006F);
// static const UChar PLURRULE_LOW_P = ((UChar) 0x0070);
// static const UChar PLURRULE_LOW_Q = ((UChar) 0x0071);
static const UChar PLURRULE_LOW_R = ((UChar) 0x0072);
static const UChar PLURRULE_LOW_S = ((UChar) 0x0073);
static const UChar PLURRULE_LOW_T = ((UChar) 0x0074);
// static const UChar PLURRULE_LOW_U = ((UChar) 0x0075);
static const UChar PLURRULE_LOW_V = ((UChar) 0x0076);
static const UChar PLURRULE_LOW_W = ((UChar) 0x0077);
// static const UChar PLURRULE_LOW_Y = ((UChar) 0x0079);
static const UChar PLURRULE_LOW_Z = ((UChar) 0x007A);

// }


static const int32_t PLURAL_RANGE_HIGH = 0x7fffffff;

enum tokenType {
  plurrule_token_none,
  plurrule_token_tNumber,
  plurrule_token_tComma,
  plurrule_token_tSemiColon,
  plurrule_token_tSpace,
  plurrule_token_tColon,
  plurrule_token_tAt,           // '@'
  plurrule_token_tDot,
  plurrule_token_tDot2,
  plurrule_token_tEllipsis,
  plurrule_token_tKeyword,
  plurrule_token_tAnd,
  plurrule_token_tOr,
  plurrule_token_tMod,          // 'mod' or '%'
  plurrule_token_tNot,          //  'not' only.
  plurrule_token_tIn,           //  'in'  only.
  plurrule_token_tEqual,        //  '='   only.
  plurrule_token_tNotEqual,     //  '!='
  plurrule_token_tTilde,
  plurrule_token_tWithin,
  plurrule_token_tIs,
  plurrule_token_tVariableN,
  plurrule_token_tVariableI,
  plurrule_token_tVariableF,
  plurrule_token_tVariableV,
  plurrule_token_tVariableT,
  plurrule_token_tDecimal,
  plurrule_token_tInteger,
  plurrule_token_tEOF
};


class PluralRuleParser: public UMemory {
public:
    PluralRuleParser();
    virtual ~PluralRuleParser();

    void parse(const UnicodeString &rules, PluralRules *dest, UErrorCode &status);
    void getNextToken(UErrorCode &status);
    void checkSyntax(UErrorCode &status);
    static int32_t getNumberValue(const UnicodeString &token);

private:
    static tokenType getKeyType(const UnicodeString& token, tokenType type);
    static tokenType charType(UChar ch);
    static UBool isValidKeyword(const UnicodeString& token);

    const UnicodeString  *ruleSrc;  // The rules string.
    int32_t        ruleIndex;       // String index in the input rules, the current parse position.
    UnicodeString  token;           // Token most recently scanned.
    tokenType      type;
    tokenType      prevType;

                                    // The items currently being parsed & built.
                                    // Note: currentChain may not be the last RuleChain in the
                                    //       list because the "other" chain is forced to the end.
    AndConstraint *curAndConstraint;
    RuleChain     *currentChain;

    int32_t        rangeLowIdx;     // Indices in the UVector of ranges of the
    int32_t        rangeHiIdx;      //    low and hi values currently being parsed.

    enum EParseState {
       kKeyword,
       kExpr,
       kValue,
       kRangeList,
       kSamples
    };
};

enum PluralOperand {
    /**
    * The double value of the entire number.
    */
    PLURAL_OPERAND_N,

    /**
     * The integer value, with the fraction digits truncated off.
     */
    PLURAL_OPERAND_I,

    /**
     * All visible fraction digits as an integer, including trailing zeros.
     */
    PLURAL_OPERAND_F,

    /**
     * Visible fraction digits as an integer, not including trailing zeros.
     */
    PLURAL_OPERAND_T,

    /**
     * Number of visible fraction digits.
     */
    PLURAL_OPERAND_V,

    /**
     * Number of visible fraction digits, not including trailing zeros.
     */
    PLURAL_OPERAND_W,

    /**
     * THIS OPERAND IS DEPRECATED AND HAS BEEN REMOVED FROM THE SPEC.
     *
     * <p>Returns the integer value, but will fail if the number has fraction digits.
     * That is, using "j" instead of "i" is like implicitly adding "v is 0".
     *
     * <p>For example, "j is 3" is equivalent to "i is 3 and v is 0": it matches
     * "3" but not "3.1" or "3.0".
     */
    PLURAL_OPERAND_J
};

/**
 * Converts from the tokenType enum to PluralOperand. Asserts that the given
 * tokenType can be mapped to a PluralOperand.
 */
PluralOperand tokenTypeToPluralOperand(tokenType tt);

/**
 * An interface to FixedDecimal, allowing for other implementations.
 * @internal
 */
class U_I18N_API IFixedDecimal {
  public:
    virtual ~IFixedDecimal();

    /**
     * Returns the value corresponding to the specified operand (n, i, f, t, v, or w).
     * If the operand is 'n', returns a double; otherwise, returns an integer.
     */
    virtual double getPluralOperand(PluralOperand operand) const = 0;

    virtual bool isNaN() const = 0;

    virtual bool isInfinite() const = 0;

    /** Whether the number has no nonzero fraction digits. */
    virtual bool hasIntegerValue() const = 0;
};

/**
 * class FixedDecimal serves to communicate the properties
 * of a formatted number from a decimal formatter to PluralRules::select()
 *
 * see DecimalFormat::getFixedDecimal()
 * @internal
 */
class U_I18N_API FixedDecimal: public IFixedDecimal, public UObject {
  public:
    /**
      * @param n   the number, e.g. 12.345
      * @param v   The number of visible fraction digits, e.g. 3
      * @param f   The fraction digits, e.g. 345
      */
    FixedDecimal(double  n, int32_t v, int64_t f);
    FixedDecimal(double n, int32_t);
    explicit FixedDecimal(double n);
    FixedDecimal();
    ~FixedDecimal() U_OVERRIDE;
    FixedDecimal(const UnicodeString &s, UErrorCode &ec);
    FixedDecimal(const FixedDecimal &other);

    double getPluralOperand(PluralOperand operand) const U_OVERRIDE;
    bool isNaN() const U_OVERRIDE;
    bool isInfinite() const U_OVERRIDE;
    bool hasIntegerValue() const U_OVERRIDE;

    bool isNanOrInfinity() const;  // used in decimfmtimpl.cpp

    int32_t getVisibleFractionDigitCount() const;

    void init(double n, int32_t v, int64_t f);
    void init(double n);
    UBool quickInit(double n);  // Try a fast-path only initialization,
                                //    return TRUE if successful.
    void adjustForMinFractionDigits(int32_t min);
    static int64_t getFractionalDigits(double n, int32_t v);
    static int32_t decimals(double n);

    double      source;
    int32_t     visibleDecimalDigitCount;
    int64_t     decimalDigits;
    int64_t     decimalDigitsWithoutTrailingZeros;
    int64_t     intValue;
    UBool       _hasIntegerValue;
    UBool       isNegative;
    UBool       _isNaN;
    UBool       _isInfinite;
};

class AndConstraint : public UMemory  {
public:
    typedef enum RuleOp {
        NONE,
        MOD
    } RuleOp;
    RuleOp op = AndConstraint::NONE;
    int32_t opNum = -1;             // for mod expressions, the right operand of the mod.
    int32_t value = -1;             // valid for 'is' rules only.
    UVector32 *rangeList = nullptr; // for 'in', 'within' rules. Null otherwise.
    UBool negated = FALSE;          // TRUE for negated rules.
    UBool integerOnly = FALSE;      // TRUE for 'within' rules.
    tokenType digitsType = plurrule_token_none;    // n | i | v | f constraint.
    AndConstraint *next = nullptr;
    // Internal error status, used for errors that occur during the copy constructor.
    UErrorCode fInternalStatus = U_ZERO_ERROR;

    AndConstraint() = default;
    AndConstraint(const AndConstraint& other);
    virtual ~AndConstraint();
    AndConstraint* add(UErrorCode& status);
    // UBool isFulfilled(double number);
    UBool isFulfilled(const IFixedDecimal &number);
};

class OrConstraint : public UMemory  {
public:
    AndConstraint *childNode = nullptr;
    OrConstraint *next = nullptr;
    // Internal error status, used for errors that occur during the copy constructor.
    UErrorCode fInternalStatus = U_ZERO_ERROR;

    OrConstraint() = default;
    OrConstraint(const OrConstraint& other);
    virtual ~OrConstraint();
    AndConstraint* add(UErrorCode& status);
    // UBool isFulfilled(double number);
    UBool isFulfilled(const IFixedDecimal &number);
};

class RuleChain : public UMemory  {
public:
    UnicodeString   fKeyword;
    RuleChain      *fNext = nullptr;
    OrConstraint   *ruleHeader = nullptr;
    UnicodeString   fDecimalSamples;  // Samples strings from rule source
    UnicodeString   fIntegerSamples;  //   without @decimal or @integer, otherwise unprocessed.
    UBool           fDecimalSamplesUnbounded = FALSE;
    UBool           fIntegerSamplesUnbounded = FALSE;
    // Internal error status, used for errors that occur during the copy constructor.
    UErrorCode      fInternalStatus = U_ZERO_ERROR;

    RuleChain() = default;
    RuleChain(const RuleChain& other);
    virtual ~RuleChain();

    UnicodeString select(const IFixedDecimal &number) const;
    void          dumpRules(UnicodeString& result);
    UErrorCode    getKeywords(int32_t maxArraySize, UnicodeString *keywords, int32_t& arraySize) const;
    UBool         isKeyword(const UnicodeString& keyword) const;
};

class PluralKeywordEnumeration : public StringEnumeration {
public:
    PluralKeywordEnumeration(RuleChain *header, UErrorCode& status);
    virtual ~PluralKeywordEnumeration();
    static UClassID U_EXPORT2 getStaticClassID(void);
    virtual UClassID getDynamicClassID(void) const;
    virtual const UnicodeString* snext(UErrorCode& status);
    virtual void reset(UErrorCode& status);
    virtual int32_t count(UErrorCode& status) const;
private:
    int32_t         pos;
    UVector         fKeywordNames;
};


class U_I18N_API PluralAvailableLocalesEnumeration: public StringEnumeration {
  public:
    PluralAvailableLocalesEnumeration(UErrorCode &status);
    virtual ~PluralAvailableLocalesEnumeration();
    virtual const char* next(int32_t *resultLength, UErrorCode& status);
    virtual void reset(UErrorCode& status);
    virtual int32_t count(UErrorCode& status) const;
  private:
    UErrorCode      fOpenStatus;
    UResourceBundle *fLocales = nullptr;
    UResourceBundle *fRes = nullptr;
};

U_NAMESPACE_END

#endif /* #if !UCONFIG_NO_FORMATTING */

#endif // _PLURRULE_IMPL
//eof
