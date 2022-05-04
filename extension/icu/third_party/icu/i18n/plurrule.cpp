// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
*******************************************************************************
* Copyright (C) 2007-2016, International Business Machines Corporation and
* others. All Rights Reserved.
*******************************************************************************
*
* File plurrule.cpp
*/

#include <math.h>
#include <stdio.h>

#include "unicode/utypes.h"
#include "unicode/localpointer.h"
#include "unicode/plurrule.h"
#include "unicode/upluralrules.h"
#include "unicode/ures.h"
#include "unicode/numfmt.h"
#include "unicode/decimfmt.h"
#include "charstr.h"
#include "cmemory.h"
#include "cstring.h"
#include "hash.h"
#include "locutil.h"
#include "mutex.h"
#include "patternprops.h"
#include "plurrule_impl.h"
#include "putilimp.h"
#include "ucln_in.h"
#include "ustrfmt.h"
#include "uassert.h"
#include "uvectr32.h"
#include "sharedpluralrules.h"
#include "unifiedcache.h"
#include "number_decimalquantity.h"
#include "util.h"

#if !UCONFIG_NO_FORMATTING

U_NAMESPACE_BEGIN

using icu::number::impl::DecimalQuantity;

static const UChar PLURAL_KEYWORD_OTHER[]={PLURRULE_LOW_O,PLURRULE_LOW_T,PLURRULE_LOW_H,PLURRULE_LOW_E,PLURRULE_LOW_R,0};
static const UChar PLURAL_DEFAULT_RULE[]={PLURRULE_LOW_O,PLURRULE_LOW_T,PLURRULE_LOW_H,PLURRULE_LOW_E,PLURRULE_LOW_R,PLURRULE_COLON,PLURRULE_SPACE,PLURRULE_LOW_N,0};
static const UChar PK_IN[]={PLURRULE_LOW_I,PLURRULE_LOW_N,0};
static const UChar PK_NOT[]={PLURRULE_LOW_N,PLURRULE_LOW_O,PLURRULE_LOW_T,0};
static const UChar PK_IS[]={PLURRULE_LOW_I,PLURRULE_LOW_S,0};
static const UChar PK_MOD[]={PLURRULE_LOW_M,PLURRULE_LOW_O,PLURRULE_LOW_D,0};
static const UChar PK_AND[]={PLURRULE_LOW_A,PLURRULE_LOW_N,PLURRULE_LOW_D,0};
static const UChar PK_OR[]={PLURRULE_LOW_O,PLURRULE_LOW_R,0};
static const UChar PK_VAR_N[]={PLURRULE_LOW_N,0};
static const UChar PK_VAR_I[]={PLURRULE_LOW_I,0};
static const UChar PK_VAR_F[]={PLURRULE_LOW_F,0};
static const UChar PK_VAR_T[]={PLURRULE_LOW_T,0};
static const UChar PK_VAR_V[]={PLURRULE_LOW_V,0};
static const UChar PK_WITHIN[]={PLURRULE_LOW_W,PLURRULE_LOW_I,PLURRULE_LOW_T,PLURRULE_LOW_H,PLURRULE_LOW_I,PLURRULE_LOW_N,0};
static const UChar PK_DECIMAL[]={PLURRULE_LOW_D,PLURRULE_LOW_E,PLURRULE_LOW_C,PLURRULE_LOW_I,PLURRULE_LOW_M,PLURRULE_LOW_A,PLURRULE_LOW_L,0};
static const UChar PK_INTEGER[]={PLURRULE_LOW_I,PLURRULE_LOW_N,PLURRULE_LOW_T,PLURRULE_LOW_E,PLURRULE_LOW_G,PLURRULE_LOW_E,PLURRULE_LOW_R,0};

UOBJECT_DEFINE_RTTI_IMPLEMENTATION(PluralRules)
UOBJECT_DEFINE_RTTI_IMPLEMENTATION(PluralKeywordEnumeration)

PluralRules::PluralRules(UErrorCode& /*status*/)
:   UObject(),
    mRules(nullptr),
    mInternalStatus(U_ZERO_ERROR)
{
}

PluralRules::PluralRules(const PluralRules& other)
: UObject(other),
    mRules(nullptr),
    mInternalStatus(U_ZERO_ERROR)
{
    *this=other;
}

PluralRules::~PluralRules() {
    delete mRules;
}

SharedPluralRules::~SharedPluralRules() {
    delete ptr;
}

PluralRules*
PluralRules::clone() const {
    PluralRules* newObj = new PluralRules(*this);
    // Since clone doesn't have a 'status' parameter, the best we can do is return nullptr if
    // the newly created object was not fully constructed properly (an error occurred).
    if (newObj != nullptr && U_FAILURE(newObj->mInternalStatus)) {
        delete newObj;
        newObj = nullptr;
    }
    return newObj;
}

PluralRules&
PluralRules::operator=(const PluralRules& other) {
    if (this != &other) {
        delete mRules;
        mRules = nullptr;
        mInternalStatus = other.mInternalStatus;
        if (U_FAILURE(mInternalStatus)) {
            // bail out early if the object we were copying from was already 'invalid'.
            return *this;
        }
        if (other.mRules != nullptr) {
            mRules = new RuleChain(*other.mRules);
            if (mRules == nullptr) {
                mInternalStatus = U_MEMORY_ALLOCATION_ERROR;
            }
            else if (U_FAILURE(mRules->fInternalStatus)) {
                // If the RuleChain wasn't fully copied, then set our status to failure as well.
                mInternalStatus = mRules->fInternalStatus;
            }
        }
    }
    return *this;
}

StringEnumeration* PluralRules::getAvailableLocales(UErrorCode &status) {
    if (U_FAILURE(status)) {
        return nullptr;
    }
    LocalPointer<StringEnumeration> result(new PluralAvailableLocalesEnumeration(status), status);
    if (U_FAILURE(status)) {
        return nullptr;
    }
    return result.orphan();
}


PluralRules* U_EXPORT2
PluralRules::createRules(const UnicodeString& description, UErrorCode& status) {
    if (U_FAILURE(status)) {
        return nullptr;
    }
    PluralRuleParser parser;
    LocalPointer<PluralRules> newRules(new PluralRules(status), status);
    if (U_FAILURE(status)) {
        return nullptr;
    }
    parser.parse(description, newRules.getAlias(), status);
    if (U_FAILURE(status)) {
        newRules.adoptInstead(nullptr);
    }
    return newRules.orphan();
}


PluralRules* U_EXPORT2
PluralRules::createDefaultRules(UErrorCode& status) {
    return createRules(UnicodeString(TRUE, PLURAL_DEFAULT_RULE, -1), status);
}

/******************************************************************************/
/* Create PluralRules cache */

template<> U_I18N_API
const SharedPluralRules *LocaleCacheKey<SharedPluralRules>::createObject(
        const void * /*unused*/, UErrorCode &status) const {
    const char *localeId = fLoc.getName();
    LocalPointer<PluralRules> pr(PluralRules::internalForLocale(localeId, UPLURAL_TYPE_CARDINAL, status), status);
    if (U_FAILURE(status)) {
        return nullptr;
    }
    LocalPointer<SharedPluralRules> result(new SharedPluralRules(pr.getAlias()), status);
    if (U_FAILURE(status)) {
        return nullptr;
    }
    pr.orphan(); // result was successfully created so it nows pr.
    result->addRef();
    return result.orphan();
}

/* end plural rules cache */
/******************************************************************************/

const SharedPluralRules* U_EXPORT2
PluralRules::createSharedInstance(
        const Locale& locale, UPluralType type, UErrorCode& status) {
    if (U_FAILURE(status)) {
        return nullptr;
    }
    if (type != UPLURAL_TYPE_CARDINAL) {
        status = U_UNSUPPORTED_ERROR;
        return nullptr;
    }
    const SharedPluralRules *result = nullptr;
    UnifiedCache::getByLocale(locale, result, status);
    return result;
}

PluralRules* U_EXPORT2
PluralRules::forLocale(const Locale& locale, UErrorCode& status) {
    return forLocale(locale, UPLURAL_TYPE_CARDINAL, status);
}

PluralRules* U_EXPORT2
PluralRules::forLocale(const Locale& locale, UPluralType type, UErrorCode& status) {
    if (type != UPLURAL_TYPE_CARDINAL) {
        return internalForLocale(locale, type, status);
    }
    const SharedPluralRules *shared = createSharedInstance(
            locale, type, status);
    if (U_FAILURE(status)) {
        return nullptr;
    }
    PluralRules *result = (*shared)->clone();
    shared->removeRef();
    if (result == nullptr) {
        status = U_MEMORY_ALLOCATION_ERROR;
    }
    return result;
}

PluralRules* U_EXPORT2
PluralRules::internalForLocale(const Locale& locale, UPluralType type, UErrorCode& status) {
    if (U_FAILURE(status)) {
        return nullptr;
    }
    if (type >= UPLURAL_TYPE_COUNT) {
        status = U_ILLEGAL_ARGUMENT_ERROR;
        return nullptr;
    }
    LocalPointer<PluralRules> newObj(new PluralRules(status), status);
    if (U_FAILURE(status)) {
        return nullptr;
    }
    UnicodeString locRule = newObj->getRuleFromResource(locale, type, status);
    // TODO: which other errors, if any, should be returned?
    if (locRule.length() == 0) {
        // If an out-of-memory error occurred, then stop and report the failure.
        if (status == U_MEMORY_ALLOCATION_ERROR) {
            return nullptr;
        }
        // Locales with no specific rules (all numbers have the "other" category
        //   will return a U_MISSING_RESOURCE_ERROR at this point. This is not
        //   an error.
        locRule =  UnicodeString(PLURAL_DEFAULT_RULE);
        status = U_ZERO_ERROR;
    }
    PluralRuleParser parser;
    parser.parse(locRule, newObj.getAlias(), status);
        //  TODO: should rule parse errors be returned, or
        //        should we silently use default rules?
        //        Original impl used default rules.
        //        Ask the question to ICU Core.

    return newObj.orphan();
}

UnicodeString
PluralRules::select(int32_t number) const {
    return select(FixedDecimal(number));
}

UnicodeString
PluralRules::select(double number) const {
    return select(FixedDecimal(number));
}

UnicodeString
PluralRules::select(const number::FormattedNumber& number, UErrorCode& status) const {
    DecimalQuantity dq;
    number.getDecimalQuantity(dq, status);
    if (U_FAILURE(status)) {
        return ICU_Utility::makeBogusString();
    }
    return select(dq);
}

UnicodeString
PluralRules::select(const IFixedDecimal &number) const {
    if (mRules == nullptr) {
        return UnicodeString(TRUE, PLURAL_DEFAULT_RULE, -1);
    }
    else {
        return mRules->select(number);
    }
}



StringEnumeration*
PluralRules::getKeywords(UErrorCode& status) const {
    if (U_FAILURE(status)) {
        return nullptr;
    }
    if (U_FAILURE(mInternalStatus)) {
        status = mInternalStatus;
        return nullptr;
    }
    LocalPointer<StringEnumeration> nameEnumerator(new PluralKeywordEnumeration(mRules, status), status);
    if (U_FAILURE(status)) {
        return nullptr;
    }
    return nameEnumerator.orphan();
}

double
PluralRules::getUniqueKeywordValue(const UnicodeString& /* keyword */) {
  // Not Implemented.
  return UPLRULES_NO_UNIQUE_VALUE;
}

int32_t
PluralRules::getAllKeywordValues(const UnicodeString & /* keyword */, double * /* dest */,
                                 int32_t /* destCapacity */, UErrorCode& error) {
    error = U_UNSUPPORTED_ERROR;
    return 0;
}


static double scaleForInt(double d) {
    double scale = 1.0;
    while (d != floor(d)) {
        d = d * 10.0;
        scale = scale * 10.0;
    }
    return scale;
}

static int32_t
getSamplesFromString(const UnicodeString &samples, double *dest,
                        int32_t destCapacity, UErrorCode& status) {
    int32_t sampleCount = 0;
    int32_t sampleStartIdx = 0;
    int32_t sampleEndIdx = 0;

    //std::string ss;  // TODO: debugging.
    // std::cout << "PluralRules::getSamples(), samples = \"" << samples.toUTF8String(ss) << "\"\n";
    for (sampleCount = 0; sampleCount < destCapacity && sampleStartIdx < samples.length(); ) {
        sampleEndIdx = samples.indexOf(PLURRULE_COMMA, sampleStartIdx);
        if (sampleEndIdx == -1) {
            sampleEndIdx = samples.length();
        }
        const UnicodeString &sampleRange = samples.tempSubStringBetween(sampleStartIdx, sampleEndIdx);
        // ss.erase();
        // std::cout << "PluralRules::getSamples(), samplesRange = \"" << sampleRange.toUTF8String(ss) << "\"\n";
        int32_t tildeIndex = sampleRange.indexOf(PLURRULE_TILDE);
        if (tildeIndex < 0) {
            FixedDecimal fixed(sampleRange, status);
            double sampleValue = fixed.source;
            if (fixed.visibleDecimalDigitCount == 0 || sampleValue != floor(sampleValue)) {
                dest[sampleCount++] = sampleValue;
            }
        } else {

            FixedDecimal fixedLo(sampleRange.tempSubStringBetween(0, tildeIndex), status);
            FixedDecimal fixedHi(sampleRange.tempSubStringBetween(tildeIndex+1), status);
            double rangeLo = fixedLo.source;
            double rangeHi = fixedHi.source;
            if (U_FAILURE(status)) {
                break;
            }
            if (rangeHi < rangeLo) {
                status = U_INVALID_FORMAT_ERROR;
                break;
            }

            // For ranges of samples with fraction decimal digits, scale the number up so that we
            //   are adding one in the units place. Avoids roundoffs from repetitive adds of tenths.

            double scale = scaleForInt(rangeLo);
            double t = scaleForInt(rangeHi);
            if (t > scale) {
                scale = t;
            }
            rangeLo *= scale;
            rangeHi *= scale;
            for (double n=rangeLo; n<=rangeHi; n+=1) {
                // Hack Alert: don't return any decimal samples with integer values that
                //    originated from a format with trailing decimals.
                //    This API is returning doubles, which can't distinguish having displayed
                //    zeros to the right of the decimal.
                //    This results in test failures with values mapping back to a different keyword.
                double sampleValue = n/scale;
                if (!(sampleValue == floor(sampleValue) && fixedLo.visibleDecimalDigitCount > 0)) {
                    dest[sampleCount++] = sampleValue;
                }
                if (sampleCount >= destCapacity) {
                    break;
                }
            }
        }
        sampleStartIdx = sampleEndIdx + 1;
    }
    return sampleCount;
}


int32_t
PluralRules::getSamples(const UnicodeString &keyword, double *dest,
                        int32_t destCapacity, UErrorCode& status) {
    if (destCapacity == 0 || U_FAILURE(status)) {
        return 0;
    }
    if (U_FAILURE(mInternalStatus)) {
        status = mInternalStatus;
        return 0;
    }
    RuleChain *rc = rulesForKeyword(keyword);
    if (rc == nullptr) {
        return 0;
    }
    int32_t numSamples = getSamplesFromString(rc->fIntegerSamples, dest, destCapacity, status);
    if (numSamples == 0) {
        numSamples = getSamplesFromString(rc->fDecimalSamples, dest, destCapacity, status);
    }
    return numSamples;
}


RuleChain *PluralRules::rulesForKeyword(const UnicodeString &keyword) const {
    RuleChain *rc;
    for (rc = mRules; rc != nullptr; rc = rc->fNext) {
        if (rc->fKeyword == keyword) {
            break;
        }
    }
    return rc;
}


UBool
PluralRules::isKeyword(const UnicodeString& keyword) const {
    if (0 == keyword.compare(PLURAL_KEYWORD_OTHER, 5)) {
        return true;
    }
    return rulesForKeyword(keyword) != nullptr;
}

UnicodeString
PluralRules::getKeywordOther() const {
    return UnicodeString(TRUE, PLURAL_KEYWORD_OTHER, 5);
}

UBool
PluralRules::operator==(const PluralRules& other) const  {
    const UnicodeString *ptrKeyword;
    UErrorCode status= U_ZERO_ERROR;

    if ( this == &other ) {
        return TRUE;
    }
    LocalPointer<StringEnumeration> myKeywordList(getKeywords(status));
    LocalPointer<StringEnumeration> otherKeywordList(other.getKeywords(status));
    if (U_FAILURE(status)) {
        return FALSE;
    }

    if (myKeywordList->count(status)!=otherKeywordList->count(status)) {
        return FALSE;
    }
    myKeywordList->reset(status);
    while ((ptrKeyword=myKeywordList->snext(status))!=nullptr) {
        if (!other.isKeyword(*ptrKeyword)) {
            return FALSE;
        }
    }
    otherKeywordList->reset(status);
    while ((ptrKeyword=otherKeywordList->snext(status))!=nullptr) {
        if (!this->isKeyword(*ptrKeyword)) {
            return FALSE;
        }
    }
    if (U_FAILURE(status)) {
        return FALSE;
    }

    return TRUE;
}


void
PluralRuleParser::parse(const UnicodeString& ruleData, PluralRules *prules, UErrorCode &status)
{
    if (U_FAILURE(status)) {
        return;
    }
    U_ASSERT(ruleIndex == 0);    // Parsers are good for a single use only!
    ruleSrc = &ruleData;

    while (ruleIndex< ruleSrc->length()) {
        getNextToken(status);
        if (U_FAILURE(status)) {
            return;
        }
        checkSyntax(status);
        if (U_FAILURE(status)) {
            return;
        }
        switch (type) {
        case plurrule_token_tAnd:
            U_ASSERT(curAndConstraint != nullptr);
            curAndConstraint = curAndConstraint->add(status);
            break;
        case plurrule_token_tOr:
            {
                U_ASSERT(currentChain != nullptr);
                OrConstraint *orNode=currentChain->ruleHeader;
                while (orNode->next != nullptr) {
                    orNode = orNode->next;
                }
                orNode->next= new OrConstraint();
                if (orNode->next == nullptr) {
                    status = U_MEMORY_ALLOCATION_ERROR;
                    break;
                }
                orNode=orNode->next;
                orNode->next=nullptr;
                curAndConstraint = orNode->add(status);
            }
            break;
        case plurrule_token_tIs:
            U_ASSERT(curAndConstraint != nullptr);
            U_ASSERT(curAndConstraint->value == -1);
            U_ASSERT(curAndConstraint->rangeList == nullptr);
            break;
        case plurrule_token_tNot:
            U_ASSERT(curAndConstraint != nullptr);
            curAndConstraint->negated=TRUE;
            break;

        case plurrule_token_tNotEqual:
            curAndConstraint->negated=TRUE;
            U_FALLTHROUGH;
        case plurrule_token_tIn:
        case plurrule_token_tWithin:
        case plurrule_token_tEqual:
            {
                U_ASSERT(curAndConstraint != nullptr);
                LocalPointer<UVector32> newRangeList(new UVector32(status), status);
                if (U_FAILURE(status)) {
                    break;
                }
                curAndConstraint->rangeList = newRangeList.orphan();
                curAndConstraint->rangeList->addElement(-1, status);  // range Low
                curAndConstraint->rangeList->addElement(-1, status);  // range Hi
                rangeLowIdx = 0;
                rangeHiIdx  = 1;
                curAndConstraint->value=PLURAL_RANGE_HIGH;
                curAndConstraint->integerOnly = (type != plurrule_token_tWithin);
            }
            break;
        case plurrule_token_tNumber:
            U_ASSERT(curAndConstraint != nullptr);
            if ( (curAndConstraint->op==AndConstraint::MOD)&&
                 (curAndConstraint->opNum == -1 ) ) {
                curAndConstraint->opNum=getNumberValue(token);
            }
            else {
                if (curAndConstraint->rangeList == nullptr) {
                    // this is for an 'is' rule
                    curAndConstraint->value = getNumberValue(token);
                } else {
                    // this is for an 'in' or 'within' rule
                    if (curAndConstraint->rangeList->elementAti(rangeLowIdx) == -1) {
                        curAndConstraint->rangeList->setElementAt(getNumberValue(token), rangeLowIdx);
                        curAndConstraint->rangeList->setElementAt(getNumberValue(token), rangeHiIdx);
                    }
                    else {
                        curAndConstraint->rangeList->setElementAt(getNumberValue(token), rangeHiIdx);
                        if (curAndConstraint->rangeList->elementAti(rangeLowIdx) >
                                curAndConstraint->rangeList->elementAti(rangeHiIdx)) {
                            // Range Lower bound > Range Upper bound.
                            // U_UNEXPECTED_TOKEN seems a little funny, but it is consistently
                            // used for all plural rule parse errors.
                            status = U_UNEXPECTED_TOKEN;
                            break;
                        }
                    }
                }
            }
            break;
        case plurrule_token_tComma:
            // TODO: rule syntax checking is inadequate, can happen with badly formed rules.
            //       Catch cases like "n mod 10, is 1" here instead.
            if (curAndConstraint == nullptr || curAndConstraint->rangeList == nullptr) {
                status = U_UNEXPECTED_TOKEN;
                break;
            }
            U_ASSERT(curAndConstraint->rangeList->size() >= 2);
            rangeLowIdx = curAndConstraint->rangeList->size();
            curAndConstraint->rangeList->addElement(-1, status);  // range Low
            rangeHiIdx = curAndConstraint->rangeList->size();
            curAndConstraint->rangeList->addElement(-1, status);  // range Hi
            break;
        case plurrule_token_tMod:
            U_ASSERT(curAndConstraint != nullptr);
            curAndConstraint->op=AndConstraint::MOD;
            break;
        case plurrule_token_tVariableN:
        case plurrule_token_tVariableI:
        case plurrule_token_tVariableF:
        case plurrule_token_tVariableT:
        case plurrule_token_tVariableV:
            U_ASSERT(curAndConstraint != nullptr);
            curAndConstraint->digitsType = type;
            break;
        case plurrule_token_tKeyword:
            {
            RuleChain *newChain = new RuleChain;
            if (newChain == nullptr) {
                status = U_MEMORY_ALLOCATION_ERROR;
                break;
            }
            newChain->fKeyword = token;
            if (prules->mRules == nullptr) {
                prules->mRules = newChain;
            } else {
                // The new rule chain goes at the end of the linked list of rule chains,
                //   unless there is an "other" keyword & chain. "other" must remain last.
                RuleChain *insertAfter = prules->mRules;
                while (insertAfter->fNext!=nullptr &&
                       insertAfter->fNext->fKeyword.compare(PLURAL_KEYWORD_OTHER, 5) != 0 ){
                    insertAfter=insertAfter->fNext;
                }
                newChain->fNext = insertAfter->fNext;
                insertAfter->fNext = newChain;
            }
            OrConstraint *orNode = new OrConstraint();
            if (orNode == nullptr) {
                status = U_MEMORY_ALLOCATION_ERROR;
                break;
            }
            newChain->ruleHeader = orNode;
            curAndConstraint = orNode->add(status);
            currentChain = newChain;
            }
            break;

        case plurrule_token_tInteger:
            for (;;) {
                getNextToken(status);
                if (U_FAILURE(status) || type == plurrule_token_tSemiColon || type == plurrule_token_tEOF || type == plurrule_token_tAt) {
                    break;
                }
                if (type == plurrule_token_tEllipsis) {
                    currentChain->fIntegerSamplesUnbounded = TRUE;
                    continue;
                }
                currentChain->fIntegerSamples.append(token);
            }
            break;

        case plurrule_token_tDecimal:
            for (;;) {
                getNextToken(status);
                if (U_FAILURE(status) || type == plurrule_token_tSemiColon || type == plurrule_token_tEOF || type == plurrule_token_tAt) {
                    break;
                }
                if (type == plurrule_token_tEllipsis) {
                    currentChain->fDecimalSamplesUnbounded = TRUE;
                    continue;
                }
                currentChain->fDecimalSamples.append(token);
            }
            break;

        default:
            break;
        }
        prevType=type;
        if (U_FAILURE(status)) {
            break;
        }
    }
}

UnicodeString
PluralRules::getRuleFromResource(const Locale& locale, UPluralType type, UErrorCode& errCode) {
    UnicodeString emptyStr;

    if (U_FAILURE(errCode)) {
        return emptyStr;
    }
    LocalUResourceBundlePointer rb(ures_openDirect(nullptr, "plurals", &errCode));
    if(U_FAILURE(errCode)) {
        return emptyStr;
    }
    const char *typeKey;
    switch (type) {
    case UPLURAL_TYPE_CARDINAL:
        typeKey = "locales";
        break;
    case UPLURAL_TYPE_ORDINAL:
        typeKey = "locales_ordinals";
        break;
    default:
        // Must not occur: The caller should have checked for valid types.
        errCode = U_ILLEGAL_ARGUMENT_ERROR;
        return emptyStr;
    }
    LocalUResourceBundlePointer locRes(ures_getByKey(rb.getAlias(), typeKey, nullptr, &errCode));
    if(U_FAILURE(errCode)) {
        return emptyStr;
    }
    int32_t resLen=0;
    const char *curLocaleName=locale.getBaseName();
    const UChar* s = ures_getStringByKey(locRes.getAlias(), curLocaleName, &resLen, &errCode);

    if (s == nullptr) {
        // Check parent locales.
        UErrorCode status = U_ZERO_ERROR;
        char parentLocaleName[ULOC_FULLNAME_CAPACITY];
        const char *curLocaleName2=locale.getBaseName();
        uprv_strcpy(parentLocaleName, curLocaleName2);

        while (uloc_getParent(parentLocaleName, parentLocaleName,
                                       ULOC_FULLNAME_CAPACITY, &status) > 0) {
            resLen=0;
            s = ures_getStringByKey(locRes.getAlias(), parentLocaleName, &resLen, &status);
            if (s != nullptr) {
                errCode = U_ZERO_ERROR;
                break;
            }
            status = U_ZERO_ERROR;
        }
    }
    if (s==nullptr) {
        return emptyStr;
    }

    char setKey[256];
    u_UCharsToChars(s, setKey, resLen + 1);
    // printf("\n PluralRule: %s\n", setKey);

    LocalUResourceBundlePointer ruleRes(ures_getByKey(rb.getAlias(), "rules", nullptr, &errCode));
    if(U_FAILURE(errCode)) {
        return emptyStr;
    }
    LocalUResourceBundlePointer setRes(ures_getByKey(ruleRes.getAlias(), setKey, nullptr, &errCode));
    if (U_FAILURE(errCode)) {
        return emptyStr;
    }

    int32_t numberKeys = ures_getSize(setRes.getAlias());
    UnicodeString result;
    const char *key=nullptr;
    for(int32_t i=0; i<numberKeys; ++i) {   // Keys are zero, one, few, ...
        UnicodeString rules = ures_getNextUnicodeString(setRes.getAlias(), &key, &errCode);
        UnicodeString uKey(key, -1, US_INV);
        result.append(uKey);
        result.append(PLURRULE_COLON);
        result.append(rules);
        result.append(PLURRULE_SEMI_COLON);
    }
    return result;
}


UnicodeString
PluralRules::getRules() const {
    UnicodeString rules;
    if (mRules != nullptr) {
        mRules->dumpRules(rules);
    }
    return rules;
}

AndConstraint::AndConstraint(const AndConstraint& other) {
    this->fInternalStatus = other.fInternalStatus;
    if (U_FAILURE(fInternalStatus)) {
        return; // stop early if the object we are copying from is invalid.
    }
    this->op = other.op;
    this->opNum=other.opNum;
    this->value=other.value;
    if (other.rangeList != nullptr) {
        LocalPointer<UVector32> newRangeList(new UVector32(fInternalStatus), fInternalStatus);
        if (U_FAILURE(fInternalStatus)) {
            return;
        }
        this->rangeList = newRangeList.orphan();
        this->rangeList->assign(*other.rangeList, fInternalStatus);
    }
    this->integerOnly=other.integerOnly;
    this->negated=other.negated;
    this->digitsType = other.digitsType;
    if (other.next != nullptr) {
        this->next = new AndConstraint(*other.next);
        if (this->next == nullptr) {
            fInternalStatus = U_MEMORY_ALLOCATION_ERROR;
        }
    }
}

AndConstraint::~AndConstraint() {
    delete rangeList;
    rangeList = nullptr;
    delete next;
    next = nullptr;
}

UBool
AndConstraint::isFulfilled(const IFixedDecimal &number) {
    UBool result = TRUE;
    if (digitsType == plurrule_token_none) {
        // An empty AndConstraint, created by a rule with a keyword but no following expression.
        return TRUE;
    }

    PluralOperand operand = tokenTypeToPluralOperand(digitsType);
    double n = number.getPluralOperand(operand);     // pulls n | i | v | f value for the number.
                                                     // Will always be positive.
                                                     // May be non-integer (n option only)
    do {
        if (integerOnly && n != uprv_floor(n)) {
            result = FALSE;
            break;
        }

        if (op == MOD) {
            n = fmod(n, opNum);
        }
        if (rangeList == nullptr) {
            result = value == -1 ||    // empty rule
                     n == value;       //  'is' rule
            break;
        }
        result = FALSE;                // 'in' or 'within' rule
        for (int32_t r=0; r<rangeList->size(); r+=2) {
            if (rangeList->elementAti(r) <= n && n <= rangeList->elementAti(r+1)) {
                result = TRUE;
                break;
            }
        }
    } while (FALSE);

    if (negated) {
        result = !result;
    }
    return result;
}

AndConstraint*
AndConstraint::add(UErrorCode& status) {
    if (U_FAILURE(fInternalStatus)) {
        status = fInternalStatus;
        return nullptr;
    }
    this->next = new AndConstraint();
    if (this->next == nullptr) {
        status = U_MEMORY_ALLOCATION_ERROR;
    }
    return this->next;
}


OrConstraint::OrConstraint(const OrConstraint& other) {
    this->fInternalStatus = other.fInternalStatus;
    if (U_FAILURE(fInternalStatus)) {
        return; // stop early if the object we are copying from is invalid.
    }
    if ( other.childNode != nullptr ) {
        this->childNode = new AndConstraint(*(other.childNode));
        if (this->childNode == nullptr) {
            fInternalStatus = U_MEMORY_ALLOCATION_ERROR;
            return;
        }
    }
    if (other.next != nullptr ) {
        this->next = new OrConstraint(*(other.next));
        if (this->next == nullptr) {
            fInternalStatus = U_MEMORY_ALLOCATION_ERROR;
            return;
        }
        if (U_FAILURE(this->next->fInternalStatus)) {
            this->fInternalStatus = this->next->fInternalStatus;
        }
    }
}

OrConstraint::~OrConstraint() {
    delete childNode;
    childNode = nullptr;
    delete next;
    next = nullptr;
}

AndConstraint*
OrConstraint::add(UErrorCode& status) {
    if (U_FAILURE(fInternalStatus)) {
        status = fInternalStatus;
        return nullptr;
    }
    OrConstraint *curOrConstraint=this;
    {
        while (curOrConstraint->next!=nullptr) {
            curOrConstraint = curOrConstraint->next;
        }
        U_ASSERT(curOrConstraint->childNode == nullptr);
        curOrConstraint->childNode = new AndConstraint();
        if (curOrConstraint->childNode == nullptr) {
            status = U_MEMORY_ALLOCATION_ERROR;
        }
    }
    return curOrConstraint->childNode;
}

UBool
OrConstraint::isFulfilled(const IFixedDecimal &number) {
    OrConstraint* orRule=this;
    UBool result=FALSE;

    while (orRule!=nullptr && !result) {
        result=TRUE;
        AndConstraint* andRule = orRule->childNode;
        while (andRule!=nullptr && result) {
            result = andRule->isFulfilled(number);
            andRule=andRule->next;
        }
        orRule = orRule->next;
    }

    return result;
}


RuleChain::RuleChain(const RuleChain& other) :
        fKeyword(other.fKeyword), fDecimalSamples(other.fDecimalSamples),
        fIntegerSamples(other.fIntegerSamples), fDecimalSamplesUnbounded(other.fDecimalSamplesUnbounded),
        fIntegerSamplesUnbounded(other.fIntegerSamplesUnbounded), fInternalStatus(other.fInternalStatus) {
    if (U_FAILURE(this->fInternalStatus)) {
        return; // stop early if the object we are copying from is invalid.
    }
    if (other.ruleHeader != nullptr) {
        this->ruleHeader = new OrConstraint(*(other.ruleHeader));
        if (this->ruleHeader == nullptr) {
            this->fInternalStatus = U_MEMORY_ALLOCATION_ERROR;
        }
        else if (U_FAILURE(this->ruleHeader->fInternalStatus)) {
            // If the OrConstraint wasn't fully copied, then set our status to failure as well.
            this->fInternalStatus = this->ruleHeader->fInternalStatus;
            return; // exit early.
        }
    }
    if (other.fNext != nullptr ) {
        this->fNext = new RuleChain(*other.fNext);
        if (this->fNext == nullptr) {
            this->fInternalStatus = U_MEMORY_ALLOCATION_ERROR;
        }
        else if (U_FAILURE(this->fNext->fInternalStatus)) {
            // If the RuleChain wasn't fully copied, then set our status to failure as well.
            this->fInternalStatus = this->fNext->fInternalStatus;
        }
    }
}

RuleChain::~RuleChain() {
    delete fNext;
    delete ruleHeader;
}

UnicodeString
RuleChain::select(const IFixedDecimal &number) const {
    if (!number.isNaN() && !number.isInfinite()) {
        for (const RuleChain *rules = this; rules != nullptr; rules = rules->fNext) {
             if (rules->ruleHeader->isFulfilled(number)) {
                 return rules->fKeyword;
             }
        }
    }
    return UnicodeString(TRUE, PLURAL_KEYWORD_OTHER, 5);
}

static UnicodeString tokenString(tokenType tok) {
    UnicodeString s;
    switch (tok) {
      case plurrule_token_tVariableN:
        s.append(PLURRULE_LOW_N); break;
      case plurrule_token_tVariableI:
        s.append(PLURRULE_LOW_I); break;
      case plurrule_token_tVariableF:
        s.append(PLURRULE_LOW_F); break;
      case plurrule_token_tVariableV:
        s.append(PLURRULE_LOW_V); break;
      case plurrule_token_tVariableT:
        s.append(PLURRULE_LOW_T); break;
      default:
        s.append(PLURRULE_TILDE);
    }
    return s;
}

void
RuleChain::dumpRules(UnicodeString& result) {
    UChar digitString[16];

    if ( ruleHeader != nullptr ) {
        result +=  fKeyword;
        result += PLURRULE_COLON;
        result += PLURRULE_SPACE;
        OrConstraint* orRule=ruleHeader;
        while ( orRule != nullptr ) {
            AndConstraint* andRule=orRule->childNode;
            while ( andRule != nullptr ) {
                if ((andRule->op==AndConstraint::NONE) &&  (andRule->rangeList==nullptr) && (andRule->value == -1)) {
                    // Empty Rules.
                } else if ( (andRule->op==AndConstraint::NONE) && (andRule->rangeList==nullptr) ) {
                    result += tokenString(andRule->digitsType);
                    result += UNICODE_STRING_SIMPLE(" is ");
                    if (andRule->negated) {
                        result += UNICODE_STRING_SIMPLE("not ");
                    }
                    uprv_itou(digitString,16, andRule->value,10,0);
                    result += UnicodeString(digitString);
                }
                else {
                    result += tokenString(andRule->digitsType);
                    result += PLURRULE_SPACE;
                    if (andRule->op==AndConstraint::MOD) {
                        result += UNICODE_STRING_SIMPLE("mod ");
                        uprv_itou(digitString,16, andRule->opNum,10,0);
                        result += UnicodeString(digitString);
                    }
                    if (andRule->rangeList==nullptr) {
                        if (andRule->negated) {
                            result += UNICODE_STRING_SIMPLE(" is not ");
                            uprv_itou(digitString,16, andRule->value,10,0);
                            result += UnicodeString(digitString);
                        }
                        else {
                            result += UNICODE_STRING_SIMPLE(" is ");
                            uprv_itou(digitString,16, andRule->value,10,0);
                            result += UnicodeString(digitString);
                        }
                    }
                    else {
                        if (andRule->negated) {
                            if ( andRule->integerOnly ) {
                                result += UNICODE_STRING_SIMPLE(" not in ");
                            }
                            else {
                                result += UNICODE_STRING_SIMPLE(" not within ");
                            }
                        }
                        else {
                            if ( andRule->integerOnly ) {
                                result += UNICODE_STRING_SIMPLE(" in ");
                            }
                            else {
                                result += UNICODE_STRING_SIMPLE(" within ");
                            }
                        }
                        for (int32_t r=0; r<andRule->rangeList->size(); r+=2) {
                            int32_t rangeLo = andRule->rangeList->elementAti(r);
                            int32_t rangeHi = andRule->rangeList->elementAti(r+1);
                            uprv_itou(digitString,16, rangeLo, 10, 0);
                            result += UnicodeString(digitString);
                            result += UNICODE_STRING_SIMPLE("..");
                            uprv_itou(digitString,16, rangeHi, 10,0);
                            result += UnicodeString(digitString);
                            if (r+2 < andRule->rangeList->size()) {
                                result += UNICODE_STRING_SIMPLE(", ");
                            }
                        }
                    }
                }
                if ( (andRule=andRule->next) != nullptr) {
                    result += UNICODE_STRING_SIMPLE(" and ");
                }
            }
            if ( (orRule = orRule->next) != nullptr ) {
                result += UNICODE_STRING_SIMPLE(" or ");
            }
        }
    }
    if ( fNext != nullptr ) {
        result += UNICODE_STRING_SIMPLE("; ");
        fNext->dumpRules(result);
    }
}


UErrorCode
RuleChain::getKeywords(int32_t capacityOfKeywords, UnicodeString* keywords, int32_t& arraySize) const {
    if (U_FAILURE(fInternalStatus)) {
        return fInternalStatus;
    }
    if ( arraySize < capacityOfKeywords-1 ) {
        keywords[arraySize++]=fKeyword;
    }
    else {
        return U_BUFFER_OVERFLOW_ERROR;
    }

    if ( fNext != nullptr ) {
        return fNext->getKeywords(capacityOfKeywords, keywords, arraySize);
    }
    else {
        return U_ZERO_ERROR;
    }
}

UBool
RuleChain::isKeyword(const UnicodeString& keywordParam) const {
    if ( fKeyword == keywordParam ) {
        return TRUE;
    }

    if ( fNext != nullptr ) {
        return fNext->isKeyword(keywordParam);
    }
    else {
        return FALSE;
    }
}


PluralRuleParser::PluralRuleParser() :
        ruleIndex(0), token(), type(plurrule_token_none), prevType(plurrule_token_none),
        curAndConstraint(nullptr), currentChain(nullptr), rangeLowIdx(-1), rangeHiIdx(-1)
{
}

PluralRuleParser::~PluralRuleParser() {
}


int32_t
PluralRuleParser::getNumberValue(const UnicodeString& token) {
    int32_t i;
    char digits[128];

    i = token.extract(0, token.length(), digits, UPRV_LENGTHOF(digits), US_INV);
    digits[i]='\0';

    return((int32_t)atoi(digits));
}


void
PluralRuleParser::checkSyntax(UErrorCode &status)
{
    if (U_FAILURE(status)) {
        return;
    }
    if (!(prevType==plurrule_token_none || prevType==plurrule_token_tSemiColon)) {
        type = getKeyType(token, type);  // Switch token type from plurrule_token_tKeyword if we scanned a reserved word,
                                               //   and we are not at the start of a rule, where a
                                               //   keyword is expected.
    }

    switch(prevType) {
    case plurrule_token_none:
    case plurrule_token_tSemiColon:
        if (type!=plurrule_token_tKeyword && type != plurrule_token_tEOF) {
            status = U_UNEXPECTED_TOKEN;
        }
        break;
    case plurrule_token_tVariableN:
    case plurrule_token_tVariableI:
    case plurrule_token_tVariableF:
    case plurrule_token_tVariableT:
    case plurrule_token_tVariableV:
        if (type != plurrule_token_tIs && type != plurrule_token_tMod && type != plurrule_token_tIn &&
            type != plurrule_token_tNot && type != plurrule_token_tWithin && type != plurrule_token_tEqual && type != plurrule_token_tNotEqual) {
            status = U_UNEXPECTED_TOKEN;
        }
        break;
    case plurrule_token_tKeyword:
        if (type != plurrule_token_tColon) {
            status = U_UNEXPECTED_TOKEN;
        }
        break;
    case plurrule_token_tColon:
        if (!(type == plurrule_token_tVariableN ||
              type == plurrule_token_tVariableI ||
              type == plurrule_token_tVariableF ||
              type == plurrule_token_tVariableT ||
              type == plurrule_token_tVariableV ||
              type == plurrule_token_tAt)) {
            status = U_UNEXPECTED_TOKEN;
        }
        break;
    case plurrule_token_tIs:
        if ( type != plurrule_token_tNumber && type != plurrule_token_tNot) {
            status = U_UNEXPECTED_TOKEN;
        }
        break;
    case plurrule_token_tNot:
        if (type != plurrule_token_tNumber && type != plurrule_token_tIn && type != plurrule_token_tWithin) {
            status = U_UNEXPECTED_TOKEN;
        }
        break;
    case plurrule_token_tMod:
    case plurrule_token_tDot2:
    case plurrule_token_tIn:
    case plurrule_token_tWithin:
    case plurrule_token_tEqual:
    case plurrule_token_tNotEqual:
        if (type != plurrule_token_tNumber) {
            status = U_UNEXPECTED_TOKEN;
        }
        break;
    case plurrule_token_tAnd:
    case plurrule_token_tOr:
        if ( type != plurrule_token_tVariableN &&
             type != plurrule_token_tVariableI &&
             type != plurrule_token_tVariableF &&
             type != plurrule_token_tVariableT &&
             type != plurrule_token_tVariableV) {
            status = U_UNEXPECTED_TOKEN;
        }
        break;
    case plurrule_token_tComma:
        if (type != plurrule_token_tNumber) {
            status = U_UNEXPECTED_TOKEN;
        }
        break;
    case plurrule_token_tNumber:
        if (type != plurrule_token_tDot2  && type != plurrule_token_tSemiColon && type != plurrule_token_tIs       && type != plurrule_token_tNot    &&
            type != plurrule_token_tIn    && type != plurrule_token_tEqual     && type != plurrule_token_tNotEqual && type != plurrule_token_tWithin &&
            type != plurrule_token_tAnd   && type != plurrule_token_tOr        && type != plurrule_token_tComma    && type != plurrule_token_tAt     &&
            type != plurrule_token_tEOF)
        {
            status = U_UNEXPECTED_TOKEN;
        }
        // TODO: a comma following a number that is not part of a range will be allowed.
        //       It's not the only case of this sort of thing. Parser needs a re-write.
        break;
    case plurrule_token_tAt:
        if (type != plurrule_token_tDecimal && type != plurrule_token_tInteger) {
            status = U_UNEXPECTED_TOKEN;
        }
        break;
    default:
        status = U_UNEXPECTED_TOKEN;
        break;
    }
}


/*
 *  Scan the next token from the input rules.
 *     rules and returned token type are in the parser state variables.
 */
void
PluralRuleParser::getNextToken(UErrorCode &status)
{
    if (U_FAILURE(status)) {
        return;
    }

    UChar ch;
    while (ruleIndex < ruleSrc->length()) {
        ch = ruleSrc->charAt(ruleIndex);
        type = charType(ch);
        if (type != plurrule_token_tSpace) {
            break;
        }
        ++(ruleIndex);
    }
    if (ruleIndex >= ruleSrc->length()) {
        type = plurrule_token_tEOF;
        return;
    }
    int32_t curIndex= ruleIndex;

    switch (type) {
      case plurrule_token_tColon:
      case plurrule_token_tSemiColon:
      case plurrule_token_tComma:
      case plurrule_token_tEllipsis:
      case plurrule_token_tTilde:   // scanned '~'
      case plurrule_token_tAt:      // scanned '@'
      case plurrule_token_tEqual:   // scanned '='
      case plurrule_token_tMod:     // scanned '%'
        // Single character tokens.
        ++curIndex;
        break;

      case plurrule_token_tNotEqual:  // scanned '!'
        if (ruleSrc->charAt(curIndex+1) == PLURRULE_EQUALS) {
            curIndex += 2;
        } else {
            type = plurrule_token_none;
            curIndex += 1;
        }
        break;

      case plurrule_token_tKeyword:
         while (type == plurrule_token_tKeyword && ++curIndex < ruleSrc->length()) {
             ch = ruleSrc->charAt(curIndex);
             type = charType(ch);
         }
         type = plurrule_token_tKeyword;
         break;

      case plurrule_token_tNumber:
         while (type == plurrule_token_tNumber && ++curIndex < ruleSrc->length()) {
             ch = ruleSrc->charAt(curIndex);
             type = charType(ch);
         }
         type = plurrule_token_tNumber;
         break;

       case plurrule_token_tDot:
         // We could be looking at either ".." in a range, or "..." at the end of a sample.
         if (curIndex+1 >= ruleSrc->length() || ruleSrc->charAt(curIndex+1) != PLURRULE_DOT) {
             ++curIndex;
             break; // Single dot
         }
         if (curIndex+2 >= ruleSrc->length() || ruleSrc->charAt(curIndex+2) != PLURRULE_DOT) {
             curIndex += 2;
             type = plurrule_token_tDot2;
             break; // double dot
         }
         type = plurrule_token_tEllipsis;
         curIndex += 3;
         break;     // triple dot

       default:
         status = U_UNEXPECTED_TOKEN;
         ++curIndex;
         break;
    }

    U_ASSERT(ruleIndex <= ruleSrc->length());
    U_ASSERT(curIndex <= ruleSrc->length());
    token=UnicodeString(*ruleSrc, ruleIndex, curIndex-ruleIndex);
    ruleIndex = curIndex;
}

tokenType
PluralRuleParser::charType(UChar ch) {
    if ((ch>=PLURRULE_U_ZERO) && (ch<=PLURRULE_U_NINE)) {
        return plurrule_token_tNumber;
    }
    if (ch>=PLURRULE_LOW_A && ch<=PLURRULE_LOW_Z) {
        return plurrule_token_tKeyword;
    }
    switch (ch) {
    case PLURRULE_COLON:
        return plurrule_token_tColon;
    case PLURRULE_SPACE:
        return plurrule_token_tSpace;
    case PLURRULE_SEMI_COLON:
        return plurrule_token_tSemiColon;
    case PLURRULE_DOT:
        return plurrule_token_tDot;
    case PLURRULE_COMMA:
        return plurrule_token_tComma;
    case PLURRULE_EXCLAMATION:
        return plurrule_token_tNotEqual;
    case PLURRULE_EQUALS:
        return plurrule_token_tEqual;
    case PLURRULE_PERCENT_SIGN:
        return plurrule_token_tMod;
    case PLURRULE_AT:
        return plurrule_token_tAt;
    case PLURRULE_ELLIPSIS:
        return plurrule_token_tEllipsis;
    case PLURRULE_TILDE:
        return plurrule_token_tTilde;
    default :
        return plurrule_token_none;
    }
}


//  Set token type for reserved words in the Plural Rule syntax.

tokenType
PluralRuleParser::getKeyType(const UnicodeString &token, tokenType keyType)
{
    if (keyType != plurrule_token_tKeyword) {
        return keyType;
    }

    if (0 == token.compare(PK_VAR_N, 1)) {
        keyType = plurrule_token_tVariableN;
    } else if (0 == token.compare(PK_VAR_I, 1)) {
        keyType = plurrule_token_tVariableI;
    } else if (0 == token.compare(PK_VAR_F, 1)) {
        keyType = plurrule_token_tVariableF;
    } else if (0 == token.compare(PK_VAR_T, 1)) {
        keyType = plurrule_token_tVariableT;
    } else if (0 == token.compare(PK_VAR_V, 1)) {
        keyType = plurrule_token_tVariableV;
    } else if (0 == token.compare(PK_IS, 2)) {
        keyType = plurrule_token_tIs;
    } else if (0 == token.compare(PK_AND, 3)) {
        keyType = plurrule_token_tAnd;
    } else if (0 == token.compare(PK_IN, 2)) {
        keyType = plurrule_token_tIn;
    } else if (0 == token.compare(PK_WITHIN, 6)) {
        keyType = plurrule_token_tWithin;
    } else if (0 == token.compare(PK_NOT, 3)) {
        keyType = plurrule_token_tNot;
    } else if (0 == token.compare(PK_MOD, 3)) {
        keyType = plurrule_token_tMod;
    } else if (0 == token.compare(PK_OR, 2)) {
        keyType = plurrule_token_tOr;
    } else if (0 == token.compare(PK_DECIMAL, 7)) {
        keyType = plurrule_token_tDecimal;
    } else if (0 == token.compare(PK_INTEGER, 7)) {
        keyType = plurrule_token_tInteger;
    }
    return keyType;
}


PluralKeywordEnumeration::PluralKeywordEnumeration(RuleChain *header, UErrorCode& status)
        : pos(0), fKeywordNames(status) {
    if (U_FAILURE(status)) {
        return;
    }
    fKeywordNames.setDeleter(uprv_deleteUObject);
    UBool  addKeywordOther = TRUE;
    RuleChain *node = header;
    while (node != nullptr) {
        auto newElem = new UnicodeString(node->fKeyword);
        if (newElem == nullptr) {
            status = U_MEMORY_ALLOCATION_ERROR;
            return;
        }
        fKeywordNames.addElement(newElem, status);
        if (U_FAILURE(status)) {
            delete newElem;
            return;
        }
        if (0 == node->fKeyword.compare(PLURAL_KEYWORD_OTHER, 5)) {
            addKeywordOther = FALSE;
        }
        node = node->fNext;
    }

    if (addKeywordOther) {
        auto newElem = new UnicodeString(PLURAL_KEYWORD_OTHER);
        if (newElem == nullptr) {
            status = U_MEMORY_ALLOCATION_ERROR;
            return;
        }
        fKeywordNames.addElement(newElem, status);
        if (U_FAILURE(status)) {
            delete newElem;
            return;
        }
    }
}

const UnicodeString*
PluralKeywordEnumeration::snext(UErrorCode& status) {
    if (U_SUCCESS(status) && pos < fKeywordNames.size()) {
        return (const UnicodeString*)fKeywordNames.elementAt(pos++);
    }
    return nullptr;
}

void
PluralKeywordEnumeration::reset(UErrorCode& /*status*/) {
    pos=0;
}

int32_t
PluralKeywordEnumeration::count(UErrorCode& /*status*/) const {
    return fKeywordNames.size();
}

PluralKeywordEnumeration::~PluralKeywordEnumeration() {
}

PluralOperand tokenTypeToPluralOperand(tokenType tt) {
    switch(tt) {
    case plurrule_token_tVariableN:
        return PLURAL_OPERAND_N;
    case plurrule_token_tVariableI:
        return PLURAL_OPERAND_I;
    case plurrule_token_tVariableF:
        return PLURAL_OPERAND_F;
    case plurrule_token_tVariableV:
        return PLURAL_OPERAND_V;
    case plurrule_token_tVariableT:
        return PLURAL_OPERAND_T;
    default:
        UPRV_UNREACHABLE;  // unexpected.
    }
}

FixedDecimal::FixedDecimal(double n, int32_t v, int64_t f) {
    init(n, v, f);
    // check values. TODO make into unit test.
    //
    //            long visiblePower = (int) Math.pow(10, v);
    //            if (decimalDigits > visiblePower) {
    //                throw new IllegalArgumentException();
    //            }
    //            double fraction = intValue + (decimalDigits / (double) visiblePower);
    //            if (fraction != source) {
    //                double diff = Math.abs(fraction - source)/(Math.abs(fraction) + Math.abs(source));
    //                if (diff > 0.00000001d) {
    //                    throw new IllegalArgumentException();
    //                }
    //            }
}

FixedDecimal::FixedDecimal(double n, int32_t v) {
    // Ugly, but for samples we don't care.
    init(n, v, getFractionalDigits(n, v));
}

FixedDecimal::FixedDecimal(double n) {
    init(n);
}

FixedDecimal::FixedDecimal() {
    init(0, 0, 0);
}


// Create a FixedDecimal from a UnicodeString containing a number.
//    Inefficient, but only used for samples, so simplicity trumps efficiency.

FixedDecimal::FixedDecimal(const UnicodeString &num, UErrorCode &status) {
    CharString cs;
    cs.appendInvariantChars(num, status);
    DecimalQuantity dl;
    dl.setToDecNumber(cs.toStringPiece(), status);
    if (U_FAILURE(status)) {
        init(0, 0, 0);
        return;
    }
    int32_t decimalPoint = num.indexOf(PLURRULE_DOT);
    double n = dl.toDouble();
    if (decimalPoint == -1) {
        init(n, 0, 0);
    } else {
        int32_t v = num.length() - decimalPoint - 1;
        init(n, v, getFractionalDigits(n, v));
    }
}


FixedDecimal::FixedDecimal(const FixedDecimal &other) {
    source = other.source;
    visibleDecimalDigitCount = other.visibleDecimalDigitCount;
    decimalDigits = other.decimalDigits;
    decimalDigitsWithoutTrailingZeros = other.decimalDigitsWithoutTrailingZeros;
    intValue = other.intValue;
    _hasIntegerValue = other._hasIntegerValue;
    isNegative = other.isNegative;
    _isNaN = other._isNaN;
    _isInfinite = other._isInfinite;
}

FixedDecimal::~FixedDecimal() = default;


void FixedDecimal::init(double n) {
    int32_t numFractionDigits = decimals(n);
    init(n, numFractionDigits, getFractionalDigits(n, numFractionDigits));
}


void FixedDecimal::init(double n, int32_t v, int64_t f) {
    isNegative = n < 0.0;
    source = fabs(n);
    _isNaN = uprv_isNaN(source);
    _isInfinite = uprv_isInfinite(source);
    if (_isNaN || _isInfinite) {
        v = 0;
        f = 0;
        intValue = 0;
        _hasIntegerValue = FALSE;
    } else {
        intValue = (int64_t)source;
        _hasIntegerValue = (source == intValue);
    }

    visibleDecimalDigitCount = v;
    decimalDigits = f;
    if (f == 0) {
         decimalDigitsWithoutTrailingZeros = 0;
    } else {
        int64_t fdwtz = f;
        while ((fdwtz%10) == 0) {
            fdwtz /= 10;
        }
        decimalDigitsWithoutTrailingZeros = fdwtz;
    }
}


//  Fast path only exact initialization. Return true if successful.
//     Note: Do not multiply by 10 each time through loop, rounding cruft can build
//           up that makes the check for an integer result fail.
//           A single multiply of the original number works more reliably.
static int32_t p10[] = {1, 10, 100, 1000, 10000};
UBool FixedDecimal::quickInit(double n) {
    UBool success = FALSE;
    n = fabs(n);
    int32_t numFractionDigits;
    for (numFractionDigits = 0; numFractionDigits <= 3; numFractionDigits++) {
        double scaledN = n * p10[numFractionDigits];
        if (scaledN == floor(scaledN)) {
            success = TRUE;
            break;
        }
    }
    if (success) {
        init(n, numFractionDigits, getFractionalDigits(n, numFractionDigits));
    }
    return success;
}



int32_t FixedDecimal::decimals(double n) {
    // Count the number of decimal digits in the fraction part of the number, excluding trailing zeros.
    // fastpath the common cases, integers or fractions with 3 or fewer digits
    n = fabs(n);
    for (int ndigits=0; ndigits<=3; ndigits++) {
        double scaledN = n * p10[ndigits];
        if (scaledN == floor(scaledN)) {
            return ndigits;
        }
    }

    // Slow path, convert with sprintf, parse converted output.
    char  buf[30] = {0};
    sprintf(buf, "%1.15e", n);
    // formatted number looks like this: 1.234567890123457e-01
    int exponent = atoi(buf+18);
    int numFractionDigits = 15;
    for (int i=16; ; --i) {
        if (buf[i] != '0') {
            break;
        }
        --numFractionDigits;
    }
    numFractionDigits -= exponent;   // Fraction part of fixed point representation.
    return numFractionDigits;
}


// Get the fraction digits of a double, represented as an integer.
//    v is the number of visible fraction digits in the displayed form of the number.
//       Example: n = 1001.234, v = 6, result = 234000
//    TODO: need to think through how this is used in the plural rule context.
//          This function can easily encounter integer overflow,
//          and can easily return noise digits when the precision of a double is exceeded.

int64_t FixedDecimal::getFractionalDigits(double n, int32_t v) {
    if (v == 0 || n == floor(n) || uprv_isNaN(n) || uprv_isPositiveInfinity(n)) {
        return 0;
    }
    n = fabs(n);
    double fract = n - floor(n);
    switch (v) {
      case 1: return (int64_t)(fract*10.0 + 0.5);
      case 2: return (int64_t)(fract*100.0 + 0.5);
      case 3: return (int64_t)(fract*1000.0 + 0.5);
      default:
          double scaled = floor(fract * pow(10.0, (double)v) + 0.5);
          if (scaled > U_INT64_MAX) {
              return U_INT64_MAX;
          } else {
              return (int64_t)scaled;
          }
      }
}


void FixedDecimal::adjustForMinFractionDigits(int32_t minFractionDigits) {
    int32_t numTrailingFractionZeros = minFractionDigits - visibleDecimalDigitCount;
    if (numTrailingFractionZeros > 0) {
        for (int32_t i=0; i<numTrailingFractionZeros; i++) {
            // Do not let the decimalDigits value overflow if there are many trailing zeros.
            // Limit the value to 18 digits, the most that a 64 bit int can fully represent.
            if (decimalDigits >= 100000000000000000LL) {
                break;
            }
            decimalDigits *= 10;
        }
        visibleDecimalDigitCount += numTrailingFractionZeros;
    }
}


double FixedDecimal::getPluralOperand(PluralOperand operand) const {
    switch(operand) {
        case PLURAL_OPERAND_N: return source;
        case PLURAL_OPERAND_I: return static_cast<double>(intValue);
        case PLURAL_OPERAND_F: return static_cast<double>(decimalDigits);
        case PLURAL_OPERAND_T: return static_cast<double>(decimalDigitsWithoutTrailingZeros);
        case PLURAL_OPERAND_V: return visibleDecimalDigitCount;
        default:
             UPRV_UNREACHABLE;  // unexpected.
    }
}

bool FixedDecimal::isNaN() const {
    return _isNaN;
}

bool FixedDecimal::isInfinite() const {
    return _isInfinite;
}

bool FixedDecimal::hasIntegerValue() const {
    return _hasIntegerValue;
}

bool FixedDecimal::isNanOrInfinity() const {
    return _isNaN || _isInfinite;
}

int32_t FixedDecimal::getVisibleFractionDigitCount() const {
    return visibleDecimalDigitCount;
}



PluralAvailableLocalesEnumeration::PluralAvailableLocalesEnumeration(UErrorCode &status) {
    fOpenStatus = status;
    if (U_FAILURE(status)) {
        return;
    }
    fOpenStatus = U_ZERO_ERROR; // clear any warnings.
    LocalUResourceBundlePointer rb(ures_openDirect(nullptr, "plurals", &fOpenStatus));
    fLocales = ures_getByKey(rb.getAlias(), "locales", nullptr, &fOpenStatus);
}

PluralAvailableLocalesEnumeration::~PluralAvailableLocalesEnumeration() {
    ures_close(fLocales);
    ures_close(fRes);
    fLocales = nullptr;
    fRes = nullptr;
}

const char *PluralAvailableLocalesEnumeration::next(int32_t *resultLength, UErrorCode &status) {
    if (U_FAILURE(status)) {
        return nullptr;
    }
    if (U_FAILURE(fOpenStatus)) {
        status = fOpenStatus;
        return nullptr;
    }
    fRes = ures_getNextResource(fLocales, fRes, &status);
    if (fRes == nullptr || U_FAILURE(status)) {
        if (status == U_INDEX_OUTOFBOUNDS_ERROR) {
            status = U_ZERO_ERROR;
        }
        return nullptr;
    }
    const char *result = ures_getKey(fRes);
    if (resultLength != nullptr) {
        *resultLength = static_cast<int32_t>(uprv_strlen(result));
    }
    return result;
}


void PluralAvailableLocalesEnumeration::reset(UErrorCode &status) {
    if (U_FAILURE(status)) {
       return;
    }
    if (U_FAILURE(fOpenStatus)) {
        status = fOpenStatus;
        return;
    }
    ures_resetIterator(fLocales);
}

int32_t PluralAvailableLocalesEnumeration::count(UErrorCode &status) const {
    if (U_FAILURE(status)) {
        return 0;
    }
    if (U_FAILURE(fOpenStatus)) {
        status = fOpenStatus;
        return 0;
    }
    return ures_getSize(fLocales);
}

U_NAMESPACE_END


#endif /* #if !UCONFIG_NO_FORMATTING */

//eof
