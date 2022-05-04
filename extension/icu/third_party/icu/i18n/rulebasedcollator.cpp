// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
*******************************************************************************
* Copyright (C) 1996-2015, International Business Machines
* Corporation and others.  All Rights Reserved.
*******************************************************************************
* rulebasedcollator.cpp
*
* (replaced the former tblcoll.cpp)
*
* created on: 2012feb14 with new and old collation code
* created by: Markus W. Scherer
*/

#include "unicode/utypes.h"

#if !UCONFIG_NO_COLLATION

#include "unicode/coll.h"
#include "unicode/coleitr.h"
#include "unicode/localpointer.h"
#include "unicode/locid.h"
#include "unicode/sortkey.h"
#include "unicode/tblcoll.h"
#include "unicode/ucol.h"
#include "unicode/uiter.h"
#include "unicode/uloc.h"
#include "unicode/uniset.h"
#include "unicode/unistr.h"
#include "unicode/usetiter.h"
#include "unicode/utf8.h"
#include "unicode/uversion.h"
#include "bocsu.h"
#include "charstr.h"
#include "cmemory.h"
#include "collation.h"
#include "collationcompare.h"
#include "collationdata.h"
#include "collationdatareader.h"
#include "collationfastlatin.h"
#include "collationiterator.h"
#include "collationkeys.h"
#include "collationroot.h"
#include "collationsets.h"
#include "collationsettings.h"
#include "collationtailoring.h"
#include "cstring.h"
#include "uassert.h"
#include "ucol_imp.h"
#include "uhash.h"
#include "uitercollationiterator.h"
#include "ustr_imp.h"
#include "utf16collationiterator.h"
#include "utf8collationiterator.h"
#include "uvectr64.h"

U_NAMESPACE_BEGIN

namespace {

class FixedSortKeyByteSink : public SortKeyByteSink {
public:
    FixedSortKeyByteSink(char *dest, int32_t destCapacity)
            : SortKeyByteSink(dest, destCapacity) {}
    virtual ~FixedSortKeyByteSink();

private:
    virtual void AppendBeyondCapacity(const char *bytes, int32_t n, int32_t length);
    virtual UBool Resize(int32_t appendCapacity, int32_t length);
};

FixedSortKeyByteSink::~FixedSortKeyByteSink() {}

void
FixedSortKeyByteSink::AppendBeyondCapacity(const char *bytes, int32_t /*n*/, int32_t length) {
    // buffer_ != NULL && bytes != NULL && n > 0 && appended_ > capacity_
    // Fill the buffer completely.
    int32_t available = capacity_ - length;
    if (available > 0) {
        uprv_memcpy(buffer_ + length, bytes, available);
    }
}

UBool
FixedSortKeyByteSink::Resize(int32_t /*appendCapacity*/, int32_t /*length*/) {
    return FALSE;
}

}  // namespace

// Not in an anonymous namespace, so that it can be a friend of CollationKey.
class CollationKeyByteSink : public SortKeyByteSink {
public:
    CollationKeyByteSink(CollationKey &key)
            : SortKeyByteSink(reinterpret_cast<char *>(key.getBytes()), key.getCapacity()),
              key_(key) {}
    virtual ~CollationKeyByteSink();

private:
    virtual void AppendBeyondCapacity(const char *bytes, int32_t n, int32_t length);
    virtual UBool Resize(int32_t appendCapacity, int32_t length);

    CollationKey &key_;
};

CollationKeyByteSink::~CollationKeyByteSink() {}

void
CollationKeyByteSink::AppendBeyondCapacity(const char *bytes, int32_t n, int32_t length) {
    // buffer_ != NULL && bytes != NULL && n > 0 && appended_ > capacity_
    if (Resize(n, length)) {
        uprv_memcpy(buffer_ + length, bytes, n);
    }
}

UBool
CollationKeyByteSink::Resize(int32_t appendCapacity, int32_t length) {
    if (buffer_ == NULL) {
        return FALSE;  // allocation failed before already
    }
    int32_t newCapacity = 2 * capacity_;
    int32_t altCapacity = length + 2 * appendCapacity;
    if (newCapacity < altCapacity) {
        newCapacity = altCapacity;
    }
    if (newCapacity < 200) {
        newCapacity = 200;
    }
    uint8_t *newBuffer = key_.reallocate(newCapacity, length);
    if (newBuffer == NULL) {
        SetNotOk();
        return FALSE;
    }
    buffer_ = reinterpret_cast<char *>(newBuffer);
    capacity_ = newCapacity;
    return TRUE;
}

RuleBasedCollator::RuleBasedCollator(const RuleBasedCollator &other)
        : Collator(other),
          data(other.data),
          settings(other.settings),
          tailoring(other.tailoring),
          cacheEntry(other.cacheEntry),
          validLocale(other.validLocale),
          explicitlySetAttributes(other.explicitlySetAttributes),
          actualLocaleIsSameAsValid(other.actualLocaleIsSameAsValid) {
    settings->addRef();
    cacheEntry->addRef();
}

RuleBasedCollator::RuleBasedCollator(const uint8_t *bin, int32_t length,
                                     const RuleBasedCollator *base, UErrorCode &errorCode)
        : data(NULL),
          settings(NULL),
          tailoring(NULL),
          cacheEntry(NULL),
          validLocale(""),
          explicitlySetAttributes(0),
          actualLocaleIsSameAsValid(FALSE) {
    if(U_FAILURE(errorCode)) { return; }
    if(bin == NULL || length == 0 || base == NULL) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return;
    }
    const CollationTailoring *root = CollationRoot::getRoot(errorCode);
    if(U_FAILURE(errorCode)) { return; }
    if(base->tailoring != root) {
        errorCode = U_UNSUPPORTED_ERROR;
        return;
    }
    LocalPointer<CollationTailoring> t(new CollationTailoring(base->tailoring->settings));
    if(t.isNull() || t->isBogus()) {
        errorCode = U_MEMORY_ALLOCATION_ERROR;
        return;
    }
    CollationDataReader::read(base->tailoring, bin, length, *t, errorCode);
    if(U_FAILURE(errorCode)) { return; }
    t->actualLocale.setToBogus();
    adoptTailoring(t.orphan(), errorCode);
}

RuleBasedCollator::RuleBasedCollator(const CollationCacheEntry *entry)
        : data(entry->tailoring->data),
          settings(entry->tailoring->settings),
          tailoring(entry->tailoring),
          cacheEntry(entry),
          validLocale(entry->validLocale),
          explicitlySetAttributes(0),
          actualLocaleIsSameAsValid(FALSE) {
    settings->addRef();
    cacheEntry->addRef();
}

RuleBasedCollator::~RuleBasedCollator() {
    SharedObject::clearPtr(settings);
    SharedObject::clearPtr(cacheEntry);
}

void
RuleBasedCollator::adoptTailoring(CollationTailoring *t, UErrorCode &errorCode) {
    if(U_FAILURE(errorCode)) {
        t->deleteIfZeroRefCount();
        return;
    }
    U_ASSERT(settings == NULL && data == NULL && tailoring == NULL && cacheEntry == NULL);
    cacheEntry = new CollationCacheEntry(t->actualLocale, t);
    if(cacheEntry == NULL) {
        errorCode = U_MEMORY_ALLOCATION_ERROR;
        t->deleteIfZeroRefCount();
        return;
    }
    data = t->data;
    settings = t->settings;
    settings->addRef();
    tailoring = t;
    cacheEntry->addRef();
    validLocale = t->actualLocale;
    actualLocaleIsSameAsValid = FALSE;
}

RuleBasedCollator *
RuleBasedCollator::clone() const {
    return new RuleBasedCollator(*this);
}

RuleBasedCollator &RuleBasedCollator::operator=(const RuleBasedCollator &other) {
    if(this == &other) { return *this; }
    SharedObject::copyPtr(other.settings, settings);
    tailoring = other.tailoring;
    SharedObject::copyPtr(other.cacheEntry, cacheEntry);
    data = tailoring->data;
    validLocale = other.validLocale;
    explicitlySetAttributes = other.explicitlySetAttributes;
    actualLocaleIsSameAsValid = other.actualLocaleIsSameAsValid;
    return *this;
}

UOBJECT_DEFINE_RTTI_IMPLEMENTATION(RuleBasedCollator)

UBool
RuleBasedCollator::operator==(const Collator& other) const {
    if(this == &other) { return TRUE; }
    if(!Collator::operator==(other)) { return FALSE; }
    const RuleBasedCollator &o = static_cast<const RuleBasedCollator &>(other);
    if(*settings != *o.settings) { return FALSE; }
    if(data == o.data) { return TRUE; }
    UBool thisIsRoot = data->base == NULL;
    UBool otherIsRoot = o.data->base == NULL;
    U_ASSERT(!thisIsRoot || !otherIsRoot);  // otherwise their data pointers should be ==
    if(thisIsRoot != otherIsRoot) { return FALSE; }
    if((thisIsRoot || !tailoring->rules.isEmpty()) &&
            (otherIsRoot || !o.tailoring->rules.isEmpty())) {
        // Shortcut: If both collators have valid rule strings, then compare those.
        if(tailoring->rules == o.tailoring->rules) { return TRUE; }
    }
    // Different rule strings can result in the same or equivalent tailoring.
    // The rule strings are optional in ICU resource bundles, although included by default.
    // cloneBinary() drops the rule string.
    UErrorCode errorCode = U_ZERO_ERROR;
    LocalPointer<UnicodeSet> thisTailored(getTailoredSet(errorCode));
    LocalPointer<UnicodeSet> otherTailored(o.getTailoredSet(errorCode));
    if(U_FAILURE(errorCode)) { return FALSE; }
    if(*thisTailored != *otherTailored) { return FALSE; }
    // For completeness, we should compare all of the mappings;
    // or we should create a list of strings, sort it with one collator,
    // and check if both collators compare adjacent strings the same
    // (order & strength, down to quaternary); or similar.
    // Testing equality of collators seems unusual.
    return TRUE;
}

int32_t
RuleBasedCollator::hashCode() const {
    int32_t h = settings->hashCode();
    if(data->base == NULL) { return h; }  // root collator
    // Do not rely on the rule string, see comments in operator==().
    UErrorCode errorCode = U_ZERO_ERROR;
    LocalPointer<UnicodeSet> set(getTailoredSet(errorCode));
    if(U_FAILURE(errorCode)) { return 0; }
    UnicodeSetIterator iter(*set);
    while(iter.next() && !iter.isString()) {
        h ^= data->getCE32(iter.getCodepoint());
    }
    return h;
}

void
RuleBasedCollator::setLocales(const Locale &requested, const Locale &valid,
                              const Locale &actual) {
    if(actual == tailoring->actualLocale) {
        actualLocaleIsSameAsValid = FALSE;
    } else {
        U_ASSERT(actual == valid);
        actualLocaleIsSameAsValid = TRUE;
    }
    // Do not modify tailoring.actualLocale:
    // We cannot be sure that that would be thread-safe.
    validLocale = valid;
    (void)requested;  // Ignore, see also ticket #10477.
}

Locale
RuleBasedCollator::getLocale(ULocDataLocaleType type, UErrorCode& errorCode) const {
    if(U_FAILURE(errorCode)) {
        return Locale::getRoot();
    }
    switch(type) {
    case ULOC_ACTUAL_LOCALE:
        return actualLocaleIsSameAsValid ? validLocale : tailoring->actualLocale;
    case ULOC_VALID_LOCALE:
        return validLocale;
    case ULOC_REQUESTED_LOCALE:
    default:
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return Locale::getRoot();
    }
}

const char *
RuleBasedCollator::internalGetLocaleID(ULocDataLocaleType type, UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) {
        return NULL;
    }
    const Locale *result;
    switch(type) {
    case ULOC_ACTUAL_LOCALE:
        result = actualLocaleIsSameAsValid ? &validLocale : &tailoring->actualLocale;
        break;
    case ULOC_VALID_LOCALE:
        result = &validLocale;
        break;
    case ULOC_REQUESTED_LOCALE:
    default:
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return NULL;
    }
    if(result->isBogus()) { return NULL; }
    const char *id = result->getName();
    return id[0] == 0 ? "root" : id;
}

const UnicodeString&
RuleBasedCollator::getRules() const {
    return tailoring->rules;
}

void
RuleBasedCollator::getRules(UColRuleOption delta, UnicodeString &buffer) const {
    if(delta == UCOL_TAILORING_ONLY) {
        buffer = tailoring->rules;
        return;
    }
    // UCOL_FULL_RULES
    buffer.remove();
    CollationLoader::appendRootRules(buffer);
    buffer.append(tailoring->rules).getTerminatedBuffer();
}

void
RuleBasedCollator::getVersion(UVersionInfo version) const {
    uprv_memcpy(version, tailoring->version, U_MAX_VERSION_LENGTH);
    version[0] += (UCOL_RUNTIME_VERSION << 4) + (UCOL_RUNTIME_VERSION >> 4);
}

UnicodeSet *
RuleBasedCollator::getTailoredSet(UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return NULL; }
    UnicodeSet *tailored = new UnicodeSet();
    if(tailored == NULL) {
        errorCode = U_MEMORY_ALLOCATION_ERROR;
        return NULL;
    }
    if(data->base != NULL) {
        TailoredSet(tailored).forData(data, errorCode);
        if(U_FAILURE(errorCode)) {
            delete tailored;
            return NULL;
        }
    }
    return tailored;
}

void
RuleBasedCollator::internalGetContractionsAndExpansions(
        UnicodeSet *contractions, UnicodeSet *expansions,
        UBool addPrefixes, UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return; }
    if(contractions != NULL) {
        contractions->clear();
    }
    if(expansions != NULL) {
        expansions->clear();
    }
    ContractionsAndExpansions(contractions, expansions, NULL, addPrefixes).forData(data, errorCode);
}

void
RuleBasedCollator::internalAddContractions(UChar32 c, UnicodeSet &set, UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return; }
    ContractionsAndExpansions(&set, NULL, NULL, FALSE).forCodePoint(data, c, errorCode);
}

const CollationSettings &
RuleBasedCollator::getDefaultSettings() const {
    return *tailoring->settings;
}

UColAttributeValue
RuleBasedCollator::getAttribute(UColAttribute attr, UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return UCOL_DEFAULT; }
    int32_t option;
    switch(attr) {
    case UCOL_FRENCH_COLLATION:
        option = CollationSettings::BACKWARD_SECONDARY;
        break;
    case UCOL_ALTERNATE_HANDLING:
        return settings->getAlternateHandling();
    case UCOL_CASE_FIRST:
        return settings->getCaseFirst();
    case UCOL_CASE_LEVEL:
        option = CollationSettings::CASE_LEVEL;
        break;
    case UCOL_NORMALIZATION_MODE:
        option = CollationSettings::CHECK_FCD;
        break;
    case UCOL_STRENGTH:
        return (UColAttributeValue)settings->getStrength();
    case UCOL_HIRAGANA_QUATERNARY_MODE:
        // Deprecated attribute, unsettable.
        return UCOL_OFF;
    case UCOL_NUMERIC_COLLATION:
        option = CollationSettings::NUMERIC;
        break;
    default:
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return UCOL_DEFAULT;
    }
    return ((settings->options & option) == 0) ? UCOL_OFF : UCOL_ON;
}

void
RuleBasedCollator::setAttribute(UColAttribute attr, UColAttributeValue value,
                                UErrorCode &errorCode) {
    UColAttributeValue oldValue = getAttribute(attr, errorCode);
    if(U_FAILURE(errorCode)) { return; }
    if(value == oldValue) {
        setAttributeExplicitly(attr);
        return;
    }
    const CollationSettings &defaultSettings = getDefaultSettings();
    if(settings == &defaultSettings) {
        if(value == UCOL_DEFAULT) {
            setAttributeDefault(attr);
            return;
        }
    }
    CollationSettings *ownedSettings = SharedObject::copyOnWrite(settings);
    if(ownedSettings == NULL) {
        errorCode = U_MEMORY_ALLOCATION_ERROR;
        return;
    }

    switch(attr) {
    case UCOL_FRENCH_COLLATION:
        ownedSettings->setFlag(CollationSettings::BACKWARD_SECONDARY, value,
                               defaultSettings.options, errorCode);
        break;
    case UCOL_ALTERNATE_HANDLING:
        ownedSettings->setAlternateHandling(value, defaultSettings.options, errorCode);
        break;
    case UCOL_CASE_FIRST:
        ownedSettings->setCaseFirst(value, defaultSettings.options, errorCode);
        break;
    case UCOL_CASE_LEVEL:
        ownedSettings->setFlag(CollationSettings::CASE_LEVEL, value,
                               defaultSettings.options, errorCode);
        break;
    case UCOL_NORMALIZATION_MODE:
        ownedSettings->setFlag(CollationSettings::CHECK_FCD, value,
                               defaultSettings.options, errorCode);
        break;
    case UCOL_STRENGTH:
        ownedSettings->setStrength(value, defaultSettings.options, errorCode);
        break;
    case UCOL_HIRAGANA_QUATERNARY_MODE:
        // Deprecated attribute. Check for valid values but do not change anything.
        if(value != UCOL_OFF && value != UCOL_ON && value != UCOL_DEFAULT) {
            errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        }
        break;
    case UCOL_NUMERIC_COLLATION:
        ownedSettings->setFlag(CollationSettings::NUMERIC, value, defaultSettings.options, errorCode);
        break;
    default:
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        break;
    }
    if(U_FAILURE(errorCode)) { return; }
    setFastLatinOptions(*ownedSettings);
    if(value == UCOL_DEFAULT) {
        setAttributeDefault(attr);
    } else {
        setAttributeExplicitly(attr);
    }
}

Collator &
RuleBasedCollator::setMaxVariable(UColReorderCode group, UErrorCode &errorCode) {
    if(U_FAILURE(errorCode)) { return *this; }
    // Convert the reorder code into a MaxVariable number, or UCOL_DEFAULT=-1.
    int32_t value;
    if(group == UCOL_REORDER_CODE_DEFAULT) {
        value = UCOL_DEFAULT;
    } else if(UCOL_REORDER_CODE_FIRST <= group && group <= UCOL_REORDER_CODE_CURRENCY) {
        value = group - UCOL_REORDER_CODE_FIRST;
    } else {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return *this;
    }
    CollationSettings::MaxVariable oldValue = settings->getMaxVariable();
    if(value == oldValue) {
        setAttributeExplicitly(ATTR_VARIABLE_TOP);
        return *this;
    }
    const CollationSettings &defaultSettings = getDefaultSettings();
    if(settings == &defaultSettings) {
        if(value == UCOL_DEFAULT) {
            setAttributeDefault(ATTR_VARIABLE_TOP);
            return *this;
        }
    }
    CollationSettings *ownedSettings = SharedObject::copyOnWrite(settings);
    if(ownedSettings == NULL) {
        errorCode = U_MEMORY_ALLOCATION_ERROR;
        return *this;
    }

    if(group == UCOL_REORDER_CODE_DEFAULT) {
        group = (UColReorderCode)(UCOL_REORDER_CODE_FIRST + defaultSettings.getMaxVariable());
    }
    uint32_t varTop = data->getLastPrimaryForGroup(group);
    U_ASSERT(varTop != 0);
    ownedSettings->setMaxVariable(value, defaultSettings.options, errorCode);
    if(U_FAILURE(errorCode)) { return *this; }
    ownedSettings->variableTop = varTop;
    setFastLatinOptions(*ownedSettings);
    if(value == UCOL_DEFAULT) {
        setAttributeDefault(ATTR_VARIABLE_TOP);
    } else {
        setAttributeExplicitly(ATTR_VARIABLE_TOP);
    }
    return *this;
}

UColReorderCode
RuleBasedCollator::getMaxVariable() const {
    return (UColReorderCode)(UCOL_REORDER_CODE_FIRST + settings->getMaxVariable());
}

uint32_t
RuleBasedCollator::getVariableTop(UErrorCode & /*errorCode*/) const {
    return settings->variableTop;
}

uint32_t
RuleBasedCollator::setVariableTop(const UChar *varTop, int32_t len, UErrorCode &errorCode) {
    if(U_FAILURE(errorCode)) { return 0; }
    if(varTop == NULL && len !=0) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return 0;
    }
    if(len < 0) { len = u_strlen(varTop); }
    if(len == 0) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return 0;
    }
    UBool numeric = settings->isNumeric();
    int64_t ce1, ce2;
    if(settings->dontCheckFCD()) {
        UTF16CollationIterator ci(data, numeric, varTop, varTop, varTop + len);
        ce1 = ci.nextCE(errorCode);
        ce2 = ci.nextCE(errorCode);
    } else {
        FCDUTF16CollationIterator ci(data, numeric, varTop, varTop, varTop + len);
        ce1 = ci.nextCE(errorCode);
        ce2 = ci.nextCE(errorCode);
    }
    if(ce1 == Collation::NO_CE || ce2 != Collation::NO_CE) {
        errorCode = U_CE_NOT_FOUND_ERROR;
        return 0;
    }
    setVariableTop((uint32_t)(ce1 >> 32), errorCode);
    return settings->variableTop;
}

uint32_t
RuleBasedCollator::setVariableTop(const UnicodeString &varTop, UErrorCode &errorCode) {
    return setVariableTop(varTop.getBuffer(), varTop.length(), errorCode);
}

void
RuleBasedCollator::setVariableTop(uint32_t varTop, UErrorCode &errorCode) {
    if(U_FAILURE(errorCode)) { return; }
    if(varTop != settings->variableTop) {
        // Pin the variable top to the end of the reordering group which contains it.
        // Only a few special groups are supported.
        int32_t group = data->getGroupForPrimary(varTop);
        if(group < UCOL_REORDER_CODE_FIRST || UCOL_REORDER_CODE_CURRENCY < group) {
            errorCode = U_ILLEGAL_ARGUMENT_ERROR;
            return;
        }
        uint32_t v = data->getLastPrimaryForGroup(group);
        U_ASSERT(v != 0 && v >= varTop);
        varTop = v;
        if(varTop != settings->variableTop) {
            CollationSettings *ownedSettings = SharedObject::copyOnWrite(settings);
            if(ownedSettings == NULL) {
                errorCode = U_MEMORY_ALLOCATION_ERROR;
                return;
            }
            ownedSettings->setMaxVariable(group - UCOL_REORDER_CODE_FIRST,
                                          getDefaultSettings().options, errorCode);
            if(U_FAILURE(errorCode)) { return; }
            ownedSettings->variableTop = varTop;
            setFastLatinOptions(*ownedSettings);
        }
    }
    if(varTop == getDefaultSettings().variableTop) {
        setAttributeDefault(ATTR_VARIABLE_TOP);
    } else {
        setAttributeExplicitly(ATTR_VARIABLE_TOP);
    }
}

int32_t
RuleBasedCollator::getReorderCodes(int32_t *dest, int32_t capacity,
                                   UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return 0; }
    if(capacity < 0 || (dest == NULL && capacity > 0)) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return 0;
    }
    int32_t length = settings->reorderCodesLength;
    if(length == 0) { return 0; }
    if(length > capacity) {
        errorCode = U_BUFFER_OVERFLOW_ERROR;
        return length;
    }
    uprv_memcpy(dest, settings->reorderCodes, length * 4);
    return length;
}

void
RuleBasedCollator::setReorderCodes(const int32_t *reorderCodes, int32_t length,
                                   UErrorCode &errorCode) {
    if(U_FAILURE(errorCode)) { return; }
    if(length < 0 || (reorderCodes == NULL && length > 0)) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return;
    }
    if(length == 1 && reorderCodes[0] == UCOL_REORDER_CODE_NONE) {
        length = 0;
    }
    if(length == settings->reorderCodesLength &&
            uprv_memcmp(reorderCodes, settings->reorderCodes, length * 4) == 0) {
        return;
    }
    const CollationSettings &defaultSettings = getDefaultSettings();
    if(length == 1 && reorderCodes[0] == UCOL_REORDER_CODE_DEFAULT) {
        if(settings != &defaultSettings) {
            CollationSettings *ownedSettings = SharedObject::copyOnWrite(settings);
            if(ownedSettings == NULL) {
                errorCode = U_MEMORY_ALLOCATION_ERROR;
                return;
            }
            ownedSettings->copyReorderingFrom(defaultSettings, errorCode);
            setFastLatinOptions(*ownedSettings);
        }
        return;
    }
    CollationSettings *ownedSettings = SharedObject::copyOnWrite(settings);
    if(ownedSettings == NULL) {
        errorCode = U_MEMORY_ALLOCATION_ERROR;
        return;
    }
    ownedSettings->setReordering(*data, reorderCodes, length, errorCode);
    setFastLatinOptions(*ownedSettings);
}

void
RuleBasedCollator::setFastLatinOptions(CollationSettings &ownedSettings) const {
    ownedSettings.fastLatinOptions = CollationFastLatin::getOptions(
            data, ownedSettings,
            ownedSettings.fastLatinPrimaries, UPRV_LENGTHOF(ownedSettings.fastLatinPrimaries));
}

UCollationResult
RuleBasedCollator::compare(const UnicodeString &left, const UnicodeString &right,
                           UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return UCOL_EQUAL; }
    return doCompare(left.getBuffer(), left.length(),
                     right.getBuffer(), right.length(), errorCode);
}

UCollationResult
RuleBasedCollator::compare(const UnicodeString &left, const UnicodeString &right,
                           int32_t length, UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode) || length == 0) { return UCOL_EQUAL; }
    if(length < 0) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return UCOL_EQUAL;
    }
    int32_t leftLength = left.length();
    int32_t rightLength = right.length();
    if(leftLength > length) { leftLength = length; }
    if(rightLength > length) { rightLength = length; }
    return doCompare(left.getBuffer(), leftLength,
                     right.getBuffer(), rightLength, errorCode);
}

UCollationResult
RuleBasedCollator::compare(const UChar *left, int32_t leftLength,
                           const UChar *right, int32_t rightLength,
                           UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return UCOL_EQUAL; }
    if((left == NULL && leftLength != 0) || (right == NULL && rightLength != 0)) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return UCOL_EQUAL;
    }
    // Make sure both or neither strings have a known length.
    // We do not optimize for mixed length/termination.
    if(leftLength >= 0) {
        if(rightLength < 0) { rightLength = u_strlen(right); }
    } else {
        if(rightLength >= 0) { leftLength = u_strlen(left); }
    }
    return doCompare(left, leftLength, right, rightLength, errorCode);
}

UCollationResult
RuleBasedCollator::compareUTF8(const StringPiece &left, const StringPiece &right,
                               UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return UCOL_EQUAL; }
    const uint8_t *leftBytes = reinterpret_cast<const uint8_t *>(left.data());
    const uint8_t *rightBytes = reinterpret_cast<const uint8_t *>(right.data());
    if((leftBytes == NULL && !left.empty()) || (rightBytes == NULL && !right.empty())) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return UCOL_EQUAL;
    }
    return doCompare(leftBytes, left.length(), rightBytes, right.length(), errorCode);
}

UCollationResult
RuleBasedCollator::internalCompareUTF8(const char *left, int32_t leftLength,
                                       const char *right, int32_t rightLength,
                                       UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return UCOL_EQUAL; }
    if((left == NULL && leftLength != 0) || (right == NULL && rightLength != 0)) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return UCOL_EQUAL;
    }
    // Make sure both or neither strings have a known length.
    // We do not optimize for mixed length/termination.
    if(leftLength >= 0) {
        if(rightLength < 0) { rightLength = static_cast<int32_t>(uprv_strlen(right)); }
    } else {
        if(rightLength >= 0) { leftLength = static_cast<int32_t>(uprv_strlen(left)); }
    }
    return doCompare(reinterpret_cast<const uint8_t *>(left), leftLength,
                     reinterpret_cast<const uint8_t *>(right), rightLength, errorCode);
}

namespace {

/**
 * Abstract iterator for identical-level string comparisons.
 * Returns FCD code points and handles temporary switching to NFD.
 */
class NFDIterator : public UObject {
public:
    NFDIterator() : index(-1), length(0) {}
    virtual ~NFDIterator() {}
    /**
     * Returns the next code point from the internal normalization buffer,
     * or else the next text code point.
     * Returns -1 at the end of the text.
     */
    UChar32 nextCodePoint() {
        if(index >= 0) {
            if(index == length) {
                index = -1;
            } else {
                UChar32 c;
                U16_NEXT_UNSAFE(decomp, index, c);
                return c;
            }
        }
        return nextRawCodePoint();
    }
    /**
     * @param nfcImpl
     * @param c the last code point returned by nextCodePoint() or nextDecomposedCodePoint()
     * @return the first code point in c's decomposition,
     *         or c itself if it was decomposed already or if it does not decompose
     */
    UChar32 nextDecomposedCodePoint(const Normalizer2Impl &nfcImpl, UChar32 c) {
        if(index >= 0) { return c; }
        decomp = nfcImpl.getDecomposition(c, buffer, length);
        if(decomp == NULL) { return c; }
        index = 0;
        U16_NEXT_UNSAFE(decomp, index, c);
        return c;
    }
protected:
    /**
     * Returns the next text code point in FCD order.
     * Returns -1 at the end of the text.
     */
    virtual UChar32 nextRawCodePoint() = 0;
private:
    const UChar *decomp;
    UChar buffer[4];
    int32_t index;
    int32_t length;
};

class UTF16NFDIterator : public NFDIterator {
public:
    UTF16NFDIterator(const UChar *text, const UChar *textLimit) : s(text), limit(textLimit) {}
protected:
    virtual UChar32 nextRawCodePoint() {
        if(s == limit) { return U_SENTINEL; }
        UChar32 c = *s++;
        if(limit == NULL && c == 0) {
            s = NULL;
            return U_SENTINEL;
        }
        UChar trail;
        if(U16_IS_LEAD(c) && s != limit && U16_IS_TRAIL(trail = *s)) {
            ++s;
            c = U16_GET_SUPPLEMENTARY(c, trail);
        }
        return c;
    }

    const UChar *s;
    const UChar *limit;
};

class FCDUTF16NFDIterator : public UTF16NFDIterator {
public:
    FCDUTF16NFDIterator(const Normalizer2Impl &nfcImpl, const UChar *text, const UChar *textLimit)
            : UTF16NFDIterator(NULL, NULL) {
        UErrorCode errorCode = U_ZERO_ERROR;
        const UChar *spanLimit = nfcImpl.makeFCD(text, textLimit, NULL, errorCode);
        if(U_FAILURE(errorCode)) { return; }
        if(spanLimit == textLimit || (textLimit == NULL && *spanLimit == 0)) {
            s = text;
            limit = spanLimit;
        } else {
            str.setTo(text, (int32_t)(spanLimit - text));
            {
                ReorderingBuffer r_buffer(nfcImpl, str);
                if(r_buffer.init(str.length(), errorCode)) {
                    nfcImpl.makeFCD(spanLimit, textLimit, &r_buffer, errorCode);
                }
            }
            if(U_SUCCESS(errorCode)) {
                s = str.getBuffer();
                limit = s + str.length();
            }
        }
    }
private:
    UnicodeString str;
};

class UTF8NFDIterator : public NFDIterator {
public:
    UTF8NFDIterator(const uint8_t *text, int32_t textLength)
        : s(text), pos(0), length(textLength) {}
protected:
    virtual UChar32 nextRawCodePoint() {
        if(pos == length || (s[pos] == 0 && length < 0)) { return U_SENTINEL; }
        UChar32 c;
        U8_NEXT_OR_FFFD(s, pos, length, c);
        return c;
    }

    const uint8_t *s;
    int32_t pos;
    int32_t length;
};

class FCDUTF8NFDIterator : public NFDIterator {
public:
    FCDUTF8NFDIterator(const CollationData *data, const uint8_t *text, int32_t textLength)
            : u8ci(data, FALSE, text, 0, textLength) {}
protected:
    virtual UChar32 nextRawCodePoint() {
        UErrorCode errorCode = U_ZERO_ERROR;
        return u8ci.nextCodePoint(errorCode);
    }
private:
    FCDUTF8CollationIterator u8ci;
};

class UIterNFDIterator : public NFDIterator {
public:
    UIterNFDIterator(UCharIterator &it) : iter(it) {}
protected:
    virtual UChar32 nextRawCodePoint() {
        return uiter_next32(&iter);
    }
private:
    UCharIterator &iter;
};

class FCDUIterNFDIterator : public NFDIterator {
public:
    FCDUIterNFDIterator(const CollationData *data, UCharIterator &it, int32_t startIndex)
            : uici(data, FALSE, it, startIndex) {}
protected:
    virtual UChar32 nextRawCodePoint() {
        UErrorCode errorCode = U_ZERO_ERROR;
        return uici.nextCodePoint(errorCode);
    }
private:
    FCDUIterCollationIterator uici;
};

UCollationResult compareNFDIter(const Normalizer2Impl &nfcImpl,
                                NFDIterator &left, NFDIterator &right) {
    for(;;) {
        // Fetch the next FCD code point from each string.
        UChar32 leftCp = left.nextCodePoint();
        UChar32 rightCp = right.nextCodePoint();
        if(leftCp == rightCp) {
            if(leftCp < 0) { break; }
            continue;
        }
        // If they are different, then decompose each and compare again.
        if(leftCp < 0) {
            leftCp = -2;  // end of string
        } else if(leftCp == 0xfffe) {
            leftCp = -1;  // U+FFFE: merge separator
        } else {
            leftCp = left.nextDecomposedCodePoint(nfcImpl, leftCp);
        }
        if(rightCp < 0) {
            rightCp = -2;  // end of string
        } else if(rightCp == 0xfffe) {
            rightCp = -1;  // U+FFFE: merge separator
        } else {
            rightCp = right.nextDecomposedCodePoint(nfcImpl, rightCp);
        }
        if(leftCp < rightCp) { return UCOL_LESS; }
        if(leftCp > rightCp) { return UCOL_GREATER; }
    }
    return UCOL_EQUAL;
}

}  // namespace

UCollationResult
RuleBasedCollator::doCompare(const UChar *left, int32_t leftLength,
                             const UChar *right, int32_t rightLength,
                             UErrorCode &errorCode) const {
    // U_FAILURE(errorCode) checked by caller.
    if(left == right && leftLength == rightLength) {
        return UCOL_EQUAL;
    }

    // Identical-prefix test.
    const UChar *leftLimit;
    const UChar *rightLimit;
    int32_t equalPrefixLength = 0;
    if(leftLength < 0) {
        leftLimit = NULL;
        rightLimit = NULL;
        UChar c;
        while((c = left[equalPrefixLength]) == right[equalPrefixLength]) {
            if(c == 0) { return UCOL_EQUAL; }
            ++equalPrefixLength;
        }
    } else {
        leftLimit = left + leftLength;
        rightLimit = right + rightLength;
        for(;;) {
            if(equalPrefixLength == leftLength) {
                if(equalPrefixLength == rightLength) { return UCOL_EQUAL; }
                break;
            } else if(equalPrefixLength == rightLength ||
                      left[equalPrefixLength] != right[equalPrefixLength]) {
                break;
            }
            ++equalPrefixLength;
        }
    }

    UBool numeric = settings->isNumeric();
    if(equalPrefixLength > 0) {
        if((equalPrefixLength != leftLength &&
                    data->isUnsafeBackward(left[equalPrefixLength], numeric)) ||
                (equalPrefixLength != rightLength &&
                    data->isUnsafeBackward(right[equalPrefixLength], numeric))) {
            // Identical prefix: Back up to the start of a contraction or reordering sequence.
            while(--equalPrefixLength > 0 &&
                    data->isUnsafeBackward(left[equalPrefixLength], numeric)) {}
        }
        // Notes:
        // - A longer string can compare equal to a prefix of it if only ignorables follow.
        // - With a backward level, a longer string can compare less-than a prefix of it.

        // Pass the actual start of each string into the CollationIterators,
        // plus the equalPrefixLength position,
        // so that prefix matches back into the equal prefix work.
    }

    int32_t result;
    int32_t fastLatinOptions = settings->fastLatinOptions;
    if(fastLatinOptions >= 0 &&
            (equalPrefixLength == leftLength ||
                left[equalPrefixLength] <= CollationFastLatin::LATIN_MAX) &&
            (equalPrefixLength == rightLength ||
                right[equalPrefixLength] <= CollationFastLatin::LATIN_MAX)) {
        if(leftLength >= 0) {
            result = CollationFastLatin::compareUTF16(data->fastLatinTable,
                                                      settings->fastLatinPrimaries,
                                                      fastLatinOptions,
                                                      left + equalPrefixLength,
                                                      leftLength - equalPrefixLength,
                                                      right + equalPrefixLength,
                                                      rightLength - equalPrefixLength);
        } else {
            result = CollationFastLatin::compareUTF16(data->fastLatinTable,
                                                      settings->fastLatinPrimaries,
                                                      fastLatinOptions,
                                                      left + equalPrefixLength, -1,
                                                      right + equalPrefixLength, -1);
        }
    } else {
        result = CollationFastLatin::BAIL_OUT_RESULT;
    }

    if(result == CollationFastLatin::BAIL_OUT_RESULT) {
        if(settings->dontCheckFCD()) {
            UTF16CollationIterator leftIter(data, numeric,
                                            left, left + equalPrefixLength, leftLimit);
            UTF16CollationIterator rightIter(data, numeric,
                                            right, right + equalPrefixLength, rightLimit);
            result = CollationCompare::compareUpToQuaternary(leftIter, rightIter, *settings, errorCode);
        } else {
            FCDUTF16CollationIterator leftIter(data, numeric,
                                              left, left + equalPrefixLength, leftLimit);
            FCDUTF16CollationIterator rightIter(data, numeric,
                                                right, right + equalPrefixLength, rightLimit);
            result = CollationCompare::compareUpToQuaternary(leftIter, rightIter, *settings, errorCode);
        }
    }
    if(result != UCOL_EQUAL || settings->getStrength() < UCOL_IDENTICAL || U_FAILURE(errorCode)) {
        return (UCollationResult)result;
    }

    // Note: If NUL-terminated, we could get the actual limits from the iterators now.
    // That would complicate the iterators a bit, NUL-terminated strings are only a C convenience,
    // and the benefit seems unlikely to be measurable.

    // Compare identical level.
    const Normalizer2Impl &nfcImpl = data->nfcImpl;
    left += equalPrefixLength;
    right += equalPrefixLength;
    if(settings->dontCheckFCD()) {
        UTF16NFDIterator leftIter(left, leftLimit);
        UTF16NFDIterator rightIter(right, rightLimit);
        return compareNFDIter(nfcImpl, leftIter, rightIter);
    } else {
        FCDUTF16NFDIterator leftIter(nfcImpl, left, leftLimit);
        FCDUTF16NFDIterator rightIter(nfcImpl, right, rightLimit);
        return compareNFDIter(nfcImpl, leftIter, rightIter);
    }
}

UCollationResult
RuleBasedCollator::doCompare(const uint8_t *left, int32_t leftLength,
                             const uint8_t *right, int32_t rightLength,
                             UErrorCode &errorCode) const {
    // U_FAILURE(errorCode) checked by caller.
    if(left == right && leftLength == rightLength) {
        return UCOL_EQUAL;
    }

    // Identical-prefix test.
    int32_t equalPrefixLength = 0;
    if(leftLength < 0) {
        uint8_t c;
        while((c = left[equalPrefixLength]) == right[equalPrefixLength]) {
            if(c == 0) { return UCOL_EQUAL; }
            ++equalPrefixLength;
        }
    } else {
        for(;;) {
            if(equalPrefixLength == leftLength) {
                if(equalPrefixLength == rightLength) { return UCOL_EQUAL; }
                break;
            } else if(equalPrefixLength == rightLength ||
                      left[equalPrefixLength] != right[equalPrefixLength]) {
                break;
            }
            ++equalPrefixLength;
        }
    }
    // Back up to the start of a partially-equal code point.
    if(equalPrefixLength > 0 &&
            ((equalPrefixLength != leftLength && U8_IS_TRAIL(left[equalPrefixLength])) ||
            (equalPrefixLength != rightLength && U8_IS_TRAIL(right[equalPrefixLength])))) {
        while(--equalPrefixLength > 0 && U8_IS_TRAIL(left[equalPrefixLength])) {}
    }

    UBool numeric = settings->isNumeric();
    if(equalPrefixLength > 0) {
        UBool unsafe = FALSE;
        if(equalPrefixLength != leftLength) {
            int32_t i = equalPrefixLength;
            UChar32 c;
            U8_NEXT_OR_FFFD(left, i, leftLength, c);
            unsafe = data->isUnsafeBackward(c, numeric);
        }
        if(!unsafe && equalPrefixLength != rightLength) {
            int32_t i = equalPrefixLength;
            UChar32 c;
            U8_NEXT_OR_FFFD(right, i, rightLength, c);
            unsafe = data->isUnsafeBackward(c, numeric);
        }
        if(unsafe) {
            // Identical prefix: Back up to the start of a contraction or reordering sequence.
            UChar32 c;
            do {
                U8_PREV_OR_FFFD(left, 0, equalPrefixLength, c);
            } while(equalPrefixLength > 0 && data->isUnsafeBackward(c, numeric));
        }
        // See the notes in the UTF-16 version.

        // Pass the actual start of each string into the CollationIterators,
        // plus the equalPrefixLength position,
        // so that prefix matches back into the equal prefix work.
    }

    int32_t result;
    int32_t fastLatinOptions = settings->fastLatinOptions;
    if(fastLatinOptions >= 0 &&
            (equalPrefixLength == leftLength ||
                left[equalPrefixLength] <= CollationFastLatin::LATIN_MAX_UTF8_LEAD) &&
            (equalPrefixLength == rightLength ||
                right[equalPrefixLength] <= CollationFastLatin::LATIN_MAX_UTF8_LEAD)) {
        if(leftLength >= 0) {
            result = CollationFastLatin::compareUTF8(data->fastLatinTable,
                                                     settings->fastLatinPrimaries,
                                                     fastLatinOptions,
                                                     left + equalPrefixLength,
                                                     leftLength - equalPrefixLength,
                                                     right + equalPrefixLength,
                                                     rightLength - equalPrefixLength);
        } else {
            result = CollationFastLatin::compareUTF8(data->fastLatinTable,
                                                     settings->fastLatinPrimaries,
                                                     fastLatinOptions,
                                                     left + equalPrefixLength, -1,
                                                     right + equalPrefixLength, -1);
        }
    } else {
        result = CollationFastLatin::BAIL_OUT_RESULT;
    }

    if(result == CollationFastLatin::BAIL_OUT_RESULT) {
        if(settings->dontCheckFCD()) {
            UTF8CollationIterator leftIter(data, numeric, left, equalPrefixLength, leftLength);
            UTF8CollationIterator rightIter(data, numeric, right, equalPrefixLength, rightLength);
            result = CollationCompare::compareUpToQuaternary(leftIter, rightIter, *settings, errorCode);
        } else {
            FCDUTF8CollationIterator leftIter(data, numeric, left, equalPrefixLength, leftLength);
            FCDUTF8CollationIterator rightIter(data, numeric, right, equalPrefixLength, rightLength);
            result = CollationCompare::compareUpToQuaternary(leftIter, rightIter, *settings, errorCode);
        }
    }
    if(result != UCOL_EQUAL || settings->getStrength() < UCOL_IDENTICAL || U_FAILURE(errorCode)) {
        return (UCollationResult)result;
    }

    // Note: If NUL-terminated, we could get the actual limits from the iterators now.
    // That would complicate the iterators a bit, NUL-terminated strings are only a C convenience,
    // and the benefit seems unlikely to be measurable.

    // Compare identical level.
    const Normalizer2Impl &nfcImpl = data->nfcImpl;
    left += equalPrefixLength;
    right += equalPrefixLength;
    if(leftLength > 0) {
        leftLength -= equalPrefixLength;
        rightLength -= equalPrefixLength;
    }
    if(settings->dontCheckFCD()) {
        UTF8NFDIterator leftIter(left, leftLength);
        UTF8NFDIterator rightIter(right, rightLength);
        return compareNFDIter(nfcImpl, leftIter, rightIter);
    } else {
        FCDUTF8NFDIterator leftIter(data, left, leftLength);
        FCDUTF8NFDIterator rightIter(data, right, rightLength);
        return compareNFDIter(nfcImpl, leftIter, rightIter);
    }
}

UCollationResult
RuleBasedCollator::compare(UCharIterator &left, UCharIterator &right,
                           UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode) || &left == &right) { return UCOL_EQUAL; }
    UBool numeric = settings->isNumeric();

    // Identical-prefix test.
    int32_t equalPrefixLength = 0;
    {
        UChar32 leftUnit;
        UChar32 rightUnit;
        while((leftUnit = left.next(&left)) == (rightUnit = right.next(&right))) {
            if(leftUnit < 0) { return UCOL_EQUAL; }
            ++equalPrefixLength;
        }

        // Back out the code units that differed, for the real collation comparison.
        if(leftUnit >= 0) { left.previous(&left); }
        if(rightUnit >= 0) { right.previous(&right); }

        if(equalPrefixLength > 0) {
            if((leftUnit >= 0 && data->isUnsafeBackward(leftUnit, numeric)) ||
                    (rightUnit >= 0 && data->isUnsafeBackward(rightUnit, numeric))) {
                // Identical prefix: Back up to the start of a contraction or reordering sequence.
                do {
                    --equalPrefixLength;
                    leftUnit = left.previous(&left);
                    right.previous(&right);
                } while(equalPrefixLength > 0 && data->isUnsafeBackward(leftUnit, numeric));
            }
            // See the notes in the UTF-16 version.
        }
    }

    UCollationResult result;
    if(settings->dontCheckFCD()) {
        UIterCollationIterator leftIter(data, numeric, left);
        UIterCollationIterator rightIter(data, numeric, right);
        result = CollationCompare::compareUpToQuaternary(leftIter, rightIter, *settings, errorCode);
    } else {
        FCDUIterCollationIterator leftIter(data, numeric, left, equalPrefixLength);
        FCDUIterCollationIterator rightIter(data, numeric, right, equalPrefixLength);
        result = CollationCompare::compareUpToQuaternary(leftIter, rightIter, *settings, errorCode);
    }
    if(result != UCOL_EQUAL || settings->getStrength() < UCOL_IDENTICAL || U_FAILURE(errorCode)) {
        return result;
    }

    // Compare identical level.
    left.move(&left, equalPrefixLength, UITER_ZERO);
    right.move(&right, equalPrefixLength, UITER_ZERO);
    const Normalizer2Impl &nfcImpl = data->nfcImpl;
    if(settings->dontCheckFCD()) {
        UIterNFDIterator leftIter(left);
        UIterNFDIterator rightIter(right);
        return compareNFDIter(nfcImpl, leftIter, rightIter);
    } else {
        FCDUIterNFDIterator leftIter(data, left, equalPrefixLength);
        FCDUIterNFDIterator rightIter(data, right, equalPrefixLength);
        return compareNFDIter(nfcImpl, leftIter, rightIter);
    }
}

CollationKey &
RuleBasedCollator::getCollationKey(const UnicodeString &s, CollationKey &key,
                                   UErrorCode &errorCode) const {
    return getCollationKey(s.getBuffer(), s.length(), key, errorCode);
}

CollationKey &
RuleBasedCollator::getCollationKey(const UChar *s, int32_t length, CollationKey& key,
                                   UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) {
        return key.setToBogus();
    }
    if(s == NULL && length != 0) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return key.setToBogus();
    }
    key.reset();  // resets the "bogus" state
    CollationKeyByteSink sink(key);
    writeSortKey(s, length, sink, errorCode);
    if(U_FAILURE(errorCode)) {
        key.setToBogus();
    } else if(key.isBogus()) {
        errorCode = U_MEMORY_ALLOCATION_ERROR;
    } else {
        key.setLength(sink.NumberOfBytesAppended());
    }
    return key;
}

int32_t
RuleBasedCollator::getSortKey(const UnicodeString &s,
                              uint8_t *dest, int32_t capacity) const {
    return getSortKey(s.getBuffer(), s.length(), dest, capacity);
}

int32_t
RuleBasedCollator::getSortKey(const UChar *s, int32_t length,
                              uint8_t *dest, int32_t capacity) const {
    if((s == NULL && length != 0) || capacity < 0 || (dest == NULL && capacity > 0)) {
        return 0;
    }
    uint8_t noDest[1] = { 0 };
    if(dest == NULL) {
        // Distinguish pure preflighting from an allocation error.
        dest = noDest;
        capacity = 0;
    }
    FixedSortKeyByteSink sink(reinterpret_cast<char *>(dest), capacity);
    UErrorCode errorCode = U_ZERO_ERROR;
    writeSortKey(s, length, sink, errorCode);
    return U_SUCCESS(errorCode) ? sink.NumberOfBytesAppended() : 0;
}

void
RuleBasedCollator::writeSortKey(const UChar *s, int32_t length,
                                SortKeyByteSink &sink, UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return; }
    const UChar *limit = (length >= 0) ? s + length : NULL;
    UBool numeric = settings->isNumeric();
    CollationKeys::LevelCallback callback;
    if(settings->dontCheckFCD()) {
        UTF16CollationIterator iter(data, numeric, s, s, limit);
        CollationKeys::writeSortKeyUpToQuaternary(iter, data->compressibleBytes, *settings,
                                                  sink, Collation::PRIMARY_LEVEL,
                                                  callback, TRUE, errorCode);
    } else {
        FCDUTF16CollationIterator iter(data, numeric, s, s, limit);
        CollationKeys::writeSortKeyUpToQuaternary(iter, data->compressibleBytes, *settings,
                                                  sink, Collation::PRIMARY_LEVEL,
                                                  callback, TRUE, errorCode);
    }
    if(settings->getStrength() == UCOL_IDENTICAL) {
        writeIdenticalLevel(s, limit, sink, errorCode);
    }
    static const char terminator = 0;  // TERMINATOR_BYTE
    sink.Append(&terminator, 1);
}

void
RuleBasedCollator::writeIdenticalLevel(const UChar *s, const UChar *limit,
                                       SortKeyByteSink &sink, UErrorCode &errorCode) const {
    // NFD quick check
    const UChar *nfdQCYesLimit = data->nfcImpl.decompose(s, limit, NULL, errorCode);
    if(U_FAILURE(errorCode)) { return; }
    sink.Append(Collation::LEVEL_SEPARATOR_BYTE);
    UChar32 prev = 0;
    if(nfdQCYesLimit != s) {
        prev = u_writeIdenticalLevelRun(prev, s, (int32_t)(nfdQCYesLimit - s), sink);
    }
    // Is there non-NFD text?
    int32_t destLengthEstimate;
    if(limit != NULL) {
        if(nfdQCYesLimit == limit) { return; }
        destLengthEstimate = (int32_t)(limit - nfdQCYesLimit);
    } else {
        // s is NUL-terminated
        if(*nfdQCYesLimit == 0) { return; }
        destLengthEstimate = -1;
    }
    UnicodeString nfd;
    data->nfcImpl.decompose(nfdQCYesLimit, limit, nfd, destLengthEstimate, errorCode);
    u_writeIdenticalLevelRun(prev, nfd.getBuffer(), nfd.length(), sink);
}

namespace {

/**
 * internalNextSortKeyPart() calls CollationKeys::writeSortKeyUpToQuaternary()
 * with an instance of this callback class.
 * When another level is about to be written, the callback
 * records the level and the number of bytes that will be written until
 * the sink (which is actually a FixedSortKeyByteSink) fills up.
 *
 * When internalNextSortKeyPart() is called again, it restarts with the last level
 * and ignores as many bytes as were written previously for that level.
 */
class PartLevelCallback : public CollationKeys::LevelCallback {
public:
    PartLevelCallback(const SortKeyByteSink &s)
            : sink(s), level(Collation::PRIMARY_LEVEL) {
        levelCapacity = sink.GetRemainingCapacity();
    }
    virtual ~PartLevelCallback() {}
    virtual UBool needToWrite(Collation::Level l) {
        if(!sink.Overflowed()) {
            // Remember a level that will be at least partially written.
            level = l;
            levelCapacity = sink.GetRemainingCapacity();
            return TRUE;
        } else {
            return FALSE;
        }
    }
    Collation::Level getLevel() const { return level; }
    int32_t getLevelCapacity() const { return levelCapacity; }

private:
    const SortKeyByteSink &sink;
    Collation::Level level;
    int32_t levelCapacity;
};

}  // namespace

int32_t
RuleBasedCollator::internalNextSortKeyPart(UCharIterator *iter, uint32_t state[2],
                                           uint8_t *dest, int32_t count, UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return 0; }
    if(iter == NULL || state == NULL || count < 0 || (count > 0 && dest == NULL)) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return 0;
    }
    if(count == 0) { return 0; }

    FixedSortKeyByteSink sink(reinterpret_cast<char *>(dest), count);
    sink.IgnoreBytes((int32_t)state[1]);
    iter->move(iter, 0, UITER_START);

    Collation::Level level = (Collation::Level)state[0];
    if(level <= Collation::QUATERNARY_LEVEL) {
        UBool numeric = settings->isNumeric();
        PartLevelCallback callback(sink);
        if(settings->dontCheckFCD()) {
            UIterCollationIterator ci(data, numeric, *iter);
            CollationKeys::writeSortKeyUpToQuaternary(ci, data->compressibleBytes, *settings,
                                                      sink, level, callback, FALSE, errorCode);
        } else {
            FCDUIterCollationIterator ci(data, numeric, *iter, 0);
            CollationKeys::writeSortKeyUpToQuaternary(ci, data->compressibleBytes, *settings,
                                                      sink, level, callback, FALSE, errorCode);
        }
        if(U_FAILURE(errorCode)) { return 0; }
        if(sink.NumberOfBytesAppended() > count) {
            state[0] = (uint32_t)callback.getLevel();
            state[1] = (uint32_t)callback.getLevelCapacity();
            return count;
        }
        // All of the normal levels are done.
        if(settings->getStrength() == UCOL_IDENTICAL) {
            level = Collation::IDENTICAL_LEVEL;
            iter->move(iter, 0, UITER_START);
        }
        // else fall through to setting ZERO_LEVEL
    }

    if(level == Collation::IDENTICAL_LEVEL) {
        int32_t levelCapacity = sink.GetRemainingCapacity();
        UnicodeString s;
        for(;;) {
            UChar32 c = iter->next(iter);
            if(c < 0) { break; }
            s.append((UChar)c);
        }
        const UChar *sArray = s.getBuffer();
        writeIdenticalLevel(sArray, sArray + s.length(), sink, errorCode);
        if(U_FAILURE(errorCode)) { return 0; }
        if(sink.NumberOfBytesAppended() > count) {
            state[0] = (uint32_t)level;
            state[1] = (uint32_t)levelCapacity;
            return count;
        }
    }

    // ZERO_LEVEL: Fill the remainder of dest with 00 bytes.
    state[0] = (uint32_t)Collation::ZERO_LEVEL;
    state[1] = 0;
    int32_t length = sink.NumberOfBytesAppended();
    int32_t i = length;
    while(i < count) { dest[i++] = 0; }
    return length;
}

void
RuleBasedCollator::internalGetCEs(const UnicodeString &str, UVector64 &ces,
                                  UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return; }
    const UChar *s = str.getBuffer();
    const UChar *limit = s + str.length();
    UBool numeric = settings->isNumeric();
    if(settings->dontCheckFCD()) {
        UTF16CollationIterator iter(data, numeric, s, s, limit);
        int64_t ce;
        while((ce = iter.nextCE(errorCode)) != Collation::NO_CE) {
            ces.addElement(ce, errorCode);
        }
    } else {
        FCDUTF16CollationIterator iter(data, numeric, s, s, limit);
        int64_t ce;
        while((ce = iter.nextCE(errorCode)) != Collation::NO_CE) {
            ces.addElement(ce, errorCode);
        }
    }
}

namespace {

void appendSubtag(CharString &s, char letter, const char *subtag, int32_t length,
                  UErrorCode &errorCode) {
    if(U_FAILURE(errorCode) || length == 0) { return; }
    if(!s.isEmpty()) {
        s.append('_', errorCode);
    }
    s.append(letter, errorCode);
    for(int32_t i = 0; i < length; ++i) {
        s.append(uprv_toupper(subtag[i]), errorCode);
    }
}

void appendAttribute(CharString &s, char letter, UColAttributeValue value,
                     UErrorCode &errorCode) {
    if(U_FAILURE(errorCode)) { return; }
    if(!s.isEmpty()) {
        s.append('_', errorCode);
    }
    static const char *valueChars = "1234...........IXO..SN..LU......";
    s.append(letter, errorCode);
    s.append(valueChars[value], errorCode);
}

}  // namespace

int32_t
RuleBasedCollator::internalGetShortDefinitionString(const char *locale,
                                                    char *buffer, int32_t capacity,
                                                    UErrorCode &errorCode) const {
    if(U_FAILURE(errorCode)) { return 0; }
    if(buffer == NULL ? capacity != 0 : capacity < 0) {
        errorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return 0;
    }
    if(locale == NULL) {
        locale = internalGetLocaleID(ULOC_VALID_LOCALE, errorCode);
    }

    char resultLocale[ULOC_FULLNAME_CAPACITY + 1];
    int32_t length = ucol_getFunctionalEquivalent(resultLocale, ULOC_FULLNAME_CAPACITY,
                                                  "collation", locale,
                                                  NULL, &errorCode);
    if(U_FAILURE(errorCode)) { return 0; }
    resultLocale[length] = 0;

    // Append items in alphabetic order of their short definition letters.
    CharString result;
    char subtag[ULOC_KEYWORD_AND_VALUES_CAPACITY];

    if(attributeHasBeenSetExplicitly(UCOL_ALTERNATE_HANDLING)) {
        appendAttribute(result, 'A', getAttribute(UCOL_ALTERNATE_HANDLING, errorCode), errorCode);
    }
    // ATTR_VARIABLE_TOP not supported because 'B' was broken.
    // See ICU tickets #10372 and #10386.
    if(attributeHasBeenSetExplicitly(UCOL_CASE_FIRST)) {
        appendAttribute(result, 'C', getAttribute(UCOL_CASE_FIRST, errorCode), errorCode);
    }
    if(attributeHasBeenSetExplicitly(UCOL_NUMERIC_COLLATION)) {
        appendAttribute(result, 'D', getAttribute(UCOL_NUMERIC_COLLATION, errorCode), errorCode);
    }
    if(attributeHasBeenSetExplicitly(UCOL_CASE_LEVEL)) {
        appendAttribute(result, 'E', getAttribute(UCOL_CASE_LEVEL, errorCode), errorCode);
    }
    if(attributeHasBeenSetExplicitly(UCOL_FRENCH_COLLATION)) {
        appendAttribute(result, 'F', getAttribute(UCOL_FRENCH_COLLATION, errorCode), errorCode);
    }
    // Note: UCOL_HIRAGANA_QUATERNARY_MODE is deprecated and never changes away from default.
    length = uloc_getKeywordValue(resultLocale, "collation", subtag, UPRV_LENGTHOF(subtag), &errorCode);
    appendSubtag(result, 'K', subtag, length, errorCode);
    length = uloc_getLanguage(resultLocale, subtag, UPRV_LENGTHOF(subtag), &errorCode);
    if (length == 0) {
        appendSubtag(result, 'L', "root", 4, errorCode);
    } else {
        appendSubtag(result, 'L', subtag, length, errorCode);
    }
    if(attributeHasBeenSetExplicitly(UCOL_NORMALIZATION_MODE)) {
        appendAttribute(result, 'N', getAttribute(UCOL_NORMALIZATION_MODE, errorCode), errorCode);
    }
    length = uloc_getCountry(resultLocale, subtag, UPRV_LENGTHOF(subtag), &errorCode);
    appendSubtag(result, 'R', subtag, length, errorCode);
    if(attributeHasBeenSetExplicitly(UCOL_STRENGTH)) {
        appendAttribute(result, 'S', getAttribute(UCOL_STRENGTH, errorCode), errorCode);
    }
    length = uloc_getVariant(resultLocale, subtag, UPRV_LENGTHOF(subtag), &errorCode);
    appendSubtag(result, 'V', subtag, length, errorCode);
    length = uloc_getScript(resultLocale, subtag, UPRV_LENGTHOF(subtag), &errorCode);
    appendSubtag(result, 'Z', subtag, length, errorCode);

    if(U_FAILURE(errorCode)) { return 0; }
    if(result.length() <= capacity) {
        uprv_memcpy(buffer, result.data(), result.length());
    }
    return u_terminateChars(buffer, capacity, result.length(), &errorCode);
}

UBool
RuleBasedCollator::isUnsafe(UChar32 c) const {
    return data->isUnsafeBackward(c, settings->isNumeric());
}

void U_CALLCONV
RuleBasedCollator::computeMaxExpansions(const CollationTailoring *t, UErrorCode &errorCode) {
    t->maxExpansions = CollationElementIterator::computeMaxExpansions(t->data, errorCode);
}

UBool
RuleBasedCollator::initMaxExpansions(UErrorCode &errorCode) const {
    umtx_initOnce(tailoring->maxExpansionsInitOnce, computeMaxExpansions, tailoring, errorCode);
    return U_SUCCESS(errorCode);
}

CollationElementIterator *
RuleBasedCollator::createCollationElementIterator(const UnicodeString& source) const {
    UErrorCode errorCode = U_ZERO_ERROR;
    if(!initMaxExpansions(errorCode)) { return NULL; }
    CollationElementIterator *cei = new CollationElementIterator(source, this, errorCode);
    if(U_FAILURE(errorCode)) {
        delete cei;
        return NULL;
    }
    return cei;
}

CollationElementIterator *
RuleBasedCollator::createCollationElementIterator(const CharacterIterator& source) const {
    UErrorCode errorCode = U_ZERO_ERROR;
    if(!initMaxExpansions(errorCode)) { return NULL; }
    CollationElementIterator *cei = new CollationElementIterator(source, this, errorCode);
    if(U_FAILURE(errorCode)) {
        delete cei;
        return NULL;
    }
    return cei;
}

int32_t
RuleBasedCollator::getMaxExpansion(int32_t order) const {
    UErrorCode errorCode = U_ZERO_ERROR;
    (void)initMaxExpansions(errorCode);
    return CollationElementIterator::getMaxExpansion(tailoring->maxExpansions, order);
}

U_NAMESPACE_END

#endif  // !UCONFIG_NO_COLLATION
