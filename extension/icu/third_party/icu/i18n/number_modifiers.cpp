// © 2017 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html

#include "unicode/utypes.h"

#if !UCONFIG_NO_FORMATTING

#include "umutex.h"
#include "ucln_cmn.h"
#include "ucln_in.h"
#include "number_modifiers.h"

using namespace icu;
using namespace icu::number;
using namespace icu::number::impl;

namespace {

// TODO: This is copied from simpleformatter.cpp
const int32_t NUMBER_MODIFIERS_ARG_NUM_LIMIT = 0x100;

// These are the default currency spacing UnicodeSets in CLDR.
// Pre-compute them for performance.
// The Java unit test testCurrencySpacingPatternStability() will start failing if these change in CLDR.
icu::UInitOnce gDefaultCurrencySpacingInitOnce = U_INITONCE_INITIALIZER;

UnicodeSet *UNISET_DIGIT = nullptr;
UnicodeSet *UNISET_NOTS = nullptr;

UBool U_CALLCONV cleanupDefaultCurrencySpacing() {
    delete UNISET_DIGIT;
    UNISET_DIGIT = nullptr;
    delete UNISET_NOTS;
    UNISET_NOTS = nullptr;
    gDefaultCurrencySpacingInitOnce.reset();
    return TRUE;
}

void U_CALLCONV initDefaultCurrencySpacing(UErrorCode &status) {
    ucln_i18n_registerCleanup(UCLN_I18N_CURRENCY_SPACING, cleanupDefaultCurrencySpacing);
    UNISET_DIGIT = new UnicodeSet(UnicodeString(u"[:digit:]"), status);
    UNISET_NOTS = new UnicodeSet(UnicodeString(u"[:^S:]"), status);
    if (UNISET_DIGIT == nullptr || UNISET_NOTS == nullptr) {
        status = U_MEMORY_ALLOCATION_ERROR;
        return;
    }
    UNISET_DIGIT->freeze();
    UNISET_NOTS->freeze();
}

}  // namespace


Modifier::~Modifier() = default;

Modifier::Parameters::Parameters()
        : obj(nullptr) {}

Modifier::Parameters::Parameters(
    const ModifierStore* _obj, Signum _signum, StandardPlural::Form _plural)
        : obj(_obj), signum(_signum), plural(_plural) {}

ModifierStore::~ModifierStore() = default;

AdoptingModifierStore::~AdoptingModifierStore()  {
    for (const Modifier *mod : mods) {
        delete mod;
    }
}


int32_t ConstantAffixModifier::apply(FormattedStringBuilder &output, int leftIndex, int rightIndex,
                                     UErrorCode &status) const {
    // Insert the suffix first since inserting the prefix will change the rightIndex
    int length = output.insert(rightIndex, fSuffix, fField, status);
    length += output.insert(leftIndex, fPrefix, fField, status);
    return length;
}

int32_t ConstantAffixModifier::getPrefixLength() const {
    return fPrefix.length();
}

int32_t ConstantAffixModifier::getCodePointCount() const {
    return fPrefix.countChar32() + fSuffix.countChar32();
}

bool ConstantAffixModifier::isStrong() const {
    return fStrong;
}

bool ConstantAffixModifier::containsField(UNumberFormatFields field) const {
    (void)field;
    // This method is not currently used.
    UPRV_UNREACHABLE;
}

void ConstantAffixModifier::getParameters(Parameters& output) const {
    (void)output;
    // This method is not currently used.
    UPRV_UNREACHABLE;
}

bool ConstantAffixModifier::semanticallyEquivalent(const Modifier& other) const {
    auto* _other = dynamic_cast<const ConstantAffixModifier*>(&other);
    if (_other == nullptr) {
        return false;
    }
    return fPrefix == _other->fPrefix
        && fSuffix == _other->fSuffix
        && fField == _other->fField
        && fStrong == _other->fStrong;
}


SimpleModifier::SimpleModifier(const SimpleFormatter &simpleFormatter, Field field, bool strong)
        : SimpleModifier(simpleFormatter, field, strong, {}) {}

SimpleModifier::SimpleModifier(const SimpleFormatter &simpleFormatter, Field field, bool strong,
                               const Modifier::Parameters parameters)
        : fCompiledPattern(simpleFormatter.compiledPattern), fField(field), fStrong(strong),
          fParameters(parameters) {
    int32_t argLimit = SimpleFormatter::getArgumentLimit(
            fCompiledPattern.getBuffer(), fCompiledPattern.length());
    if (argLimit == 0) {
        // No arguments in compiled pattern
        fPrefixLength = fCompiledPattern.charAt(1) - NUMBER_MODIFIERS_ARG_NUM_LIMIT;
        U_ASSERT(2 + fPrefixLength == fCompiledPattern.length());
        // Set suffixOffset = -1 to indicate no arguments in compiled pattern.
        fSuffixOffset = -1;
        fSuffixLength = 0;
    } else {
        U_ASSERT(argLimit == 1);
        if (fCompiledPattern.charAt(1) != 0) {
            // Found prefix
            fPrefixLength = fCompiledPattern.charAt(1) - NUMBER_MODIFIERS_ARG_NUM_LIMIT;
            fSuffixOffset = 3 + fPrefixLength;
        } else {
            // No prefix
            fPrefixLength = 0;
            fSuffixOffset = 2;
        }
        if (3 + fPrefixLength < fCompiledPattern.length()) {
            // Found suffix
            fSuffixLength = fCompiledPattern.charAt(fSuffixOffset) - NUMBER_MODIFIERS_ARG_NUM_LIMIT;
        } else {
            // No suffix
            fSuffixLength = 0;
        }
    }
}

SimpleModifier::SimpleModifier()
        : fField(UNUM_FIELD_COUNT), fStrong(false), fPrefixLength(0), fSuffixLength(0) {
}

int32_t SimpleModifier::apply(FormattedStringBuilder &output, int leftIndex, int rightIndex,
                              UErrorCode &status) const {
    return formatAsPrefixSuffix(output, leftIndex, rightIndex, status);
}

int32_t SimpleModifier::getPrefixLength() const {
    return fPrefixLength;
}

int32_t SimpleModifier::getCodePointCount() const {
    int32_t count = 0;
    if (fPrefixLength > 0) {
        count += fCompiledPattern.countChar32(2, fPrefixLength);
    }
    if (fSuffixLength > 0) {
        count += fCompiledPattern.countChar32(1 + fSuffixOffset, fSuffixLength);
    }
    return count;
}

bool SimpleModifier::isStrong() const {
    return fStrong;
}

bool SimpleModifier::containsField(UNumberFormatFields field) const {
    (void)field;
    // This method is not currently used.
    UPRV_UNREACHABLE;
}

void SimpleModifier::getParameters(Parameters& output) const {
    output = fParameters;
}

bool SimpleModifier::semanticallyEquivalent(const Modifier& other) const {
    auto* _other = dynamic_cast<const SimpleModifier*>(&other);
    if (_other == nullptr) {
        return false;
    }
    if (fParameters.obj != nullptr) {
        return fParameters.obj == _other->fParameters.obj;
    }
    return fCompiledPattern == _other->fCompiledPattern
        && fField == _other->fField
        && fStrong == _other->fStrong;
}


int32_t
SimpleModifier::formatAsPrefixSuffix(FormattedStringBuilder &result, int32_t startIndex, int32_t endIndex,
                                     UErrorCode &status) const {
    if (fSuffixOffset == -1 && fPrefixLength + fSuffixLength > 0) {
        // There is no argument for the inner number; overwrite the entire segment with our string.
        return result.splice(startIndex, endIndex, fCompiledPattern, 2, 2 + fPrefixLength, fField, status);
    } else {
        if (fPrefixLength > 0) {
            result.insert(startIndex, fCompiledPattern, 2, 2 + fPrefixLength, fField, status);
        }
        if (fSuffixLength > 0) {
            result.insert(
                    endIndex + fPrefixLength,
                    fCompiledPattern,
                    1 + fSuffixOffset,
                    1 + fSuffixOffset + fSuffixLength,
                    fField,
                    status);
        }
        return fPrefixLength + fSuffixLength;
    }
}


int32_t
SimpleModifier::formatTwoArgPattern(const SimpleFormatter& compiled, FormattedStringBuilder& result,
                                    int32_t index, int32_t* outPrefixLength, int32_t* outSuffixLength,
                                    Field field, UErrorCode& status) {
    const UnicodeString& compiledPattern = compiled.compiledPattern;
    int32_t argLimit = SimpleFormatter::getArgumentLimit(
            compiledPattern.getBuffer(), compiledPattern.length());
    if (argLimit != 2) {
        status = U_INTERNAL_PROGRAM_ERROR;
        return 0;
    }
    int32_t offset = 1; // offset into compiledPattern
    int32_t length = 0; // chars added to result

    int32_t prefixLength = compiledPattern.charAt(offset);
    offset++;
    if (prefixLength < NUMBER_MODIFIERS_ARG_NUM_LIMIT) {
        // No prefix
        prefixLength = 0;
    } else {
        prefixLength -= NUMBER_MODIFIERS_ARG_NUM_LIMIT;
        result.insert(index + length, compiledPattern, offset, offset + prefixLength, field, status);
        offset += prefixLength;
        length += prefixLength;
        offset++;
    }

    int32_t infixLength = compiledPattern.charAt(offset);
    offset++;
    if (infixLength < NUMBER_MODIFIERS_ARG_NUM_LIMIT) {
        // No infix
        infixLength = 0;
    } else {
        infixLength -= NUMBER_MODIFIERS_ARG_NUM_LIMIT;
        result.insert(index + length, compiledPattern, offset, offset + infixLength, field, status);
        offset += infixLength;
        length += infixLength;
        offset++;
    }

    int32_t suffixLength;
    if (offset == compiledPattern.length()) {
        // No suffix
        suffixLength = 0;
    } else {
        suffixLength = compiledPattern.charAt(offset) -  NUMBER_MODIFIERS_ARG_NUM_LIMIT;
        offset++;
        result.insert(index + length, compiledPattern, offset, offset + suffixLength, field, status);
        length += suffixLength;
    }

    *outPrefixLength = prefixLength;
    *outSuffixLength = suffixLength;

    return length;
}


int32_t ConstantMultiFieldModifier::apply(FormattedStringBuilder &output, int leftIndex, int rightIndex,
                                          UErrorCode &status) const {
    int32_t length = output.insert(leftIndex, fPrefix, status);
    if (fOverwrite) {
        length += output.splice(
            leftIndex + length,
            rightIndex + length,
            UnicodeString(), 0, 0,
            UNUM_FIELD_COUNT, status);
    }
    length += output.insert(rightIndex + length, fSuffix, status);
    return length;
}

int32_t ConstantMultiFieldModifier::getPrefixLength() const {
    return fPrefix.length();
}

int32_t ConstantMultiFieldModifier::getCodePointCount() const {
    return fPrefix.codePointCount() + fSuffix.codePointCount();
}

bool ConstantMultiFieldModifier::isStrong() const {
    return fStrong;
}

bool ConstantMultiFieldModifier::containsField(UNumberFormatFields field) const {
    return fPrefix.containsField(field) || fSuffix.containsField(field);
}

void ConstantMultiFieldModifier::getParameters(Parameters& output) const {
    output = fParameters;
}

bool ConstantMultiFieldModifier::semanticallyEquivalent(const Modifier& other) const {
    auto* _other = dynamic_cast<const ConstantMultiFieldModifier*>(&other);
    if (_other == nullptr) {
        return false;
    }
    if (fParameters.obj != nullptr) {
        return fParameters.obj == _other->fParameters.obj;
    }
    return fPrefix.contentEquals(_other->fPrefix)
        && fSuffix.contentEquals(_other->fSuffix)
        && fOverwrite == _other->fOverwrite
        && fStrong == _other->fStrong;
}


CurrencySpacingEnabledModifier::CurrencySpacingEnabledModifier(const FormattedStringBuilder &prefix,
                                                               const FormattedStringBuilder &suffix,
                                                               bool overwrite,
                                                               bool strong,
                                                               const DecimalFormatSymbols &symbols,
                                                               UErrorCode &status)
        : ConstantMultiFieldModifier(prefix, suffix, overwrite, strong) {
    // Check for currency spacing. Do not build the UnicodeSets unless there is
    // a currency code point at a boundary.
    if (prefix.length() > 0 && prefix.fieldAt(prefix.length() - 1) == UNUM_CURRENCY_FIELD) {
        int prefixCp = prefix.getLastCodePoint();
        UnicodeSet prefixUnicodeSet = getUnicodeSet(symbols, IN_CURRENCY, PREFIX, status);
        if (prefixUnicodeSet.contains(prefixCp)) {
            fAfterPrefixUnicodeSet = getUnicodeSet(symbols, IN_NUMBER, PREFIX, status);
            fAfterPrefixUnicodeSet.freeze();
            fAfterPrefixInsert = getInsertString(symbols, PREFIX, status);
        } else {
            fAfterPrefixUnicodeSet.setToBogus();
            fAfterPrefixInsert.setToBogus();
        }
    } else {
        fAfterPrefixUnicodeSet.setToBogus();
        fAfterPrefixInsert.setToBogus();
    }
    if (suffix.length() > 0 && suffix.fieldAt(0) == UNUM_CURRENCY_FIELD) {
        int suffixCp = suffix.getLastCodePoint();
        UnicodeSet suffixUnicodeSet = getUnicodeSet(symbols, IN_CURRENCY, SUFFIX, status);
        if (suffixUnicodeSet.contains(suffixCp)) {
            fBeforeSuffixUnicodeSet = getUnicodeSet(symbols, IN_NUMBER, SUFFIX, status);
            fBeforeSuffixUnicodeSet.freeze();
            fBeforeSuffixInsert = getInsertString(symbols, SUFFIX, status);
        } else {
            fBeforeSuffixUnicodeSet.setToBogus();
            fBeforeSuffixInsert.setToBogus();
        }
    } else {
        fBeforeSuffixUnicodeSet.setToBogus();
        fBeforeSuffixInsert.setToBogus();
    }
}

int32_t CurrencySpacingEnabledModifier::apply(FormattedStringBuilder &output, int leftIndex, int rightIndex,
                                              UErrorCode &status) const {
    // Currency spacing logic
    int length = 0;
    if (rightIndex - leftIndex > 0 && !fAfterPrefixUnicodeSet.isBogus() &&
        fAfterPrefixUnicodeSet.contains(output.codePointAt(leftIndex))) {
        // TODO: Should we use the CURRENCY field here?
        length += output.insert(leftIndex, fAfterPrefixInsert, UNUM_FIELD_COUNT, status);
    }
    if (rightIndex - leftIndex > 0 && !fBeforeSuffixUnicodeSet.isBogus() &&
        fBeforeSuffixUnicodeSet.contains(output.codePointBefore(rightIndex))) {
        // TODO: Should we use the CURRENCY field here?
        length += output.insert(rightIndex + length, fBeforeSuffixInsert, UNUM_FIELD_COUNT, status);
    }

    // Call super for the remaining logic
    length += ConstantMultiFieldModifier::apply(output, leftIndex, rightIndex + length, status);
    return length;
}

int32_t
CurrencySpacingEnabledModifier::applyCurrencySpacing(FormattedStringBuilder &output, int32_t prefixStart,
                                                     int32_t prefixLen, int32_t suffixStart,
                                                     int32_t suffixLen,
                                                     const DecimalFormatSymbols &symbols,
                                                     UErrorCode &status) {
    int length = 0;
    bool hasPrefix = (prefixLen > 0);
    bool hasSuffix = (suffixLen > 0);
    bool hasNumber = (suffixStart - prefixStart - prefixLen > 0); // could be empty string
    if (hasPrefix && hasNumber) {
        length += applyCurrencySpacingAffix(output, prefixStart + prefixLen, PREFIX, symbols, status);
    }
    if (hasSuffix && hasNumber) {
        length += applyCurrencySpacingAffix(output, suffixStart + length, SUFFIX, symbols, status);
    }
    return length;
}

int32_t
CurrencySpacingEnabledModifier::applyCurrencySpacingAffix(FormattedStringBuilder &output, int32_t index,
                                                          EAffix affix,
                                                          const DecimalFormatSymbols &symbols,
                                                          UErrorCode &status) {
    // NOTE: For prefix, output.fieldAt(index-1) gets the last field type in the prefix.
    // This works even if the last code point in the prefix is 2 code units because the
    // field value gets populated to both indices in the field array.
    Field affixField = (affix == PREFIX) ? output.fieldAt(index - 1) : output.fieldAt(index);
    if (affixField != UNUM_CURRENCY_FIELD) {
        return 0;
    }
    int affixCp = (affix == PREFIX) ? output.codePointBefore(index) : output.codePointAt(index);
    UnicodeSet affixUniset = getUnicodeSet(symbols, IN_CURRENCY, affix, status);
    if (!affixUniset.contains(affixCp)) {
        return 0;
    }
    int numberCp = (affix == PREFIX) ? output.codePointAt(index) : output.codePointBefore(index);
    UnicodeSet numberUniset = getUnicodeSet(symbols, IN_NUMBER, affix, status);
    if (!numberUniset.contains(numberCp)) {
        return 0;
    }
    UnicodeString spacingString = getInsertString(symbols, affix, status);

    // NOTE: This next line *inserts* the spacing string, triggering an arraycopy.
    // It would be more efficient if this could be done before affixes were attached,
    // so that it could be prepended/appended instead of inserted.
    // However, the build code path is more efficient, and this is the most natural
    // place to put currency spacing in the non-build code path.
    // TODO: Should we use the CURRENCY field here?
    return output.insert(index, spacingString, UNUM_FIELD_COUNT, status);
}

UnicodeSet
CurrencySpacingEnabledModifier::getUnicodeSet(const DecimalFormatSymbols &symbols, EPosition position,
                                              EAffix affix, UErrorCode &status) {
    // Ensure the static defaults are initialized:
    umtx_initOnce(gDefaultCurrencySpacingInitOnce, &initDefaultCurrencySpacing, status);
    if (U_FAILURE(status)) {
        return UnicodeSet();
    }

    const UnicodeString& pattern = symbols.getPatternForCurrencySpacing(
            position == IN_CURRENCY ? UNUM_CURRENCY_MATCH : UNUM_CURRENCY_SURROUNDING_MATCH,
            affix == SUFFIX,
            status);
    if (pattern.compare(u"[:digit:]", -1) == 0) {
        return *UNISET_DIGIT;
    } else if (pattern.compare(u"[:^S:]", -1) == 0) {
        return *UNISET_NOTS;
    } else {
        return UnicodeSet(pattern, status);
    }
}

UnicodeString
CurrencySpacingEnabledModifier::getInsertString(const DecimalFormatSymbols &symbols, EAffix affix,
                                                UErrorCode &status) {
    return symbols.getPatternForCurrencySpacing(UNUM_CURRENCY_INSERT, affix == SUFFIX, status);
}

#endif /* #if !UCONFIG_NO_FORMATTING */
