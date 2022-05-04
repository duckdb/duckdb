// © 2017 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html

#include "unicode/utypes.h"

#if !UCONFIG_NO_FORMATTING

#include "unicode/simpleformatter.h"
#include "unicode/ures.h"
#include "ureslocs.h"
#include "charstr.h"
#include "uresimp.h"
#include "number_longnames.h"
#include "number_microprops.h"
#include <algorithm>
#include "cstring.h"
#include "util.h"

using namespace icu;
using namespace icu::number;
using namespace icu::number::impl;

namespace {

constexpr int32_t number_longnames_DNAM_INDEX = StandardPlural::Form::COUNT;
constexpr int32_t number_longnames_PER_INDEX = StandardPlural::Form::COUNT + 1;
constexpr int32_t number_longnames_ARRAY_LENGTH = StandardPlural::Form::COUNT + 2;

static int32_t number_longnames_getIndex(const char* pluralKeyword, UErrorCode& status) {
    // pluralKeyword can also be "dnam" or "per"
    if (uprv_strcmp(pluralKeyword, "dnam") == 0) {
        return number_longnames_DNAM_INDEX;
    } else if (uprv_strcmp(pluralKeyword, "per") == 0) {
        return number_longnames_PER_INDEX;
    } else {
        StandardPlural::Form plural = StandardPlural::fromString(pluralKeyword, status);
        return plural;
    }
}

static UnicodeString getWithPlural(
        const UnicodeString* strings,
        StandardPlural::Form plural,
        UErrorCode& status) {
    UnicodeString result = strings[plural];
    if (result.isBogus()) {
        result = strings[StandardPlural::Form::OTHER];
    }
    if (result.isBogus()) {
        // There should always be data in the "other" plural variant.
        status = U_INTERNAL_PROGRAM_ERROR;
    }
    return result;
}


//////////////////////////
/// BEGIN DATA LOADING ///
//////////////////////////

class PluralTableSink : public ResourceSink {
  public:
    explicit PluralTableSink(UnicodeString *outArray) : outArray(outArray) {
        // Initialize the array to bogus strings.
        for (int32_t i = 0; i < number_longnames_ARRAY_LENGTH; i++) {
            outArray[i].setToBogus();
        }
    }

    void put(const char *key, ResourceValue &value, UBool /*noFallback*/, UErrorCode &status) U_OVERRIDE {
        ResourceTable pluralsTable = value.getTable(status);
        if (U_FAILURE(status)) { return; }
        for (int32_t i = 0; pluralsTable.getKeyAndValue(i, key, value); ++i) {
            int32_t index = number_longnames_getIndex(key, status);
            if (U_FAILURE(status)) { return; }
            if (!outArray[index].isBogus()) {
                continue;
            }
            outArray[index] = value.getUnicodeString(status);
            if (U_FAILURE(status)) { return; }
        }
    }

  private:
    UnicodeString *outArray;
};

// NOTE: outArray MUST have room for all StandardPlural values.  No bounds checking is performed.

void getMeasureData(const Locale &locale, const MeasureUnit &unit, const UNumberUnitWidth &width,
                    UnicodeString *outArray, UErrorCode &status) {
    PluralTableSink sink(outArray);
    LocalUResourceBundlePointer unitsBundle(ures_open(U_ICUDATA_UNIT, locale.getName(), &status));
    if (U_FAILURE(status)) { return; }

    // Map duration-year-person, duration-week-person, etc. to duration-year, duration-week, ...
    // TODO(ICU-20400): Get duration-*-person data properly with aliases.
    StringPiece subtypeForResource;
    int32_t subtypeLen = static_cast<int32_t>(uprv_strlen(unit.getSubtype()));
    if (subtypeLen > 7 && uprv_strcmp(unit.getSubtype() + subtypeLen - 7, "-person") == 0) {
        subtypeForResource = {unit.getSubtype(), subtypeLen - 7};
    } else {
        subtypeForResource = unit.getSubtype();
    }

    CharString key;
    key.append("units", status);
    if (width == UNUM_UNIT_WIDTH_NARROW) {
        key.append("Narrow", status);
    } else if (width == UNUM_UNIT_WIDTH_SHORT) {
        key.append("Short", status);
    }
    key.append("/", status);
    key.append(unit.getType(), status);
    key.append("/", status);
    key.append(subtypeForResource, status);

    UErrorCode localStatus = U_ZERO_ERROR;
    ures_getAllItemsWithFallback(unitsBundle.getAlias(), key.data(), sink, localStatus);
    if (width == UNUM_UNIT_WIDTH_SHORT) {
        if (U_FAILURE(localStatus)) {
            status = localStatus;
        }
        return;
    }

    // TODO(ICU-13353): The fallback to short does not work in ICU4C.
    // Manually fall back to short (this is done automatically in Java).
    key.clear();
    key.append("unitsShort/", status);
    key.append(unit.getType(), status);
    key.append("/", status);
    key.append(subtypeForResource, status);
    ures_getAllItemsWithFallback(unitsBundle.getAlias(), key.data(), sink, status);
}

void getCurrencyLongNameData(const Locale &locale, const CurrencyUnit &currency, UnicodeString *outArray,
                             UErrorCode &status) {
    // In ICU4J, this method gets a CurrencyData from CurrencyData.provider.
    // TODO(ICU4J): Implement this without going through CurrencyData, like in ICU4C?
    PluralTableSink sink(outArray);
    LocalUResourceBundlePointer unitsBundle(ures_open(U_ICUDATA_CURR, locale.getName(), &status));
    if (U_FAILURE(status)) { return; }
    ures_getAllItemsWithFallback(unitsBundle.getAlias(), "CurrencyUnitPatterns", sink, status);
    if (U_FAILURE(status)) { return; }
    for (int32_t i = 0; i < StandardPlural::Form::COUNT; i++) {
        UnicodeString &pattern = outArray[i];
        if (pattern.isBogus()) {
            continue;
        }
        int32_t longNameLen = 0;
        const char16_t *longName = ucurr_getPluralName(
                currency.getISOCurrency(),
                locale.getName(),
                nullptr /* isChoiceFormat */,
                StandardPlural::getKeyword(static_cast<StandardPlural::Form>(i)),
                &longNameLen,
                &status);
        // Example pattern from data: "{0} {1}"
        // Example output after find-and-replace: "{0} US dollars"
        pattern.findAndReplace(UnicodeString(u"{1}"), UnicodeString(longName, longNameLen));
    }
}

UnicodeString getPerUnitFormat(const Locale& locale, const UNumberUnitWidth &width, UErrorCode& status) {
    LocalUResourceBundlePointer unitsBundle(ures_open(U_ICUDATA_UNIT, locale.getName(), &status));
    if (U_FAILURE(status)) { return {}; }
    CharString key;
    key.append("units", status);
    if (width == UNUM_UNIT_WIDTH_NARROW) {
        key.append("Narrow", status);
    } else if (width == UNUM_UNIT_WIDTH_SHORT) {
        key.append("Short", status);
    }
    key.append("/compound/per", status);
    int32_t len = 0;
    const UChar* ptr = ures_getStringByKeyWithFallback(unitsBundle.getAlias(), key.data(), &len, &status);
    return UnicodeString(ptr, len);
}

////////////////////////
/// END DATA LOADING ///
////////////////////////

} // namespace

LongNameHandler*
LongNameHandler::forMeasureUnit(const Locale &loc, const MeasureUnit &unitRef, const MeasureUnit &perUnit,
                                const UNumberUnitWidth &width, const PluralRules *rules,
                                const MicroPropsGenerator *parent, UErrorCode &status) {
    MeasureUnit unit = unitRef;
    if (uprv_strcmp(perUnit.getType(), "none") != 0) {
        // Compound unit: first try to simplify (e.g., meters per second is its own unit).
        bool isResolved = false;
        MeasureUnit resolved = MeasureUnit::resolveUnitPerUnit(unit, perUnit, &isResolved);
        if (isResolved) {
            unit = resolved;
        } else {
            // No simplified form is available.
            return forCompoundUnit(loc, unit, perUnit, width, rules, parent, status);
        }
    }

    auto* result = new LongNameHandler(rules, parent);
    if (result == nullptr) {
        status = U_MEMORY_ALLOCATION_ERROR;
        return nullptr;
    }
    UnicodeString simpleFormats[number_longnames_ARRAY_LENGTH];
    getMeasureData(loc, unit, width, simpleFormats, status);
    if (U_FAILURE(status)) { return result; }
    result->simpleFormatsToModifiers(simpleFormats, UNUM_MEASURE_UNIT_FIELD, status);
    return result;
}

LongNameHandler*
LongNameHandler::forCompoundUnit(const Locale &loc, const MeasureUnit &unit, const MeasureUnit &perUnit,
                                 const UNumberUnitWidth &width, const PluralRules *rules,
                                 const MicroPropsGenerator *parent, UErrorCode &status) {
    auto* result = new LongNameHandler(rules, parent);
    if (result == nullptr) {
        status = U_MEMORY_ALLOCATION_ERROR;
        return nullptr;
    }
    UnicodeString primaryData[number_longnames_ARRAY_LENGTH];
    getMeasureData(loc, unit, width, primaryData, status);
    if (U_FAILURE(status)) { return result; }
    UnicodeString secondaryData[number_longnames_ARRAY_LENGTH];
    getMeasureData(loc, perUnit, width, secondaryData, status);
    if (U_FAILURE(status)) { return result; }

    UnicodeString perUnitFormat;
    if (!secondaryData[number_longnames_PER_INDEX].isBogus()) {
        perUnitFormat = secondaryData[number_longnames_PER_INDEX];
    } else {
        UnicodeString rawPerUnitFormat = getPerUnitFormat(loc, width, status);
        if (U_FAILURE(status)) { return result; }
        // rawPerUnitFormat is something like "{0}/{1}"; we need to substitute in the secondary unit.
        SimpleFormatter compiled(rawPerUnitFormat, 2, 2, status);
        if (U_FAILURE(status)) { return result; }
        UnicodeString secondaryFormat = getWithPlural(secondaryData, StandardPlural::Form::ONE, status);
        if (U_FAILURE(status)) { return result; }
        SimpleFormatter secondaryCompiled(secondaryFormat, 1, 1, status);
        if (U_FAILURE(status)) { return result; }
        UnicodeString secondaryString = secondaryCompiled.getTextWithNoArguments().trim();
        // TODO: Why does UnicodeString need to be explicit in the following line?
        compiled.format(UnicodeString(u"{0}"), secondaryString, perUnitFormat, status);
        if (U_FAILURE(status)) { return result; }
    }
    result->multiSimpleFormatsToModifiers(primaryData, perUnitFormat, UNUM_MEASURE_UNIT_FIELD, status);
    return result;
}

UnicodeString LongNameHandler::getUnitDisplayName(
        const Locale& loc,
        const MeasureUnit& unit,
        UNumberUnitWidth width,
        UErrorCode& status) {
    if (U_FAILURE(status)) {
        return ICU_Utility::makeBogusString();
    }
    UnicodeString simpleFormats[number_longnames_ARRAY_LENGTH];
    getMeasureData(loc, unit, width, simpleFormats, status);
    return simpleFormats[number_longnames_DNAM_INDEX];
}

UnicodeString LongNameHandler::getUnitPattern(
        const Locale& loc,
        const MeasureUnit& unit,
        UNumberUnitWidth width,
        StandardPlural::Form pluralForm,
        UErrorCode& status) {
    if (U_FAILURE(status)) {
        return ICU_Utility::makeBogusString();
    }
    UnicodeString simpleFormats[number_longnames_ARRAY_LENGTH];
    getMeasureData(loc, unit, width, simpleFormats, status);
    // The above already handles fallback from other widths to short
    if (U_FAILURE(status)) {
        return ICU_Utility::makeBogusString();
    }
    // Now handle fallback from other plural forms to OTHER
    return (!(simpleFormats[pluralForm]).isBogus())? simpleFormats[pluralForm]:
            simpleFormats[StandardPlural::Form::OTHER];
}

LongNameHandler* LongNameHandler::forCurrencyLongNames(const Locale &loc, const CurrencyUnit &currency,
                                                      const PluralRules *rules,
                                                      const MicroPropsGenerator *parent,
                                                      UErrorCode &status) {
    auto* result = new LongNameHandler(rules, parent);
    if (result == nullptr) {
        status = U_MEMORY_ALLOCATION_ERROR;
        return nullptr;
    }
    UnicodeString simpleFormats[number_longnames_ARRAY_LENGTH];
    getCurrencyLongNameData(loc, currency, simpleFormats, status);
    if (U_FAILURE(status)) { return nullptr; }
    result->simpleFormatsToModifiers(simpleFormats, UNUM_CURRENCY_FIELD, status);
    return result;
}

void LongNameHandler::simpleFormatsToModifiers(const UnicodeString *simpleFormats, Field field,
                                               UErrorCode &status) {
    for (int32_t i = 0; i < StandardPlural::Form::COUNT; i++) {
        StandardPlural::Form plural = static_cast<StandardPlural::Form>(i);
        UnicodeString simpleFormat = getWithPlural(simpleFormats, plural, status);
        if (U_FAILURE(status)) { return; }
        SimpleFormatter compiledFormatter(simpleFormat, 0, 1, status);
        if (U_FAILURE(status)) { return; }
        fModifiers[i] = SimpleModifier(compiledFormatter, field, false, {this, SIGNUM_ZERO, plural});
    }
}

void LongNameHandler::multiSimpleFormatsToModifiers(const UnicodeString *leadFormats, UnicodeString trailFormat,
                                                    Field field, UErrorCode &status) {
    SimpleFormatter trailCompiled(trailFormat, 1, 1, status);
    if (U_FAILURE(status)) { return; }
    for (int32_t i = 0; i < StandardPlural::Form::COUNT; i++) {
        StandardPlural::Form plural = static_cast<StandardPlural::Form>(i);
        UnicodeString leadFormat = getWithPlural(leadFormats, plural, status);
        if (U_FAILURE(status)) { return; }
        UnicodeString compoundFormat;
        trailCompiled.format(leadFormat, compoundFormat, status);
        if (U_FAILURE(status)) { return; }
        SimpleFormatter compoundCompiled(compoundFormat, 0, 1, status);
        if (U_FAILURE(status)) { return; }
        fModifiers[i] = SimpleModifier(compoundCompiled, field, false, {this, SIGNUM_ZERO, plural});
    }
}

void LongNameHandler::processQuantity(DecimalQuantity &quantity, MicroProps &micros,
                                      UErrorCode &status) const {
    parent->processQuantity(quantity, micros, status);
    StandardPlural::Form pluralForm = utils::getPluralSafe(micros.rounder, rules, quantity, status);
    micros.modOuter = &fModifiers[pluralForm];
}

const Modifier* LongNameHandler::getModifier(Signum /*signum*/, StandardPlural::Form plural) const {
    return &fModifiers[plural];
}

#endif /* #if !UCONFIG_NO_FORMATTING */
