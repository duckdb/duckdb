// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
*****************************************************************************************
* Copyright (C) 2010-2012, International Business Machines
* Corporation and others. All Rights Reserved.
*****************************************************************************************
*/

#include "unicode/utypes.h"

#if !UCONFIG_NO_FORMATTING

#include "unicode/upluralrules.h"
#include "unicode/plurrule.h"
#include "unicode/locid.h"
#include "unicode/unistr.h"
#include "unicode/unum.h"
#include "unicode/numfmt.h"
#include "unicode/unumberformatter.h"
#include "number_decimalquantity.h"
#include "number_utypes.h"

U_NAMESPACE_USE

namespace {

/**
 * Given a number and a format, returns the keyword of the first applicable
 * rule for the PluralRules object.
 * @param rules The plural rules.
 * @param obj The numeric object for which the rule should be determined.
 * @param fmt The NumberFormat specifying how the number will be formatted
 *        (this can affect the plural form, e.g. "1 dollar" vs "1.0 dollars").
 * @param status  Input/output parameter. If at entry this indicates a
 *                failure status, the method returns immediately; otherwise
 *                this is set to indicate the outcome of the call.
 * @return The keyword of the selected rule. Undefined in the case of an error.
 */
UnicodeString select(const PluralRules &rules, const Formattable& obj, const NumberFormat& fmt, UErrorCode& status) {
    if (U_SUCCESS(status)) {
        const DecimalFormat *decFmt = dynamic_cast<const DecimalFormat *>(&fmt);
        if (decFmt != NULL) {
            number::impl::DecimalQuantity dq;
            decFmt->formatToDecimalQuantity(obj, dq, status);
            if (U_SUCCESS(status)) {
                return rules.select(dq);
            }
        } else {
            double number = obj.getDouble(status);
            if (U_SUCCESS(status)) {
                return rules.select(number);
            }
        }
    }
    return UnicodeString();
}

}  // namespace

U_CAPI UPluralRules* U_EXPORT2
uplrules_open(const char *locale, UErrorCode *status)
{
    return uplrules_openForType(locale, UPLURAL_TYPE_CARDINAL, status);
}

U_CAPI UPluralRules* U_EXPORT2
uplrules_openForType(const char *locale, UPluralType type, UErrorCode *status)
{
    return (UPluralRules*)PluralRules::forLocale(Locale(locale), type, *status);
}

U_CAPI void U_EXPORT2
uplrules_close(UPluralRules *uplrules)
{
    delete (PluralRules*)uplrules;
}

U_CAPI int32_t U_EXPORT2
uplrules_select(const UPluralRules *uplrules,
                double number,
                UChar *keyword, int32_t capacity,
                UErrorCode *status)
{
    if (U_FAILURE(*status)) {
        return 0;
    }
    if (keyword == NULL ? capacity != 0 : capacity < 0) {
        *status = U_ILLEGAL_ARGUMENT_ERROR;
        return 0;
    }
    UnicodeString result = ((PluralRules*)uplrules)->select(number);
    return result.extract(keyword, capacity, *status);
}

U_CAPI int32_t U_EXPORT2
uplrules_selectFormatted(const UPluralRules *uplrules,
                const UFormattedNumber* number,
                UChar *keyword, int32_t capacity,
                UErrorCode *status)
{
    if (U_FAILURE(*status)) {
        return 0;
    }
    if (keyword == NULL ? capacity != 0 : capacity < 0) {
        *status = U_ILLEGAL_ARGUMENT_ERROR;
        return 0;
    }
    const number::impl::DecimalQuantity* dq =
        number::impl::validateUFormattedNumberToDecimalQuantity(number, *status);
    if (U_FAILURE(*status)) {
        return 0;
    }
    UnicodeString result = ((PluralRules*)uplrules)->select(*dq);
    return result.extract(keyword, capacity, *status);
}

U_CAPI int32_t U_EXPORT2
uplrules_selectWithFormat(const UPluralRules *uplrules,
                          double number,
                          const UNumberFormat *fmt,
                          UChar *keyword, int32_t capacity,
                          UErrorCode *status)
{
    if (U_FAILURE(*status)) {
        return 0;
    }
    const PluralRules* plrules = reinterpret_cast<const PluralRules*>(uplrules);
    const NumberFormat* nf = reinterpret_cast<const NumberFormat*>(fmt);
    if (plrules == NULL || nf == NULL || ((keyword == NULL)? capacity != 0 : capacity < 0)) {
        *status = U_ILLEGAL_ARGUMENT_ERROR;
        return 0;
    }
    Formattable obj(number);
    UnicodeString result = select(*plrules, obj, *nf, *status);
    return result.extract(keyword, capacity, *status);
}

U_CAPI UEnumeration* U_EXPORT2
uplrules_getKeywords(const UPluralRules *uplrules,
                     UErrorCode *status)
{
    if (U_FAILURE(*status)) {
        return NULL;
    }
    const PluralRules* plrules = reinterpret_cast<const PluralRules*>(uplrules);
    if (plrules == NULL) {
        *status = U_ILLEGAL_ARGUMENT_ERROR;
        return NULL;
    }
    StringEnumeration *senum = plrules->getKeywords(*status);
    if (U_FAILURE(*status)) {
        return NULL;
    }
    if (senum == NULL) {
        *status = U_MEMORY_ALLOCATION_ERROR;
        return NULL;
    }
    return uenum_openFromStringEnumeration(senum, status);
}

#endif /* #if !UCONFIG_NO_FORMATTING */
