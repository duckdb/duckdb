// © 2017 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html

#include "unicode/utypes.h"

#if !UCONFIG_NO_FORMATTING
#ifndef __NUMBER_LONGNAMES_H__
#define __NUMBER_LONGNAMES_H__

#include "unicode/uversion.h"
#include "number_utils.h"
#include "number_modifiers.h"

U_NAMESPACE_BEGIN namespace number {
namespace impl {

class LongNameHandler : public MicroPropsGenerator, public ModifierStore, public UMemory {
  public:
    static UnicodeString getUnitDisplayName(
        const Locale& loc,
        const MeasureUnit& unit,
        UNumberUnitWidth width,
        UErrorCode& status);

    static UnicodeString getUnitPattern(
        const Locale& loc,
        const MeasureUnit& unit,
        UNumberUnitWidth width,
        StandardPlural::Form pluralForm,
        UErrorCode& status);

    static LongNameHandler*
    forCurrencyLongNames(const Locale &loc, const CurrencyUnit &currency, const PluralRules *rules,
                         const MicroPropsGenerator *parent, UErrorCode &status);

    static LongNameHandler*
    forMeasureUnit(const Locale &loc, const MeasureUnit &unit, const MeasureUnit &perUnit,
                   const UNumberUnitWidth &width, const PluralRules *rules,
                   const MicroPropsGenerator *parent, UErrorCode &status);

    void
    processQuantity(DecimalQuantity &quantity, MicroProps &micros, UErrorCode &status) const U_OVERRIDE;

    const Modifier* getModifier(Signum signum, StandardPlural::Form plural) const U_OVERRIDE;

  private:
    SimpleModifier fModifiers[StandardPlural::Form::COUNT];
    const PluralRules *rules;
    const MicroPropsGenerator *parent;

    LongNameHandler(const PluralRules *rules, const MicroPropsGenerator *parent)
            : rules(rules), parent(parent) {}

    static LongNameHandler*
    forCompoundUnit(const Locale &loc, const MeasureUnit &unit, const MeasureUnit &perUnit,
                    const UNumberUnitWidth &width, const PluralRules *rules,
                    const MicroPropsGenerator *parent, UErrorCode &status);

    void simpleFormatsToModifiers(const UnicodeString *simpleFormats, Field field, UErrorCode &status);
    void multiSimpleFormatsToModifiers(const UnicodeString *leadFormats, UnicodeString trailFormat,
                                       Field field, UErrorCode &status);
};

}  // namespace impl
}  // namespace number
U_NAMESPACE_END

#endif //__NUMBER_LONGNAMES_H__

#endif /* #if !UCONFIG_NO_FORMATTING */
