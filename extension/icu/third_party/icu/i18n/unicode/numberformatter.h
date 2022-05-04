// © 2017 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html

#ifndef __NUMBERFORMATTER_H__
#define __NUMBERFORMATTER_H__

#include "unicode/utypes.h"

#if U_SHOW_CPLUSPLUS_API

#if !UCONFIG_NO_FORMATTING

#include "unicode/appendable.h"
#include "unicode/bytestream.h"
#include "unicode/currunit.h"
#include "unicode/dcfmtsym.h"
#include "unicode/fieldpos.h"
#include "unicode/formattedvalue.h"
#include "unicode/fpositer.h"
#include "unicode/measunit.h"
#include "unicode/nounit.h"
#include "unicode/parseerr.h"
#include "unicode/plurrule.h"
#include "unicode/ucurr.h"
#include "unicode/unum.h"
#include "unicode/unumberformatter.h"
#include "unicode/uobject.h"

/**
 * \file
 * \brief C++ API: Library for localized number formatting introduced in ICU 60.
 *
 * This library was introduced in ICU 60 to simplify the process of formatting localized number strings.
 * Basic usage examples:
 *
 * <pre>
 * // Most basic usage:
 * NumberFormatter::withLocale(...).format(123).toString();  // 1,234 in en-US
 *
 * // Custom notation, unit, and rounding precision:
 * NumberFormatter::with()
 *     .notation(Notation::compactShort())
 *     .unit(CurrencyUnit("EUR", status))
 *     .precision(Precision::maxDigits(2))
 *     .locale(...)
 *     .format(1234)
 *     .toString();  // €1.2K in en-US
 *
 * // Create a formatter in a singleton by value for use later:
 * static const LocalizedNumberFormatter formatter = NumberFormatter::withLocale(...)
 *     .unit(NoUnit::percent())
 *     .precision(Precision::fixedFraction(3));
 * formatter.format(5.9831).toString();  // 5.983% in en-US
 *
 * // Create a "template" in a singleton unique_ptr but without setting a locale until the call site:
 * std::unique_ptr<UnlocalizedNumberFormatter> template = NumberFormatter::with()
 *     .sign(UNumberSignDisplay::UNUM_SIGN_ALWAYS)
 *     .unit(MeasureUnit::getMeter())
 *     .unitWidth(UNumberUnitWidth::UNUM_UNIT_WIDTH_FULL_NAME)
 *     .clone();
 * template->locale(...).format(1234).toString();  // +1,234 meters in en-US
 * </pre>
 *
 * <p>
 * This API offers more features than DecimalFormat and is geared toward new users of ICU.
 *
 * <p>
 * NumberFormatter instances (i.e., LocalizedNumberFormatter and UnlocalizedNumberFormatter)
 * are immutable and thread safe. This means that invoking a configuration method has no
 * effect on the receiving instance; you must store and use the new number formatter instance it returns instead.
 *
 * <pre>
 * UnlocalizedNumberFormatter formatter = UnlocalizedNumberFormatter::with().notation(Notation::scientific());
 * formatter.precision(Precision.maxFraction(2)); // does nothing!
 * formatter.locale(Locale.getEnglish()).format(9.8765).toString(); // prints "9.8765E0", not "9.88E0"
 * </pre>
 *
 * <p>
 * This API is based on the <em>fluent</em> design pattern popularized by libraries such as Google's Guava. For
 * extensive details on the design of this API, read <a href="https://goo.gl/szi5VB">the design doc</a>.
 *
 * @author Shane Carr
 */

U_NAMESPACE_BEGIN

// Forward declarations:
class IFixedDecimal;
class FieldPositionIteratorHandler;
class FormattedStringBuilder;

namespace numparse {
namespace impl {

// Forward declarations:
class NumberParserImpl;
class MultiplierParseHandler;

}
}

namespace number {  // icu::number

// Forward declarations:
class UnlocalizedNumberFormatter;
class LocalizedNumberFormatter;
class FormattedNumber;
class Notation;
class ScientificNotation;
class Precision;
class FractionPrecision;
class CurrencyPrecision;
class IncrementPrecision;
class IntegerWidth;

namespace impl {

// can't be #ifndef U_HIDE_INTERNAL_API; referenced throughout this file in public classes
/**
 * Datatype for minimum/maximum fraction digits. Must be able to hold kMaxIntFracSig.
 *
 * @internal
 */
typedef int16_t digits_t;

// can't be #ifndef U_HIDE_INTERNAL_API; needed for struct initialization
/**
 * Use a default threshold of 3. This means that the third time .format() is called, the data structures get built
 * using the "safe" code path. The first two calls to .format() will trigger the unsafe code path.
 *
 * @internal
 */
static constexpr int32_t kInternalDefaultThreshold = 3;

// Forward declarations:
class Padder;
struct MacroProps;
struct MicroProps;
class DecimalQuantity;
class UFormattedNumberData;
class NumberFormatterImpl;
struct ParsedPatternInfo;
class ScientificModifier;
class MultiplierProducer;
class RoundingImpl;
class ScientificHandler;
class Modifier;
class AffixPatternProvider;
class NumberPropertyMapper;
struct DecimalFormatProperties;
class MultiplierFormatHandler;
class CurrencySymbols;
class GeneratorHelpers;
class DecNum;
class NumberRangeFormatterImpl;
struct RangeMacroProps;
struct UFormattedNumberImpl;

/**
 * Used for NumberRangeFormatter and implemented in numrange_fluent.cpp.
 * Declared here so it can be friended.
 *
 * @internal
 */
void touchRangeLocales(impl::RangeMacroProps& macros);

} // namespace impl

/**
 * Extra name reserved in case it is needed in the future.
 *
 * @stable ICU 63
 */
typedef Notation CompactNotation;

/**
 * Extra name reserved in case it is needed in the future.
 *
 * @stable ICU 63
 */
typedef Notation SimpleNotation;

/**
 * A class that defines the notation style to be used when formatting numbers in NumberFormatter.
 *
 * @stable ICU 60
 */
class U_I18N_API Notation : public UMemory {
  public:
    /**
     * Print the number using scientific notation (also known as scientific form, standard index form, or standard form
     * in the UK). The format for scientific notation varies by locale; for example, many Western locales display the
     * number in the form "#E0", where the number is displayed with one digit before the decimal separator, zero or more
     * digits after the decimal separator, and the corresponding power of 10 displayed after the "E".
     *
     * <p>
     * Example outputs in <em>en-US</em> when printing 8.765E4 through 8.765E-3:
     *
     * <pre>
     * 8.765E4
     * 8.765E3
     * 8.765E2
     * 8.765E1
     * 8.765E0
     * 8.765E-1
     * 8.765E-2
     * 8.765E-3
     * 0E0
     * </pre>
     *
     * @return A ScientificNotation for chaining or passing to the NumberFormatter notation() setter.
     * @stable ICU 60
     */
    static ScientificNotation scientific();

    /**
     * Print the number using engineering notation, a variant of scientific notation in which the exponent must be
     * divisible by 3.
     *
     * <p>
     * Example outputs in <em>en-US</em> when printing 8.765E4 through 8.765E-3:
     *
     * <pre>
     * 87.65E3
     * 8.765E3
     * 876.5E0
     * 87.65E0
     * 8.765E0
     * 876.5E-3
     * 87.65E-3
     * 8.765E-3
     * 0E0
     * </pre>
     *
     * @return A ScientificNotation for chaining or passing to the NumberFormatter notation() setter.
     * @stable ICU 60
     */
    static ScientificNotation engineering();

    /**
     * Print the number using short-form compact notation.
     *
     * <p>
     * <em>Compact notation</em>, defined in Unicode Technical Standard #35 Part 3 Section 2.4.1, prints numbers with
     * localized prefixes or suffixes corresponding to different powers of ten. Compact notation is similar to
     * engineering notation in how it scales numbers.
     *
     * <p>
     * Compact notation is ideal for displaying large numbers (over ~1000) to humans while at the same time minimizing
     * screen real estate.
     *
     * <p>
     * In short form, the powers of ten are abbreviated. In <em>en-US</em>, the abbreviations are "K" for thousands, "M"
     * for millions, "B" for billions, and "T" for trillions. Example outputs in <em>en-US</em> when printing 8.765E7
     * through 8.765E0:
     *
     * <pre>
     * 88M
     * 8.8M
     * 876K
     * 88K
     * 8.8K
     * 876
     * 88
     * 8.8
     * </pre>
     *
     * <p>
     * When compact notation is specified without an explicit rounding precision, numbers are rounded off to the closest
     * integer after scaling the number by the corresponding power of 10, but with a digit shown after the decimal
     * separator if there is only one digit before the decimal separator. The default compact notation rounding precision
     * is equivalent to:
     *
     * <pre>
     * Precision::integer().withMinDigits(2)
     * </pre>
     *
     * @return A CompactNotation for passing to the NumberFormatter notation() setter.
     * @stable ICU 60
     */
    static CompactNotation compactShort();

    /**
     * Print the number using long-form compact notation. For more information on compact notation, see
     * {@link #compactShort}.
     *
     * <p>
     * In long form, the powers of ten are spelled out fully. Example outputs in <em>en-US</em> when printing 8.765E7
     * through 8.765E0:
     *
     * <pre>
     * 88 million
     * 8.8 million
     * 876 thousand
     * 88 thousand
     * 8.8 thousand
     * 876
     * 88
     * 8.8
     * </pre>
     *
     * @return A CompactNotation for passing to the NumberFormatter notation() setter.
     * @stable ICU 60
     */
    static CompactNotation compactLong();

    /**
     * Print the number using simple notation without any scaling by powers of ten. This is the default behavior.
     *
     * <p>
     * Since this is the default behavior, this method needs to be called only when it is necessary to override a
     * previous setting.
     *
     * <p>
     * Example outputs in <em>en-US</em> when printing 8.765E7 through 8.765E0:
     *
     * <pre>
     * 87,650,000
     * 8,765,000
     * 876,500
     * 87,650
     * 8,765
     * 876.5
     * 87.65
     * 8.765
     * </pre>
     *
     * @return A SimpleNotation for passing to the NumberFormatter notation() setter.
     * @stable ICU 60
     */
    static SimpleNotation simple();

  private:
    enum NotationType {
        NTN_SCIENTIFIC, NTN_COMPACT, NTN_SIMPLE, NTN_ERROR
    } fType;

    union NotationUnion {
        // For NTN_SCIENTIFIC
        /** @internal */
        struct ScientificSettings {
            /** @internal */
            int8_t fEngineeringInterval;
            /** @internal */
            bool fRequireMinInt;
            /** @internal */
            number::impl::digits_t fMinExponentDigits;
            /** @internal */
            UNumberSignDisplay fExponentSignDisplay;
        } scientific;

        // For NTN_COMPACT
        UNumberCompactStyle compactStyle;

        // For NTN_ERROR
        UErrorCode errorCode;
    } fUnion;

    typedef NotationUnion::ScientificSettings ScientificSettings;

    Notation(const NotationType &type, const NotationUnion &union_) : fType(type), fUnion(union_) {}

    Notation(UErrorCode errorCode) : fType(NTN_ERROR) {
        fUnion.errorCode = errorCode;
    }

    Notation() : fType(NTN_SIMPLE), fUnion() {}

    UBool copyErrorTo(UErrorCode &status) const {
        if (fType == NTN_ERROR) {
            status = fUnion.errorCode;
            return TRUE;
        }
        return FALSE;
    }

    // To allow MacroProps to initialize empty instances:
    friend struct number::impl::MacroProps;
    friend class ScientificNotation;

    // To allow implementation to access internal types:
    friend class number::impl::NumberFormatterImpl;
    friend class number::impl::ScientificModifier;
    friend class number::impl::ScientificHandler;

    // To allow access to the skeleton generation code:
    friend class number::impl::GeneratorHelpers;
};

/**
 * A class that defines the scientific notation style to be used when formatting numbers in NumberFormatter.
 *
 * <p>
 * To create a ScientificNotation, use one of the factory methods in {@link Notation}.
 *
 * @stable ICU 60
 */
class U_I18N_API ScientificNotation : public Notation {
  public:
    /**
     * Sets the minimum number of digits to show in the exponent of scientific notation, padding with zeros if
     * necessary. Useful for fixed-width display.
     *
     * <p>
     * For example, with minExponentDigits=2, the number 123 will be printed as "1.23E02" in <em>en-US</em> instead of
     * the default "1.23E2".
     *
     * @param minExponentDigits
     *            The minimum number of digits to show in the exponent.
     * @return A ScientificNotation, for chaining.
     * @stable ICU 60
     */
    ScientificNotation withMinExponentDigits(int32_t minExponentDigits) const;

    /**
     * Sets whether to show the sign on positive and negative exponents in scientific notation. The default is AUTO,
     * showing the minus sign but not the plus sign.
     *
     * <p>
     * For example, with exponentSignDisplay=ALWAYS, the number 123 will be printed as "1.23E+2" in <em>en-US</em>
     * instead of the default "1.23E2".
     *
     * @param exponentSignDisplay
     *            The strategy for displaying the sign in the exponent.
     * @return A ScientificNotation, for chaining.
     * @stable ICU 60
     */
    ScientificNotation withExponentSignDisplay(UNumberSignDisplay exponentSignDisplay) const;

  private:
    // Inherit constructor
    using Notation::Notation;

    // Raw constructor for NumberPropertyMapper
    ScientificNotation(int8_t fEngineeringInterval, bool fRequireMinInt, number::impl::digits_t fMinExponentDigits,
                       UNumberSignDisplay fExponentSignDisplay);

    friend class Notation;

    // So that NumberPropertyMapper can create instances
    friend class number::impl::NumberPropertyMapper;
};

/**
 * Extra name reserved in case it is needed in the future.
 *
 * @stable ICU 63
 */
typedef Precision SignificantDigitsPrecision;

/**
 * A class that defines the rounding precision to be used when formatting numbers in NumberFormatter.
 *
 * <p>
 * To create a Precision, use one of the factory methods.
 *
 * @stable ICU 60
 */
class U_I18N_API Precision : public UMemory {

  public:
    /**
     * Show all available digits to full precision.
     *
     * <p>
     * <strong>NOTE:</strong> When formatting a <em>double</em>, this method, along with {@link #minFraction} and
     * {@link #minSignificantDigits}, will trigger complex algorithm similar to <em>Dragon4</em> to determine the
     * low-order digits and the number of digits to display based on the value of the double.
     * If the number of fraction places or significant digits can be bounded, consider using {@link #maxFraction}
     * or {@link #maxSignificantDigits} instead to maximize performance.
     * For more information, read the following blog post.
     *
     * <p>
     * http://www.serpentine.com/blog/2011/06/29/here-be-dragons-advances-in-problems-you-didnt-even-know-you-had/
     *
     * @return A Precision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 60
     */
    static Precision unlimited();

    /**
     * Show numbers rounded if necessary to the nearest integer.
     *
     * @return A FractionPrecision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 60
     */
    static FractionPrecision integer();

    /**
     * Show numbers rounded if necessary to a certain number of fraction places (numerals after the decimal separator).
     * Additionally, pad with zeros to ensure that this number of places are always shown.
     *
     * <p>
     * Example output with minMaxFractionPlaces = 3:
     *
     * <p>
     * 87,650.000<br>
     * 8,765.000<br>
     * 876.500<br>
     * 87.650<br>
     * 8.765<br>
     * 0.876<br>
     * 0.088<br>
     * 0.009<br>
     * 0.000 (zero)
     *
     * <p>
     * This method is equivalent to {@link #minMaxFraction} with both arguments equal.
     *
     * @param minMaxFractionPlaces
     *            The minimum and maximum number of numerals to display after the decimal separator (rounding if too
     *            long or padding with zeros if too short).
     * @return A FractionPrecision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 60
     */
    static FractionPrecision fixedFraction(int32_t minMaxFractionPlaces);

    /**
     * Always show at least a certain number of fraction places after the decimal separator, padding with zeros if
     * necessary. Do not perform rounding (display numbers to their full precision).
     *
     * <p>
     * <strong>NOTE:</strong> If you are formatting <em>doubles</em>, see the performance note in {@link #unlimited}.
     *
     * @param minFractionPlaces
     *            The minimum number of numerals to display after the decimal separator (padding with zeros if
     *            necessary).
     * @return A FractionPrecision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 60
     */
    static FractionPrecision minFraction(int32_t minFractionPlaces);

    /**
     * Show numbers rounded if necessary to a certain number of fraction places (numerals after the decimal separator).
     * Unlike the other fraction rounding strategies, this strategy does <em>not</em> pad zeros to the end of the
     * number.
     *
     * @param maxFractionPlaces
     *            The maximum number of numerals to display after the decimal mark (rounding if necessary).
     * @return A FractionPrecision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 60
     */
    static FractionPrecision maxFraction(int32_t maxFractionPlaces);

    /**
     * Show numbers rounded if necessary to a certain number of fraction places (numerals after the decimal separator);
     * in addition, always show at least a certain number of places after the decimal separator, padding with zeros if
     * necessary.
     *
     * @param minFractionPlaces
     *            The minimum number of numerals to display after the decimal separator (padding with zeros if
     *            necessary).
     * @param maxFractionPlaces
     *            The maximum number of numerals to display after the decimal separator (rounding if necessary).
     * @return A FractionPrecision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 60
     */
    static FractionPrecision minMaxFraction(int32_t minFractionPlaces, int32_t maxFractionPlaces);

    /**
     * Show numbers rounded if necessary to a certain number of significant digits or significant figures. Additionally,
     * pad with zeros to ensure that this number of significant digits/figures are always shown.
     *
     * <p>
     * This method is equivalent to {@link #minMaxSignificantDigits} with both arguments equal.
     *
     * @param minMaxSignificantDigits
     *            The minimum and maximum number of significant digits to display (rounding if too long or padding with
     *            zeros if too short).
     * @return A precision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 62
     */
    static SignificantDigitsPrecision fixedSignificantDigits(int32_t minMaxSignificantDigits);

    /**
     * Always show at least a certain number of significant digits/figures, padding with zeros if necessary. Do not
     * perform rounding (display numbers to their full precision).
     *
     * <p>
     * <strong>NOTE:</strong> If you are formatting <em>doubles</em>, see the performance note in {@link #unlimited}.
     *
     * @param minSignificantDigits
     *            The minimum number of significant digits to display (padding with zeros if too short).
     * @return A precision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 62
     */
    static SignificantDigitsPrecision minSignificantDigits(int32_t minSignificantDigits);

    /**
     * Show numbers rounded if necessary to a certain number of significant digits/figures.
     *
     * @param maxSignificantDigits
     *            The maximum number of significant digits to display (rounding if too long).
     * @return A precision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 62
     */
    static SignificantDigitsPrecision maxSignificantDigits(int32_t maxSignificantDigits);

    /**
     * Show numbers rounded if necessary to a certain number of significant digits/figures; in addition, always show at
     * least a certain number of significant digits, padding with zeros if necessary.
     *
     * @param minSignificantDigits
     *            The minimum number of significant digits to display (padding with zeros if necessary).
     * @param maxSignificantDigits
     *            The maximum number of significant digits to display (rounding if necessary).
     * @return A precision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 62
     */
    static SignificantDigitsPrecision minMaxSignificantDigits(int32_t minSignificantDigits,
                                                              int32_t maxSignificantDigits);

    /**
     * Show numbers rounded if necessary to the closest multiple of a certain rounding increment. For example, if the
     * rounding increment is 0.5, then round 1.2 to 1 and round 1.3 to 1.5.
     *
     * <p>
     * In order to ensure that numbers are padded to the appropriate number of fraction places, call
     * withMinFraction() on the return value of this method.
     * For example, to round to the nearest 0.5 and always display 2 numerals after the
     * decimal separator (to display 1.2 as "1.00" and 1.3 as "1.50"), you can run:
     *
     * <pre>
     * Precision::increment(0.5).withMinFraction(2)
     * </pre>
     *
     * @param roundingIncrement
     *            The increment to which to round numbers.
     * @return A precision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 60
     */
    static IncrementPrecision increment(double roundingIncrement);

    /**
     * Show numbers rounded and padded according to the rules for the currency unit. The most common
     * rounding precision settings for currencies include <code>Precision::fixedFraction(2)</code>,
     * <code>Precision::integer()</code>, and <code>Precision::increment(0.05)</code> for cash transactions
     * ("nickel rounding").
     *
     * <p>
     * The exact rounding details will be resolved at runtime based on the currency unit specified in the
     * NumberFormatter chain. To round according to the rules for one currency while displaying the symbol for another
     * currency, the withCurrency() method can be called on the return value of this method.
     *
     * @param currencyUsage
     *            Either STANDARD (for digital transactions) or CASH (for transactions where the rounding increment may
     *            be limited by the available denominations of cash or coins).
     * @return A CurrencyPrecision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 60
     */
    static CurrencyPrecision currency(UCurrencyUsage currencyUsage);

  private:
    enum PrecisionType {
        RND_BOGUS,
        RND_NONE,
        RND_FRACTION,
        RND_SIGNIFICANT,
        RND_FRACTION_SIGNIFICANT,

        // Used for strange increments like 3.14.
        RND_INCREMENT,

        // Used for increments with 1 as the only digit. This is different than fraction
        // rounding because it supports having additional trailing zeros. For example, this
        // class is used to round with the increment 0.010.
        RND_INCREMENT_ONE,

        // Used for increments with 5 as the only digit (nickel rounding).
        RND_INCREMENT_FIVE,

        RND_CURRENCY,
        RND_ERROR
    } fType;

    union PrecisionUnion {
        /** @internal */
        struct FractionSignificantSettings {
            // For RND_FRACTION, RND_SIGNIFICANT, and RND_FRACTION_SIGNIFICANT
            /** @internal */
            number::impl::digits_t fMinFrac;
            /** @internal */
            number::impl::digits_t fMaxFrac;
            /** @internal */
            number::impl::digits_t fMinSig;
            /** @internal */
            number::impl::digits_t fMaxSig;
        } fracSig;
        /** @internal */
        struct IncrementSettings {
            // For RND_INCREMENT, RND_INCREMENT_ONE, and RND_INCREMENT_FIVE
            /** @internal */
            double fIncrement;
            /** @internal */
            number::impl::digits_t fMinFrac;
            /** @internal */
            number::impl::digits_t fMaxFrac;
        } increment;
        UCurrencyUsage currencyUsage; // For RND_CURRENCY
        UErrorCode errorCode; // For RND_ERROR
    } fUnion;

    typedef PrecisionUnion::FractionSignificantSettings FractionSignificantSettings;
    typedef PrecisionUnion::IncrementSettings IncrementSettings;

    /** The Precision encapsulates the RoundingMode when used within the implementation. */
    UNumberFormatRoundingMode fRoundingMode;

    Precision(const PrecisionType& type, const PrecisionUnion& union_,
              UNumberFormatRoundingMode roundingMode)
            : fType(type), fUnion(union_), fRoundingMode(roundingMode) {}

    Precision(UErrorCode errorCode) : fType(RND_ERROR) {
        fUnion.errorCode = errorCode;
    }

    Precision() : fType(RND_BOGUS) {}

    bool isBogus() const {
        return fType == RND_BOGUS;
    }

    UBool copyErrorTo(UErrorCode &status) const {
        if (fType == RND_ERROR) {
            status = fUnion.errorCode;
            return TRUE;
        }
        return FALSE;
    }

    // On the parent type so that this method can be called internally on Precision instances.
    Precision withCurrency(const CurrencyUnit &currency, UErrorCode &status) const;

    static FractionPrecision constructFraction(int32_t minFrac, int32_t maxFrac);

    static Precision constructSignificant(int32_t minSig, int32_t maxSig);

    static Precision
    constructFractionSignificant(const FractionPrecision &base, int32_t minSig, int32_t maxSig);

    static IncrementPrecision constructIncrement(double increment, int32_t minFrac);

    static CurrencyPrecision constructCurrency(UCurrencyUsage usage);

    static Precision constructPassThrough();

    // To allow MacroProps/MicroProps to initialize bogus instances:
    friend struct number::impl::MacroProps;
    friend struct number::impl::MicroProps;

    // To allow NumberFormatterImpl to access isBogus() and other internal methods:
    friend class number::impl::NumberFormatterImpl;

    // To allow NumberPropertyMapper to create instances from DecimalFormatProperties:
    friend class number::impl::NumberPropertyMapper;

    // To allow access to the main implementation class:
    friend class number::impl::RoundingImpl;

    // To allow child classes to call private methods:
    friend class FractionPrecision;
    friend class CurrencyPrecision;
    friend class IncrementPrecision;

    // To allow access to the skeleton generation code:
    friend class number::impl::GeneratorHelpers;
};

/**
 * A class that defines a rounding precision based on a number of fraction places and optionally significant digits to be
 * used when formatting numbers in NumberFormatter.
 *
 * <p>
 * To create a FractionPrecision, use one of the factory methods on Precision.
 *
 * @stable ICU 60
 */
class U_I18N_API FractionPrecision : public Precision {
  public:
    /**
     * Ensure that no less than this number of significant digits are retained when rounding according to fraction
     * rules.
     *
     * <p>
     * For example, with integer rounding, the number 3.141 becomes "3". However, with minimum figures set to 2, 3.141
     * becomes "3.1" instead.
     *
     * <p>
     * This setting does not affect the number of trailing zeros. For example, 3.01 would print as "3", not "3.0".
     *
     * @param minSignificantDigits
     *            The number of significant figures to guarantee.
     * @return A precision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 60
     */
    Precision withMinDigits(int32_t minSignificantDigits) const;

    /**
     * Ensure that no more than this number of significant digits are retained when rounding according to fraction
     * rules.
     *
     * <p>
     * For example, with integer rounding, the number 123.4 becomes "123". However, with maximum figures set to 2, 123.4
     * becomes "120" instead.
     *
     * <p>
     * This setting does not affect the number of trailing zeros. For example, with fixed fraction of 2, 123.4 would
     * become "120.00".
     *
     * @param maxSignificantDigits
     *            Round the number to no more than this number of significant figures.
     * @return A precision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 60
     */
    Precision withMaxDigits(int32_t maxSignificantDigits) const;

  private:
    // Inherit constructor
    using Precision::Precision;

    // To allow parent class to call this class's constructor:
    friend class Precision;
};

/**
 * A class that defines a rounding precision parameterized by a currency to be used when formatting numbers in
 * NumberFormatter.
 *
 * <p>
 * To create a CurrencyPrecision, use one of the factory methods on Precision.
 *
 * @stable ICU 60
 */
class U_I18N_API CurrencyPrecision : public Precision {
  public:
    /**
      * Associates a currency with this rounding precision.
      *
      * <p>
      * <strong>Calling this method is <em>not required</em></strong>, because the currency specified in unit()
      * is automatically applied to currency rounding precisions. However,
      * this method enables you to override that automatic association.
      *
      * <p>
      * This method also enables numbers to be formatted using currency rounding rules without explicitly using a
      * currency format.
      *
      * @param currency
      *            The currency to associate with this rounding precision.
      * @return A precision for chaining or passing to the NumberFormatter precision() setter.
      * @stable ICU 60
      */
    Precision withCurrency(const CurrencyUnit &currency) const;

  private:
    // Inherit constructor
    using Precision::Precision;

    // To allow parent class to call this class's constructor:
    friend class Precision;
};

/**
 * A class that defines a rounding precision parameterized by a rounding increment to be used when formatting numbers in
 * NumberFormatter.
 *
 * <p>
 * To create an IncrementPrecision, use one of the factory methods on Precision.
 *
 * @stable ICU 60
 */
class U_I18N_API IncrementPrecision : public Precision {
  public:
    /**
     * Specifies the minimum number of fraction digits to render after the decimal separator, padding with zeros if
     * necessary.  By default, no trailing zeros are added.
     *
     * <p>
     * For example, if the rounding increment is 0.5 and minFrac is 2, then the resulting strings include "0.00",
     * "0.50", "1.00", and "1.50".
     *
     * <p>
     * Note: In ICU4J, this functionality is accomplished via the scale of the BigDecimal rounding increment.
     *
     * @param minFrac The minimum number of digits after the decimal separator.
     * @return A precision for chaining or passing to the NumberFormatter precision() setter.
     * @stable ICU 60
     */
    Precision withMinFraction(int32_t minFrac) const;

  private:
    // Inherit constructor
    using Precision::Precision;

    // To allow parent class to call this class's constructor:
    friend class Precision;
};

/**
 * A class that defines the strategy for padding and truncating integers before the decimal separator.
 *
 * <p>
 * To create an IntegerWidth, use one of the factory methods.
 *
 * @stable ICU 60
 * @see NumberFormatter
 */
class U_I18N_API IntegerWidth : public UMemory {
  public:
    /**
     * Pad numbers at the beginning with zeros to guarantee a certain number of numerals before the decimal separator.
     *
     * <p>
     * For example, with minInt=3, the number 55 will get printed as "055".
     *
     * @param minInt
     *            The minimum number of places before the decimal separator.
     * @return An IntegerWidth for chaining or passing to the NumberFormatter integerWidth() setter.
     * @stable ICU 60
     */
    static IntegerWidth zeroFillTo(int32_t minInt);

    /**
     * Truncate numbers exceeding a certain number of numerals before the decimal separator.
     *
     * For example, with maxInt=3, the number 1234 will get printed as "234".
     *
     * @param maxInt
     *            The maximum number of places before the decimal separator. maxInt == -1 means no
     *            truncation.
     * @return An IntegerWidth for passing to the NumberFormatter integerWidth() setter.
     * @stable ICU 60
     */
    IntegerWidth truncateAt(int32_t maxInt);

  private:
    union {
        struct {
            number::impl::digits_t fMinInt;
            number::impl::digits_t fMaxInt;
            bool fFormatFailIfMoreThanMaxDigits;
        } minMaxInt;
        UErrorCode errorCode;
    } fUnion;
    bool fHasError = false;

    IntegerWidth(impl::digits_t minInt, number::impl::digits_t maxInt, bool formatFailIfMoreThanMaxDigits);

    IntegerWidth(UErrorCode errorCode) { // NOLINT
        fUnion.errorCode = errorCode;
        fHasError = true;
    }

    IntegerWidth() { // NOLINT
        fUnion.minMaxInt.fMinInt = -1;
    }

    /** Returns the default instance. */
    static IntegerWidth standard() {
        return IntegerWidth::zeroFillTo(1);
    }

    bool isBogus() const {
        return !fHasError && fUnion.minMaxInt.fMinInt == -1;
    }

    UBool copyErrorTo(UErrorCode &status) const {
        if (fHasError) {
            status = fUnion.errorCode;
            return TRUE;
        }
        return FALSE;
    }

    void apply(number::impl::DecimalQuantity &quantity, UErrorCode &status) const;

    bool operator==(const IntegerWidth& other) const;

    // To allow MacroProps/MicroProps to initialize empty instances:
    friend struct number::impl::MacroProps;
    friend struct number::impl::MicroProps;

    // To allow NumberFormatterImpl to access isBogus() and perform other operations:
    friend class number::impl::NumberFormatterImpl;

    // So that NumberPropertyMapper can create instances
    friend class number::impl::NumberPropertyMapper;

    // To allow access to the skeleton generation code:
    friend class number::impl::GeneratorHelpers;
};

/**
 * A class that defines a quantity by which a number should be multiplied when formatting.
 *
 * <p>
 * To create a Scale, use one of the factory methods.
 *
 * @stable ICU 62
 */
class U_I18N_API Scale : public UMemory {
  public:
    /**
     * Do not change the value of numbers when formatting or parsing.
     *
     * @return A Scale to prevent any multiplication.
     * @stable ICU 62
     */
    static Scale none();

    /**
     * Multiply numbers by a power of ten before formatting. Useful for combining with a percent unit:
     *
     * <pre>
     * NumberFormatter::with().unit(NoUnit::percent()).multiplier(Scale::powerOfTen(2))
     * </pre>
     *
     * @return A Scale for passing to the setter in NumberFormatter.
     * @stable ICU 62
     */
    static Scale powerOfTen(int32_t power);

    /**
     * Multiply numbers by an arbitrary value before formatting. Useful for unit conversions.
     *
     * This method takes a string in a decimal number format with syntax
     * as defined in the Decimal Arithmetic Specification, available at
     * http://speleotrove.com/decimal
     *
     * Also see the version of this method that takes a double.
     *
     * @return A Scale for passing to the setter in NumberFormatter.
     * @stable ICU 62
     */
    static Scale byDecimal(StringPiece multiplicand);

    /**
     * Multiply numbers by an arbitrary value before formatting. Useful for unit conversions.
     *
     * This method takes a double; also see the version of this method that takes an exact decimal.
     *
     * @return A Scale for passing to the setter in NumberFormatter.
     * @stable ICU 62
     */
    static Scale byDouble(double multiplicand);

    /**
     * Multiply a number by both a power of ten and by an arbitrary double value.
     *
     * @return A Scale for passing to the setter in NumberFormatter.
     * @stable ICU 62
     */
    static Scale byDoubleAndPowerOfTen(double multiplicand, int32_t power);

    // We need a custom destructor for the DecNum, which means we need to declare
    // the copy/move constructor/assignment quartet.

    /** @stable ICU 62 */
    Scale(const Scale& other);

    /** @stable ICU 62 */
    Scale& operator=(const Scale& other);

    /** @stable ICU 62 */
    Scale(Scale&& src) U_NOEXCEPT;

    /** @stable ICU 62 */
    Scale& operator=(Scale&& src) U_NOEXCEPT;

    /** @stable ICU 62 */
    ~Scale();

#ifndef U_HIDE_INTERNAL_API
    /** @internal */
    Scale(int32_t magnitude, number::impl::DecNum* arbitraryToAdopt);
#endif  /* U_HIDE_INTERNAL_API */

  private:
    int32_t fMagnitude;
    number::impl::DecNum* fArbitrary;
    UErrorCode fError;

    Scale(UErrorCode error) : fMagnitude(0), fArbitrary(nullptr), fError(error) {}

    Scale() : fMagnitude(0), fArbitrary(nullptr), fError(U_ZERO_ERROR) {}

    bool isValid() const {
        return fMagnitude != 0 || fArbitrary != nullptr;
    }

    UBool copyErrorTo(UErrorCode &status) const {
        if (fError != U_ZERO_ERROR) {
            status = fError;
            return TRUE;
        }
        return FALSE;
    }

    void applyTo(impl::DecimalQuantity& quantity) const;

    void applyReciprocalTo(impl::DecimalQuantity& quantity) const;

    // To allow MacroProps/MicroProps to initialize empty instances:
    friend struct number::impl::MacroProps;
    friend struct number::impl::MicroProps;

    // To allow NumberFormatterImpl to access isBogus() and perform other operations:
    friend class number::impl::NumberFormatterImpl;

    // To allow the helper class MultiplierFormatHandler access to private fields:
    friend class number::impl::MultiplierFormatHandler;

    // To allow access to the skeleton generation code:
    friend class number::impl::GeneratorHelpers;

    // To allow access to parsing code:
    friend class ::icu::numparse::impl::NumberParserImpl;
    friend class ::icu::numparse::impl::MultiplierParseHandler;
};

namespace impl {

// Do not enclose entire SymbolsWrapper with #ifndef U_HIDE_INTERNAL_API, needed for a protected field
/** @internal */
class U_I18N_API SymbolsWrapper : public UMemory {
  public:
    /** @internal */
    SymbolsWrapper() : fType(SYMPTR_NONE), fPtr{nullptr} {}

    /** @internal */
    SymbolsWrapper(const SymbolsWrapper &other);

    /** @internal */
    SymbolsWrapper &operator=(const SymbolsWrapper &other);

    /** @internal */
    SymbolsWrapper(SymbolsWrapper&& src) U_NOEXCEPT;

    /** @internal */
    SymbolsWrapper &operator=(SymbolsWrapper&& src) U_NOEXCEPT;

    /** @internal */
    ~SymbolsWrapper();

#ifndef U_HIDE_INTERNAL_API

    /**
     * The provided object is copied, but we do not adopt it.
     * @internal
     */
    void setTo(const DecimalFormatSymbols &dfs);

    /**
     * Adopt the provided object.
     * @internal
     */
    void setTo(const NumberingSystem *ns);

    /**
     * Whether the object is currently holding a DecimalFormatSymbols.
     * @internal
     */
    bool isDecimalFormatSymbols() const;

    /**
     * Whether the object is currently holding a NumberingSystem.
     * @internal
     */
    bool isNumberingSystem() const;

    /**
     * Get the DecimalFormatSymbols pointer. No ownership change.
     * @internal
     */
    const DecimalFormatSymbols *getDecimalFormatSymbols() const;

    /**
     * Get the NumberingSystem pointer. No ownership change.
     * @internal
     */
    const NumberingSystem *getNumberingSystem() const;

#endif  // U_HIDE_INTERNAL_API

    /** @internal */
    UBool copyErrorTo(UErrorCode &status) const {
        if (fType == SYMPTR_DFS && fPtr.dfs == nullptr) {
            status = U_MEMORY_ALLOCATION_ERROR;
            return TRUE;
        } else if (fType == SYMPTR_NS && fPtr.ns == nullptr) {
            status = U_MEMORY_ALLOCATION_ERROR;
            return TRUE;
        }
        return FALSE;
    }

  private:
    enum SymbolsPointerType {
        SYMPTR_NONE, SYMPTR_DFS, SYMPTR_NS
    } fType;

    union {
        const DecimalFormatSymbols *dfs;
        const NumberingSystem *ns;
    } fPtr;

    void doCopyFrom(const SymbolsWrapper &other);

    void doMoveFrom(SymbolsWrapper&& src);

    void doCleanup();
};

// Do not enclose entire Grouper with #ifndef U_HIDE_INTERNAL_API, needed for a protected field
/** @internal */
class U_I18N_API Grouper : public UMemory {
  public:
#ifndef U_HIDE_INTERNAL_API
    /** @internal */
    static Grouper forStrategy(UNumberGroupingStrategy grouping);

    /**
     * Resolve the values in Properties to a Grouper object.
     * @internal
     */
    static Grouper forProperties(const DecimalFormatProperties& properties);

    // Future: static Grouper forProperties(DecimalFormatProperties& properties);

    /** @internal */
    Grouper(int16_t grouping1, int16_t grouping2, int16_t minGrouping, UNumberGroupingStrategy strategy)
            : fGrouping1(grouping1),
              fGrouping2(grouping2),
              fMinGrouping(minGrouping),
              fStrategy(strategy) {}
#endif  // U_HIDE_INTERNAL_API

    /** @internal */
    int16_t getPrimary() const;

    /** @internal */
    int16_t getSecondary() const;

  private:
    /**
     * The grouping sizes, with the following special values:
     * <ul>
     * <li>-1 = no grouping
     * <li>-2 = needs locale data
     * <li>-4 = fall back to Western grouping if not in locale
     * </ul>
     */
    int16_t fGrouping1;
    int16_t fGrouping2;

    /**
     * The minimum grouping size, with the following special values:
     * <ul>
     * <li>-2 = needs locale data
     * <li>-3 = no less than 2
     * </ul>
     */
    int16_t fMinGrouping;

    /**
     * The UNumberGroupingStrategy that was used to create this Grouper, or UNUM_GROUPING_COUNT if this
     * was not created from a UNumberGroupingStrategy.
     */
    UNumberGroupingStrategy fStrategy;

    Grouper() : fGrouping1(-3) {}

    bool isBogus() const {
        return fGrouping1 == -3;
    }

    /** NON-CONST: mutates the current instance. */
    void setLocaleData(const number::impl::ParsedPatternInfo &patternInfo, const Locale& locale);

    bool groupAtPosition(int32_t position, const number::impl::DecimalQuantity &value) const;

    // To allow MacroProps/MicroProps to initialize empty instances:
    friend struct MacroProps;
    friend struct MicroProps;

    // To allow NumberFormatterImpl to access isBogus() and perform other operations:
    friend class NumberFormatterImpl;

    // To allow NumberParserImpl to perform setLocaleData():
    friend class ::icu::numparse::impl::NumberParserImpl;

    // To allow access to the skeleton generation code:
    friend class number::impl::GeneratorHelpers;
};

// Do not enclose entire Padder with #ifndef U_HIDE_INTERNAL_API, needed for a protected field
/** @internal */
class U_I18N_API Padder : public UMemory {
  public:
#ifndef U_HIDE_INTERNAL_API
    /** @internal */
    static Padder none();

    /** @internal */
    static Padder codePoints(UChar32 cp, int32_t targetWidth, UNumberFormatPadPosition position);
#endif  // U_HIDE_INTERNAL_API

    /** @internal */
    static Padder forProperties(const DecimalFormatProperties& properties);

  private:
    UChar32 fWidth;  // -3 = error; -2 = bogus; -1 = no padding
    union {
        struct {
            int32_t fCp;
            UNumberFormatPadPosition fPosition;
        } padding;
        UErrorCode errorCode;
    } fUnion;

    Padder(UChar32 cp, int32_t width, UNumberFormatPadPosition position);

    Padder(int32_t width);

    Padder(UErrorCode errorCode) : fWidth(-3) { // NOLINT
        fUnion.errorCode = errorCode;
    }

    Padder() : fWidth(-2) {} // NOLINT

    bool isBogus() const {
        return fWidth == -2;
    }

    UBool copyErrorTo(UErrorCode &status) const {
        if (fWidth == -3) {
            status = fUnion.errorCode;
            return TRUE;
        }
        return FALSE;
    }

    bool isValid() const {
        return fWidth > 0;
    }

    int32_t padAndApply(const number::impl::Modifier &mod1, const number::impl::Modifier &mod2,
                        FormattedStringBuilder &string, int32_t leftIndex, int32_t rightIndex,
                        UErrorCode &status) const;

    // To allow MacroProps/MicroProps to initialize empty instances:
    friend struct MacroProps;
    friend struct MicroProps;

    // To allow NumberFormatterImpl to access isBogus() and perform other operations:
    friend class number::impl::NumberFormatterImpl;

    // To allow access to the skeleton generation code:
    friend class number::impl::GeneratorHelpers;
};

// Do not enclose entire MacroProps with #ifndef U_HIDE_INTERNAL_API, needed for a protected field
/** @internal */
struct U_I18N_API MacroProps : public UMemory {
    /** @internal */
    Notation notation;

    /** @internal */
    MeasureUnit unit; // = NoUnit::base();

    /** @internal */
    MeasureUnit perUnit; // = NoUnit::base();

    /** @internal */
    Precision precision;  // = Precision();  (bogus)

    /** @internal */
    UNumberFormatRoundingMode roundingMode = UNUM_ROUND_HALFEVEN;

    /** @internal */
    Grouper grouper;  // = Grouper();  (bogus)

    /** @internal */
    Padder padder;    // = Padder();   (bogus)

    /** @internal */
    IntegerWidth integerWidth; // = IntegerWidth(); (bogus)

    /** @internal */
    SymbolsWrapper symbols;

    // UNUM_XYZ_COUNT denotes null (bogus) values.

    /** @internal */
    UNumberUnitWidth unitWidth = UNUM_UNIT_WIDTH_COUNT;

    /** @internal */
    UNumberSignDisplay sign = UNUM_SIGN_COUNT;

    /** @internal */
    UNumberDecimalSeparatorDisplay decimal = UNUM_DECIMAL_SEPARATOR_COUNT;

    /** @internal */
    Scale scale;  // = Scale();  (benign value)

    /** @internal */
    const AffixPatternProvider* affixProvider = nullptr;  // no ownership

    /** @internal */
    const PluralRules* rules = nullptr;  // no ownership

    /** @internal */
    const CurrencySymbols* currencySymbols = nullptr;  // no ownership

    /** @internal */
    int32_t threshold = kInternalDefaultThreshold;

    /** @internal */
    Locale locale;

    // NOTE: Uses default copy and move constructors.

    /**
     * Check all members for errors.
     * @internal
     */
    bool copyErrorTo(UErrorCode &status) const {
        return notation.copyErrorTo(status) || precision.copyErrorTo(status) ||
               padder.copyErrorTo(status) || integerWidth.copyErrorTo(status) ||
               symbols.copyErrorTo(status) || scale.copyErrorTo(status);
    }
};

} // namespace impl

/**
 * An abstract base class for specifying settings related to number formatting. This class is implemented by
 * {@link UnlocalizedNumberFormatter} and {@link LocalizedNumberFormatter}. This class is not intended for
 * public subclassing.
 */
template<typename Derived>
class U_I18N_API NumberFormatterSettings {
  public:
    /**
     * Specifies the notation style (simple, scientific, or compact) for rendering numbers.
     *
     * <ul>
     * <li>Simple notation: "12,300"
     * <li>Scientific notation: "1.23E4"
     * <li>Compact notation: "12K"
     * </ul>
     *
     * <p>
     * All notation styles will be properly localized with locale data, and all notation styles are compatible with
     * units, rounding precisions, and other number formatter settings.
     *
     * <p>
     * Pass this method the return value of a {@link Notation} factory method. For example:
     *
     * <pre>
     * NumberFormatter::with().notation(Notation::compactShort())
     * </pre>
     *
     * The default is to use simple notation.
     *
     * @param notation
     *            The notation strategy to use.
     * @return The fluent chain.
     * @see Notation
     * @stable ICU 60
     */
    Derived notation(const Notation &notation) const &;

    /**
     * Overload of notation() for use on an rvalue reference.
     *
     * @param notation
     *            The notation strategy to use.
     * @return The fluent chain.
     * @see #notation
     * @stable ICU 62
     */
    Derived notation(const Notation &notation) &&;

    /**
     * Specifies the unit (unit of measure, currency, or percent) to associate with rendered numbers.
     *
     * <ul>
     * <li>Unit of measure: "12.3 meters"
     * <li>Currency: "$12.30"
     * <li>Percent: "12.3%"
     * </ul>
     *
     * All units will be properly localized with locale data, and all units are compatible with notation styles,
     * rounding precisions, and other number formatter settings.
     *
     * Pass this method any instance of {@link MeasureUnit}. For units of measure:
     *
     * <pre>
     * NumberFormatter::with().unit(MeasureUnit::getMeter())
     * </pre>
     *
     * Currency:
     *
     * <pre>
     * NumberFormatter::with().unit(CurrencyUnit(u"USD", status))
     * </pre>
     *
     * Percent:
     *
     * <pre>
     * NumberFormatter::with().unit(NoUnit.percent())
     * </pre>
     *
     * See {@link #perUnit} for information on how to format strings like "5 meters per second".
     *
     * The default is to render without units (equivalent to NoUnit.base()).
     *
     * @param unit
     *            The unit to render.
     * @return The fluent chain.
     * @see MeasureUnit
     * @see Currency
     * @see NoUnit
     * @see #perUnit
     * @stable ICU 60
     */
    Derived unit(const icu::MeasureUnit &unit) const &;

    /**
     * Overload of unit() for use on an rvalue reference.
     *
     * @param unit
     *            The unit to render.
     * @return The fluent chain.
     * @see #unit
     * @stable ICU 62
     */
    Derived unit(const icu::MeasureUnit &unit) &&;

    /**
     * Like unit(), but takes ownership of a pointer.  Convenient for use with the MeasureFormat factory
     * methods that return pointers that need ownership.
     *
     * Note: consider using the MeasureFormat factory methods that return by value.
     *
     * @param unit
     *            The unit to render.
     * @return The fluent chain.
     * @see #unit
     * @see MeasureUnit
     * @stable ICU 60
     */
    Derived adoptUnit(icu::MeasureUnit *unit) const &;

    /**
     * Overload of adoptUnit() for use on an rvalue reference.
     *
     * @param unit
     *            The unit to render.
     * @return The fluent chain.
     * @see #adoptUnit
     * @stable ICU 62
     */
    Derived adoptUnit(icu::MeasureUnit *unit) &&;

    /**
     * Sets a unit to be used in the denominator. For example, to format "3 m/s", pass METER to the unit and SECOND to
     * the perUnit.
     *
     * Pass this method any instance of {@link MeasureUnit}. Example:
     *
     * <pre>
     * NumberFormatter::with()
     *      .unit(MeasureUnit::getMeter())
     *      .perUnit(MeasureUnit::getSecond())
     * </pre>
     *
     * The default is not to display any unit in the denominator.
     *
     * If a per-unit is specified without a primary unit via {@link #unit}, the behavior is undefined.
     *
     * @param perUnit
     *            The unit to render in the denominator.
     * @return The fluent chain
     * @see #unit
     * @stable ICU 61
     */
    Derived perUnit(const icu::MeasureUnit &perUnit) const &;

    /**
     * Overload of perUnit() for use on an rvalue reference.
     *
     * @param perUnit
     *            The unit to render in the denominator.
     * @return The fluent chain.
     * @see #perUnit
     * @stable ICU 62
     */
    Derived perUnit(const icu::MeasureUnit &perUnit) &&;

    /**
     * Like perUnit(), but takes ownership of a pointer.  Convenient for use with the MeasureFormat factory
     * methods that return pointers that need ownership.
     *
     * Note: consider using the MeasureFormat factory methods that return by value.
     *
     * @param perUnit
     *            The unit to render in the denominator.
     * @return The fluent chain.
     * @see #perUnit
     * @see MeasureUnit
     * @stable ICU 61
     */
    Derived adoptPerUnit(icu::MeasureUnit *perUnit) const &;

    /**
     * Overload of adoptPerUnit() for use on an rvalue reference.
     *
     * @param perUnit
     *            The unit to render in the denominator.
     * @return The fluent chain.
     * @see #adoptPerUnit
     * @stable ICU 62
     */
    Derived adoptPerUnit(icu::MeasureUnit *perUnit) &&;

    /**
     * Specifies the rounding precision to use when formatting numbers.
     *
     * <ul>
     * <li>Round to 3 decimal places: "3.142"
     * <li>Round to 3 significant figures: "3.14"
     * <li>Round to the closest nickel: "3.15"
     * <li>Do not perform rounding: "3.1415926..."
     * </ul>
     *
     * <p>
     * Pass this method the return value of one of the factory methods on {@link Precision}. For example:
     *
     * <pre>
     * NumberFormatter::with().precision(Precision::fixedFraction(2))
     * </pre>
     *
     * <p>
     * In most cases, the default rounding strategy is to round to 6 fraction places; i.e.,
     * <code>Precision.maxFraction(6)</code>. The exceptions are if compact notation is being used, then the compact
     * notation rounding strategy is used (see {@link Notation#compactShort} for details), or if the unit is a currency,
     * then standard currency rounding is used, which varies from currency to currency (see {@link Precision#currency} for
     * details).
     *
     * @param precision
     *            The rounding precision to use.
     * @return The fluent chain.
     * @see Precision
     * @stable ICU 62
     */
    Derived precision(const Precision& precision) const &;

    /**
     * Overload of precision() for use on an rvalue reference.
     *
     * @param precision
     *            The rounding precision to use.
     * @return The fluent chain.
     * @see #precision
     * @stable ICU 62
     */
    Derived precision(const Precision& precision) &&;

    /**
     * Specifies how to determine the direction to round a number when it has more digits than fit in the
     * desired precision.  When formatting 1.235:
     *
     * <ul>
     * <li>Ceiling rounding mode with integer precision: "2"
     * <li>Half-down rounding mode with 2 fixed fraction digits: "1.23"
     * <li>Half-up rounding mode with 2 fixed fraction digits: "1.24"
     * </ul>
     *
     * The default is HALF_EVEN. For more information on rounding mode, see the ICU userguide here:
     *
     * http://userguide.icu-project.org/formatparse/numbers/rounding-modes
     *
     * @param roundingMode The rounding mode to use.
     * @return The fluent chain.
     * @stable ICU 62
     */
    Derived roundingMode(UNumberFormatRoundingMode roundingMode) const &;

    /**
     * Overload of roundingMode() for use on an rvalue reference.
     *
     * @param roundingMode The rounding mode to use.
     * @return The fluent chain.
     * @see #roundingMode
     * @stable ICU 62
     */
    Derived roundingMode(UNumberFormatRoundingMode roundingMode) &&;

    /**
     * Specifies the grouping strategy to use when formatting numbers.
     *
     * <ul>
     * <li>Default grouping: "12,300" and "1,230"
     * <li>Grouping with at least 2 digits: "12,300" and "1230"
     * <li>No grouping: "12300" and "1230"
     * </ul>
     *
     * <p>
     * The exact grouping widths will be chosen based on the locale.
     *
     * <p>
     * Pass this method an element from the {@link UNumberGroupingStrategy} enum. For example:
     *
     * <pre>
     * NumberFormatter::with().grouping(UNUM_GROUPING_MIN2)
     * </pre>
     *
     * The default is to perform grouping according to locale data; most locales, but not all locales,
     * enable it by default.
     *
     * @param strategy
     *            The grouping strategy to use.
     * @return The fluent chain.
     * @stable ICU 61
     */
    Derived grouping(UNumberGroupingStrategy strategy) const &;

    /**
     * Overload of grouping() for use on an rvalue reference.
     *
     * @param strategy
     *            The grouping strategy to use.
     * @return The fluent chain.
     * @see #grouping
     * @stable ICU 62
     */
    Derived grouping(UNumberGroupingStrategy strategy) &&;

    /**
     * Specifies the minimum and maximum number of digits to render before the decimal mark.
     *
     * <ul>
     * <li>Zero minimum integer digits: ".08"
     * <li>One minimum integer digit: "0.08"
     * <li>Two minimum integer digits: "00.08"
     * </ul>
     *
     * <p>
     * Pass this method the return value of {@link IntegerWidth#zeroFillTo}. For example:
     *
     * <pre>
     * NumberFormatter::with().integerWidth(IntegerWidth::zeroFillTo(2))
     * </pre>
     *
     * The default is to have one minimum integer digit.
     *
     * @param style
     *            The integer width to use.
     * @return The fluent chain.
     * @see IntegerWidth
     * @stable ICU 60
     */
    Derived integerWidth(const IntegerWidth &style) const &;

    /**
     * Overload of integerWidth() for use on an rvalue reference.
     *
     * @param style
     *            The integer width to use.
     * @return The fluent chain.
     * @see #integerWidth
     * @stable ICU 62
     */
    Derived integerWidth(const IntegerWidth &style) &&;

    /**
     * Specifies the symbols (decimal separator, grouping separator, percent sign, numerals, etc.) to use when rendering
     * numbers.
     *
     * <ul>
     * <li><em>en_US</em> symbols: "12,345.67"
     * <li><em>fr_FR</em> symbols: "12&nbsp;345,67"
     * <li><em>de_CH</em> symbols: "12’345.67"
     * <li><em>my_MY</em> symbols: "၁၂,၃၄၅.၆၇"
     * </ul>
     *
     * <p>
     * Pass this method an instance of {@link DecimalFormatSymbols}. For example:
     *
     * <pre>
     * NumberFormatter::with().symbols(DecimalFormatSymbols(Locale("de_CH"), status))
     * </pre>
     *
     * <p>
     * <strong>Note:</strong> DecimalFormatSymbols automatically chooses the best numbering system based on the locale.
     * In the examples above, the first three are using the Latin numbering system, and the fourth is using the Myanmar
     * numbering system.
     *
     * <p>
     * <strong>Note:</strong> The instance of DecimalFormatSymbols will be copied: changes made to the symbols object
     * after passing it into the fluent chain will not be seen.
     *
     * <p>
     * <strong>Note:</strong> Calling this method will override any previously specified DecimalFormatSymbols
     * or NumberingSystem.
     *
     * <p>
     * The default is to choose the symbols based on the locale specified in the fluent chain.
     *
     * @param symbols
     *            The DecimalFormatSymbols to use.
     * @return The fluent chain.
     * @see DecimalFormatSymbols
     * @stable ICU 60
     */
    Derived symbols(const DecimalFormatSymbols &symbols) const &;

    /**
     * Overload of symbols() for use on an rvalue reference.
     *
     * @param symbols
     *            The DecimalFormatSymbols to use.
     * @return The fluent chain.
     * @see #symbols
     * @stable ICU 62
     */
    Derived symbols(const DecimalFormatSymbols &symbols) &&;

    /**
     * Specifies that the given numbering system should be used when fetching symbols.
     *
     * <ul>
     * <li>Latin numbering system: "12,345"
     * <li>Myanmar numbering system: "၁၂,၃၄၅"
     * <li>Math Sans Bold numbering system: "𝟭𝟮,𝟯𝟰𝟱"
     * </ul>
     *
     * <p>
     * Pass this method an instance of {@link NumberingSystem}. For example, to force the locale to always use the Latin
     * alphabet numbering system (ASCII digits):
     *
     * <pre>
     * NumberFormatter::with().adoptSymbols(NumberingSystem::createInstanceByName("latn", status))
     * </pre>
     *
     * <p>
     * <strong>Note:</strong> Calling this method will override any previously specified DecimalFormatSymbols
     * or NumberingSystem.
     *
     * <p>
     * The default is to choose the best numbering system for the locale.
     *
     * <p>
     * This method takes ownership of a pointer in order to work nicely with the NumberingSystem factory methods.
     *
     * @param symbols
     *            The NumberingSystem to use.
     * @return The fluent chain.
     * @see NumberingSystem
     * @stable ICU 60
     */
    Derived adoptSymbols(NumberingSystem *symbols) const &;

    /**
     * Overload of adoptSymbols() for use on an rvalue reference.
     *
     * @param symbols
     *            The NumberingSystem to use.
     * @return The fluent chain.
     * @see #adoptSymbols
     * @stable ICU 62
     */
    Derived adoptSymbols(NumberingSystem *symbols) &&;

    /**
     * Sets the width of the unit (measure unit or currency).  Most common values:
     *
     * <ul>
     * <li>Short: "$12.00", "12 m"
     * <li>ISO Code: "USD 12.00"
     * <li>Full name: "12.00 US dollars", "12 meters"
     * </ul>
     *
     * <p>
     * Pass an element from the {@link UNumberUnitWidth} enum to this setter. For example:
     *
     * <pre>
     * NumberFormatter::with().unitWidth(UNumberUnitWidth::UNUM_UNIT_WIDTH_FULL_NAME)
     * </pre>
     *
     * <p>
     * The default is the SHORT width.
     *
     * @param width
     *            The width to use when rendering numbers.
     * @return The fluent chain
     * @see UNumberUnitWidth
     * @stable ICU 60
     */
    Derived unitWidth(UNumberUnitWidth width) const &;

    /**
     * Overload of unitWidth() for use on an rvalue reference.
     *
     * @param width
     *            The width to use when rendering numbers.
     * @return The fluent chain.
     * @see #unitWidth
     * @stable ICU 62
     */
    Derived unitWidth(UNumberUnitWidth width) &&;

    /**
     * Sets the plus/minus sign display strategy. Most common values:
     *
     * <ul>
     * <li>Auto: "123", "-123"
     * <li>Always: "+123", "-123"
     * <li>Accounting: "$123", "($123)"
     * </ul>
     *
     * <p>
     * Pass an element from the {@link UNumberSignDisplay} enum to this setter. For example:
     *
     * <pre>
     * NumberFormatter::with().sign(UNumberSignDisplay::UNUM_SIGN_ALWAYS)
     * </pre>
     *
     * <p>
     * The default is AUTO sign display.
     *
     * @param style
     *            The sign display strategy to use when rendering numbers.
     * @return The fluent chain
     * @see UNumberSignDisplay
     * @stable ICU 60
     */
    Derived sign(UNumberSignDisplay style) const &;

    /**
     * Overload of sign() for use on an rvalue reference.
     *
     * @param style
     *            The sign display strategy to use when rendering numbers.
     * @return The fluent chain.
     * @see #sign
     * @stable ICU 62
     */
    Derived sign(UNumberSignDisplay style) &&;

    /**
     * Sets the decimal separator display strategy. This affects integer numbers with no fraction part. Most common
     * values:
     *
     * <ul>
     * <li>Auto: "1"
     * <li>Always: "1."
     * </ul>
     *
     * <p>
     * Pass an element from the {@link UNumberDecimalSeparatorDisplay} enum to this setter. For example:
     *
     * <pre>
     * NumberFormatter::with().decimal(UNumberDecimalSeparatorDisplay::UNUM_DECIMAL_SEPARATOR_ALWAYS)
     * </pre>
     *
     * <p>
     * The default is AUTO decimal separator display.
     *
     * @param style
     *            The decimal separator display strategy to use when rendering numbers.
     * @return The fluent chain
     * @see UNumberDecimalSeparatorDisplay
     * @stable ICU 60
     */
    Derived decimal(UNumberDecimalSeparatorDisplay style) const &;

    /**
     * Overload of decimal() for use on an rvalue reference.
     *
     * @param style
     *            The decimal separator display strategy to use when rendering numbers.
     * @return The fluent chain.
     * @see #decimal
     * @stable ICU 62
     */
    Derived decimal(UNumberDecimalSeparatorDisplay style) &&;

    /**
     * Sets a scale (multiplier) to be used to scale the number by an arbitrary amount before formatting.
     * Most common values:
     *
     * <ul>
     * <li>Multiply by 100: useful for percentages.
     * <li>Multiply by an arbitrary value: useful for unit conversions.
     * </ul>
     *
     * <p>
     * Pass an element from a {@link Scale} factory method to this setter. For example:
     *
     * <pre>
     * NumberFormatter::with().scale(Scale::powerOfTen(2))
     * </pre>
     *
     * <p>
     * The default is to not apply any multiplier.
     *
     * @param scale
     *            The scale to apply when rendering numbers.
     * @return The fluent chain
     * @stable ICU 62
     */
    Derived scale(const Scale &scale) const &;

    /**
     * Overload of scale() for use on an rvalue reference.
     *
     * @param scale
     *            The scale to apply when rendering numbers.
     * @return The fluent chain.
     * @see #scale
     * @stable ICU 62
     */
    Derived scale(const Scale &scale) &&;

#ifndef U_HIDE_INTERNAL_API

    /**
     * Set the padding strategy. May be added in the future; see #13338.
     *
     * @internal ICU 60: This API is ICU internal only.
     */
    Derived padding(const number::impl::Padder &padder) const &;

    /** @internal */
    Derived padding(const number::impl::Padder &padder) &&;

    /**
     * Internal fluent setter to support a custom regulation threshold. A threshold of 1 causes the data structures to
     * be built right away. A threshold of 0 prevents the data structures from being built.
     *
     * @internal ICU 60: This API is ICU internal only.
     */
    Derived threshold(int32_t threshold) const &;

    /** @internal */
    Derived threshold(int32_t threshold) &&;

    /**
     * Internal fluent setter to overwrite the entire macros object.
     *
     * @internal ICU 60: This API is ICU internal only.
     */
    Derived macros(const number::impl::MacroProps& macros) const &;

    /** @internal */
    Derived macros(const number::impl::MacroProps& macros) &&;

    /** @internal */
    Derived macros(impl::MacroProps&& macros) const &;

    /** @internal */
    Derived macros(impl::MacroProps&& macros) &&;

#endif  /* U_HIDE_INTERNAL_API */

    /**
     * Creates a skeleton string representation of this number formatter. A skeleton string is a
     * locale-agnostic serialized form of a number formatter.
     *
     * Not all options are capable of being represented in the skeleton string; for example, a
     * DecimalFormatSymbols object. If any such option is encountered, the error code is set to
     * U_UNSUPPORTED_ERROR.
     *
     * The returned skeleton is in normalized form, such that two number formatters with equivalent
     * behavior should produce the same skeleton.
     *
     * @return A number skeleton string with behavior corresponding to this number formatter.
     * @stable ICU 62
     */
    UnicodeString toSkeleton(UErrorCode& status) const;

#ifndef U_HIDE_DRAFT_API
    /**
     * Returns the current (Un)LocalizedNumberFormatter as a LocalPointer
     * wrapping a heap-allocated copy of the current object.
     *
     * This is equivalent to new-ing the move constructor with a value object
     * as the argument.
     *
     * @return A wrapped (Un)LocalizedNumberFormatter pointer, or a wrapped
     *         nullptr on failure.
     * @draft ICU 64
     */
    LocalPointer<Derived> clone() const &;

    /**
     * Overload of clone for use on an rvalue reference.
     *
     * @return A wrapped (Un)LocalizedNumberFormatter pointer, or a wrapped
     *         nullptr on failure.
     * @draft ICU 64
     */
    LocalPointer<Derived> clone() &&;
#endif  /* U_HIDE_DRAFT_API */

    /**
     * Sets the UErrorCode if an error occurred in the fluent chain.
     * Preserves older error codes in the outErrorCode.
     * @return TRUE if U_FAILURE(outErrorCode)
     * @stable ICU 60
     */
    UBool copyErrorTo(UErrorCode &outErrorCode) const {
        if (U_FAILURE(outErrorCode)) {
            // Do not overwrite the older error code
            return TRUE;
        }
        fMacros.copyErrorTo(outErrorCode);
        return U_FAILURE(outErrorCode);
    }

    // NOTE: Uses default copy and move constructors.

  private:
    number::impl::MacroProps fMacros;

    // Don't construct me directly!  Use (Un)LocalizedNumberFormatter.
    NumberFormatterSettings() = default;

    friend class LocalizedNumberFormatter;
    friend class UnlocalizedNumberFormatter;

    // Give NumberRangeFormatter access to the MacroProps
    friend void number::impl::touchRangeLocales(impl::RangeMacroProps& macros);
    friend class number::impl::NumberRangeFormatterImpl;
};

/**
 * A NumberFormatter that does not yet have a locale. In order to format numbers, a locale must be specified.
 *
 * Instances of this class are immutable and thread-safe.
 *
 * @see NumberFormatter
 * @stable ICU 60
 */
class U_I18N_API UnlocalizedNumberFormatter
        : public NumberFormatterSettings<UnlocalizedNumberFormatter>, public UMemory {

  public:
    /**
     * Associate the given locale with the number formatter. The locale is used for picking the appropriate symbols,
     * formats, and other data for number display.
     *
     * @param locale
     *            The locale to use when loading data for number formatting.
     * @return The fluent chain.
     * @stable ICU 60
     */
    LocalizedNumberFormatter locale(const icu::Locale &locale) const &;

    /**
     * Overload of locale() for use on an rvalue reference.
     *
     * @param locale
     *            The locale to use when loading data for number formatting.
     * @return The fluent chain.
     * @see #locale
     * @stable ICU 62
     */
    LocalizedNumberFormatter locale(const icu::Locale &locale) &&;

    /**
     * Default constructor: puts the formatter into a valid but undefined state.
     *
     * @stable ICU 62
     */
    UnlocalizedNumberFormatter() = default;

    /**
     * Returns a copy of this UnlocalizedNumberFormatter.
     * @stable ICU 60
     */
    UnlocalizedNumberFormatter(const UnlocalizedNumberFormatter &other);

    /**
     * Move constructor:
     * The source UnlocalizedNumberFormatter will be left in a valid but undefined state.
     * @stable ICU 62
     */
    UnlocalizedNumberFormatter(UnlocalizedNumberFormatter&& src) U_NOEXCEPT;

    /**
     * Copy assignment operator.
     * @stable ICU 62
     */
    UnlocalizedNumberFormatter& operator=(const UnlocalizedNumberFormatter& other);

    /**
     * Move assignment operator:
     * The source UnlocalizedNumberFormatter will be left in a valid but undefined state.
     * @stable ICU 62
     */
    UnlocalizedNumberFormatter& operator=(UnlocalizedNumberFormatter&& src) U_NOEXCEPT;

  private:
    explicit UnlocalizedNumberFormatter(const NumberFormatterSettings<UnlocalizedNumberFormatter>& other);

    explicit UnlocalizedNumberFormatter(
            NumberFormatterSettings<UnlocalizedNumberFormatter>&& src) U_NOEXCEPT;

    // To give the fluent setters access to this class's constructor:
    friend class NumberFormatterSettings<UnlocalizedNumberFormatter>;

    // To give NumberFormatter::with() access to this class's constructor:
    friend class NumberFormatter;
};

/**
 * A NumberFormatter that has a locale associated with it; this means .format() methods are available.
 *
 * Instances of this class are immutable and thread-safe.
 *
 * @see NumberFormatter
 * @stable ICU 60
 */
class U_I18N_API LocalizedNumberFormatter
        : public NumberFormatterSettings<LocalizedNumberFormatter>, public UMemory {
  public:
    /**
     * Format the given integer number to a string using the settings specified in the NumberFormatter fluent
     * setting chain.
     *
     * @param value
     *            The number to format.
     * @param status
     *            Set to an ErrorCode if one occurred in the setter chain or during formatting.
     * @return A FormattedNumber object; call .toString() to get the string.
     * @stable ICU 60
     */
    FormattedNumber formatInt(int64_t value, UErrorCode &status) const;

    /**
     * Format the given float or double to a string using the settings specified in the NumberFormatter fluent setting
     * chain.
     *
     * @param value
     *            The number to format.
     * @param status
     *            Set to an ErrorCode if one occurred in the setter chain or during formatting.
     * @return A FormattedNumber object; call .toString() to get the string.
     * @stable ICU 60
     */
    FormattedNumber formatDouble(double value, UErrorCode &status) const;

    /**
     * Format the given decimal number to a string using the settings
     * specified in the NumberFormatter fluent setting chain.
     * The syntax of the unformatted number is a "numeric string"
     * as defined in the Decimal Arithmetic Specification, available at
     * http://speleotrove.com/decimal
     *
     * @param value
     *            The number to format.
     * @param status
     *            Set to an ErrorCode if one occurred in the setter chain or during formatting.
     * @return A FormattedNumber object; call .toString() to get the string.
     * @stable ICU 60
     */
    FormattedNumber formatDecimal(StringPiece value, UErrorCode& status) const;

#ifndef U_HIDE_INTERNAL_API

    /** Internal method.
     * @internal
     */
    FormattedNumber formatDecimalQuantity(const number::impl::DecimalQuantity& dq, UErrorCode& status) const;

    /** Internal method for DecimalFormat compatibility.
     * @internal
     */
    void getAffixImpl(bool isPrefix, bool isNegative, UnicodeString& result, UErrorCode& status) const;

    /**
     * Internal method for testing.
     * @internal
     */
    const number::impl::NumberFormatterImpl* getCompiled() const;

    /**
     * Internal method for testing.
     * @internal
     */
    int32_t getCallCount() const;

#endif  /* U_HIDE_INTERNAL_API */

    /**
     * Creates a representation of this LocalizedNumberFormat as an icu::Format, enabling the use
     * of this number formatter with APIs that need an object of that type, such as MessageFormat.
     *
     * This API is not intended to be used other than for enabling API compatibility. The formatDouble,
     * formatInt, and formatDecimal methods should normally be used when formatting numbers, not the Format
     * object returned by this method.
     *
     * The caller owns the returned object and must delete it when finished.
     *
     * @return A Format wrapping this LocalizedNumberFormatter.
     * @stable ICU 62
     */
    Format* toFormat(UErrorCode& status) const;

    /**
     * Default constructor: puts the formatter into a valid but undefined state.
     *
     * @stable ICU 62
     */
    LocalizedNumberFormatter() = default;

    /**
     * Returns a copy of this LocalizedNumberFormatter.
     * @stable ICU 60
     */
    LocalizedNumberFormatter(const LocalizedNumberFormatter &other);

    /**
     * Move constructor:
     * The source LocalizedNumberFormatter will be left in a valid but undefined state.
     * @stable ICU 62
     */
    LocalizedNumberFormatter(LocalizedNumberFormatter&& src) U_NOEXCEPT;

    /**
     * Copy assignment operator.
     * @stable ICU 62
     */
    LocalizedNumberFormatter& operator=(const LocalizedNumberFormatter& other);

    /**
     * Move assignment operator:
     * The source LocalizedNumberFormatter will be left in a valid but undefined state.
     * @stable ICU 62
     */
    LocalizedNumberFormatter& operator=(LocalizedNumberFormatter&& src) U_NOEXCEPT;

#ifndef U_HIDE_INTERNAL_API

    /**
     * This is the core entrypoint to the number formatting pipeline. It performs self-regulation: a static code path
     * for the first few calls, and compiling a more efficient data structure if called repeatedly.
     *
     * <p>
     * This function is very hot, being called in every call to the number formatting pipeline.
     *
     * @param results
     *            The results object. This method will mutate it to save the results.
     * @param status
     * @internal
     */
    void formatImpl(impl::UFormattedNumberData *results, UErrorCode &status) const;

#endif  /* U_HIDE_INTERNAL_API */

    /**
     * Destruct this LocalizedNumberFormatter, cleaning up any memory it might own.
     * @stable ICU 60
     */
    ~LocalizedNumberFormatter();

  private:
    // Note: fCompiled can't be a LocalPointer because number::impl::NumberFormatterImpl is defined in an internal
    // header, and LocalPointer needs the full class definition in order to delete the instance.
    const number::impl::NumberFormatterImpl* fCompiled {nullptr};
    char fUnsafeCallCount[8] {};  // internally cast to u_atomic_int32_t

    explicit LocalizedNumberFormatter(const NumberFormatterSettings<LocalizedNumberFormatter>& other);

    explicit LocalizedNumberFormatter(NumberFormatterSettings<LocalizedNumberFormatter>&& src) U_NOEXCEPT;

    LocalizedNumberFormatter(const number::impl::MacroProps &macros, const Locale &locale);

    LocalizedNumberFormatter(impl::MacroProps &&macros, const Locale &locale);

    void clear();

    void lnfMoveHelper(LocalizedNumberFormatter&& src);

    /**
     * @return true if the compiled formatter is available.
     */
    bool computeCompiled(UErrorCode& status) const;

    // To give the fluent setters access to this class's constructor:
    friend class NumberFormatterSettings<UnlocalizedNumberFormatter>;
    friend class NumberFormatterSettings<LocalizedNumberFormatter>;

    // To give UnlocalizedNumberFormatter::locale() access to this class's constructor:
    friend class UnlocalizedNumberFormatter;
};

/**
 * The result of a number formatting operation. This class allows the result to be exported in several data types,
 * including a UnicodeString and a FieldPositionIterator.
 *
 * Instances of this class are immutable and thread-safe.
 *
 * @stable ICU 60
 */
class U_I18N_API FormattedNumber : public UMemory, public FormattedValue {
  public:

    // Default constructor cannot have #ifndef U_HIDE_DRAFT_API
#ifndef U_FORCE_HIDE_DRAFT_API
    /**
     * Default constructor; makes an empty FormattedNumber.
     * @draft ICU 64
     */
    FormattedNumber()
        : fData(nullptr), fErrorCode(U_INVALID_STATE_ERROR) {}
#endif  // U_FORCE_HIDE_DRAFT_API

    /**
     * Move constructor: Leaves the source FormattedNumber in an undefined state.
     * @stable ICU 62
     */
    FormattedNumber(FormattedNumber&& src) U_NOEXCEPT;

    /**
     * Destruct an instance of FormattedNumber.
     * @stable ICU 60
     */
    virtual ~FormattedNumber() U_OVERRIDE;

    /** Copying not supported; use move constructor instead. */
    FormattedNumber(const FormattedNumber&) = delete;

    /** Copying not supported; use move assignment instead. */
    FormattedNumber& operator=(const FormattedNumber&) = delete;

    /**
     * Move assignment: Leaves the source FormattedNumber in an undefined state.
     * @stable ICU 62
     */
    FormattedNumber& operator=(FormattedNumber&& src) U_NOEXCEPT;

    // Copybrief: this method is older than the parent method
    /**
     * @copybrief FormattedValue::toString()
     *
     * For more information, see FormattedValue::toString()
     *
     * @stable ICU 62
     */
    UnicodeString toString(UErrorCode& status) const U_OVERRIDE;

    // Copydoc: this method is new in ICU 64
    /** @copydoc FormattedValue::toTempString() */
    UnicodeString toTempString(UErrorCode& status) const U_OVERRIDE;

    // Copybrief: this method is older than the parent method
    /**
     * @copybrief FormattedValue::appendTo()
     *
     * For more information, see FormattedValue::appendTo()
     *
     * @stable ICU 62
     */
    Appendable &appendTo(Appendable& appendable, UErrorCode& status) const U_OVERRIDE;

    // Copydoc: this method is new in ICU 64
    /** @copydoc FormattedValue::nextPosition() */
    UBool nextPosition(ConstrainedFieldPosition& cfpos, UErrorCode& status) const U_OVERRIDE;

#ifndef U_HIDE_DRAFT_API
    /**
     * Determines the start (inclusive) and end (exclusive) indices of the next occurrence of the given
     * <em>field</em> in the output string. This allows you to determine the locations of, for example,
     * the integer part, fraction part, or symbols.
     *
     * This is a simpler but less powerful alternative to {@link #nextPosition}.
     *
     * If a field occurs just once, calling this method will find that occurrence and return it. If a
     * field occurs multiple times, this method may be called repeatedly with the following pattern:
     *
     * <pre>
     * FieldPosition fpos(UNUM_GROUPING_SEPARATOR_FIELD);
     * while (formattedNumber.nextFieldPosition(fpos, status)) {
     *   // do something with fpos.
     * }
     * </pre>
     *
     * This method is useful if you know which field to query. If you want all available field position
     * information, use {@link #nextPosition} or {@link #getAllFieldPositions}.
     *
     * @param fieldPosition
     *            Input+output variable. On input, the "field" property determines which field to look
     *            up, and the "beginIndex" and "endIndex" properties determine where to begin the search.
     *            On output, the "beginIndex" is set to the beginning of the first occurrence of the
     *            field with either begin or end indices after the input indices; "endIndex" is set to
     *            the end of that occurrence of the field (exclusive index). If a field position is not
     *            found, the method returns FALSE and the FieldPosition may or may not be changed.
     * @param status
     *            Set if an error occurs while populating the FieldPosition.
     * @return TRUE if a new occurrence of the field was found; FALSE otherwise.
     * @draft ICU 62
     * @see UNumberFormatFields
     */
    UBool nextFieldPosition(FieldPosition& fieldPosition, UErrorCode& status) const;

    /**
     * Export the formatted number to a FieldPositionIterator. This allows you to determine which characters in
     * the output string correspond to which <em>fields</em>, such as the integer part, fraction part, and sign.
     *
     * This is an alternative to the more powerful #nextPosition() API.
     *
     * If information on only one field is needed, use #nextPosition() or #nextFieldPosition() instead.
     *
     * @param iterator
     *            The FieldPositionIterator to populate with all of the fields present in the formatted number.
     * @param status
     *            Set if an error occurs while populating the FieldPositionIterator.
     * @draft ICU 62
     * @see UNumberFormatFields
     */
    void getAllFieldPositions(FieldPositionIterator &iterator, UErrorCode &status) const;
#endif  /* U_HIDE_DRAFT_API */

#ifndef U_HIDE_DRAFT_API
    /**
     * Export the formatted number as a "numeric string" conforming to the
     * syntax defined in the Decimal Arithmetic Specification, available at
     * http://speleotrove.com/decimal
     *
     * This endpoint is useful for obtaining the exact number being printed
     * after scaling and rounding have been applied by the number formatter.
     *
     * Example call site:
     *
     *     auto decimalNumber = fn.toDecimalNumber<std::string>(status);
     *
     * @tparam StringClass A string class compatible with StringByteSink;
     *         for example, std::string.
     * @param status Set if an error occurs.
     * @return A StringClass containing the numeric string.
     * @draft ICU 65
     */
    template<typename StringClass>
    inline StringClass toDecimalNumber(UErrorCode& status) const;
#endif // U_HIDE_DRAFT_API

#ifndef U_HIDE_INTERNAL_API

    /**
     *  Gets the raw DecimalQuantity for plural rule selection.
     *  @internal
     */
    void getDecimalQuantity(number::impl::DecimalQuantity& output, UErrorCode& status) const;

    /**
     * Populates the mutable builder type FieldPositionIteratorHandler.
     * @internal
     */
    void getAllFieldPositionsImpl(FieldPositionIteratorHandler& fpih, UErrorCode& status) const;

#endif  /* U_HIDE_INTERNAL_API */

  private:
    // Can't use LocalPointer because UFormattedNumberData is forward-declared
    const number::impl::UFormattedNumberData *fData;

    // Error code for the terminal methods
    UErrorCode fErrorCode;

    /**
     * Internal constructor from data type. Adopts the data pointer.
     * @internal
     */
    explicit FormattedNumber(impl::UFormattedNumberData *results)
        : fData(results), fErrorCode(U_ZERO_ERROR) {}

    explicit FormattedNumber(UErrorCode errorCode)
        : fData(nullptr), fErrorCode(errorCode) {}

    // TODO(ICU-20775): Propose this as API.
    void toDecimalNumber(ByteSink& sink, UErrorCode& status) const;

    // To give LocalizedNumberFormatter format methods access to this class's constructor:
    friend class LocalizedNumberFormatter;

    // To give C API access to internals
    friend struct number::impl::UFormattedNumberImpl;
};

#ifndef U_HIDE_DRAFT_API
// Note: This is draft ICU 65
template<typename StringClass>
StringClass FormattedNumber::toDecimalNumber(UErrorCode& status) const {
    StringClass result;
    StringByteSink<StringClass> sink(&result);
    toDecimalNumber(sink, status);
    return result;
}
#endif // U_HIDE_DRAFT_API

/**
 * See the main description in numberformatter.h for documentation and examples.
 *
 * @stable ICU 60
 */
class U_I18N_API NumberFormatter final {
  public:
    /**
     * Call this method at the beginning of a NumberFormatter fluent chain in which the locale is not currently known at
     * the call site.
     *
     * @return An {@link UnlocalizedNumberFormatter}, to be used for chaining.
     * @stable ICU 60
     */
    static UnlocalizedNumberFormatter with();

    /**
     * Call this method at the beginning of a NumberFormatter fluent chain in which the locale is known at the call
     * site.
     *
     * @param locale
     *            The locale from which to load formats and symbols for number formatting.
     * @return A {@link LocalizedNumberFormatter}, to be used for chaining.
     * @stable ICU 60
     */
    static LocalizedNumberFormatter withLocale(const Locale &locale);

    /**
     * Call this method at the beginning of a NumberFormatter fluent chain to create an instance based
     * on a given number skeleton string.
     *
     * It is possible for an error to occur while parsing. See the overload of this method if you are
     * interested in the location of a possible parse error.
     *
     * @param skeleton
     *            The skeleton string off of which to base this NumberFormatter.
     * @param status
     *            Set to U_NUMBER_SKELETON_SYNTAX_ERROR if the skeleton was invalid.
     * @return An UnlocalizedNumberFormatter, to be used for chaining.
     * @stable ICU 62
     */
    static UnlocalizedNumberFormatter forSkeleton(const UnicodeString& skeleton, UErrorCode& status);

#ifndef U_HIDE_DRAFT_API
    /**
     * Call this method at the beginning of a NumberFormatter fluent chain to create an instance based
     * on a given number skeleton string.
     *
     * If an error occurs while parsing the skeleton string, the offset into the skeleton string at
     * which the error occurred will be saved into the UParseError, if provided.
     *
     * @param skeleton
     *            The skeleton string off of which to base this NumberFormatter.
     * @param perror
     *            A parse error struct populated if an error occurs when parsing.
 *                If no error occurs, perror.offset will be set to -1.
     * @param status
     *            Set to U_NUMBER_SKELETON_SYNTAX_ERROR if the skeleton was invalid.
     * @return An UnlocalizedNumberFormatter, to be used for chaining.
     * @draft ICU 64
     */
    static UnlocalizedNumberFormatter forSkeleton(const UnicodeString& skeleton,
                                                  UParseError& perror, UErrorCode& status);
#endif

    /**
     * Use factory methods instead of the constructor to create a NumberFormatter.
     */
    NumberFormatter() = delete;
};

}  // namespace number
U_NAMESPACE_END

#endif /* #if !UCONFIG_NO_FORMATTING */

#endif /* U_SHOW_CPLUSPLUS_API */

#endif // __NUMBERFORMATTER_H__

