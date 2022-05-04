// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
 *******************************************************************************
 * Copyright (C) 2008-2015, Google, International Business Machines Corporation
 * and others. All Rights Reserved.
 *******************************************************************************
 */

#include "unicode/tmutfmt.h"

#if !UCONFIG_NO_FORMATTING

#include "unicode/decimfmt.h"
#include "unicode/localpointer.h"
#include "plurrule_impl.h"
#include "uvector.h"
#include "charstr.h"
#include "cmemory.h"
#include "cstring.h"
#include "hash.h"
#include "uresimp.h"
#include "ureslocs.h"
#include "unicode/msgfmt.h"
#include "uassert.h"

#define tmutfmt_LEFT_CURLY_BRACKET  ((UChar)0x007B)
#define tmutfmt_RIGHT_CURLY_BRACKET ((UChar)0x007D)
#define tmutfmt_SPACE             ((UChar)0x0020)
#define tmutfmt_DIGIT_ZERO        ((UChar)0x0030)
#define tmutfmt_LOW_S             ((UChar)0x0073)
#define tmutfmt_LOW_M             ((UChar)0x006D)
#define tmutfmt_LOW_I             ((UChar)0x0069)
#define tmutfmt_LOW_N             ((UChar)0x006E)
#define tmutfmt_LOW_H             ((UChar)0x0068)
#define tmutfmt_LOW_W             ((UChar)0x0077)
#define tmutfmt_LOW_D             ((UChar)0x0064)
#define tmutfmt_LOW_Y             ((UChar)0x0079)
#define tmutfmt_LOW_Z             ((UChar)0x007A)
#define tmutfmt_LOW_E             ((UChar)0x0065)
#define tmutfmt_LOW_R             ((UChar)0x0072)
#define tmutfmt_LOW_O             ((UChar)0x006F)
#define tmutfmt_LOW_N             ((UChar)0x006E)
#define tmutfmt_LOW_T             ((UChar)0x0074)


//TODO: define in compile time
//#define TMUTFMT_DEBUG 1

#ifdef TMUTFMT_DEBUG
#include <iostream>
#endif

U_NAMESPACE_BEGIN



UOBJECT_DEFINE_RTTI_IMPLEMENTATION(TimeUnitFormat)

static const char gUnitsTag[] = "units";
static const char gShortUnitsTag[] = "unitsShort";
static const char gTimeUnitYear[] = "year";
static const char gTimeUnitMonth[] = "month";
static const char gTimeUnitDay[] = "day";
static const char gTimeUnitWeek[] = "week";
static const char gTimeUnitHour[] = "hour";
static const char gTimeUnitMinute[] = "minute";
static const char gTimeUnitSecond[] = "second";
static const char gPluralCountOther[] = "other";

static const UChar DEFAULT_PATTERN_FOR_SECOND[] = {tmutfmt_LEFT_CURLY_BRACKET, tmutfmt_DIGIT_ZERO, tmutfmt_RIGHT_CURLY_BRACKET, tmutfmt_SPACE, tmutfmt_LOW_S, 0};
static const UChar DEFAULT_PATTERN_FOR_MINUTE[] = {tmutfmt_LEFT_CURLY_BRACKET, tmutfmt_DIGIT_ZERO, tmutfmt_RIGHT_CURLY_BRACKET, tmutfmt_SPACE, tmutfmt_LOW_M, tmutfmt_LOW_I, tmutfmt_LOW_N, 0};
static const UChar DEFAULT_PATTERN_FOR_HOUR[] = {tmutfmt_LEFT_CURLY_BRACKET, tmutfmt_DIGIT_ZERO, tmutfmt_RIGHT_CURLY_BRACKET, tmutfmt_SPACE, tmutfmt_LOW_H, 0};
static const UChar DEFAULT_PATTERN_FOR_WEEK[] = {tmutfmt_LEFT_CURLY_BRACKET, tmutfmt_DIGIT_ZERO, tmutfmt_RIGHT_CURLY_BRACKET, tmutfmt_SPACE, tmutfmt_LOW_W, 0};
static const UChar DEFAULT_PATTERN_FOR_DAY[] = {tmutfmt_LEFT_CURLY_BRACKET, tmutfmt_DIGIT_ZERO, tmutfmt_RIGHT_CURLY_BRACKET, tmutfmt_SPACE, tmutfmt_LOW_D, 0};
static const UChar DEFAULT_PATTERN_FOR_MONTH[] = {tmutfmt_LEFT_CURLY_BRACKET, tmutfmt_DIGIT_ZERO, tmutfmt_RIGHT_CURLY_BRACKET, tmutfmt_SPACE, tmutfmt_LOW_M, 0};
static const UChar DEFAULT_PATTERN_FOR_YEAR[] = {tmutfmt_LEFT_CURLY_BRACKET, tmutfmt_DIGIT_ZERO, tmutfmt_RIGHT_CURLY_BRACKET, tmutfmt_SPACE, tmutfmt_LOW_Y, 0};

static const UChar PLURAL_COUNT_ZERO[] = {tmutfmt_LOW_Z, tmutfmt_LOW_E, tmutfmt_LOW_R, tmutfmt_LOW_O, 0};
static const UChar PLURAL_COUNT_ONE[] = {tmutfmt_LOW_O, tmutfmt_LOW_N, tmutfmt_LOW_E, 0};
static const UChar PLURAL_COUNT_TWO[] = {tmutfmt_LOW_T, tmutfmt_LOW_W, tmutfmt_LOW_O, 0};

TimeUnitFormat::TimeUnitFormat(UErrorCode& status) {
    initMeasureFormat(Locale::getDefault(), UMEASFMT_WIDTH_WIDE, NULL, status);
    create(UTMUTFMT_FULL_STYLE, status);
}


TimeUnitFormat::TimeUnitFormat(const Locale& locale, UErrorCode& status) {
    initMeasureFormat(locale, UMEASFMT_WIDTH_WIDE, NULL, status);
    create(UTMUTFMT_FULL_STYLE, status);
}


TimeUnitFormat::TimeUnitFormat(const Locale& locale, UTimeUnitFormatStyle style, UErrorCode& status) {
    switch (style) {
    case UTMUTFMT_FULL_STYLE:
        initMeasureFormat(locale, UMEASFMT_WIDTH_WIDE, NULL, status);
        break;
    case UTMUTFMT_ABBREVIATED_STYLE:
        initMeasureFormat(locale, UMEASFMT_WIDTH_SHORT, NULL, status);
        break;
    default:
        initMeasureFormat(locale, UMEASFMT_WIDTH_WIDE, NULL, status);
        break;
    }
    create(style, status);
}

TimeUnitFormat::TimeUnitFormat(const TimeUnitFormat& other)
:   MeasureFormat(other),
    fStyle(other.fStyle)
{
    for (TimeUnit::UTimeUnitFields i = TimeUnit::UTIMEUNIT_YEAR;
         i < TimeUnit::UTIMEUNIT_FIELD_COUNT;
         i = (TimeUnit::UTimeUnitFields)(i+1)) {
        UErrorCode status = U_ZERO_ERROR;
        fTimeUnitToCountToPatterns[i] = initHash(status);
        if (U_SUCCESS(status)) {
            copyHash(other.fTimeUnitToCountToPatterns[i], fTimeUnitToCountToPatterns[i], status);
        } else {
            delete fTimeUnitToCountToPatterns[i];
            fTimeUnitToCountToPatterns[i] = NULL;
        }
    }
}


TimeUnitFormat::~TimeUnitFormat() {
    for (TimeUnit::UTimeUnitFields i = TimeUnit::UTIMEUNIT_YEAR;
         i < TimeUnit::UTIMEUNIT_FIELD_COUNT;
         i = (TimeUnit::UTimeUnitFields)(i+1)) {
        deleteHash(fTimeUnitToCountToPatterns[i]);
        fTimeUnitToCountToPatterns[i] = NULL;
    }
}


TimeUnitFormat*
TimeUnitFormat::clone() const {
    return new TimeUnitFormat(*this);
}


TimeUnitFormat&
TimeUnitFormat::operator=(const TimeUnitFormat& other) {
    if (this == &other) {
        return *this;
    }
    MeasureFormat::operator=(other);
    for (TimeUnit::UTimeUnitFields i = TimeUnit::UTIMEUNIT_YEAR;
         i < TimeUnit::UTIMEUNIT_FIELD_COUNT;
         i = (TimeUnit::UTimeUnitFields)(i+1)) {
        deleteHash(fTimeUnitToCountToPatterns[i]);
        fTimeUnitToCountToPatterns[i] = NULL;
    }
    for (TimeUnit::UTimeUnitFields i = TimeUnit::UTIMEUNIT_YEAR;
         i < TimeUnit::UTIMEUNIT_FIELD_COUNT;
         i = (TimeUnit::UTimeUnitFields)(i+1)) {
        UErrorCode status = U_ZERO_ERROR;
        fTimeUnitToCountToPatterns[i] = initHash(status);
        if (U_SUCCESS(status)) {
            copyHash(other.fTimeUnitToCountToPatterns[i], fTimeUnitToCountToPatterns[i], status);
        } else {
            delete fTimeUnitToCountToPatterns[i];
            fTimeUnitToCountToPatterns[i] = NULL;
        }
    }
    fStyle = other.fStyle;
    return *this;
}

void
TimeUnitFormat::parseObject(const UnicodeString& source,
                            Formattable& result,
                            ParsePosition& pos) const {
    Formattable resultNumber(0.0);
    UBool withNumberFormat = false;
    TimeUnit::UTimeUnitFields resultTimeUnit = TimeUnit::UTIMEUNIT_FIELD_COUNT;
    int32_t oldPos = pos.getIndex();
    int32_t newPos = -1;
    int32_t longestParseDistance = 0;
    UnicodeString* countOfLongestMatch = NULL;
#ifdef TMUTFMT_DEBUG
    char res[1000];
    source.extract(0, source.length(), res, "UTF-8");
    std::cout << "parse source: " << res << "\n";
#endif
    // parse by iterating through all available patterns
    // and looking for the longest match.
    for (TimeUnit::UTimeUnitFields i = TimeUnit::UTIMEUNIT_YEAR;
         i < TimeUnit::UTIMEUNIT_FIELD_COUNT;
         i = (TimeUnit::UTimeUnitFields)(i+1)) {
        Hashtable* countToPatterns = fTimeUnitToCountToPatterns[i];
        int32_t elemPos = UHASH_FIRST;
        const UHashElement* elem = NULL;
        while ((elem = countToPatterns->nextElement(elemPos)) != NULL){
            const UHashTok keyTok = elem->key;
            UnicodeString* count = (UnicodeString*)keyTok.pointer;
#ifdef TMUTFMT_DEBUG
            count->extract(0, count->length(), res, "UTF-8");
            std::cout << "parse plural count: " << res << "\n";
#endif
            const UHashTok valueTok = elem->value;
            // the value is a pair of MessageFormat*
            MessageFormat** patterns = (MessageFormat**)valueTok.pointer;
            for (UTimeUnitFormatStyle style = UTMUTFMT_FULL_STYLE; style < UTMUTFMT_FORMAT_STYLE_COUNT;
                 style = (UTimeUnitFormatStyle)(style + 1)) {
                MessageFormat* pattern = patterns[style];
                pos.setErrorIndex(-1);
                pos.setIndex(oldPos);
                // see if we can parse
                Formattable parsed;
                pattern->parseObject(source, parsed, pos);
                if (pos.getErrorIndex() != -1 || pos.getIndex() == oldPos) {
                    continue;
                }
    #ifdef TMUTFMT_DEBUG
                std::cout << "parsed.getType: " << parsed.getType() << "\n";
    #endif
                Formattable tmpNumber(0.0);
                if (pattern->getArgTypeCount() != 0) {
                    Formattable& temp = parsed[0];
                    if (temp.getType() == Formattable::kString) {
                        UnicodeString tmpString;
                        UErrorCode pStatus = U_ZERO_ERROR;
                        getNumberFormatInternal().parse(temp.getString(tmpString), tmpNumber, pStatus);
                        if (U_FAILURE(pStatus)) {
                            continue;
                        }
                    } else if (temp.isNumeric()) {
                        tmpNumber = temp;
                    } else {
                        continue;
                    }
                }
                int32_t parseDistance = pos.getIndex() - oldPos;
                if (parseDistance > longestParseDistance) {
                    if (pattern->getArgTypeCount() != 0) {
                        resultNumber = tmpNumber;
                        withNumberFormat = true;
                    } else {
                        withNumberFormat = false;
                    }
                    resultTimeUnit = i;
                    newPos = pos.getIndex();
                    longestParseDistance = parseDistance;
                    countOfLongestMatch = count;
                }
            }
        }
    }
    /* After find the longest match, parse the number.
     * Result number could be null for the pattern without number pattern.
     * such as unit pattern in Arabic.
     * When result number is null, use plural rule to set the number.
     */
    if (withNumberFormat == false && longestParseDistance != 0) {
        // set the number using plurrual count
        if (0 == countOfLongestMatch->compare(PLURAL_COUNT_ZERO, 4)) {
            resultNumber = Formattable(0.0);
        } else if (0 == countOfLongestMatch->compare(PLURAL_COUNT_ONE, 3)) {
            resultNumber = Formattable(1.0);
        } else if (0 == countOfLongestMatch->compare(PLURAL_COUNT_TWO, 3)) {
            resultNumber = Formattable(2.0);
        } else {
            // should not happen.
            // TODO: how to handle?
            resultNumber = Formattable(3.0);
        }
    }
    if (longestParseDistance == 0) {
        pos.setIndex(oldPos);
        pos.setErrorIndex(0);
    } else {
        UErrorCode status = U_ZERO_ERROR;
        LocalPointer<TimeUnitAmount> tmutamt(new TimeUnitAmount(resultNumber, resultTimeUnit, status), status);
        if (U_SUCCESS(status)) {
            result.adoptObject(tmutamt.orphan());
            pos.setIndex(newPos);
            pos.setErrorIndex(-1);
        } else {
            pos.setIndex(oldPos);
            pos.setErrorIndex(0);
        }
    }
}

void
TimeUnitFormat::create(UTimeUnitFormatStyle style, UErrorCode& status) {
    // fTimeUnitToCountToPatterns[] must have its elements initialized to NULL first
    // before checking for failure status.
    for (TimeUnit::UTimeUnitFields i = TimeUnit::UTIMEUNIT_YEAR;
         i < TimeUnit::UTIMEUNIT_FIELD_COUNT;
         i = (TimeUnit::UTimeUnitFields)(i+1)) {
        fTimeUnitToCountToPatterns[i] = NULL;
    }

    if (U_FAILURE(status)) {
        return;
    }
    if (style < UTMUTFMT_FULL_STYLE || style >= UTMUTFMT_FORMAT_STYLE_COUNT) {
        status = U_ILLEGAL_ARGUMENT_ERROR;
        return;
    }
    fStyle = style;

    //TODO: format() and parseObj() are const member functions,
    //so, can not do lazy initialization in C++.
    //setup has to be done in constructors.
    //and here, the behavior is not consistent with Java.
    //In Java, create an empty instance does not setup locale as
    //default locale. If it followed by setNumberFormat(),
    //in format(), the locale will set up as the locale in fNumberFormat.
    //But in C++, this sets the locale as the default locale.
    setup(status);
}

void
TimeUnitFormat::setup(UErrorCode& err) {
    initDataMembers(err);

    UVector pluralCounts(0, uhash_compareUnicodeString, 6, err);
    LocalPointer<StringEnumeration> keywords(getPluralRules().getKeywords(err), err);
    if (U_FAILURE(err)) {
        return;
    }
    UnicodeString* pluralCount;
    while ((pluralCount = const_cast<UnicodeString*>(keywords->snext(err))) != NULL) {
      pluralCounts.addElement(pluralCount, err);
    }
    readFromCurrentLocale(UTMUTFMT_FULL_STYLE, gUnitsTag, pluralCounts, err);
    checkConsistency(UTMUTFMT_FULL_STYLE, gUnitsTag, err);
    readFromCurrentLocale(UTMUTFMT_ABBREVIATED_STYLE, gShortUnitsTag, pluralCounts, err);
    checkConsistency(UTMUTFMT_ABBREVIATED_STYLE, gShortUnitsTag, err);
}


void
TimeUnitFormat::initDataMembers(UErrorCode& err){
    if (U_FAILURE(err)) {
        return;
    }
    for (TimeUnit::UTimeUnitFields i = TimeUnit::UTIMEUNIT_YEAR;
         i < TimeUnit::UTIMEUNIT_FIELD_COUNT;
         i = (TimeUnit::UTimeUnitFields)(i+1)) {
        deleteHash(fTimeUnitToCountToPatterns[i]);
        fTimeUnitToCountToPatterns[i] = NULL;
    }
}

struct TimeUnitFormatReadSink : public ResourceSink {
    TimeUnitFormat *timeUnitFormatObj;
    const UVector &pluralCounts;
    UTimeUnitFormatStyle style;
    UBool beenHere;

    TimeUnitFormatReadSink(TimeUnitFormat *timeUnitFormatObj,
            const UVector &pluralCounts, UTimeUnitFormatStyle style) :
            timeUnitFormatObj(timeUnitFormatObj), pluralCounts(pluralCounts),
            style(style), beenHere(FALSE){}

    virtual ~TimeUnitFormatReadSink();

    virtual void put(const char *key, ResourceValue &value, UBool, UErrorCode &errorCode) {
        // Skip all put() calls except the first one -- discard all fallback data.
        if (beenHere) {
            return;
        } else {
            beenHere = TRUE;
        }

        ResourceTable units = value.getTable(errorCode);
        if (U_FAILURE(errorCode)) { return; }

        for (int32_t i = 0; units.getKeyAndValue(i, key, value); ++i) {
            const char* timeUnitName = key;
            if (timeUnitName == NULL) {
                continue;
            }

            TimeUnit::UTimeUnitFields timeUnitField = TimeUnit::UTIMEUNIT_FIELD_COUNT;
            if ( uprv_strcmp(timeUnitName, gTimeUnitYear) == 0 ) {
                timeUnitField = TimeUnit::UTIMEUNIT_YEAR;
            } else if ( uprv_strcmp(timeUnitName, gTimeUnitMonth) == 0 ) {
                timeUnitField = TimeUnit::UTIMEUNIT_MONTH;
            } else if ( uprv_strcmp(timeUnitName, gTimeUnitDay) == 0 ) {
                timeUnitField = TimeUnit::UTIMEUNIT_DAY;
            } else if ( uprv_strcmp(timeUnitName, gTimeUnitHour) == 0 ) {
                timeUnitField = TimeUnit::UTIMEUNIT_HOUR;
            } else if ( uprv_strcmp(timeUnitName, gTimeUnitMinute) == 0 ) {
                timeUnitField = TimeUnit::UTIMEUNIT_MINUTE;
            } else if ( uprv_strcmp(timeUnitName, gTimeUnitSecond) == 0 ) {
                timeUnitField = TimeUnit::UTIMEUNIT_SECOND;
            } else if ( uprv_strcmp(timeUnitName, gTimeUnitWeek) == 0 ) {
                timeUnitField = TimeUnit::UTIMEUNIT_WEEK;
            } else {
                continue;
            }
            LocalPointer<Hashtable> localCountToPatterns;
            Hashtable *countToPatterns =
                timeUnitFormatObj->fTimeUnitToCountToPatterns[timeUnitField];
            if (countToPatterns == NULL) {
                localCountToPatterns.adoptInsteadAndCheckErrorCode(
                    timeUnitFormatObj->initHash(errorCode), errorCode);
                countToPatterns = localCountToPatterns.getAlias();
                if (U_FAILURE(errorCode)) {
                    return;
                }
            }

            ResourceTable countsToPatternTable = value.getTable(errorCode);
            if (U_FAILURE(errorCode)) {
                continue;
            }
            for (int32_t j = 0; countsToPatternTable.getKeyAndValue(j, key, value); ++j) {
                errorCode = U_ZERO_ERROR;
                UnicodeString pattern = value.getUnicodeString(errorCode);
                if (U_FAILURE(errorCode)) {
                    continue;
                }
                UnicodeString pluralCountUniStr(key, -1, US_INV);
                if (!pluralCounts.contains(&pluralCountUniStr)) {
                    continue;
                }
                LocalPointer<MessageFormat> messageFormat(new MessageFormat(
                    pattern, timeUnitFormatObj->getLocale(errorCode), errorCode), errorCode);
                if (U_FAILURE(errorCode)) {
                    return;
                }
                MessageFormat** formatters =
                    (MessageFormat**)countToPatterns->get(pluralCountUniStr);
                if (formatters == NULL) {
                    LocalMemory<MessageFormat *> localFormatters(
                        (MessageFormat **)uprv_malloc(UTMUTFMT_FORMAT_STYLE_COUNT*sizeof(MessageFormat*)));
                    if (localFormatters.isNull()) {
                        errorCode = U_MEMORY_ALLOCATION_ERROR;
                        return;
                    }
                    localFormatters[UTMUTFMT_FULL_STYLE] = NULL;
                    localFormatters[UTMUTFMT_ABBREVIATED_STYLE] = NULL;
                    countToPatterns->put(pluralCountUniStr, localFormatters.getAlias(), errorCode);
                    if (U_FAILURE(errorCode)) {
                        return;
                    }
                    formatters = localFormatters.orphan();
                }
                formatters[style] = messageFormat.orphan();
            }

            if (timeUnitFormatObj->fTimeUnitToCountToPatterns[timeUnitField] == NULL) {
                timeUnitFormatObj->fTimeUnitToCountToPatterns[timeUnitField] = localCountToPatterns.orphan();
            }
        }
    }

};

TimeUnitFormatReadSink::~TimeUnitFormatReadSink() {}

void
TimeUnitFormat::readFromCurrentLocale(UTimeUnitFormatStyle style, const char* key,
                                      const UVector& pluralCounts, UErrorCode& err) {
    if (U_FAILURE(err)) {
        return;
    }
    // fill timeUnitToCountToPatterns from resource file
    // err is used to indicate wrong status except missing resource.
    // status is an error code used in resource lookup.
    // status does not affect "err".
    UErrorCode status = U_ZERO_ERROR;
    LocalUResourceBundlePointer rb(ures_open(U_ICUDATA_UNIT, getLocaleID(status), &status));

    LocalUResourceBundlePointer unitsRes(ures_getByKey(rb.getAlias(), key, NULL, &status));
    ures_getByKey(unitsRes.getAlias(), "duration", unitsRes.getAlias(), &status);
    if (U_FAILURE(status)) {
        return;
    }

    TimeUnitFormatReadSink sink(this, pluralCounts, style);
    ures_getAllItemsWithFallback(unitsRes.getAlias(), "", sink, status);
}

void
TimeUnitFormat::checkConsistency(UTimeUnitFormatStyle style, const char* key, UErrorCode& err) {
    if (U_FAILURE(err)) {
        return;
    }
    // there should be patterns for each plural rule in each time unit.
    // For each time unit,
    //     for each plural rule, following is unit pattern fall-back rule:
    //         ( for example: "one" hour )
    //         look for its unit pattern in its locale tree.
    //         if pattern is not found in its own locale, such as de_DE,
    //         look for the pattern in its parent, such as de,
    //         keep looking till found or till root.
    //         if the pattern is not found in root either,
    //         fallback to plural count "other",
    //         look for the pattern of "other" in the locale tree:
    //         "de_DE" to "de" to "root".
    //         If not found, fall back to value of
    //         static variable DEFAULT_PATTERN_FOR_xxx, such as "{0} h".
    //
    // Following is consistency check to create pattern for each
    // plural rule in each time unit using above fall-back rule.
    //
    LocalPointer<StringEnumeration> keywords(
            getPluralRules().getKeywords(err), err);
    const UnicodeString* pluralCount;
    while (U_SUCCESS(err) && (pluralCount = keywords->snext(err)) != NULL) {
        for (int32_t i = 0; i < TimeUnit::UTIMEUNIT_FIELD_COUNT; ++i) {
            // for each time unit,
            // get all the patterns for each plural rule in this locale.
            Hashtable* countToPatterns = fTimeUnitToCountToPatterns[i];
            if ( countToPatterns == NULL ) {
                fTimeUnitToCountToPatterns[i] = countToPatterns = initHash(err);
                if (U_FAILURE(err)) {
                    return;
                }
            }
            MessageFormat** formatters = (MessageFormat**)countToPatterns->get(*pluralCount);
            if( formatters == NULL || formatters[style] == NULL ) {
                // look through parents
                const char* localeName = getLocaleID(err);
                CharString pluralCountChars;
                pluralCountChars.appendInvariantChars(*pluralCount, err);
                searchInLocaleChain(style, key, localeName,
                                    (TimeUnit::UTimeUnitFields)i,
                                    *pluralCount, pluralCountChars.data(),
                                    countToPatterns, err);
            }
            // TODO: what to do with U_FAILURE(err) at this point.
            //       As is, the outer loop continues to run, but does nothing.
        }
    }
}



// srcPluralCount is the original plural count on which the pattern is
// searched for.
// searchPluralCount is the fallback plural count.
// For example, to search for pattern for ""one" hour",
// "one" is the srcPluralCount,
// if the pattern is not found even in root, fallback to
// using patterns of plural count "other",
// then, "other" is the searchPluralCount.
void
TimeUnitFormat::searchInLocaleChain(UTimeUnitFormatStyle style, const char* key, const char* localeName,
                                TimeUnit::UTimeUnitFields srcTimeUnitField,
                                const UnicodeString& srcPluralCount,
                                const char* searchPluralCount,
                                Hashtable* countToPatterns,
                                UErrorCode& err) {
    if (U_FAILURE(err)) {
        return;
    }
    UErrorCode status = U_ZERO_ERROR;
    char parentLocale[ULOC_FULLNAME_CAPACITY];
    uprv_strcpy(parentLocale, localeName);
    int32_t locNameLen;
    U_ASSERT(countToPatterns != NULL);
    while ((locNameLen = uloc_getParent(parentLocale, parentLocale,
                                        ULOC_FULLNAME_CAPACITY, &status)) >= 0){
        // look for pattern for srcPluralCount in locale tree
        LocalUResourceBundlePointer rb(ures_open(U_ICUDATA_UNIT, parentLocale, &status));
        LocalUResourceBundlePointer unitsRes(ures_getByKey(rb.getAlias(), key, NULL, &status));
        const char* timeUnitName = getTimeUnitName(srcTimeUnitField, status);
        LocalUResourceBundlePointer countsToPatternRB(ures_getByKey(unitsRes.getAlias(), timeUnitName, NULL, &status));
        const UChar* pattern;
        int32_t      ptLength;
        pattern = ures_getStringByKeyWithFallback(countsToPatternRB.getAlias(), searchPluralCount, &ptLength, &status);
        if (U_SUCCESS(status)) {
            //found
            LocalPointer<MessageFormat> messageFormat(
                new MessageFormat(UnicodeString(TRUE, pattern, ptLength), getLocale(err), err), err);
            if (U_FAILURE(err)) {
                return;
            }
            MessageFormat** formatters = (MessageFormat**)countToPatterns->get(srcPluralCount);
            if (formatters == NULL) {
                LocalMemory<MessageFormat *> localFormatters(
                        (MessageFormat**)uprv_malloc(UTMUTFMT_FORMAT_STYLE_COUNT*sizeof(MessageFormat*)));
                formatters = localFormatters.getAlias();
                localFormatters[UTMUTFMT_FULL_STYLE] = NULL;
                localFormatters[UTMUTFMT_ABBREVIATED_STYLE] = NULL;
                countToPatterns->put(srcPluralCount, localFormatters.orphan(), err);
                if (U_FAILURE(err)) {
                    return;
                }
            }
            //delete formatters[style];
            formatters[style] = messageFormat.orphan();
            return;
        }
        status = U_ZERO_ERROR;
        if (locNameLen == 0) {
            break;
        }
    }

    // if no unitsShort resource was found even after fallback to root locale
    // then search the units resource fallback from the current level to root
    if ( locNameLen == 0 && uprv_strcmp(key, gShortUnitsTag) == 0) {
#ifdef TMUTFMT_DEBUG
        std::cout << "loop into searchInLocaleChain since Short-Long-Alternative \n";
#endif
        CharString pLocale(localeName, -1, err);
        // Add an underscore at the tail of locale name,
        // so that searchInLocaleChain will check the current locale before falling back
        pLocale.append('_', err);
        searchInLocaleChain(style, gUnitsTag, pLocale.data(), srcTimeUnitField, srcPluralCount,
                             searchPluralCount, countToPatterns, err);
        if (U_FAILURE(err)) {
            return;
        }
        MessageFormat** formatters = (MessageFormat**)countToPatterns->get(srcPluralCount);
        if (formatters != NULL && formatters[style] != NULL) {
            return;
        }
    }

    // if not found the pattern for this plural count at all,
    // fall-back to plural count "other"
    if ( uprv_strcmp(searchPluralCount, gPluralCountOther) == 0 ) {
        // set default fall back the same as the resource in root
        LocalPointer<MessageFormat> messageFormat;
        const UChar *pattern = NULL;
        if ( srcTimeUnitField == TimeUnit::UTIMEUNIT_SECOND ) {
            pattern = DEFAULT_PATTERN_FOR_SECOND;
        } else if ( srcTimeUnitField == TimeUnit::UTIMEUNIT_MINUTE ) {
            pattern = DEFAULT_PATTERN_FOR_MINUTE;
        } else if ( srcTimeUnitField == TimeUnit::UTIMEUNIT_HOUR ) {
            pattern = DEFAULT_PATTERN_FOR_HOUR;
        } else if ( srcTimeUnitField == TimeUnit::UTIMEUNIT_WEEK ) {
            pattern = DEFAULT_PATTERN_FOR_WEEK;
        } else if ( srcTimeUnitField == TimeUnit::UTIMEUNIT_DAY ) {
            pattern = DEFAULT_PATTERN_FOR_DAY;
        } else if ( srcTimeUnitField == TimeUnit::UTIMEUNIT_MONTH ) {
            pattern = DEFAULT_PATTERN_FOR_MONTH;
        } else if ( srcTimeUnitField == TimeUnit::UTIMEUNIT_YEAR ) {
            pattern = DEFAULT_PATTERN_FOR_YEAR;
        }
        if (pattern != NULL) {
            messageFormat.adoptInsteadAndCheckErrorCode(
                     new MessageFormat(UnicodeString(TRUE, pattern, -1), getLocale(err), err), err);
        }
        if (U_FAILURE(err)) {
            return;
        }
        MessageFormat** formatters = (MessageFormat**)countToPatterns->get(srcPluralCount);
        if (formatters == NULL) {
            LocalMemory<MessageFormat *> localFormatters (
                    (MessageFormat**)uprv_malloc(UTMUTFMT_FORMAT_STYLE_COUNT*sizeof(MessageFormat*)));
            if (localFormatters.isNull()) {
                err = U_MEMORY_ALLOCATION_ERROR;
                return;
            }
            formatters = localFormatters.getAlias();
            formatters[UTMUTFMT_FULL_STYLE] = NULL;
            formatters[UTMUTFMT_ABBREVIATED_STYLE] = NULL;
            countToPatterns->put(srcPluralCount, localFormatters.orphan(), err);
        }
        if (U_SUCCESS(err)) {
            //delete formatters[style];
            formatters[style] = messageFormat.orphan();
        }
    } else {
        // fall back to rule "other", and search in parents
        searchInLocaleChain(style, key, localeName, srcTimeUnitField, srcPluralCount,
                            gPluralCountOther, countToPatterns, err);
    }
}

void
TimeUnitFormat::setLocale(const Locale& locale, UErrorCode& status) {
    if (setMeasureFormatLocale(locale, status)) {
        setup(status);
    }
}


void
TimeUnitFormat::setNumberFormat(const NumberFormat& format, UErrorCode& status){
    if (U_FAILURE(status)) {
        return;
    }
    adoptNumberFormat(format.clone(), status);
}


void
TimeUnitFormat::deleteHash(Hashtable* htable) {
    int32_t pos = UHASH_FIRST;
    const UHashElement* element = NULL;
    if ( htable ) {
        while ( (element = htable->nextElement(pos)) != NULL ) {
            const UHashTok valueTok = element->value;
            const MessageFormat** value = (const MessageFormat**)valueTok.pointer;
            delete value[UTMUTFMT_FULL_STYLE];
            delete value[UTMUTFMT_ABBREVIATED_STYLE];
            //delete[] value;
            uprv_free(value);
        }
    }
    delete htable;
}


void
TimeUnitFormat::copyHash(const Hashtable* source, Hashtable* target, UErrorCode& status) {
    if ( U_FAILURE(status) ) {
        return;
    }
    int32_t pos = UHASH_FIRST;
    const UHashElement* element = NULL;
    if ( source ) {
        while ( (element = source->nextElement(pos)) != NULL ) {
            const UHashTok keyTok = element->key;
            const UnicodeString* key = (UnicodeString*)keyTok.pointer;
            const UHashTok valueTok = element->value;
            const MessageFormat** value = (const MessageFormat**)valueTok.pointer;
            MessageFormat** newVal = (MessageFormat**)uprv_malloc(UTMUTFMT_FORMAT_STYLE_COUNT*sizeof(MessageFormat*));
            newVal[0] = value[0]->clone();
            newVal[1] = value[1]->clone();
            target->put(UnicodeString(*key), newVal, status);
            if ( U_FAILURE(status) ) {
                delete newVal[0];
                delete newVal[1];
                uprv_free(newVal);
                return;
            }
        }
    }
}


U_CDECL_BEGIN

/**
 * set hash table value comparator
 *
 * @param val1  one value in comparison
 * @param val2  the other value in comparison
 * @return      TRUE if 2 values are the same, FALSE otherwise
 */
static UBool U_CALLCONV tmutfmtHashTableValueComparator(UHashTok val1, UHashTok val2);

static UBool
U_CALLCONV tmutfmtHashTableValueComparator(UHashTok val1, UHashTok val2) {
    const MessageFormat** pattern1 = (const MessageFormat**)val1.pointer;
    const MessageFormat** pattern2 = (const MessageFormat**)val2.pointer;
    return *pattern1[0] == *pattern2[0] && *pattern1[1] == *pattern2[1];
}

U_CDECL_END

Hashtable*
TimeUnitFormat::initHash(UErrorCode& status) {
    if ( U_FAILURE(status) ) {
        return NULL;
    }
    Hashtable* hTable;
    if ( (hTable = new Hashtable(TRUE, status)) == NULL ) {
        status = U_MEMORY_ALLOCATION_ERROR;
        return NULL;
    }
    if ( U_FAILURE(status) ) {
        delete hTable;
        return NULL;
    }
    hTable->setValueComparator(tmutfmtHashTableValueComparator);
    return hTable;
}


const char*
TimeUnitFormat::getTimeUnitName(TimeUnit::UTimeUnitFields unitField,
                                UErrorCode& status) {
    if (U_FAILURE(status)) {
        return NULL;
    }
    switch (unitField) {
      case TimeUnit::UTIMEUNIT_YEAR:
        return gTimeUnitYear;
      case TimeUnit::UTIMEUNIT_MONTH:
        return gTimeUnitMonth;
      case TimeUnit::UTIMEUNIT_DAY:
        return gTimeUnitDay;
      case TimeUnit::UTIMEUNIT_WEEK:
        return gTimeUnitWeek;
      case TimeUnit::UTIMEUNIT_HOUR:
        return gTimeUnitHour;
      case TimeUnit::UTIMEUNIT_MINUTE:
        return gTimeUnitMinute;
      case TimeUnit::UTIMEUNIT_SECOND:
        return gTimeUnitSecond;
      default:
        status = U_ILLEGAL_ARGUMENT_ERROR;
        return NULL;
    }
}

U_NAMESPACE_END

#endif
