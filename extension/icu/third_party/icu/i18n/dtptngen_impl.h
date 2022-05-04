// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
*******************************************************************************
* Copyright (C) 2007-2016, International Business Machines Corporation and
* others. All Rights Reserved.
*******************************************************************************
*
* File DTPTNGEN.H
*
*******************************************************************************
*/

#ifndef __DTPTNGEN_IMPL_H__
#define __DTPTNGEN_IMPL_H__

#include "unicode/udatpg.h"

#include "unicode/strenum.h"
#include "unicode/unistr.h"
#include "uvector.h"

// TODO(claireho): Split off Builder class.
// TODO(claireho): If splitting off Builder class: As subclass or independent?

#define dtptngen_MAX_PATTERN_ENTRIES 52
#define dtptngen_MAX_CLDR_FIELD_LEN  60
#define dtptngen_MAX_DT_TOKEN        50
#define dtptngen_MAX_RESOURCE_FIELD  12
#define dtptngen_MAX_AVAILABLE_FORMATS  12
#define dtptngen_NONE          0
#define dtptngen_EXTRA_FIELD   0x10000
#define dtptngen_MISSING_FIELD  0x1000
#define dtptngen_MAX_STRING_ENUMERATION  200
#define dtptngen_SINGLE_QUOTE      ((UChar)0x0027)
#define dtptngen_FORWARDSLASH      ((UChar)0x002F)
#define dtptngen_BACKSLASH         ((UChar)0x005C)
#define dtptngen_SPACE             ((UChar)0x0020)
#define dtptngen_QUOTATION_MARK    ((UChar)0x0022)
#define dtptngen_ASTERISK          ((UChar)0x002A)
#define dtptngen_PLUSSITN          ((UChar)0x002B)
#define dtptngen_COMMA             ((UChar)0x002C)
#define dtptngen_HYPHEN            ((UChar)0x002D)
#define dtptngen_DOT               ((UChar)0x002E)
#define dtptngen_COLON             ((UChar)0x003A)
#define dtptngen_CAP_A             ((UChar)0x0041)
#define dtptngen_CAP_B             ((UChar)0x0042)
#define dtptngen_CAP_C             ((UChar)0x0043)
#define dtptngen_CAP_D             ((UChar)0x0044)
#define dtptngen_CAP_E             ((UChar)0x0045)
#define dtptngen_CAP_F             ((UChar)0x0046)
#define dtptngen_CAP_G             ((UChar)0x0047)
#define dtptngen_CAP_H             ((UChar)0x0048)
#define dtptngen_CAP_J             ((UChar)0x004A)
#define dtptngen_CAP_K             ((UChar)0x004B)
#define dtptngen_CAP_L             ((UChar)0x004C)
#define dtptngen_CAP_M             ((UChar)0x004D)
#define dtptngen_CAP_O             ((UChar)0x004F)
#define dtptngen_CAP_Q             ((UChar)0x0051)
#define dtptngen_CAP_S             ((UChar)0x0053)
#define dtptngen_CAP_T             ((UChar)0x0054)
#define dtptngen_CAP_U             ((UChar)0x0055)
#define dtptngen_CAP_V             ((UChar)0x0056)
#define dtptngen_CAP_W             ((UChar)0x0057)
#define dtptngen_CAP_X             ((UChar)0x0058)
#define dtptngen_CAP_Y             ((UChar)0x0059)
#define dtptngen_CAP_Z             ((UChar)0x005A)
#define dtptngen_LOWLINE           ((UChar)0x005F)
#define dtptngen_LOW_A             ((UChar)0x0061)
#define dtptngen_LOW_B             ((UChar)0x0062)
#define dtptngen_LOW_C             ((UChar)0x0063)
#define dtptngen_LOW_D             ((UChar)0x0064)
#define dtptngen_LOW_E             ((UChar)0x0065)
#define dtptngen_LOW_F             ((UChar)0x0066)
#define dtptngen_LOW_G             ((UChar)0x0067)
#define dtptngen_LOW_H             ((UChar)0x0068)
#define dtptngen_LOW_I             ((UChar)0x0069)
#define dtptngen_LOW_J             ((UChar)0x006A)
#define dtptngen_LOW_K             ((UChar)0x006B)
#define dtptngen_LOW_L             ((UChar)0x006C)
#define dtptngen_LOW_M             ((UChar)0x006D)
#define dtptngen_LOW_N             ((UChar)0x006E)
#define dtptngen_LOW_O             ((UChar)0x006F)
#define dtptngen_LOW_P             ((UChar)0x0070)
#define dtptngen_LOW_Q             ((UChar)0x0071)
#define dtptngen_LOW_R             ((UChar)0x0072)
#define dtptngen_LOW_S             ((UChar)0x0073)
#define dtptngen_LOW_T             ((UChar)0x0074)
#define dtptngen_LOW_U             ((UChar)0x0075)
#define dtptngen_LOW_V             ((UChar)0x0076)
#define dtptngen_LOW_W             ((UChar)0x0077)
#define dtptngen_LOW_X             ((UChar)0x0078)
#define dtptngen_LOW_Y             ((UChar)0x0079)
#define dtptngen_LOW_Z             ((UChar)0x007A)
#define dtptngen_DT_NARROW         -0x101
#define dtptngen_DT_SHORTER        -0x102
#define dtptngen_DT_SHORT          -0x103
#define dtptngen_DT_LONG           -0x104
#define dtptngen_DT_NUMERIC         0x100
#define dtptngen_DT_DELTA           0x10

U_NAMESPACE_BEGIN

const int32_t UDATPG_FRACTIONAL_MASK = 1<<UDATPG_FRACTIONAL_SECOND_FIELD;
const int32_t UDATPG_SECOND_AND_FRACTIONAL_MASK = (1<<UDATPG_SECOND_FIELD) | (1<<UDATPG_FRACTIONAL_SECOND_FIELD);

typedef enum dtStrEnum {
    DT_BASESKELETON,
    DT_SKELETON,
    DT_PATTERN
}dtStrEnum;

typedef struct dtTypeElem {
    UChar                  patternChar;
    UDateTimePatternField  field;
    int16_t                type;
    int16_t                minLen;
    int16_t                weight;
} dtTypeElem;

// A compact storage mechanism for skeleton field strings.  Several dozen of these will be created
// for a typical DateTimePatternGenerator instance.
class SkeletonFields : public UMemory {
public:
    SkeletonFields();
    void clear();
    void copyFrom(const SkeletonFields& other);
    void clearField(int32_t field);
    UChar getFieldChar(int32_t field) const;
    int32_t getFieldLength(int32_t field) const;
    void populate(int32_t field, const UnicodeString& value);
    void populate(int32_t field, UChar repeatChar, int32_t repeatCount);
    UBool isFieldEmpty(int32_t field) const;
    UnicodeString& appendTo(UnicodeString& string) const;
    UnicodeString& appendFieldTo(int32_t field, UnicodeString& string) const;
    UChar getFirstChar() const;
    inline UBool operator==(const SkeletonFields& other) const;
    inline UBool operator!=(const SkeletonFields& other) const;

private:
    int8_t chars[UDATPG_FIELD_COUNT];
    int8_t lengths[UDATPG_FIELD_COUNT];
};

inline UBool SkeletonFields::operator==(const SkeletonFields& other) const {
    return (uprv_memcmp(chars, other.chars, sizeof(chars)) == 0
        && uprv_memcmp(lengths, other.lengths, sizeof(lengths)) == 0);
}

inline UBool SkeletonFields::operator!=(const SkeletonFields& other) const {
    return (! operator==(other));
}

class PtnSkeleton : public UMemory {
public:
    int32_t type[UDATPG_FIELD_COUNT];
    SkeletonFields original;
    SkeletonFields baseOriginal;
    UBool addedDefaultDayPeriod;

    PtnSkeleton();
    PtnSkeleton(const PtnSkeleton& other);
    void copyFrom(const PtnSkeleton& other);
    void clear();
    UBool equals(const PtnSkeleton& other) const;
    UnicodeString getSkeleton() const;
    UnicodeString getBaseSkeleton() const;
    UChar getFirstChar() const;

    // TODO: Why is this virtual, as well as the other destructors in this file? We don't want
    // vtables when we don't use class objects polymorphically.
    virtual ~PtnSkeleton();
};

class PtnElem : public UMemory {
public:
    UnicodeString basePattern;
    LocalPointer<PtnSkeleton> skeleton;
    UnicodeString pattern;
    UBool         skeletonWasSpecified; // if specified in availableFormats, not derived
    LocalPointer<PtnElem> next;

    PtnElem(const UnicodeString &basePattern, const UnicodeString &pattern);
    virtual ~PtnElem();
};

class FormatParser : public UMemory {
public:
    UnicodeString items[dtptngen_MAX_DT_TOKEN];
    int32_t itemNumber;

    FormatParser();
    virtual ~FormatParser();
    void set(const UnicodeString& patternString);
    void getQuoteLiteral(UnicodeString& quote, int32_t *itemIndex);
    UBool isPatternSeparator(const UnicodeString& field) const;
    static UBool isQuoteLiteral(const UnicodeString& s);
    static int32_t getCanonicalIndex(const UnicodeString& s) { return getCanonicalIndex(s, TRUE); }
    static int32_t getCanonicalIndex(const UnicodeString& s, UBool strict);

private:
   typedef enum TokenStatus {
       START,
       ADD_TOKEN,
       SYNTAX_ERROR,
       DONE
   } TokenStatus;

   TokenStatus status;
   virtual TokenStatus setTokens(const UnicodeString& pattern, int32_t startPos, int32_t *len);
};

class DistanceInfo : public UMemory {
public:
    int32_t missingFieldMask;
    int32_t extraFieldMask;

    DistanceInfo() {}
    virtual ~DistanceInfo();
    void clear() { missingFieldMask = extraFieldMask = 0; }
    void setTo(const DistanceInfo& other);
    void addMissing(int32_t field) { missingFieldMask |= (1<<field); }
    void addExtra(int32_t field) { extraFieldMask |= (1<<field); }
};

class DateTimeMatcher: public UMemory {
public:
    PtnSkeleton skeleton;

    void getBasePattern(UnicodeString& basePattern);
    UnicodeString getPattern();
    void set(const UnicodeString& pattern, FormatParser* fp);
    void set(const UnicodeString& pattern, FormatParser* fp, PtnSkeleton& skeleton);
    void copyFrom(const PtnSkeleton& skeleton);
    void copyFrom();
    PtnSkeleton* getSkeletonPtr();
    UBool equals(const DateTimeMatcher* other) const;
    int32_t getDistance(const DateTimeMatcher& other, int32_t includeMask, DistanceInfo& distanceInfo) const;
    DateTimeMatcher();
    DateTimeMatcher(const DateTimeMatcher& other);
    virtual ~DateTimeMatcher();
    int32_t getFieldMask() const;
};

class PatternMap : public UMemory {
public:
    PtnElem *boot[dtptngen_MAX_PATTERN_ENTRIES];
    PatternMap();
    virtual  ~PatternMap();
    void  add(const UnicodeString& basePattern, const PtnSkeleton& skeleton, const UnicodeString& value, UBool skeletonWasSpecified, UErrorCode& status);
    const UnicodeString* getPatternFromBasePattern(const UnicodeString& basePattern, UBool& skeletonWasSpecified) const;
    const UnicodeString* getPatternFromSkeleton(const PtnSkeleton& skeleton, const PtnSkeleton** specifiedSkeletonPtr = 0) const;
    void copyFrom(const PatternMap& other, UErrorCode& status);
    PtnElem* getHeader(UChar baseChar) const;
    UBool equals(const PatternMap& other) const;
private:
    UBool isDupAllowed;
    PtnElem*  getDuplicateElem(const UnicodeString& basePattern, const PtnSkeleton& skeleton, PtnElem *baseElem);
}; // end  PatternMap

class PatternMapIterator : public UMemory {
public:
    PatternMapIterator(UErrorCode &status);
    virtual ~PatternMapIterator();
    void set(PatternMap& patternMap);
    PtnSkeleton* getSkeleton() const;
    UBool hasNext() const;
    DateTimeMatcher& next();
private:
    int32_t bootIndex;
    PtnElem *nodePtr;
    LocalPointer<DateTimeMatcher> matcher;
    PatternMap *patternMap;
};

class DTSkeletonEnumeration : public StringEnumeration {
public:
    DTSkeletonEnumeration(PatternMap& patternMap, dtStrEnum type, UErrorCode& status);
    virtual ~DTSkeletonEnumeration();
    static UClassID U_EXPORT2 getStaticClassID(void);
    virtual UClassID getDynamicClassID(void) const;
    virtual const UnicodeString* snext(UErrorCode& status);
    virtual void reset(UErrorCode& status);
    virtual int32_t count(UErrorCode& status) const;
private:
    int32_t pos;
    UBool isCanonicalItem(const UnicodeString& item);
    LocalPointer<UVector> fSkeletons;
};

class DTRedundantEnumeration : public StringEnumeration {
public:
    DTRedundantEnumeration();
    virtual ~DTRedundantEnumeration();
    static UClassID U_EXPORT2 getStaticClassID(void);
    virtual UClassID getDynamicClassID(void) const;
    virtual const UnicodeString* snext(UErrorCode& status);
    virtual void reset(UErrorCode& status);
    virtual int32_t count(UErrorCode& status) const;
    void add(const UnicodeString &pattern, UErrorCode& status);
private:
    int32_t pos;
    UBool isCanonicalItem(const UnicodeString& item) const;
    LocalPointer<UVector> fPatterns;
};

U_NAMESPACE_END

#endif
