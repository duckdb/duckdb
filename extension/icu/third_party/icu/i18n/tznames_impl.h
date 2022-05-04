// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
 *******************************************************************************
 * Copyright (C) 2011-2016, International Business Machines Corporation and
 * others. All Rights Reserved.
 *******************************************************************************
 */

#ifndef __TZNAMES_IMPL_H__
#define __TZNAMES_IMPL_H__


/**
 * \file
 * \brief C++ API: TimeZoneNames object
 */

#include "unicode/utypes.h"

#if !UCONFIG_NO_FORMATTING

#include "unicode/tznames.h"
#include "unicode/ures.h"
#include "unicode/locid.h"
#include "uhash.h"
#include "uvector.h"
#include "umutex.h"

// Some zone display names involving supplementary characters can be over 50 chars, 100 UTF-16 code units, 200 UTF-8 bytes
#define ZONE_NAME_U16_MAX 128

U_NAMESPACE_BEGIN

/*
 * ZNStringPool    Pool of (UChar *) strings.  Provides for sharing of repeated
 *                 zone strings.
 */
struct ZNStringPoolChunk;
class U_I18N_API ZNStringPool: public UMemory {
  public:
    ZNStringPool(UErrorCode &status);
    ~ZNStringPool();

    /* Get the pooled string that is equal to the supplied string s.
     * Copy the string into the pool if it is not already present.
     *
     * Life time of the returned string is that of the pool.
     */
    const UChar *get(const UChar *s, UErrorCode &status);

    /* Get the pooled string that is equal to the supplied string s.
     * Copy the string into the pool if it is not already present.
     */
    const UChar *get(const UnicodeString &s, UErrorCode &status);

    /* Adopt a string into the pool, without copying it.
     * Used for strings from resource bundles, which will persist without copying.
     */
    const UChar *adopt(const UChar *s, UErrorCode &status);

    /* Freeze the string pool.  Discards the hash table that is used
     * for looking up a string.  All pointers to pooled strings remain valid.
     */
    void freeze();

  private:
    ZNStringPoolChunk   *fChunks;
    UHashtable           *fHash;
};

/*
 * Character node used by TextTrieMap
 */
struct CharacterNode {
    // No constructor or destructor.
    // We malloc and free an uninitalized array of CharacterNode objects
    // and clear and delete them ourselves.

    void clear();
    void deleteValues(UObjectDeleter *valueDeleter);

    void addValue(void *value, UObjectDeleter *valueDeleter, UErrorCode &status);
    inline UBool hasValues() const;
    inline int32_t countValues() const;
    inline const void *getValue(int32_t index) const;

    void     *fValues;      // Union of one single value vs. UVector of values.
    UChar    fCharacter;    // UTF-16 code unit.
    uint16_t fFirstChild;   // 0 if no children.
    uint16_t fNextSibling;  // 0 terminates the list.
    UBool    fHasValuesVector;
    UBool    fPadding;

    // No value:   fValues == NULL               and  fHasValuesVector == FALSE
    // One value:  fValues == value              and  fHasValuesVector == FALSE
    // >=2 values: fValues == UVector of values  and  fHasValuesVector == TRUE
};

inline UBool CharacterNode::hasValues() const {
    return (UBool)(fValues != NULL);
}

inline int32_t CharacterNode::countValues() const {
    return
        fValues == NULL ? 0 :
        !fHasValuesVector ? 1 :
        ((const UVector *)fValues)->size();
}

inline const void *CharacterNode::getValue(int32_t index) const {
    if (!fHasValuesVector) {
        return fValues;  // Assume index == 0.
    } else {
        return ((const UVector *)fValues)->elementAt(index);
    }
}

/*
 * Search result handler callback interface used by TextTrieMap search.
 */
class TextTrieMapSearchResultHandler : public UMemory {
public:
    virtual UBool handleMatch(int32_t matchLength,
                              const CharacterNode *node, UErrorCode& status) = 0;
    virtual ~TextTrieMapSearchResultHandler(); //added to avoid warning
};

/**
 * TextTrieMap is a trie implementation for supporting
 * fast prefix match for the string key.
 */
class U_I18N_API TextTrieMap : public UMemory {
public:
    TextTrieMap(UBool ignoreCase, UObjectDeleter *valeDeleter);
    virtual ~TextTrieMap();

    void put(const UnicodeString &key, void *value, ZNStringPool &sp, UErrorCode &status);
    void put(const UChar*, void *value, UErrorCode &status);
    void search(const UnicodeString &text, int32_t start,
        TextTrieMapSearchResultHandler *handler, UErrorCode& status) const;
    int32_t isEmpty() const;

private:
    UBool           fIgnoreCase;
    CharacterNode   *fNodes;
    int32_t         fNodesCapacity;
    int32_t         fNodesCount;

    UVector         *fLazyContents;
    UBool           fIsEmpty;
    UObjectDeleter  *fValueDeleter;

    UBool growNodes();
    CharacterNode* addChildNode(CharacterNode *parent, UChar c, UErrorCode &status);
    CharacterNode* getChildNode(CharacterNode *parent, UChar c) const;

    void putImpl(const UnicodeString &key, void *value, UErrorCode &status);
    void buildTrie(UErrorCode &status);
    void search(CharacterNode *node, const UnicodeString &text, int32_t start,
        int32_t index, TextTrieMapSearchResultHandler *handler, UErrorCode &status) const;
};



class ZNames;
class TextTrieMap;
class ZNameSearchHandler;

class TimeZoneNamesImpl : public TimeZoneNames {
public:
    TimeZoneNamesImpl(const Locale& locale, UErrorCode& status);

    virtual ~TimeZoneNamesImpl();

    virtual UBool operator==(const TimeZoneNames& other) const;
    virtual TimeZoneNamesImpl* clone() const;

    StringEnumeration* getAvailableMetaZoneIDs(UErrorCode& status) const;
    StringEnumeration* getAvailableMetaZoneIDs(const UnicodeString& tzID, UErrorCode& status) const;

    UnicodeString& getMetaZoneID(const UnicodeString& tzID, UDate date, UnicodeString& mzID) const;
    UnicodeString& getReferenceZoneID(const UnicodeString& mzID, const char* region, UnicodeString& tzID) const;

    UnicodeString& getMetaZoneDisplayName(const UnicodeString& mzID, UTimeZoneNameType type, UnicodeString& name) const;
    UnicodeString& getTimeZoneDisplayName(const UnicodeString& tzID, UTimeZoneNameType type, UnicodeString& name) const;

    UnicodeString& getExemplarLocationName(const UnicodeString& tzID, UnicodeString& name) const;

    TimeZoneNames::MatchInfoCollection* find(const UnicodeString& text, int32_t start, uint32_t types, UErrorCode& status) const;

    void loadAllDisplayNames(UErrorCode& status);
    void getDisplayNames(const UnicodeString& tzID, const UTimeZoneNameType types[], int32_t numTypes, UDate date, UnicodeString dest[], UErrorCode& status) const;

    static UnicodeString& getDefaultExemplarLocationName(const UnicodeString& tzID, UnicodeString& name);

    static StringEnumeration* _getAvailableMetaZoneIDs(UErrorCode& status);
    static StringEnumeration* _getAvailableMetaZoneIDs(const UnicodeString& tzID, UErrorCode& status);
    static UnicodeString& _getMetaZoneID(const UnicodeString& tzID, UDate date, UnicodeString& mzID);
    static UnicodeString& _getReferenceZoneID(const UnicodeString& mzID, const char* region, UnicodeString& tzID);

private:

    Locale fLocale;

    UResourceBundle* fZoneStrings;

    UHashtable* fTZNamesMap;
    UHashtable* fMZNamesMap;

    UBool fNamesTrieFullyLoaded;
    UBool fNamesFullyLoaded;
    TextTrieMap fNamesTrie;

    void initialize(const Locale& locale, UErrorCode& status);
    void cleanup();

    void loadStrings(const UnicodeString& tzCanonicalID, UErrorCode& status);

    ZNames* loadMetaZoneNames(const UnicodeString& mzId, UErrorCode& status);
    ZNames* loadTimeZoneNames(const UnicodeString& mzId, UErrorCode& status);
    TimeZoneNames::MatchInfoCollection* doFind(ZNameSearchHandler& handler,
        const UnicodeString& text, int32_t start, UErrorCode& status) const;
    void addAllNamesIntoTrie(UErrorCode& errorCode);

    void internalLoadAllDisplayNames(UErrorCode& status);

    struct ZoneStringsLoader;
};

class TZDBNames;

class TZDBTimeZoneNames : public TimeZoneNames {
public:
    TZDBTimeZoneNames(const Locale& locale);
    virtual ~TZDBTimeZoneNames();

    virtual UBool operator==(const TimeZoneNames& other) const;
    virtual TZDBTimeZoneNames* clone() const;

    StringEnumeration* getAvailableMetaZoneIDs(UErrorCode& status) const;
    StringEnumeration* getAvailableMetaZoneIDs(const UnicodeString& tzID, UErrorCode& status) const;

    UnicodeString& getMetaZoneID(const UnicodeString& tzID, UDate date, UnicodeString& mzID) const;
    UnicodeString& getReferenceZoneID(const UnicodeString& mzID, const char* region, UnicodeString& tzID) const;

    UnicodeString& getMetaZoneDisplayName(const UnicodeString& mzID, UTimeZoneNameType type, UnicodeString& name) const;
    UnicodeString& getTimeZoneDisplayName(const UnicodeString& tzID, UTimeZoneNameType type, UnicodeString& name) const;

    TimeZoneNames::MatchInfoCollection* find(const UnicodeString& text, int32_t start, uint32_t types, UErrorCode& status) const;

    // When TZDBNames for the metazone is not available, this method returns NULL,
    // but does NOT set U_MISSING_RESOURCE_ERROR to status.
    static const TZDBNames* getMetaZoneNames(const UnicodeString& mzId, UErrorCode& status);

private:
    Locale fLocale;
    char fRegion[ULOC_COUNTRY_CAPACITY];
};

U_NAMESPACE_END

#endif /* #if !UCONFIG_NO_FORMATTING */

#endif // __TZNAMES_IMPL_H__
//eof
//
