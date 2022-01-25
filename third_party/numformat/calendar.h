#ifndef _CALENDAR_H
#define _CALENDAR_H

#include "define.h"
#include <string>
#include "datetime.hxx"
#include "date.hxx"
#include "time.hxx"
#include "localedata.h"

namespace duckdb_numformat {

//	----------------------------------------------------
//	class Calendar
//	----------------------------------------------------

struct Era {
    sal_Int32 year;
    sal_Int32 month;
    sal_Int32 day;
};

const sal_Int16 FIELD_INDEX_COUNT = CalendarFieldIndex::FIELD_COUNT2;

class LocaleData;

class Calendar
{
public:

    // Constructors
    Calendar(LocaleData* pFormatter);
    Calendar(Era *_eraArray);
    void  init(Era *_eraArray);

    /**
    * Destructor
    */
    ~Calendar();

    void  setValue( sal_Int16 nFieldIndex, sal_Int16 nValue );
    sal_Int16  getValue(sal_Int16 nFieldIndex);
    sal_Bool  isValid();
    std::wstring  getDisplayName(sal_Int16 nCalendarDisplayIndex, sal_Int16 nIdx, sal_Int16 nNameType);

    // Methods in XExtendedCalendar
    std::wstring  getDisplayString( sal_Int32 nCalendarDisplayCode, sal_Int16 nNativeNumberMode );
	void setDateTime(double timeInDays);
	double getDateTime();
	double getLocalDateTime();
	void setLocalDateTime(double nTimeInDays);
	DateTime getEpochStart() { return aEpochStart; }
	Date* GetNullDate() const { return pNullDate; }
	inline void setGregorianDateTime(const DateTime& rDateTime) { setLocalDateTime(rDateTime - aEpochStart); }
	/// set reference date for offset calculation
	void ChangeNullDate(const sal_uInt16 nDay, const sal_uInt16 nMonth,	const sal_uInt16 nYear);	// exchanges reference date


protected:
    Era *eraArray;
    //NativeNumberSupplier aNatNum;
    sal_uInt32 fieldSet;
    sal_Int16 fieldValue[FIELD_INDEX_COUNT];

private:
	LocaleData* pFormatter;
    //Calendar aCalendar;

	DateTime aEpochStart;
	Date* pNullDate;							// 30Dec1899
	double timeInDays;

    /** Submit fieldSetValue array according to fieldSet. */
    void submitFields();
    /** Submit fieldSetValue array according to fieldSet, plus YMDhms if >=0,
        plus zone and DST if != 0 */
    void submitValues( sal_Int32 nYear, sal_Int32 nMonth, sal_Int32 nDay, sal_Int32 nHour, sal_Int32 nMinute, sal_Int32 nSecond, sal_Int32 nMilliSecond, sal_Int32 nZone, sal_Int32 nDST);
    /** Obtain combined field values for timezone offset (minutes+secondmillis)
        in milliseconds and whether fields were set. */
    bool getZoneOffset( sal_Int32 & o_nOffset ) const;
    /** Obtain combined field values for DST offset (minutes+secondmillis) in
        milliseconds and whether fields were set. */
    bool getDSTOffset( sal_Int32 & o_nOffset ) const;
    /** Used by getZoneOffset() and getDSTOffset(). Parent is
        CalendarFieldIndex for offset in minutes, child is CalendarFieldIndex
        for offset in milliseconds. */
    bool getCombinedOffset( sal_Int32 & o_nOffset, sal_Int16 nParentFieldIndex, sal_Int16 nChildFieldIndex ) const;
	sal_Int32 getCombinedOffsetInMillis(sal_Int16 nParentFieldIndex, sal_Int16 nChildFieldIndex);
	sal_Int32 getZoneOffsetInMillis();
	sal_Int32 getDSTOffsetInMillis();
};
}   // namespace duckdb_numformat

#endif
