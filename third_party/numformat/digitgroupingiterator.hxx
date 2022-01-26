#ifndef INCLUDED_UNOTOOLS_DIGITGROUPINGITERATOR_HXX
#define INCLUDED_UNOTOOLS_DIGITGROUPINGITERATOR_HXX

#include <vector>
#include "define.h"

namespace duckdb_numformat {

/** Iterator to be used with a digit grouping as obtained through
    LocaleDataWrapper::getDigitGrouping().

    The iterator advances over the digit groupings, returning the number of
    digits per group. If the last group was encountered the iterator will
    always return the last grouping.
    
    Grouping values are sanitized to be 0 <= value <= SAL_MAX_UINT16, even if
    originally Int32, to be able to easily cast it down to String's xub_StrLen.
    This shouldn't make any difference in practice.

    Usage example with a string buffer containing a decimal representation of
    an integer number. Note that of course this loop could be optimized to not
    count single characters but hunks of groups instead using the get() method,
    this is just for illustrating usage. Anyway, for double values it is highly
    more efficient to use ::rtl::math::doubleToString() and pass the grouping
    sequence, instead of using this iterator and inserting charcters into
    strings.

    DigitGroupingIterator aGrouping(...)
    sal_Int32 nCount = 0;
    sal_Int32 n = aBuffer.getLength();
    // >1 because we don't want to insert a separator if there is no leading digit.
    while (n-- > 1)
    {
        if (++nCount >= aGrouping.getPos())
        {
            aBuffer.insert( n, cSeparator);
            nGroupDigits = aGrouping.advance();
        }
    }

 */

class DigitGroupingIterator
{
	std::vector<int32_t> maGroupings;
    sal_Int32   mnGroup;        // current active grouping
    sal_Int32   mnDigits;       // current active digits per group
    sal_Int32   mnNextPos;      // position (in digits) of next grouping

    void setInfinite()
    {
        mnGroup = maGroupings.size();
    }

    bool isInfinite() const
    {
        return mnGroup >= (sal_Int32)maGroupings.size();
    }

    sal_Int32 getGrouping() const
    {
        if (mnGroup < (sal_Int32)maGroupings.size())
        {
            sal_Int32 n = maGroupings[mnGroup];
            //OSL_ENSURE( 0 <= n && n <= SAL_MAX_UINT16, "DigitGroupingIterator::getGrouping: far out");
            if (n < 0)
                n = 0;                  // sanitize ...
            else if (n > SAL_MAX_UINT16)
                n = SAL_MAX_UINT16;     // limit for use with xub_StrLen
            return n;
        }
        return 0;
    }

    void setPos()
    {
        // someone might be playing jokes on us, so check for overflow
        if (mnNextPos <= SAL_MAX_INT32 - mnDigits)
            mnNextPos += mnDigits;
    }

    void setDigits()
    {
        sal_Int32 nPrev = mnDigits;
        mnDigits = getGrouping();
        if (!mnDigits)
        {
            mnDigits = nPrev;
            setInfinite();
        }
        setPos();
    }

    void initGrouping()
    {
        mnDigits = 3;       // just in case of constructed with empty grouping
        mnGroup = 0;
        mnNextPos = 0;
        setDigits();
    }

    // not implemented, prevent usage
    DigitGroupingIterator();
    DigitGroupingIterator( const DigitGroupingIterator & );
    DigitGroupingIterator & operator=( const DigitGroupingIterator & );

public:

    explicit DigitGroupingIterator(std::vector<int32_t>& digit_grouping)
        : maGroupings(digit_grouping)
    {
        initGrouping();
    }

    /** Advance iterator to next grouping. */
    DigitGroupingIterator & advance()
    {
        if (isInfinite())
            setPos();
        else
        {
            ++mnGroup;
            setDigits();
        }
        return *this;
    }

    /** Obtain current grouping. Always > 0. */
    sal_Int32 get() const
    {
        return mnDigits;
    }

    /** The next position (in integer digits) from the right where to insert a
        group separator. */
    sal_Int32 getPos()
    {
        return mnNextPos;
    }

    /** Reset iterator to start again from the right beginning. */
    void reset()
    {
        initGrouping();
    }
};
}   // namespace duckdb_numformat

#endif // INCLUDED_UNOTOOLS_DIGITGROUPINGITERATOR_HXX
