from .duckdb import FunctionExpression

def approx_quantile(x, pos, /) -> FunctionExpression:
    """Computes the approximate quantile using T-Digest. Function category: Holistic"""
    return FunctionExpression("approx_quantile", x, pos)

def mad(x, /) -> FunctionExpression:
    """Returns the median absolute deviation for the values within x. NULL values are
    ignored. Temporal types return a positive INTERVAL. Function category:
    Holistic"""
    return FunctionExpression("mad", x)

def median(x, /) -> FunctionExpression:
    """Returns the middle value of the set. NULL values are ignored. For even value
    counts, quantitiative values are averaged and ordinal values return the
    lower value. Function category: Holistic"""
    return FunctionExpression("median", x)

def mode(x, /) -> FunctionExpression:
    """Returns the most frequent value for the values within x. NULL values are
    ignored. Function category: Holistic"""
    return FunctionExpression("mode", x)

def quantile_disc(x, pos, /) -> FunctionExpression:
    """Returns the exact quantile number between 0 and 1 . If pos is a LIST of FLOATs,
    then the result is a LIST of the corresponding exact quantiles. Function
    category: Holistic"""
    return FunctionExpression("quantile_disc", x, pos)

def quantile(x, pos, /) -> FunctionExpression:
    """Returns the exact quantile number between 0 and 1 . If pos is a LIST of FLOATs,
    then the result is a LIST of the corresponding exact quantiles. Alias for
    quantile_disc. Function category: Holistic"""
    return FunctionExpression("quantile", x, pos)

def quantile_cont(x, pos, /) -> FunctionExpression:
    """Returns the intepolated quantile number between 0 and 1 . If pos is a LIST of
    FLOATs, then the result is a LIST of the corresponding intepolated
    quantiles. Function category: Holistic"""
    return FunctionExpression("quantile_cont", x, pos)

def reservoir_quantile(x, quantile, sample_size, /) -> FunctionExpression:
    """Gives the approximate quantile using reservoir sampling, the sample size is
    optional and uses 8192 as a default size. Function category: Holistic"""
    return FunctionExpression("reservoir_quantile", x, quantile, sample_size)

def approx_count_distinct(x, /) -> FunctionExpression:
    """Computes the approximate count of distinct elements using HyperLogLog. Function
    category: Distributive"""
    return FunctionExpression("approx_count_distinct", x)

def arg_min(arg, val, /) -> FunctionExpression:
    """Finds the row with the minimum val. Calculates the non-NULL arg expression at
    that row. Function category: Distributive"""
    return FunctionExpression("arg_min", arg, val)

def argmin(arg, val, /) -> FunctionExpression:
    """Finds the row with the minimum val. Calculates the non-NULL arg expression at
    that row. Alias for arg_min. Function category: Distributive"""
    return FunctionExpression("argmin", arg, val)

def min_by(arg, val, /) -> FunctionExpression:
    """Finds the row with the minimum val. Calculates the non-NULL arg expression at
    that row. Alias for arg_min. Function category: Distributive"""
    return FunctionExpression("min_by", arg, val)

def arg_min_null(arg, val, /) -> FunctionExpression:
    """Finds the row with the minimum val. Calculates the arg expression at that row.
    Function category: Distributive"""
    return FunctionExpression("arg_min_null", arg, val)

def arg_max(arg, val, /) -> FunctionExpression:
    """Finds the row with the maximum val. Calculates the non-NULL arg expression at
    that row. Function category: Distributive"""
    return FunctionExpression("arg_max", arg, val)

def argmax(arg, val, /) -> FunctionExpression:
    """Finds the row with the maximum val. Calculates the non-NULL arg expression at
    that row. Alias for arg_max. Function category: Distributive"""
    return FunctionExpression("argmax", arg, val)

def max_by(arg, val, /) -> FunctionExpression:
    """Finds the row with the maximum val. Calculates the non-NULL arg expression at
    that row. Alias for arg_max. Function category: Distributive"""
    return FunctionExpression("max_by", arg, val)

def arg_max_null(arg, val, /) -> FunctionExpression:
    """Finds the row with the maximum val. Calculates the arg expression at that row.
    Function category: Distributive"""
    return FunctionExpression("arg_max_null", arg, val)

def bit_and(arg, /) -> FunctionExpression:
    """Returns the bitwise AND of all bits in a given expression. Function category:
    Distributive"""
    return FunctionExpression("bit_and", arg)

def bit_or(arg, /) -> FunctionExpression:
    """Returns the bitwise OR of all bits in a given expression. Function category:
    Distributive"""
    return FunctionExpression("bit_or", arg)

def bit_xor(arg, /) -> FunctionExpression:
    """Returns the bitwise XOR of all bits in a given expression. Function category:
    Distributive"""
    return FunctionExpression("bit_xor", arg)

def bitstring_agg(arg, /) -> FunctionExpression:
    """Returns a bitstring with bits set for each distinct value. Function category:
    Distributive"""
    return FunctionExpression("bitstring_agg", arg)

def bool_and(arg, /) -> FunctionExpression:
    """Returns TRUE if every input value is TRUE, otherwise FALSE. Function category:
    Distributive"""
    return FunctionExpression("bool_and", arg)

def bool_or(arg, /) -> FunctionExpression:
    """Returns TRUE if any input value is TRUE, otherwise FALSE. Function category:
    Distributive"""
    return FunctionExpression("bool_or", arg)

def entropy(x, /) -> FunctionExpression:
    """Returns the log-2 entropy of count input-values. Function category: Distributive"""
    return FunctionExpression("entropy", x)

def kahan_sum(arg, /) -> FunctionExpression:
    """Calculates the sum using a more accurate floating point summation (Kahan Sum).
    Function category: Distributive"""
    return FunctionExpression("kahan_sum", arg)

def fsum(arg, /) -> FunctionExpression:
    """Calculates the sum using a more accurate floating point summation (Kahan Sum).
    Alias for kahan_sum. Function category: Distributive"""
    return FunctionExpression("fsum", arg)

def sumkahan(arg, /) -> FunctionExpression:
    """Calculates the sum using a more accurate floating point summation (Kahan Sum).
    Alias for kahan_sum. Function category: Distributive"""
    return FunctionExpression("sumkahan", arg)

def kurtosis(x, /) -> FunctionExpression:
    """Returns the excess kurtosis (Fisher’s definition) of all input values, with a
    bias correction according to the sample size. Function category:
    Distributive"""
    return FunctionExpression("kurtosis", x)

def kurtosis_pop(x, /) -> FunctionExpression:
    """Returns the excess kurtosis (Fisher’s definition) of all input values, without
    bias correction. Function category: Distributive"""
    return FunctionExpression("kurtosis_pop", x)

def min(arg, /) -> FunctionExpression:
    """Returns the minimum value present in arg. Function category: Distributive"""
    return FunctionExpression("min", arg)

def max(arg, /) -> FunctionExpression:
    """Returns the maximum value present in arg. Function category: Distributive"""
    return FunctionExpression("max", arg)

def product(arg, /) -> FunctionExpression:
    """Calculates the product of all tuples in arg. Function category: Distributive"""
    return FunctionExpression("product", arg)

def skewness(x, /) -> FunctionExpression:
    """Returns the skewness of all input values. Function category: Distributive"""
    return FunctionExpression("skewness", x)

def string_agg(str, arg, /) -> FunctionExpression:
    """Concatenates the column string values with an optional separator. Function
    category: Distributive"""
    return FunctionExpression("string_agg", str, arg)

def group_concat(str, arg, /) -> FunctionExpression:
    """Concatenates the column string values with an optional separator. Alias for
    string_agg. Function category: Distributive"""
    return FunctionExpression("group_concat", str, arg)

def listagg(str, arg, /) -> FunctionExpression:
    """Concatenates the column string values with an optional separator. Alias for
    string_agg. Function category: Distributive"""
    return FunctionExpression("listagg", str, arg)

def sum(arg, /) -> FunctionExpression:
    """Calculates the sum value for all tuples in arg. Function category: Distributive"""
    return FunctionExpression("sum", arg)

def sum_no_overflow(arg, /) -> FunctionExpression:
    """Internal only. Calculates the sum value for all tuples in arg without overflow
    checks. Function category: Distributive"""
    return FunctionExpression("sum_no_overflow", arg)

def avg(x, /) -> FunctionExpression:
    """Calculates the average value for all tuples in x. Function category: Algebraic"""
    return FunctionExpression("avg", x)

def mean(x, /) -> FunctionExpression:
    """Calculates the average value for all tuples in x. Alias for avg. Function
    category: Algebraic"""
    return FunctionExpression("mean", x)

def corr(y, x, /) -> FunctionExpression:
    """Returns the correlation coefficient for non-null pairs in a group. Function
    category: Algebraic"""
    return FunctionExpression("corr", y, x)

def covar_pop(y, x, /) -> FunctionExpression:
    """Returns the population covariance of input values. Function category: Algebraic"""
    return FunctionExpression("covar_pop", y, x)

def covar_samp(y, x, /) -> FunctionExpression:
    """Returns the sample covariance for non-null pairs in a group. Function category:
    Algebraic"""
    return FunctionExpression("covar_samp", y, x)

def favg(x, /) -> FunctionExpression:
    """Calculates the average using a more accurate floating point summation (Kahan
    Sum). Function category: Algebraic"""
    return FunctionExpression("favg", x)

def sem(x, /) -> FunctionExpression:
    """Returns the standard error of the mean. Function category: Algebraic"""
    return FunctionExpression("sem", x)

def stddev_pop(x, /) -> FunctionExpression:
    """Returns the population standard deviation. Function category: Algebraic"""
    return FunctionExpression("stddev_pop", x)

def stddev_samp(x, /) -> FunctionExpression:
    """Returns the sample standard deviation. Function category: Algebraic"""
    return FunctionExpression("stddev_samp", x)

def stddev(x, /) -> FunctionExpression:
    """Returns the sample standard deviation Alias for stddev_samp. Function category:
    Algebraic"""
    return FunctionExpression("stddev", x)

def var_pop(x, /) -> FunctionExpression:
    """Returns the population variance. Function category: Algebraic"""
    return FunctionExpression("var_pop", x)

def var_samp(x, /) -> FunctionExpression:
    """Returns the sample variance of all input values. Function category: Algebraic"""
    return FunctionExpression("var_samp", x)

def variance(x, /) -> FunctionExpression:
    """Returns the sample variance of all input values. Alias for var_samp. Function
    category: Algebraic"""
    return FunctionExpression("variance", x)

def regr_avgx(y, x, /) -> FunctionExpression:
    """Returns the average of the independent variable for non-null pairs in a group,
    where x is the independent variable and y is the dependent variable.
    Function category: Regression"""
    return FunctionExpression("regr_avgx", y, x)

def regr_avgy(y, x, /) -> FunctionExpression:
    """Returns the average of the dependent variable for non-null pairs in a group,
    where x is the independent variable and y is the dependent variable.
    Function category: Regression"""
    return FunctionExpression("regr_avgy", y, x)

def regr_count(y, x, /) -> FunctionExpression:
    """Returns the number of non-null number pairs in a group. Function category:
    Regression"""
    return FunctionExpression("regr_count", y, x)

def regr_intercept(y, x, /) -> FunctionExpression:
    """Returns the intercept of the univariate linear regression line for non-null
    pairs in a group. Function category: Regression"""
    return FunctionExpression("regr_intercept", y, x)

def regr_r2(y, x, /) -> FunctionExpression:
    """Returns the coefficient of determination for non-null pairs in a group. Function
    category: Regression"""
    return FunctionExpression("regr_r2", y, x)

def regr_slope(y, x, /) -> FunctionExpression:
    """Returns the slope of the linear regression line for non-null pairs in a group.
    Function category: Regression"""
    return FunctionExpression("regr_slope", y, x)

def regr_sxx(y, x, /) -> FunctionExpression:
    """Function category: Regression"""
    return FunctionExpression("regr_sxx", y, x)

def regr_sxy(y, x, /) -> FunctionExpression:
    """Returns the population covariance of input values. Function category: Regression"""
    return FunctionExpression("regr_sxy", y, x)

def regr_syy(y, x, /) -> FunctionExpression:
    """Function category: Regression"""
    return FunctionExpression("regr_syy", y, x)

def histogram(arg, /) -> FunctionExpression:
    """Returns a LIST of STRUCTs with the fields bucket and count. Function category:
    Nested"""
    return FunctionExpression("histogram", arg)

def list(arg, /) -> FunctionExpression:
    """Returns a LIST containing all the values of a column. Function category: Nested"""
    return FunctionExpression("list", arg)

def array_agg(arg, /) -> FunctionExpression:
    """Returns a LIST containing all the values of a column. Alias for list. Function
    category: Nested"""
    return FunctionExpression("array_agg", arg)

def get_bit(bitstring, index, /) -> FunctionExpression:
    """Extracts the nth bit from bitstring; the first (leftmost) bit is indexed 0.
    Function category: Bit"""
    return FunctionExpression("get_bit", bitstring, index)

def set_bit(bitstring, index, new_value, /) -> FunctionExpression:
    """Sets the nth bit in bitstring to newvalue; the first (leftmost) bit is indexed
    0. Returns a new bitstring. Function category: Bit"""
    return FunctionExpression("set_bit", bitstring, index, new_value)

def bit_position(substring, bitstring, /) -> FunctionExpression:
    """Returns first starting index of the specified substring within bits, or zero if
    it is not present. The first (leftmost) bit is indexed 1. Function category:
    Bit"""
    return FunctionExpression("bit_position", substring, bitstring)

def bitstring(bitstring, length, /) -> FunctionExpression:
    """Pads the bitstring until the specified length. Function category: Bit"""
    return FunctionExpression("bitstring", bitstring, length)

def xor(left, right, /) -> FunctionExpression:
    """Bitwise XOR. Function category: Operators"""
    return FunctionExpression("xor", left, right)

def array_value(any, /, *args) -> FunctionExpression:
    """Create an ARRAY containing the argument values. Function category: Array"""
    return FunctionExpression("array_value", any, *args)

def array_cross_product(array1, array2, /) -> FunctionExpression:
    """Compute the cross product of two arrays of size 3. The array elements can not be
    NULL. Function category: Array"""
    return FunctionExpression("array_cross_product", array1, array2)

def array_cosine_similarity(array1, array2, /) -> FunctionExpression:
    """Compute the cosine similarity between two arrays of the same size. The array
    elements can not be NULL. The arrays can have any size as long as the size
    is the same for both arguments. Function category: Array"""
    return FunctionExpression("array_cosine_similarity", array1, array2)

def array_distance(array1, array2, /) -> FunctionExpression:
    """Compute the distance between two arrays of the same size. The array elements can
    not be NULL. The arrays can have any size as long as the size is the same
    for both arguments. Function category: Array"""
    return FunctionExpression("array_distance", array1, array2)

def array_inner_product(array1, array2, /) -> FunctionExpression:
    """Compute the inner product between two arrays of the same size. The array
    elements can not be NULL. The arrays can have any size as long as the size
    is the same for both arguments. Function category: Array"""
    return FunctionExpression("array_inner_product", array1, array2)

def array_dot_product(array1, array2, /) -> FunctionExpression:
    """Compute the inner product between two arrays of the same size. The array
    elements can not be NULL. The arrays can have any size as long as the size
    is the same for both arguments. Alias for array_inner_product. Function
    category: Array"""
    return FunctionExpression("array_dot_product", array1, array2)

def age(timestamp1, timestamp2, /) -> FunctionExpression:
    """Subtract arguments, resulting in the time difference between the two timestamps.
    Function category: Date"""
    return FunctionExpression("age", timestamp1, timestamp2)

def century(ts, /) -> FunctionExpression:
    """Extract the century component from a date or timestamp. Function category: Date"""
    return FunctionExpression("century", ts)

def current_date() -> FunctionExpression:
    """Returns the current date. Function category: Date"""
    return FunctionExpression("current_date", )

def today() -> FunctionExpression:
    """Returns the current date Alias for current_date. Function category: Date"""
    return FunctionExpression("today", )

def date_diff(part, startdate, enddate, /) -> FunctionExpression:
    """The number of partition boundaries between the timestamps. Function category:
    Date"""
    return FunctionExpression("date_diff", part, startdate, enddate)

def datediff(part, startdate, enddate, /) -> FunctionExpression:
    """The number of partition boundaries between the timestamps Alias for date_diff.
    Function category: Date"""
    return FunctionExpression("datediff", part, startdate, enddate)

def date_part(ts, /) -> FunctionExpression:
    """Get subfield (equivalent to extract). Function category: Date"""
    return FunctionExpression("date_part", ts)

def datepart(ts, /) -> FunctionExpression:
    """Get subfield (equivalent to extract) Alias for date_part. Function category:
    Date"""
    return FunctionExpression("datepart", ts)

def date_sub(part, startdate, enddate, /) -> FunctionExpression:
    """The number of complete partitions between the timestamps. Function category:
    Date"""
    return FunctionExpression("date_sub", part, startdate, enddate)

def datesub(part, startdate, enddate, /) -> FunctionExpression:
    """The number of complete partitions between the timestamps Alias for date_sub.
    Function category: Date"""
    return FunctionExpression("datesub", part, startdate, enddate)

def date_trunc(part, timestamp, /) -> FunctionExpression:
    """Truncate to specified precision. Function category: Date"""
    return FunctionExpression("date_trunc", part, timestamp)

def datetrunc(part, timestamp, /) -> FunctionExpression:
    """Truncate to specified precision Alias for date_trunc. Function category: Date"""
    return FunctionExpression("datetrunc", part, timestamp)

def day(ts, /) -> FunctionExpression:
    """Extract the day component from a date or timestamp. Function category: Date"""
    return FunctionExpression("day", ts)

def dayname(ts, /) -> FunctionExpression:
    """The (English) name of the weekday. Function category: Date"""
    return FunctionExpression("dayname", ts)

def dayofmonth(ts, /) -> FunctionExpression:
    """Extract the dayofmonth component from a date or timestamp. Function category:
    Date"""
    return FunctionExpression("dayofmonth", ts)

def dayofweek(ts, /) -> FunctionExpression:
    """Extract the dayofweek component from a date or timestamp. Function category:
    Date"""
    return FunctionExpression("dayofweek", ts)

def dayofyear(ts, /) -> FunctionExpression:
    """Extract the dayofyear component from a date or timestamp. Function category:
    Date"""
    return FunctionExpression("dayofyear", ts)

def decade(ts, /) -> FunctionExpression:
    """Extract the decade component from a date or timestamp. Function category: Date"""
    return FunctionExpression("decade", ts)

def epoch(temporal, /) -> FunctionExpression:
    """Extract the epoch component from a temporal type. Function category: Date"""
    return FunctionExpression("epoch", temporal)

def epoch_ms(temporal, /) -> FunctionExpression:
    """Extract the epoch component in milliseconds from a temporal type. Function
    category: Date"""
    return FunctionExpression("epoch_ms", temporal)

def epoch_us(temporal, /) -> FunctionExpression:
    """Extract the epoch component in microseconds from a temporal type. Function
    category: Date"""
    return FunctionExpression("epoch_us", temporal)

def epoch_ns(temporal, /) -> FunctionExpression:
    """Extract the epoch component in nanoseconds from a temporal type. Function
    category: Date"""
    return FunctionExpression("epoch_ns", temporal)

def era(ts, /) -> FunctionExpression:
    """Extract the era component from a date or timestamp. Function category: Date"""
    return FunctionExpression("era", ts)

def get_current_time() -> FunctionExpression:
    """Returns the current time. Function category: Date"""
    return FunctionExpression("get_current_time", )

def get_current_timestamp() -> FunctionExpression:
    """Returns the current timestamp. Function category: Date"""
    return FunctionExpression("get_current_timestamp", )

def now() -> FunctionExpression:
    """Returns the current timestamp Alias for get_current_timestamp. Function
    category: Date"""
    return FunctionExpression("now", )

def transaction_timestamp() -> FunctionExpression:
    """Returns the current timestamp Alias for get_current_timestamp. Function
    category: Date"""
    return FunctionExpression("transaction_timestamp", )

def hour(ts, /) -> FunctionExpression:
    """Extract the hour component from a date or timestamp. Function category: Date"""
    return FunctionExpression("hour", ts)

def isodow(ts, /) -> FunctionExpression:
    """Extract the isodow component from a date or timestamp. Function category: Date"""
    return FunctionExpression("isodow", ts)

def isoyear(ts, /) -> FunctionExpression:
    """Extract the isoyear component from a date or timestamp. Function category: Date"""
    return FunctionExpression("isoyear", ts)

def julian(ts, /) -> FunctionExpression:
    """Extract the Julian Day number from a date or timestamp. Function category: Date"""
    return FunctionExpression("julian", ts)

def last_day(ts, /) -> FunctionExpression:
    """Returns the last day of the month. Function category: Date"""
    return FunctionExpression("last_day", ts)

def make_date(year, month, day, /) -> FunctionExpression:
    """The date for the given parts. Function category: Date"""
    return FunctionExpression("make_date", year, month, day)

def make_time(hour, minute, seconds, /) -> FunctionExpression:
    """The time for the given parts. Function category: Date"""
    return FunctionExpression("make_time", hour, minute, seconds)

def make_timestamp(year, month, day, hour, minute, seconds, /) -> FunctionExpression:
    """The timestamp for the given parts. Function category: Date"""
    return FunctionExpression("make_timestamp", year, month, day, hour, minute, seconds)

def microsecond(ts, /) -> FunctionExpression:
    """Extract the microsecond component from a date or timestamp. Function category:
    Date"""
    return FunctionExpression("microsecond", ts)

def millennium(ts, /) -> FunctionExpression:
    """Extract the millennium component from a date or timestamp. Function category:
    Date"""
    return FunctionExpression("millennium", ts)

def millisecond(ts, /) -> FunctionExpression:
    """Extract the millisecond component from a date or timestamp. Function category:
    Date"""
    return FunctionExpression("millisecond", ts)

def minute(ts, /) -> FunctionExpression:
    """Extract the minute component from a date or timestamp. Function category: Date"""
    return FunctionExpression("minute", ts)

def month(ts, /) -> FunctionExpression:
    """Extract the month component from a date or timestamp. Function category: Date"""
    return FunctionExpression("month", ts)

def monthname(ts, /) -> FunctionExpression:
    """The (English) name of the month. Function category: Date"""
    return FunctionExpression("monthname", ts)

def quarter(ts, /) -> FunctionExpression:
    """Extract the quarter component from a date or timestamp. Function category: Date"""
    return FunctionExpression("quarter", ts)

def second(ts, /) -> FunctionExpression:
    """Extract the second component from a date or timestamp. Function category: Date"""
    return FunctionExpression("second", ts)

def strftime(text, format, /) -> FunctionExpression:
    """Converts timestamp to string according to the format string. Function category:
    Date"""
    return FunctionExpression("strftime", text, format)

def strptime(text, format, /) -> FunctionExpression:
    """Converts string to timestamp with time zone according to the format string if %Z
    is specified. Function category: Date"""
    return FunctionExpression("strptime", text, format)

def time_bucket(bucket_width, timestamp, origin, /) -> FunctionExpression:
    """Truncate TIMESTAMPTZ by the specified interval bucket_width. Buckets are aligned
    relative to origin TIMESTAMPTZ. The origin defaults to 2000-01-03
    00:00:00+00 for buckets that do not include a month or year interval, and to
    2000-01-01 00:00:00+00 for month and year buckets. Function category: Date"""
    return FunctionExpression("time_bucket", bucket_width, timestamp, origin)

def timezone(ts, /) -> FunctionExpression:
    """Extract the timezone component from a date or timestamp. Function category: Date"""
    return FunctionExpression("timezone", ts)

def timezone_hour(ts, /) -> FunctionExpression:
    """Extract the timezone_hour component from a date or timestamp. Function category:
    Date"""
    return FunctionExpression("timezone_hour", ts)

def timezone_minute(ts, /) -> FunctionExpression:
    """Extract the timezone_minute component from a date or timestamp. Function
    category: Date"""
    return FunctionExpression("timezone_minute", ts)

def timetz_byte_comparable(time_tz, /) -> FunctionExpression:
    """Converts a TIME WITH TIME ZONE to an integer sort key. Function category: Date"""
    return FunctionExpression("timetz_byte_comparable", time_tz)

def to_centuries(integer, /) -> FunctionExpression:
    """Construct a century interval. Function category: Date"""
    return FunctionExpression("to_centuries", integer)

def to_days(integer, /) -> FunctionExpression:
    """Construct a day interval. Function category: Date"""
    return FunctionExpression("to_days", integer)

def to_decades(integer, /) -> FunctionExpression:
    """Construct a decade interval. Function category: Date"""
    return FunctionExpression("to_decades", integer)

def to_hours(integer, /) -> FunctionExpression:
    """Construct a hour interval. Function category: Date"""
    return FunctionExpression("to_hours", integer)

def to_microseconds(integer, /) -> FunctionExpression:
    """Construct a microsecond interval. Function category: Date"""
    return FunctionExpression("to_microseconds", integer)

def to_millennia(integer, /) -> FunctionExpression:
    """Construct a millenium interval. Function category: Date"""
    return FunctionExpression("to_millennia", integer)

def to_milliseconds(double, /) -> FunctionExpression:
    """Construct a millisecond interval. Function category: Date"""
    return FunctionExpression("to_milliseconds", double)

def to_minutes(integer, /) -> FunctionExpression:
    """Construct a minute interval. Function category: Date"""
    return FunctionExpression("to_minutes", integer)

def to_months(integer, /) -> FunctionExpression:
    """Construct a month interval. Function category: Date"""
    return FunctionExpression("to_months", integer)

def to_quarters(integer, /) -> FunctionExpression:
    """Construct a quarter interval. Function category: Date"""
    return FunctionExpression("to_quarters", integer)

def to_seconds(double, /) -> FunctionExpression:
    """Construct a second interval. Function category: Date"""
    return FunctionExpression("to_seconds", double)

def to_timestamp(sec, /) -> FunctionExpression:
    """Converts secs since epoch to a timestamp with time zone. Function category: Date"""
    return FunctionExpression("to_timestamp", sec)

def to_weeks(integer, /) -> FunctionExpression:
    """Construct a week interval. Function category: Date"""
    return FunctionExpression("to_weeks", integer)

def to_years(integer, /) -> FunctionExpression:
    """Construct a year interval. Function category: Date"""
    return FunctionExpression("to_years", integer)

def try_strptime(text, format, /) -> FunctionExpression:
    """Converts string to timestamp using the format string (timestamp with time zone
    if %Z is specified). Returns NULL on failure. Function category: Date"""
    return FunctionExpression("try_strptime", text, format)

def week(ts, /) -> FunctionExpression:
    """Extract the week component from a date or timestamp. Function category: Date"""
    return FunctionExpression("week", ts)

def weekday(ts, /) -> FunctionExpression:
    """Extract the weekday component from a date or timestamp. Function category: Date"""
    return FunctionExpression("weekday", ts)

def weekofyear(ts, /) -> FunctionExpression:
    """Extract the weekofyear component from a date or timestamp. Function category:
    Date"""
    return FunctionExpression("weekofyear", ts)

def year(ts, /) -> FunctionExpression:
    """Extract the year component from a date or timestamp. Function category: Date"""
    return FunctionExpression("year", ts)

def yearweek(ts, /) -> FunctionExpression:
    """Extract the yearweek component from a date or timestamp. Function category: Date"""
    return FunctionExpression("yearweek", ts)

def enum_first(enum, /) -> FunctionExpression:
    """Returns the first value of the input enum type. Function category: Enum"""
    return FunctionExpression("enum_first", enum)

def enum_last(enum, /) -> FunctionExpression:
    """Returns the last value of the input enum type. Function category: Enum"""
    return FunctionExpression("enum_last", enum)

def enum_code(enum, /) -> FunctionExpression:
    """Returns the numeric value backing the given enum value. Function category: Enum"""
    return FunctionExpression("enum_code", enum)

def enum_range(enum, /) -> FunctionExpression:
    """Returns all values of the input enum type as an array. Function category: Enum"""
    return FunctionExpression("enum_range", enum)

def enum_range_boundary(start, end, /) -> FunctionExpression:
    """Returns the range between the two given enum values as an array. The values must
    be of the same enum type. When the first parameter is NULL, the result
    starts with the first value of the enum type. When the second parameter is
    NULL, the result ends with the last value of the enum type. Function
    category: Enum"""
    return FunctionExpression("enum_range_boundary", start, end)

def abs(x, /) -> FunctionExpression:
    """Absolute value Alias for @. Function category: Math"""
    return FunctionExpression("abs", x)

def pow(x, y, /) -> FunctionExpression:
    """Computes x to the power of y Alias for **. Function category: Math"""
    return FunctionExpression("pow", x, y)

def power(x, y, /) -> FunctionExpression:
    """Computes x to the power of y Alias for **. Function category: Math"""
    return FunctionExpression("power", x, y)

def factorial(x, /) -> FunctionExpression:
    """Factorial of x. Computes the product of the current integer and all integers
    below it Alias for !. Function category: Math"""
    return FunctionExpression("factorial", x)

def acos(x, /) -> FunctionExpression:
    """Computes the arccosine of x. Function category: Math"""
    return FunctionExpression("acos", x)

def asin(x, /) -> FunctionExpression:
    """Computes the arcsine of x. Function category: Math"""
    return FunctionExpression("asin", x)

def atan(x, /) -> FunctionExpression:
    """Computes the arctangent of x. Function category: Math"""
    return FunctionExpression("atan", x)

def atan2(y, x, /) -> FunctionExpression:
    """Computes the arctangent (y, x). Function category: Math"""
    return FunctionExpression("atan2", y, x)

def bit_count(x, /) -> FunctionExpression:
    """Returns the number of bits that are set. Function category: Math"""
    return FunctionExpression("bit_count", x)

def cbrt(x, /) -> FunctionExpression:
    """Returns the cube root of x. Function category: Math"""
    return FunctionExpression("cbrt", x)

def ceil(x, /) -> FunctionExpression:
    """Rounds the number up. Function category: Math"""
    return FunctionExpression("ceil", x)

def ceiling(x, /) -> FunctionExpression:
    """Rounds the number up Alias for ceil. Function category: Math"""
    return FunctionExpression("ceiling", x)

def cos(x, /) -> FunctionExpression:
    """Computes the cos of x. Function category: Math"""
    return FunctionExpression("cos", x)

def cot(x, /) -> FunctionExpression:
    """Computes the cotangent of x. Function category: Math"""
    return FunctionExpression("cot", x)

def degrees(x, /) -> FunctionExpression:
    """Converts radians to degrees. Function category: Math"""
    return FunctionExpression("degrees", x)

def even(x, /) -> FunctionExpression:
    """Rounds x to next even number by rounding away from zero. Function category: Math"""
    return FunctionExpression("even", x)

def exp(x, /) -> FunctionExpression:
    """Computes e to the power of x. Function category: Math"""
    return FunctionExpression("exp", x)

def floor(x, /) -> FunctionExpression:
    """Rounds the number down. Function category: Math"""
    return FunctionExpression("floor", x)

def isfinite(x, /) -> FunctionExpression:
    """Returns true if the floating point value is finite, false otherwise. Function
    category: Math"""
    return FunctionExpression("isfinite", x)

def isinf(x, /) -> FunctionExpression:
    """Returns true if the floating point value is infinite, false otherwise. Function
    category: Math"""
    return FunctionExpression("isinf", x)

def isnan(x, /) -> FunctionExpression:
    """Returns true if the floating point value is not a number, false otherwise.
    Function category: Math"""
    return FunctionExpression("isnan", x)

def gamma(x, /) -> FunctionExpression:
    """Interpolation of (x-1) factorial (so decimal inputs are allowed). Function
    category: Math"""
    return FunctionExpression("gamma", x)

def greatest_common_divisor(x, y, /) -> FunctionExpression:
    """Computes the greatest common divisor of x and y. Function category: Math"""
    return FunctionExpression("greatest_common_divisor", x, y)

def gcd(x, y, /) -> FunctionExpression:
    """Computes the greatest common divisor of x and y Alias for
    greatest_common_divisor. Function category: Math"""
    return FunctionExpression("gcd", x, y)

def least_common_multiple(x, y, /) -> FunctionExpression:
    """Computes the least common multiple of x and y. Function category: Math"""
    return FunctionExpression("least_common_multiple", x, y)

def lcm(x, y, /) -> FunctionExpression:
    """Computes the least common multiple of x and y Alias for least_common_multiple.
    Function category: Math"""
    return FunctionExpression("lcm", x, y)

def lgamma(x, /) -> FunctionExpression:
    """Computes the log of the gamma function. Function category: Math"""
    return FunctionExpression("lgamma", x)

def ln(x, /) -> FunctionExpression:
    """Computes the natural logarithm of x. Function category: Math"""
    return FunctionExpression("ln", x)

def log2(x, /) -> FunctionExpression:
    """Computes the 2-log of x. Function category: Math"""
    return FunctionExpression("log2", x)

def log10(x, /) -> FunctionExpression:
    """Computes the 10-log of x. Function category: Math"""
    return FunctionExpression("log10", x)

def log(b, x, /) -> FunctionExpression:
    """Computes the logarithm of x to base b. b may be omitted, in which case the
    default 10. Function category: Math"""
    return FunctionExpression("log", b, x)

def nextafter(x, y, /) -> FunctionExpression:
    """Returns the next floating point value after x in the direction of y. Function
    category: Math"""
    return FunctionExpression("nextafter", x, y)

def pi() -> FunctionExpression:
    """Returns the value of pi. Function category: Math"""
    return FunctionExpression("pi", )

def radians(x, /) -> FunctionExpression:
    """Converts degrees to radians. Function category: Math"""
    return FunctionExpression("radians", x)

def round(x, precision, /) -> FunctionExpression:
    """Rounds x to s decimal places. Function category: Math"""
    return FunctionExpression("round", x, precision)

def sign(x, /) -> FunctionExpression:
    """Returns the sign of x as -1, 0 or 1. Function category: Math"""
    return FunctionExpression("sign", x)

def signbit(x, /) -> FunctionExpression:
    """Returns whether the signbit is set or not. Function category: Math"""
    return FunctionExpression("signbit", x)

def sin(x, /) -> FunctionExpression:
    """Computes the sin of x. Function category: Math"""
    return FunctionExpression("sin", x)

def sqrt(x, /) -> FunctionExpression:
    """Returns the square root of x. Function category: Math"""
    return FunctionExpression("sqrt", x)

def tan(x, /) -> FunctionExpression:
    """Computes the tan of x. Function category: Math"""
    return FunctionExpression("tan", x)

def trunc(x, /) -> FunctionExpression:
    """Truncates the number. Function category: Math"""
    return FunctionExpression("trunc", x)

def struct_insert(struct, any, /) -> FunctionExpression:
    """Adds field(s)/value(s) to an existing STRUCT with the argument values. The entry
    name(s) will be the bound variable name(s). Function category: Struct"""
    return FunctionExpression("struct_insert", struct, any)

def struct_pack(any, /) -> FunctionExpression:
    """Creates a STRUCT containing the argument values. The entry name will be the
    bound variable name. Function category: Struct"""
    return FunctionExpression("struct_pack", any)

def row(any, /) -> FunctionExpression:
    """Creates an unnamed STRUCT containing the argument values. Function category:
    Struct"""
    return FunctionExpression("row", any)

def cardinality(map, /) -> FunctionExpression:
    """Returns the size of the map (or the number of entries in the map). Function
    category: Map"""
    return FunctionExpression("cardinality", map)

def map(keys, values, /) -> FunctionExpression:
    """Creates a map from a set of keys and values. Function category: Map"""
    return FunctionExpression("map", keys, values)

def map_entries(map, /) -> FunctionExpression:
    """Returns the map entries as a list of keys/values. Function category: Map"""
    return FunctionExpression("map_entries", map)

def map_extract(map, key, /) -> FunctionExpression:
    """Returns a list containing the value for a given key or an empty list if the key
    is not contained in the map. The type of the key provided in the second
    parameter must match the type of the map’s keys else an error is returned.
    Function category: Map"""
    return FunctionExpression("map_extract", map, key)

def element_at(map, key, /) -> FunctionExpression:
    """Returns a list containing the value for a given key or an empty list if the key
    is not contained in the map. The type of the key provided in the second
    parameter must match the type of the map’s keys else an error is returned
    Alias for map_extract. Function category: Map"""
    return FunctionExpression("element_at", map, key)

def map_from_entries(map, /) -> FunctionExpression:
    """Returns a map created from the entries of the array. Function category: Map"""
    return FunctionExpression("map_from_entries", map)

def map_concat(any, /, *args) -> FunctionExpression:
    """Returns a map created from merging the input maps, on key collision the value is
    taken from the last map with that key. Function category: Map"""
    return FunctionExpression("map_concat", any, *args)

def map_keys(map, /) -> FunctionExpression:
    """Returns the keys of a map as a list. Function category: Map"""
    return FunctionExpression("map_keys", map)

def map_values(map, /) -> FunctionExpression:
    """Returns the values of a map as a list. Function category: Map"""
    return FunctionExpression("map_values", map)

def flatten(nested_list, /) -> FunctionExpression:
    """Flatten a nested list by one level. Function category: List"""
    return FunctionExpression("flatten", nested_list)

def list_aggregate(list, name, /) -> FunctionExpression:
    """Executes the aggregate function name on the elements of list. Function category:
    List"""
    return FunctionExpression("list_aggregate", list, name)

def array_aggregate(list, name, /) -> FunctionExpression:
    """Executes the aggregate function name on the elements of list Alias for
    list_aggregate. Function category: List"""
    return FunctionExpression("array_aggregate", list, name)

def list_aggr(list, name, /) -> FunctionExpression:
    """Executes the aggregate function name on the elements of list Alias for
    list_aggregate. Function category: List"""
    return FunctionExpression("list_aggr", list, name)

def array_aggr(list, name, /) -> FunctionExpression:
    """Executes the aggregate function name on the elements of list Alias for
    list_aggregate. Function category: List"""
    return FunctionExpression("array_aggr", list, name)

def aggregate(list, name, /) -> FunctionExpression:
    """Executes the aggregate function name on the elements of list Alias for
    list_aggregate. Function category: List"""
    return FunctionExpression("aggregate", list, name)

def list_distinct(list, /) -> FunctionExpression:
    """Removes all duplicates and NULLs from a list. Does not preserve the original
    order. Function category: List"""
    return FunctionExpression("list_distinct", list)

def array_distinct(list, /) -> FunctionExpression:
    """Removes all duplicates and NULLs from a list. Does not preserve the original
    order Alias for list_distinct. Function category: List"""
    return FunctionExpression("array_distinct", list)

def list_unique(list, /) -> FunctionExpression:
    """Counts the unique elements of a list. Function category: List"""
    return FunctionExpression("list_unique", list)

def array_unique(list, /) -> FunctionExpression:
    """Counts the unique elements of a list Alias for list_unique. Function category:
    List"""
    return FunctionExpression("array_unique", list)

def list_value(any, /, *args) -> FunctionExpression:
    """Create a LIST containing the argument values. Function category: List"""
    return FunctionExpression("list_value", any, *args)

def list_pack(any, /, *args) -> FunctionExpression:
    """Create a LIST containing the argument values Alias for list_value. Function
    category: List"""
    return FunctionExpression("list_pack", any, *args)

def list_slice(list, begin, end, step=None, /) -> FunctionExpression:
    """Extract a sublist using slice conventions. Negative values are accepted.
    Function category: List"""
    if step is None:
        return FunctionExpression("list_slice", list, begin, end)
    return FunctionExpression("list_slice", list, begin, end, step)

def array_slice(list, begin, end, step=None, /) -> FunctionExpression:
    """Extract a sublist using slice conventions. Negative values are accepted Alias
    for list_slice. Function category: List"""
    if step is None:
        return FunctionExpression("array_slice", list, begin, end)
    return FunctionExpression("array_slice", list, begin, end, step)

def list_sort(list, /) -> FunctionExpression:
    """Sorts the elements of the list. Function category: List"""
    return FunctionExpression("list_sort", list)

def array_sort(list, /) -> FunctionExpression:
    """Sorts the elements of the list Alias for list_sort. Function category: List"""
    return FunctionExpression("array_sort", list)

def list_grade_up(list, /) -> FunctionExpression:
    """Returns the index of their sorted position. Function category: List"""
    return FunctionExpression("list_grade_up", list)

def array_grade_up(list, /) -> FunctionExpression:
    """Returns the index of their sorted position. Alias for list_grade_up. Function
    category: List"""
    return FunctionExpression("array_grade_up", list)

def grade_up(list, /) -> FunctionExpression:
    """Returns the index of their sorted position. Alias for list_grade_up. Function
    category: List"""
    return FunctionExpression("grade_up", list)

def list_reverse_sort(list, /) -> FunctionExpression:
    """Sorts the elements of the list in reverse order. Function category: List"""
    return FunctionExpression("list_reverse_sort", list)

def array_reverse_sort(list, /) -> FunctionExpression:
    """Sorts the elements of the list in reverse order Alias for list_reverse_sort.
    Function category: List"""
    return FunctionExpression("array_reverse_sort", list)

def generate_series(start, stop, step, /) -> FunctionExpression:
    """Create a list of values between start and stop - the stop parameter is
    inclusive. Function category: List"""
    return FunctionExpression("generate_series", start, stop, step)

def range(start, stop, step, /) -> FunctionExpression:
    """Create a list of values between start and stop - the stop parameter is
    exclusive. Function category: List"""
    return FunctionExpression("range", start, stop, step)

def list_cosine_similarity(list1, list2, /) -> FunctionExpression:
    """Compute the cosine similarity between two lists. Function category: List"""
    return FunctionExpression("list_cosine_similarity", list1, list2)

def list_distance(list1, list2, /) -> FunctionExpression:
    """Compute the distance between two lists. Function category: List"""
    return FunctionExpression("list_distance", list1, list2)

def list_inner_product(list1, list2, /) -> FunctionExpression:
    """Compute the inner product between two lists. Function category: List"""
    return FunctionExpression("list_inner_product", list1, list2)

def list_dot_product(list1, list2, /) -> FunctionExpression:
    """Compute the inner product between two lists Alias for list_inner_product.
    Function category: List"""
    return FunctionExpression("list_dot_product", list1, list2)

def unpivot_list(any, /, *args) -> FunctionExpression:
    """Identical to list_value, but generated as part of unpivot for better error
    messages. Function category: List"""
    return FunctionExpression("unpivot_list", any, *args)

def union_extract(union, tag, /) -> FunctionExpression:
    """Extract the value with the named tags from the union. NULL if the tag is not
    currently selected. Function category: Union"""
    return FunctionExpression("union_extract", union, tag)

def union_tag(union, /) -> FunctionExpression:
    """Retrieve the currently selected tag of the union as an ENUM. Function category:
    Union"""
    return FunctionExpression("union_tag", union)

def union_value(tag, /) -> FunctionExpression:
    """Create a single member UNION containing the argument value. The tag of the value
    will be the bound variable name. Function category: Union"""
    return FunctionExpression("union_value", tag)

def alias(expr, /) -> FunctionExpression:
    """Returns the name of a given expression. Function category: Generic"""
    return FunctionExpression("alias", expr)

def current_setting(setting_name, /) -> FunctionExpression:
    """Returns the current value of the configuration setting. Function category:
    Generic"""
    return FunctionExpression("current_setting", setting_name)

def error(message, /) -> FunctionExpression:
    """Throws the given error message. Function category: Generic"""
    return FunctionExpression("error", message)

def hash(param, /) -> FunctionExpression:
    """Returns an integer with the hash of the value. Note that this is not a
    cryptographic hash. Function category: Generic"""
    return FunctionExpression("hash", param)

def least(arg1, arg2, /, *args) -> FunctionExpression:
    """Returns the lowest value of the set of input parameters. Function category:
    Generic"""
    return FunctionExpression("least", arg1, arg2, *args)

def greatest(arg1, arg2, /, *args) -> FunctionExpression:
    """Returns the highest value of the set of input parameters. Function category:
    Generic"""
    return FunctionExpression("greatest", arg1, arg2, *args)

def stats(expression, /) -> FunctionExpression:
    """Returns a string with statistics about the expression. Expression can be a
    column, constant, or SQL expression. Function category: Generic"""
    return FunctionExpression("stats", expression)

def typeof(expression, /) -> FunctionExpression:
    """Returns the name of the data type of the result of the expression. Function
    category: Generic"""
    return FunctionExpression("typeof", expression)

def current_query() -> FunctionExpression:
    """Returns the current query as a string. Function category: Generic"""
    return FunctionExpression("current_query", )

def current_schema() -> FunctionExpression:
    """Returns the name of the currently active schema. Default is main. Function
    category: Generic"""
    return FunctionExpression("current_schema", )

def current_schemas(include_implicit, /) -> FunctionExpression:
    """Returns list of schemas. Pass a parameter of True to include implicit schemas.
    Function category: Generic"""
    return FunctionExpression("current_schemas", include_implicit)

def current_database() -> FunctionExpression:
    """Returns the name of the currently active database. Function category: Generic"""
    return FunctionExpression("current_database", )

def in_search_path(database_name, schema_name, /) -> FunctionExpression:
    """Returns whether or not the database/schema are in the search path. Function
    category: Generic"""
    return FunctionExpression("in_search_path", database_name, schema_name)

def txid_current() -> FunctionExpression:
    """Returns the current transaction’s ID (a BIGINT). It will assign a new one if the
    current transaction does not have one already. Function category: Generic"""
    return FunctionExpression("txid_current", )

def version() -> FunctionExpression:
    """Returns the currently active version of DuckDB in this format: v0.3.2. Function
    category: Generic"""
    return FunctionExpression("version", )

def starts_with(string, search_string, /) -> FunctionExpression:
    """Returns true if string begins with search_string Alias for ^@. Function
    category: String"""
    return FunctionExpression("starts_with", string, search_string)

def ascii(string, /) -> FunctionExpression:
    """Returns an integer that represents the Unicode code point of the first character
    of the string. Function category: String"""
    return FunctionExpression("ascii", string)

def bar(x, min, max, width, /) -> FunctionExpression:
    """Draws a band whose width is proportional to (x - min) and equal to width
    characters when x = max. width defaults to 80. Function category: String"""
    return FunctionExpression("bar", x, min, max, width)

def bin(value, /) -> FunctionExpression:
    """Converts the value to binary representation. Function category: String"""
    return FunctionExpression("bin", value)

def to_binary(value, /) -> FunctionExpression:
    """Converts the value to binary representation Alias for bin. Function category:
    String"""
    return FunctionExpression("to_binary", value)

def chr(code_point, /) -> FunctionExpression:
    """Returns a character which is corresponding the ASCII code value or Unicode code
    point. Function category: String"""
    return FunctionExpression("chr", code_point)

def damerau_levenshtein(str1, str2, /) -> FunctionExpression:
    """Extension of Levenshtein distance to also include transposition of adjacent
    characters as an allowed edit operation. In other words, the minimum number
    of edit operations (insertions, deletions, substitutions or transpositions)
    required to change one string to another. Different case is considered
    different. Function category: String"""
    return FunctionExpression("damerau_levenshtein", str1, str2)

def format(format, /, *parameters) -> FunctionExpression:
    """Formats a string using fmt syntax. Function category: String"""
    return FunctionExpression("format", format, *parameters)

def format_bytes(bytes, /) -> FunctionExpression:
    """Converts bytes to a human-readable presentation (e.g. 16000 -> 15.6 KiB).
    Function category: String"""
    return FunctionExpression("format_bytes", bytes)

def formatReadableSize(bytes, /) -> FunctionExpression:
    """Converts bytes to a human-readable presentation (e.g. 16000 -> 15.6 KiB) Alias
    for format_bytes. Function category: String"""
    return FunctionExpression("formatReadableSize", bytes)

def formatReadableDecimalSize(bytes, /) -> FunctionExpression:
    """Converts bytes to a human-readable presentation (e.g. 16000 -> 16.0 KB).
    Function category: String"""
    return FunctionExpression("formatReadableDecimalSize", bytes)

def hamming(str1, str2, /) -> FunctionExpression:
    """The number of positions with different characters for 2 strings of equal length.
    Different case is considered different. Function category: String"""
    return FunctionExpression("hamming", str1, str2)

def mismatches(str1, str2, /) -> FunctionExpression:
    """The number of positions with different characters for 2 strings of equal length.
    Different case is considered different Alias for hamming. Function category:
    String"""
    return FunctionExpression("mismatches", str1, str2)

def hex(value, /) -> FunctionExpression:
    """Converts the value to hexadecimal representation. Function category: String"""
    return FunctionExpression("hex", value)

def to_hex(value, /) -> FunctionExpression:
    """Converts the value to hexadecimal representation Alias for hex. Function
    category: String"""
    return FunctionExpression("to_hex", value)

def instr(haystack, needle, /) -> FunctionExpression:
    """Returns location of first occurrence of needle in haystack, counting from 1.
    Returns 0 if no match found. Function category: String"""
    return FunctionExpression("instr", haystack, needle)

def strpos(haystack, needle, /) -> FunctionExpression:
    """Returns location of first occurrence of needle in haystack, counting from 1.
    Returns 0 if no match found Alias for instr. Function category: String"""
    return FunctionExpression("strpos", haystack, needle)

def position(haystack, needle, /) -> FunctionExpression:
    """Returns location of first occurrence of needle in haystack, counting from 1.
    Returns 0 if no match found Alias for instr. Function category: String"""
    return FunctionExpression("position", haystack, needle)

def jaccard(str1, str2, /) -> FunctionExpression:
    """The Jaccard similarity between two strings. Different case is considered
    different. Returns a number between 0 and 1. Function category: String"""
    return FunctionExpression("jaccard", str1, str2)

def jaro_similarity(str1, str2, /) -> FunctionExpression:
    """The Jaro similarity between two strings. Different case is considered different.
    Returns a number between 0 and 1. Function category: String"""
    return FunctionExpression("jaro_similarity", str1, str2)

def jaro_winkler_similarity(str1, str2, /) -> FunctionExpression:
    """The Jaro-Winkler similarity between two strings. Different case is considered
    different. Returns a number between 0 and 1. Function category: String"""
    return FunctionExpression("jaro_winkler_similarity", str1, str2)

def left(string, count, /) -> FunctionExpression:
    """Extract the left-most count characters. Function category: String"""
    return FunctionExpression("left", string, count)

def left_grapheme(string, count, /) -> FunctionExpression:
    """Extract the left-most count grapheme clusters. Function category: String"""
    return FunctionExpression("left_grapheme", string, count)

def levenshtein(str1, str2, /) -> FunctionExpression:
    """The minimum number of single-character edits (insertions, deletions or
    substitutions) required to change one string to the other. Different case is
    considered different. Function category: String"""
    return FunctionExpression("levenshtein", str1, str2)

def editdist3(str1, str2, /) -> FunctionExpression:
    """The minimum number of single-character edits (insertions, deletions or
    substitutions) required to change one string to the other. Different case is
    considered different Alias for levenshtein. Function category: String"""
    return FunctionExpression("editdist3", str1, str2)

def lpad(string, count, character, /) -> FunctionExpression:
    """Pads the string with the character from the left until it has count characters.
    Function category: String"""
    return FunctionExpression("lpad", string, count, character)

def ltrim(string, characters, /) -> FunctionExpression:
    """Removes any occurrences of any of the characters from the left side of the
    string. Function category: String"""
    return FunctionExpression("ltrim", string, characters)

def md5(value, /) -> FunctionExpression:
    """Returns the MD5 hash of the value as a string. Function category: String"""
    return FunctionExpression("md5", value)

def md5_number(value, /) -> FunctionExpression:
    """Returns the MD5 hash of the value as an INT128. Function category: String"""
    return FunctionExpression("md5_number", value)

def md5_number_lower(value, /) -> FunctionExpression:
    """Returns the MD5 hash of the value as an INT128. Function category: String"""
    return FunctionExpression("md5_number_lower", value)

def md5_number_upper(value, /) -> FunctionExpression:
    """Returns the MD5 hash of the value as an INT128. Function category: String"""
    return FunctionExpression("md5_number_upper", value)

def parse_dirname(string, separator, /) -> FunctionExpression:
    """Returns the top-level directory name. separator options: system, both_slash
    (default), forward_slash, backslash. Function category: String"""
    return FunctionExpression("parse_dirname", string, separator)

def parse_dirpath(string, separator, /) -> FunctionExpression:
    """Returns the head of the path similarly to Python's os.path.dirname. separator
    options: system, both_slash (default), forward_slash, backslash. Function
    category: String"""
    return FunctionExpression("parse_dirpath", string, separator)

def parse_filename(string, trim_extension, separator, /) -> FunctionExpression:
    """Returns the last component of the path similarly to Python's os.path.basename.
    If trim_extension is true, the file extension will be removed (it defaults
    to false). separator options: system, both_slash (default), forward_slash,
    backslash. Function category: String"""
    return FunctionExpression("parse_filename", string, trim_extension, separator)

def parse_path(string, separator, /) -> FunctionExpression:
    """Returns a list of the components (directories and filename) in the path
    similarly to Python's pathlib.PurePath::parts. separator options: system,
    both_slash (default), forward_slash, backslash. Function category: String"""
    return FunctionExpression("parse_path", string, separator)

def printf(format, /, *parameters) -> FunctionExpression:
    """Formats a string using printf syntax. Function category: String"""
    return FunctionExpression("printf", format, *parameters)

def repeat(string, count, /) -> FunctionExpression:
    """Repeats the string count number of times. Function category: String"""
    return FunctionExpression("repeat", string, count)

def replace(string, source, target, /) -> FunctionExpression:
    """Replaces any occurrences of the source with target in string. Function category:
    String"""
    return FunctionExpression("replace", string, source, target)

def reverse(string, /) -> FunctionExpression:
    """Reverses the string. Function category: String"""
    return FunctionExpression("reverse", string)

def right(string, count, /) -> FunctionExpression:
    """Extract the right-most count characters. Function category: String"""
    return FunctionExpression("right", string, count)

def right_grapheme(string, count, /) -> FunctionExpression:
    """Extract the right-most count grapheme clusters. Function category: String"""
    return FunctionExpression("right_grapheme", string, count)

def rpad(string, count, character, /) -> FunctionExpression:
    """Pads the string with the character from the right until it has count characters.
    Function category: String"""
    return FunctionExpression("rpad", string, count, character)

def rtrim(string, characters, /) -> FunctionExpression:
    """Removes any occurrences of any of the characters from the right side of the
    string. Function category: String"""
    return FunctionExpression("rtrim", string, characters)

def sha256(value, /) -> FunctionExpression:
    """Returns the SHA256 hash of the value. Function category: String"""
    return FunctionExpression("sha256", value)

def string_split(string, separator, /) -> FunctionExpression:
    """Splits the string along the separator. Function category: String"""
    return FunctionExpression("string_split", string, separator)

def str_split(string, separator, /) -> FunctionExpression:
    """Splits the string along the separator Alias for string_split. Function category:
    String"""
    return FunctionExpression("str_split", string, separator)

def string_to_array(string, separator, /) -> FunctionExpression:
    """Splits the string along the separator Alias for string_split. Function category:
    String"""
    return FunctionExpression("string_to_array", string, separator)

def split(string, separator, /) -> FunctionExpression:
    """Splits the string along the separator Alias for string_split. Function category:
    String"""
    return FunctionExpression("split", string, separator)

def string_split_regex(string, separator, /) -> FunctionExpression:
    """Splits the string along the regex. Function category: String"""
    return FunctionExpression("string_split_regex", string, separator)

def str_split_regex(string, separator, /) -> FunctionExpression:
    """Splits the string along the regex Alias for string_split_regex. Function
    category: String"""
    return FunctionExpression("str_split_regex", string, separator)

def regexp_split_to_array(string, separator, /) -> FunctionExpression:
    """Splits the string along the regex Alias for string_split_regex. Function
    category: String"""
    return FunctionExpression("regexp_split_to_array", string, separator)

def translate(string, from_, to, /) -> FunctionExpression:
    """Replaces each character in string that matches a character in the from set with
    the corresponding character in the to set. If from is longer than to,
    occurrences of the extra characters in from are deleted. Function category:
    String"""
    return FunctionExpression("translate", string, from_, to)

def trim(string, characters, /) -> FunctionExpression:
    """Removes any occurrences of any of the characters from either side of the string.
    Function category: String"""
    return FunctionExpression("trim", string, characters)

def unbin(value, /) -> FunctionExpression:
    """Converts a value from binary representation to a blob. Function category: String"""
    return FunctionExpression("unbin", value)

def from_binary(value, /) -> FunctionExpression:
    """Converts a value from binary representation to a blob Alias for unbin. Function
    category: String"""
    return FunctionExpression("from_binary", value)

def unhex(value, /) -> FunctionExpression:
    """Converts a value from hexadecimal representation to a blob. Function category:
    String"""
    return FunctionExpression("unhex", value)

def from_hex(value, /) -> FunctionExpression:
    """Converts a value from hexadecimal representation to a blob Alias for unhex.
    Function category: String"""
    return FunctionExpression("from_hex", value)

def unicode(str, /) -> FunctionExpression:
    """Returns the unicode codepoint of the first character of the string. Function
    category: String"""
    return FunctionExpression("unicode", str)

def ord(str, /) -> FunctionExpression:
    """Returns the unicode codepoint of the first character of the string Alias for
    unicode. Function category: String"""
    return FunctionExpression("ord", str)

def to_base(number, radix, min_length, /) -> FunctionExpression:
    """Converts a value to a string in the given base radix, optionally padding with
    leading zeros to the minimum length. Function category: String"""
    return FunctionExpression("to_base", number, radix, min_length)

def regexp_escape(string, /) -> FunctionExpression:
    """Escapes all potentially meaningful regexp characters in the input string.
    Function category: String"""
    return FunctionExpression("regexp_escape", string)

def random() -> FunctionExpression:
    """Returns a random number between 0 and 1. Function category: Random"""
    return FunctionExpression("random", )

def setseed() -> FunctionExpression:
    """Sets the seed to be used for the random function. Function category: Random"""
    return FunctionExpression("setseed", )

def uuid() -> FunctionExpression:
    """Returns a random UUID similar to this: eeccb8c5-9943-b2bb-bb5e-222f4e14b687.
    Function category: Random"""
    return FunctionExpression("uuid", )

def gen_random_uuid() -> FunctionExpression:
    """Returns a random UUID similar to this: eeccb8c5-9943-b2bb-bb5e-222f4e14b687
    Alias for uuid. Function category: Random"""
    return FunctionExpression("gen_random_uuid", )

def decode(blob, /) -> FunctionExpression:
    """Convert blob to varchar. Fails if blob is not valid utf-8. Function category:
    Blob"""
    return FunctionExpression("decode", blob)

def encode(string, /) -> FunctionExpression:
    """Convert varchar to blob. Converts utf-8 characters into literal encoding.
    Function category: Blob"""
    return FunctionExpression("encode", string)

def from_base64(string, /) -> FunctionExpression:
    """Convert a base64 encoded string to a character string. Function category: Blob"""
    return FunctionExpression("from_base64", string)

def to_base64(blob, /) -> FunctionExpression:
    """Convert a blob to a base64 encoded string. Function category: Blob"""
    return FunctionExpression("to_base64", blob)

def base64(blob, /) -> FunctionExpression:
    """Convert a blob to a base64 encoded string Alias for to_base64. Function
    category: Blob"""
    return FunctionExpression("base64", blob)

def create_sort_key(*parameters) -> FunctionExpression:
    """Constructs a binary-comparable sort key based on a set of input parameters and
    sort qualifiers. Function category: Blob"""
    return FunctionExpression("create_sort_key", *parameters)

def vector_type(col, /) -> FunctionExpression:
    """Returns the VectorType of a given column. Function category: Debug"""
    return FunctionExpression("vector_type", col)