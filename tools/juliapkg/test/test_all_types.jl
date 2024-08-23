
# test_all_types.jl


@testset "Test All Types" begin
    db = DBInterface.connect(DuckDB.DB)
    con = DBInterface.connect(db)

    df = DataFrame(
        DBInterface.execute(
            con,
            """SELECT * EXCLUDE(time, time_tz, fixed_int_array, fixed_varchar_array, fixed_nested_int_array,
            		fixed_nested_varchar_array, fixed_struct_array, struct_of_fixed_array, fixed_array_of_int_list,
            		list_of_fixed_int_array, varint)
                , CASE WHEN time = '24:00:00'::TIME THEN '23:59:59.999999'::TIME ELSE time END AS time
                , CASE WHEN time_tz = '24:00:00-15:59:59'::TIMETZ THEN '23:59:59.999999-15:59:59'::TIMETZ ELSE time_tz END AS time_tz
            FROM test_all_types()
            """
        )
    )
    #println(names(df))
    # we can also use 'propertynames()' to get the column names as symbols, that might make for a better testing approach
    # If we add a dictionary that maps from the symbol to the expected result

    @test isequal(df.bool, [false, true, missing])
    @test isequal(df.tinyint, [-128, 127, missing])
    @test isequal(df.smallint, [-32768, 32767, missing])
    @test isequal(df.int, [-2147483648, 2147483647, missing])
    @test isequal(df.bigint, [-9223372036854775808, 9223372036854775807, missing])
    @test isequal(
        df.hugeint,
        [-170141183460469231731687303715884105728, 170141183460469231731687303715884105727, missing]
    )
    @test isequal(df.uhugeint, [0, 340282366920938463463374607431768211455, missing])
    @test isequal(df.utinyint, [0, 255, missing])
    @test isequal(df.usmallint, [0, 65535, missing])
    @test isequal(df.uint, [0, 4294967295, missing])
    @test isequal(df.ubigint, [0, 18446744073709551615, missing])
    @test isequal(df.float, [-3.4028235f38, 3.4028235f38, missing])
    @test isequal(df.double, [-1.7976931348623157e308, 1.7976931348623157e308, missing])
    @test isequal(df.dec_4_1, [-999.9, 999.9, missing])
    @test isequal(df.dec_9_4, [-99999.9999, 99999.9999, missing])
    @test isequal(df.dec_18_6, [-999999999999.999999, 999999999999.999999, missing])
    @test isequal(
        df.dec38_10,
        [-9999999999999999999999999999.9999999999, 9999999999999999999999999999.9999999999, missing]
    )
    @test isequal(
        df.dec38_10,
        [-9999999999999999999999999999.9999999999, 9999999999999999999999999999.9999999999, missing]
    )
    @test isequal(
        df.dec38_10,
        [-9999999999999999999999999999.9999999999, 9999999999999999999999999999.9999999999, missing]
    )
    @test isequal(df.small_enum, ["DUCK_DUCK_ENUM", "GOOSE", missing])
    @test isequal(df.medium_enum, ["enum_0", "enum_299", missing])
    @test isequal(df.large_enum, ["enum_0", "enum_69999", missing])
    @test isequal(df.date, [Dates.Date(-5877641, 6, 25), Dates.Date(5881580, 7, 10), missing])
    @test isequal(df.time, [Dates.Time(0, 0, 0), Dates.Time(23, 59, 59, 999, 999), missing])
    @test isequal(df.time_tz, [Dates.Time(0, 0, 0), Dates.Time(23, 59, 59, 999, 999), missing])
    @test isequal(
        df.timestamp,
        [Dates.DateTime(-290308, 12, 22, 0, 0, 0), Dates.DateTime(294247, 1, 10, 4, 0, 54, 775), missing]
    )
    @test isequal(
        df.timestamp_tz,
        [Dates.DateTime(-290308, 12, 22, 0, 0, 0), Dates.DateTime(294247, 1, 10, 4, 0, 54, 775), missing]
    )
    @test isequal(
        df.timestamp_s,
        [Dates.DateTime(-290308, 12, 22, 0, 0, 0), Dates.DateTime(294247, 1, 10, 4, 0, 54, 0), missing]
    )
    @test isequal(
        df.timestamp_ms,
        [Dates.DateTime(-290308, 12, 22, 0, 0, 0), Dates.DateTime(294247, 1, 10, 4, 0, 54, 775), missing]
    )
    @test isequal(
        df.timestamp_ns,
        [Dates.DateTime(1677, 9, 22, 0, 0, 0, 0), Dates.DateTime(2262, 4, 11, 23, 47, 16, 854), missing]
    )
    @test isequal(
        df.interval,
        [
            Dates.CompoundPeriod(Dates.Month(0), Dates.Day(0), Dates.Microsecond(0)),
            Dates.CompoundPeriod(Dates.Month(999), Dates.Day(999), Dates.Microsecond(999999999)),
            missing
        ]
    )
    @test isequal(df.varchar, ["🦆🦆🦆🦆🦆🦆", "goo\0se", missing])
    @test isequal(
        df.blob,
        [
            UInt8[
                0x74,
                0x68,
                0x69,
                0x73,
                0x69,
                0x73,
                0x61,
                0x6c,
                0x6f,
                0x6e,
                0x67,
                0x62,
                0x6c,
                0x6f,
                0x62,
                0x00,
                0x77,
                0x69,
                0x74,
                0x68,
                0x6e,
                0x75,
                0x6c,
                0x6c,
                0x62,
                0x79,
                0x74,
                0x65,
                0x73
            ],
            UInt8[0x00, 0x00, 0x00, 0x61],
            missing
        ]
    )
    @test isequal(df.uuid, [UUID(0), UUID(UInt128(340282366920938463463374607431768211455)), missing])
    @test isequal(df.int_array, [[], [42, 999, missing, missing, -42], missing])
    @test isequal(df.double_array, [[], [42, NaN, Inf, -Inf, missing, -42], missing])
    @test isequal(
        df.date_array,
        [
            [],
            [
                Dates.Date(1970, 1, 1),
                Dates.Date(5881580, 7, 11),
                Dates.Date(-5877641, 6, 24),
                missing,
                Dates.Date(2022, 5, 12)
            ],
            missing
        ]
    )
    @test isequal(
        df.timestamp_array,
        [
            [],
            [
                Dates.DateTime(1970, 1, 1),
                Dates.DateTime(294247, 1, 10, 4, 0, 54, 775),
                Dates.DateTime(-290308, 12, 21, 19, 59, 5, 225),
                missing,
                Dates.DateTime(2022, 5, 12, 16, 23, 45)
            ],
            missing
        ]
    )
    @test isequal(
        df.timestamptz_array,
        [
            [],
            [
                Dates.DateTime(1970, 1, 1),
                Dates.DateTime(294247, 1, 10, 4, 0, 54, 775),
                Dates.DateTime(-290308, 12, 21, 19, 59, 5, 225),
                missing,
                Dates.DateTime(2022, 05, 12, 23, 23, 45)
            ],
            missing
        ]
    )
    @test isequal(df.varchar_array, [[], ["🦆🦆🦆🦆🦆🦆", "goose", missing, ""], missing])
    @test isequal(
        df.nested_int_array,
        [[], [[], [42, 999, missing, missing, -42], missing, [], [42, 999, missing, missing, -42]], missing]
    )
    @test isequal(df.struct, [(a = missing, b = missing), (a = 42, b = "🦆🦆🦆🦆🦆🦆"), missing])
    @test isequal(
        df.struct_of_arrays,
        [
            (a = missing, b = missing),
            (a = [42, 999, missing, missing, -42], b = ["🦆🦆🦆🦆🦆🦆", "goose", missing, ""]),
            missing
        ]
    )
    @test isequal(df.array_of_structs, [[], [(a = missing, b = missing), (a = 42, b = "🦆🦆🦆🦆🦆🦆"), missing], missing])
    @test isequal(df.map, [Dict(), Dict("key1" => "🦆🦆🦆🦆🦆🦆", "key2" => "goose"), missing])
end
