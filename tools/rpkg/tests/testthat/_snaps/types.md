# test_all_types() output

    Code
      as.list(dbGetQuery(con,
        "SELECT * EXCLUDE (timestamp_tz, time_tz, uuid, interval, json, map) FROM test_all_types()"))
    Output
      $bool
      [1] FALSE  TRUE    NA
      
      $tinyint
      [1] -128  127   NA
      
      $smallint
      [1] -32768  32767     NA
      
      $int
      [1]         NA 2147483647         NA
      
      $bigint
      [1] -9.223372e+18  9.223372e+18            NA
      
      $hugeint
      [1] -1.701412e+38  1.701412e+38            NA
      
      $utinyint
      [1]   0 255  NA
      
      $usmallint
      [1]     0 65535    NA
      
      $uint
      [1]          0 4294967295         NA
      
      $ubigint
      [1] 0.000000e+00 1.844674e+19           NA
      
      $date
      [1] "-5877641-06-25" "5881580-07-10"  NA              
      
      $time
      Time differences in secs
      [1]     0 86399    NA
      
      $timestamp
      [1] "-290308-12-22 00:00:00.00000 UTC" "294247-01-10 04:00:54.77539 UTC" 
      [3] NA                                
      
      $timestamp_s
      [1] "-290308-12-22 00:00:00 UTC" "294247-01-10 04:00:54 UTC" 
      [3] NA                          
      
      $timestamp_ms
      [1] "-290308-12-22 00:00:00.00000 UTC" "294247-01-10 04:00:54.77539 UTC" 
      [3] NA                                
      
      $timestamp_ns
      [1] "1677-09-21 00:12:43.145223 UTC" "2262-04-11 23:47:16.854776 UTC"
      [3] NA                              
      
      $float
      [1] -3.402823e+38  3.402823e+38            NA
      
      $double
      [1] -1.797693e+308  1.797693e+308             NA
      
      $dec_4_1
      [1] -999.9  999.9     NA
      
      $dec_9_4
      [1] -1e+05  1e+05     NA
      
      $dec_18_6
      [1] -1e+12  1e+12     NA
      
      $dec38_10
      [1] -1e+28  1e+28     NA
      
      $varchar
      [1] "🦆🦆🦆🦆🦆🦆" "goose"        NA            
      
      $blob
      $blob[[1]]
       [1] 74 68 69 73 69 73 61 6c 6f 6e 67 62 6c 6f 62 00 77 69 74 68 6e 75 6c 6c 62
      [26] 79 74 65 73
      
      $blob[[2]]
      [1] 00 00 00 61
      
      $blob[[3]]
      [1] NA
      
      
      $small_enum
      [1] DUCK_DUCK_ENUM GOOSE          <NA>          
      Levels: DUCK_DUCK_ENUM GOOSE
      
      $medium_enum
      [1] enum_0   enum_299 <NA>    
      300 Levels: enum_0 enum_1 enum_2 enum_3 enum_4 enum_5 enum_6 enum_7 ... enum_299
      
      $large_enum
      [1] enum_0 enum_0 <NA>  
      70000 Levels: enum_0 enum_1 enum_2 enum_3 enum_4 enum_5 enum_6 enum_7 ... enum_69999
      
      $int_array
      $int_array[[1]]
      integer(0)
      
      $int_array[[2]]
      [1]  42 999  NA  NA -42
      
      $int_array[[3]]
      [1] NA
      
      
      $double_array
      $double_array[[1]]
      numeric(0)
      
      $double_array[[2]]
      [1]   42  NaN  Inf -Inf   NA  -42
      
      $double_array[[3]]
      [1] NA
      
      
      $date_array
      $date_array[[1]]
      Date of length 0
      
      $date_array[[2]]
      [1] "1970-01-01"     "5881580-07-11"  "-5877641-06-24" NA              
      [5] "2022-05-12"    
      
      $date_array[[3]]
      [1] NA
      
      
      $timestamp_array
      $timestamp_array[[1]]
      POSIXct of length 0
      
      $timestamp_array[[2]]
      [1] "1970-01-01 00:00:00.00000 UTC"    "294247-01-10 04:00:54.77539 UTC" 
      [3] "-290308-12-21 19:59:05.22460 UTC" NA                                
      [5] "2022-05-12 16:23:45.00000 UTC"   
      
      $timestamp_array[[3]]
      [1] NA
      
      
      $timestamptz_array
      $timestamptz_array[[1]]
      POSIXct of length 0
      
      $timestamptz_array[[2]]
      [1] "1970-01-01 00:00:00.00000 UTC"    "294247-01-10 04:00:54.77539 UTC" 
      [3] "-290308-12-21 19:59:05.22460 UTC" NA                                
      [5] "2022-05-12 23:23:45.00000 UTC"   
      
      $timestamptz_array[[3]]
      [1] NA
      
      
      $varchar_array
      $varchar_array[[1]]
      character(0)
      
      $varchar_array[[2]]
      [1] "🦆🦆🦆🦆🦆🦆" "goose"        NA             ""            
      
      $varchar_array[[3]]
      [1] NA
      
      
      $nested_int_array
      $nested_int_array[[1]]
      list()
      
      $nested_int_array[[2]]
      $nested_int_array[[2]][[1]]
      integer(0)
      
      $nested_int_array[[2]][[2]]
      [1]  42 999  NA  NA -42
      
      $nested_int_array[[2]][[3]]
      [1] NA
      
      $nested_int_array[[2]][[4]]
      integer(0)
      
      $nested_int_array[[2]][[5]]
      [1]  42 999  NA  NA -42
      
      
      $nested_int_array[[3]]
      [1] NA
      
      
      $struct
         a            b
      1 NA         <NA>
      2 42 🦆🦆🦆🦆🦆🦆
      3 NA         <NA>
      
      $struct_of_arrays
                           a                         b
      1                   NA                        NA
      2 42, 999, NA, NA, -42 🦆🦆🦆🦆🦆🦆, goose, NA, 
      3                   NA                        NA
      
      $array_of_structs
      $array_of_structs[[1]]
      [1] a b
      <0 rows> (or 0-length row.names)
      
      $array_of_structs[[2]]
         a            b
      1 NA         <NA>
      2 42 🦆🦆🦆🦆🦆🦆
      3 NA         <NA>
      
      $array_of_structs[[3]]
      [1] NA
      
      

