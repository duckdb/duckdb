#!/usr/bin/env bash

DB_NAME="tpch.db"

SF=${SF:-100} # Scale Factor, default to 100 if not set
LOOPS=${LOOPS:-1} # Number of loops, default to 1 if not set

OUTPUT_FILE="query_benchmark_results.txt"
TIMING_FILE="query_timing.csv"
COMPARISON_FILE="performance_comparison.csv"

# Arrays to store timing results for each query
declare -A ON_DURATIONS_ROUND1
declare -A OFF_DURATIONS_ROUND1
declare -A ON_DURATIONS_ROUND2
declare -A OFF_DURATIONS_ROUND2

# Create output directory
RESULTS_DIR="tpch_benchmark_results"
# if this directory exists, delete it, to avoid appending to the existing file
if [ -d "$RESULTS_DIR" ]; then
    rm -rf "$RESULTS_DIR"
fi
mkdir -p "$RESULTS_DIR"

# Clear output files
> "$RESULTS_DIR/$OUTPUT_FILE"
> "$RESULTS_DIR/$TIMING_FILE"
> "$RESULTS_DIR/$COMPARISON_FILE"

# Write CSV headers
echo "query_file,test_round,column_imprint_setting,start_time,end_time,duration_ns,status" > "$RESULTS_DIR/$TIMING_FILE"
echo "query_file,off_duration_ns,on_duration_ns,performance_improvement_ns,improvement_percent,off_order,on_order" > "$RESULTS_DIR/$COMPARISON_FILE"

echo "Starting TPC-H Benchmark with Scale Factor: $SF" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

# Clean up table
rm ${DB_NAME}
#./build/debug/duckdb tpch.db "DROP TABLE IF EXISTS customer;
# DROP TABLE IF EXISTS lineitem;
# DROP TABLE IF EXISTS nation;
# DROP TABLE IF EXISTS orders;
# DROP TABLE IF EXISTS part;
# DROP TABLE IF EXISTS partsupp;
# DROP TABLE IF EXISTS region;
# DROP TABLE IF EXISTS supplier;"

# Generate TPC-H data
echo "Generating TPC-H data" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
./build/release/duckdb ${DB_NAME} "CALL dbgen(sf = ${SF});"

# Build column imprint indexes
echo "Building Column Imprint Indexes" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

START_TIME=$(date +%s%N)
START_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')

./build/release/duckdb ${DB_NAME} -c ".timer on" \
    -c "PRAGMA build_column_imprints('customer', 'c_custkey');" \
    -c "PRAGMA build_column_imprints('customer', 'c_nationkey');" \
    -c "PRAGMA build_column_imprints('customer', 'c_acctbal');" \
    -c "PRAGMA build_column_imprints('lineitem', 'l_orderkey');" \
    -c "PRAGMA build_column_imprints('lineitem', 'l_partkey');" \
    -c "PRAGMA build_column_imprints('lineitem', 'l_suppkey');" \
    -c "PRAGMA build_column_imprints('lineitem', 'l_linenumber');" \
    -c "PRAGMA build_column_imprints('lineitem', 'l_quantity');" \
    -c "PRAGMA build_column_imprints('lineitem', 'l_extendedprice');" \
    -c "PRAGMA build_column_imprints('lineitem', 'l_discount');" \
    -c "PRAGMA build_column_imprints('lineitem', 'l_tax');" \
    -c "PRAGMA build_column_imprints('lineitem', 'l_shipdate');" \
    -c "PRAGMA build_column_imprints('lineitem', 'l_commitdate');" \
    -c "PRAGMA build_column_imprints('lineitem', 'l_receiptdate');" \
    -c "PRAGMA build_column_imprints('nation', 'n_nationkey');" \
    -c "PRAGMA build_column_imprints('nation', 'n_regionkey');" \
    -c "PRAGMA build_column_imprints('orders', 'o_orderkey');" \
    -c "PRAGMA build_column_imprints('orders', 'o_custkey');" \
    -c "PRAGMA build_column_imprints('orders', 'o_totalprice');" \
    -c "PRAGMA build_column_imprints('orders', 'o_shippriority');" \
    -c "PRAGMA build_column_imprints('orders', 'o_orderdate');" \
    -c "PRAGMA build_column_imprints('part', 'p_partkey');" \
    -c "PRAGMA build_column_imprints('part', 'p_size');" \
    -c "PRAGMA build_column_imprints('part', 'p_retailprice');" \
    -c "PRAGMA build_column_imprints('partsupp', 'ps_partkey');" \
    -c "PRAGMA build_column_imprints('partsupp', 'ps_suppkey');" \
    -c "PRAGMA build_column_imprints('partsupp', 'ps_availqty');" \
    -c "PRAGMA build_column_imprints('partsupp', 'ps_supplycost');" \
    -c "PRAGMA build_column_imprints('region', 'r_regionkey');" \
    -c "PRAGMA build_column_imprints('supplier', 's_suppkey');" \
    -c "PRAGMA build_column_imprints('supplier', 's_nationkey');" \
    -c "PRAGMA build_column_imprints('supplier', 's_acctbal');" \
    -c "PRAGMA force_checkpoint;" \
    -c "CHECKPOINT;" \
    | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

EXIT_CODE=$?
END_TIME=$(date +%s%N)
END_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')
DURATION=$((END_TIME - START_TIME))

echo "Column Imprints build time: ${DURATION:0:-6}ms" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

# Run TPC-H benchmark
echo "Running TPC-H benchmark" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

echo "Loop 1: Column Imprints OFF first, then ON"  | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
for i in $(seq 1 22); do
    for j in $(seq 1 $LOOPS); do
        START_TIME=$(date +%s%N)
        START_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')

        ./build/release/duckdb ${DB_NAME} -c ".timer on" -c "SET enable_column_imprint=false;" -c "PRAGMA tpch(${i});"

        EXIT_CODE=$?
        END_TIME=$(date +%s%N)
        END_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')
        DURATION=$((END_TIME - START_TIME))
        OFF_DURATIONS_ROUND1[$i]=$DURATION

        echo "Duration: ${DURATION:0:-6}ms" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
        echo "$i,1,off,$START_READABLE,$END_READABLE,$DURATION,$(if [ $EXIT_CODE -eq 0 ]; then echo "SUCCESS"; else echo "FAILED"; fi)" >> "$RESULTS_DIR/$TIMING_FILE"
    done
    for j in $(seq 1 $LOOPS); do
        START_TIME=$(date +%s%N)
        START_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')

        ./build/release/duckdb ${DB_NAME} -c ".timer on" -c "SET enable_column_imprint=true;" -c "PRAGMA tpch(${i});"

        EXIT_CODE=$?
        END_TIME=$(date +%s%N)
        END_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')
        DURATION=$((END_TIME - START_TIME))
        ON_DURATIONS_ROUND1[$i]=$DURATION

        echo "Duration: ${DURATION:0:-6}ms" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
        echo "$i,1,on,$START_READABLE,$END_READABLE,$DURATION,$(if [ $EXIT_CODE -eq 0 ]; then echo "SUCCESS"; else echo "FAILED"; fi)" >> "$RESULTS_DIR/$TIMING_FILE"
    done

done


echo "Loop 2: Column Imprints ON first, then OFF"
for i in $(seq 1 22); do
    for j in $(seq 1 $LOOPS); do
        START_TIME=$(date +%s%N)
        START_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')

        ./build/release/duckdb ${DB_NAME} -c ".timer on" -c "SET enable_column_imprint=true;" -c "PRAGMA tpch(${i});"

        EXIT_CODE=$?
        END_TIME=$(date +%s%N)
        END_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')
        DURATION=$((END_TIME - START_TIME))
        ON_DURATIONS_ROUND2[$i]=$DURATION

        echo "Duration: ${DURATION:0:-6}ms" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
        echo "$i,2,on,$START_READABLE,$END_READABLE,$DURATION,$(if [ $EXIT_CODE -eq 0 ]; then echo "SUCCESS"; else echo "FAILED"; fi)" >> "$RESULTS_DIR/$TIMING_FILE"
    done
    for j in $(seq 1 $LOOPS); do
        START_TIME=$(date +%s%N)
        START_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')

        ./build/release/duckdb ${DB_NAME} -c ".timer on" -c "SET enable_column_imprint=false;" -c "PRAGMA tpch(${i});"

        EXIT_CODE=$?
        END_TIME=$(date +%s%N)
        END_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')
        DURATION=$((END_TIME - START_TIME))
        OFF_DURATIONS_ROUND2[$i]=$DURATION

        echo "Duration: ${DURATION:0:-6}ms" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
        echo "$i,2,off,$START_READABLE,$END_READABLE,$DURATION,$(if [ $EXIT_CODE -eq 0 ]; then echo "SUCCESS"; else echo "FAILED"; fi)" >> "$RESULTS_DIR/$TIMING_FILE"
    done
done

echo "=== CALCULATING FINAL RESULTS ===" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

for i in $(seq 1 22); do
    # Calculate averages
    AVG_OFF=$(( (OFF_DURATIONS_ROUND1[$i] + OFF_DURATIONS_ROUND2[$i]) / 2 ))
    AVG_ON=$(( (ON_DURATIONS_ROUND1[$i] + ON_DURATIONS_ROUND2[$i]) / 2 ))

    # Calculate performance improvement (always relative to OFF setting)
    IMPROVEMENT=$((AVG_OFF - AVG_ON))  # Positive if ON is faster, negative if ON is slower
    if [ "$AVG_OFF" -gt 0 ]; then
        IMPROVEMENT_PERCENT=$(echo "scale=2; $IMPROVEMENT * 100 / $AVG_OFF" | bc -l)
    else
        IMPROVEMENT_PERCENT=0
    fi

    IMPROVEMENT_MS=$((IMPROVEMENT / 1000000))

    # Generate descriptive text
    if [ $IMPROVEMENT_MS -gt 0 ]; then
        IMPROVEMENT_TEXT="ON faster by ${IMPROVEMENT_MS}ms (${IMPROVEMENT_PERCENT}%)"
    elif [ $IMPROVEMENT_MS -lt 0 ]; then
        # Convert negative improvement to positive for display
        ABS_IMPROVEMENT=$((-IMPROVEMENT_MS))
        # Handle negative percentage (remove minus sign and convert to positive)
        ABS_PERCENT=$(echo "$IMPROVEMENT_PERCENT" | sed 's/^-//')
        IMPROVEMENT_TEXT="ON slower by ${ABS_IMPROVEMENT}ms (${ABS_PERCENT}%)"
    else
        IMPROVEMENT_TEXT="ON and OFF have same performance (0ms, 0.00%)"
    fi

    echo "Final Results for Query $i:" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
    echo "  enable_column_imprint = OFF : ${AVG_OFF:0:-6}ms (avg: ${OFF_DURATIONS_ROUND1[$i]:0:-6}ms, ${OFF_DURATIONS_ROUND2["$i"]:0:-6}ms)" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
    echo "  enable_column_imprint = ON:  ${AVG_ON:0:-6}ms (avg: ${ON_DURATIONS_ROUND1["$i"]:0:-6}ms, ${ON_DURATIONS_ROUND2["$i"]:0:-6}ms)" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
    echo "  Result: $IMPROVEMENT_TEXT" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
    echo "================================================" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

    # Write to comparison CSV
    echo "$i,$AVG_OFF,$AVG_ON,$IMPROVEMENT,$IMPROVEMENT_PERCENT,loop1,loop2" >> "$RESULTS_DIR/$COMPARISON_FILE"
done

echo "Performance comparison summary:" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
echo "----------------------------------------" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

# Compute stats from comparison file (exclude CSV header)
tail -n +2 "$RESULTS_DIR/$COMPARISON_FILE" | awk -F',' '
    BEGIN {
        count=0;
        off_faster=0;
        on_faster=0;
        on_faster_total_improvement=0;
        off_faster_total_improvement=0;
        max_improvement=0;
    }
    {
        count++
        off_duration=$2
        on_duration=$3
        off_duration = off_duration / 1000000
        on_duration = on_duration / 1000000

        if (off_duration > on_duration) {
            on_faster++
            improvement = off_duration - on_duration
            on_faster_total_improvement += improvement
            if (improvement > max_improvement) max_improvement = improvement
        } else if (on_duration > off_duration) {
            off_faster++
            improvement = on_duration - off_duration
            off_faster_total_improvement += improvement
            if (improvement > max_improvement) max_improvement = improvement
        }
    }
    END {
        if (count > 0) {
            printf "Total queries tested: %d\n", count
            printf "enable_column_imprint = on faster: %d (%.1f%%)\n", on_faster, (on_faster/count)*100
            printf "enable_column_imprint = off faster: %d (%.1f%%)\n", off_faster, (off_faster/count)*100
            printf "Average improvement when on is faster: %.2fms\n", (on_faster > 0) ? on_faster_total_improvement/on_faster : 0
            printf "Average improvement when off is faster: %.2fms\n", (off_faster > 0) ? off_faster_total_improvement/off_faster : 0
            printf "Maximum improvement observed: %.2fms\n", max_improvement
        }
    }' | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

echo "================================================" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
