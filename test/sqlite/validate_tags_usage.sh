#!/usr/bin/env bash

##
# assumes $SCRIPT_DIR/../../build/debug/test/unittest to be ready to run
#

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." &>/dev/null && pwd)
cd "$ROOT"

: ${UNITTEST:="build/debug/test/unittest --output-sql=true"}
: ${TESTS_SPEC:='test/sqlite/tags/*'}
export VALIDATE_TAGS=1

run() {
	$UNITTEST "$@" "$TESTS_SPEC" 2>&1 | grep "SELECT 'tagged"
}

expect() {
	output=$(cat)
	local errs=0
	if [[ "$#" -eq 0 ]]; then
		[[ $(echo -n "$output" | wc -l) -eq 0 ]] && {
			echo -n "✅ - ok"
		} || {
			printf "\n  ❌ - error - matches found but none expected:\n%s" "$output"
		}
	else
		for elt in "$@"; do
			echo -n "$output" | grep -q "'tagged $elt'" || {
				printf "\n  ❌ - error - missing %s" "$elt"
				errs=$(($errs + 1))
			}
		done
		[[ $errs -eq 0 ]] && {
			echo -n "  ✅ - ok"
		}
	fi
}

test() {
	local args="$1"
	shift
	echo -n "test $args -- "
	run $args | expect "$@"
	echo
}

test_select() {
	# select tags
	test "--select-tag 1" "1" "1 2" "1 2 3"
	test "--select-tag-set ['1']" "1" "1 2" "1 2 3"

	test "--select-tag 2" "1 2" "1 2 3"
	test "--select-tag-set ['2']" "1 2" "1 2 3"

	test "--select-tag 3" "1 2 3"
	test "--select-tag-set ['3']" "1 2 3"

	test "--select-tag-set ['1','3']" "1 2 3"
	test "--select-tag-set ['2','3']" "1 2 3"
	test "--select-tag-set ['1','2']" "1 2" "1 2 3"
	test "--select-tag-set ['1','2','3']" "1 2 3"
	test "--select-tag-set ['1','2','3','4']"
}

test_skip() {
	# skip tags
	test "--skip-tag 1"
	test "--skip-tag 1"

	test "--skip-tag 1"
	test "--skip-tag-set ['1']"

	test "--skip-tag 2" "1"
	test "--skip-tag-set ['2']" "1"

	test "--skip-tag 3" "1" "1 2"
	test "--skip-tag-set ['3']" "1" "1 2"

	test "--skip-tag-set ['1','3']" "1" "1 2"
	test "--skip-tag-set ['2','3']" "1" "1 2"
	test "--skip-tag-set ['1','2']" "1"
	test "--skip-tag-set ['1','2','3']" "1" "1 2"
	test "--skip-tag-set ['1','2','3','a']" "1" "1 2" "1 2 3" "a"
}

test_combo() {
	# crossover
	test "--select-tag 1 --skip-tag 2" "1"
	test "--skip-tag-set ['1','2','3'] --select-tag 2" "1 2"
	test "--select-tag 1 --skip-tag 1"
	test "--select-tag noexist --skip-tag 1"
	test "--select-tag 3 --skip-tag noexist" "1 2 3"

	# confirm BNF behavior
	test "--select-tag 3 --select-tag a" "1 2 3" "a"
	test "--skip-tag 3 --skip-tag a" "1" "1 2"
}

test_implicit_env() {
	test "--select-tag env[VALIDATE_TAGS]" "1" "1" "1 2" "1 2 3" "a"
	test "--select-tag env[VALIDATE_TAGS]=0"
	# NOTE: =1 not set because it's a require, not a test-env
	test "--select-tag env[VALIDATE_TAGS]=1"
}

test "" "1" "1 2" "1 2 3"
test_select
test_skip
test_combo
test_implicit_env
