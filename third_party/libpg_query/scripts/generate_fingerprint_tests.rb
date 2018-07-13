#!/usr/bin/env ruby

require 'bundler'
require 'json'

class Generator
  def initialize
    @fingerprint_tests = JSON.parse(File.read('./testdata/fingerprint.json'))
  end

  def generate!
    test_lines = []

    @fingerprint_tests.each do |test_def|
      test_lines << test_def['input']
      test_lines << test_def['expectedHash']
    end

    File.write './test/fingerprint_tests.c', <<-EOF
const char* tests[] = {
#{test_lines.map { |test_line| format('  %s,', test_line.inspect) }.join("\n")}
};

size_t testsLength = __LINE__ - 4;
EOF
  end
end

Generator.new.generate!
