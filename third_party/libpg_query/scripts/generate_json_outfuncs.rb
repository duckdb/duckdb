#!/usr/bin/env ruby

# rubocop:disable Metrics/MethodLength, Style/WordArray, Metrics/LineLength, Style/Documentation, Style/PerlBackrefs, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

require 'bundler'
require 'json'

class Generator
  def initialize
    @nodetypes = JSON.parse(File.read('./srcdata/nodetypes.json'))
    @struct_defs = JSON.parse(File.read('./srcdata/struct_defs.json'))
    @typedefs = JSON.parse(File.read('./srcdata/typedefs.json'))
  end

  TYPE_OVERRIDES = {
    ['Query', 'queryId']        => :skip, # we intentionally do not print the queryId field
    ['RangeVar', 'catalogname'] => :skip, # presently not semantically meaningful
  }

  def generate_outmethods!
    @outmethods = {}

    ['nodes/parsenodes', 'nodes/primnodes'].each do |group|
      @struct_defs[group].each do |node_type, struct_def|
        @outmethods[node_type] = format("  WRITE_NODE_TYPE(\"%s\");\n\n", node_type)

        struct_def['fields'].each do |field_def|
          name = field_def['name']
          orig_type = field_def['c_type']

          next unless name && orig_type

          type = TYPE_OVERRIDES[[node_type, name]] || orig_type

          if type == :skip || type == 'Expr'
            # Ignore
          elsif type == 'NodeTag'
            # Nothing
          elsif ['bool', 'long', 'char'].include?(type)
            @outmethods[node_type] += format("  WRITE_%s_FIELD(%s);\n", type.upcase, name)
          elsif ['int', 'int16', 'int32', 'AttrNumber'].include?(type)
            @outmethods[node_type] += format("  WRITE_INT_FIELD(%s);\n", name)
          elsif ['uint', 'uint16', 'uint32', 'Index', 'bits32', 'Oid'].include?(type)
            @outmethods[node_type] += format("  WRITE_UINT_FIELD(%s);\n", name)
          elsif type == 'char*'
            @outmethods[node_type] += format("  WRITE_STRING_FIELD(%s);\n", name)
          elsif ['float', 'double', 'Cost', 'Selectivity'].include?(type)
            @outmethods[node_type] += format("  WRITE_FLOAT_FIELD(%s);\n", name)
          elsif ['Bitmapset*', 'Relids'].include?(type)
            @outmethods[node_type] += format("  WRITE_BITMAPSET_FIELD(%s);\n", name)
          elsif ['Value'].include?(type)
            @outmethods[node_type] += format("  WRITE_NODE_FIELD(%s);\n", name)
          elsif ['CreateStmt'].include?(type)
            # Special case where the node is embedded but with the wrong tag
            @outmethods[node_type] += format("  WRITE_NODE_FIELD_WITH_TYPE(%s, %s);\n", name, type)
          elsif type == 'Node*' || @nodetypes.include?(type[0..-2])
            @outmethods[node_type] += format("  WRITE_NODE_PTR_FIELD(%s);\n", name)
          elsif type.end_with?('*')
            puts format('ERR: %s %s', name, type)
          else # Enum
            @outmethods[node_type] += format("  WRITE_ENUM_FIELD(%s);\n", name)
          end
        end
      end
    end

    @typedefs.each do |typedef|
      next unless @outmethods[typedef['source_type']]

      @outmethods[typedef['new_type_name']] = @outmethods[typedef['source_type']]
    end
  end

  IGNORE_LIST = [
    'Expr', # Unclear why this isn't needed (FIXME)
    'Value', # Special case
    'Const', # Only needed in post-parse analysis (and it introduces Datums, which we can't output)
  ]
  def generate!
    generate_outmethods!

    defs = ''
    conds = ''

    @nodetypes.each do |type|
      next if IGNORE_LIST.include?(type)
      outmethod = @outmethods[type]
      next unless outmethod

      defs += "static void\n"
      defs += format("_out%s(StringInfo str, const %s *node)\n", type, type)
      defs += "{\n"
      defs += outmethod
      defs += "}\n"
      defs += "\n"

      conds += format("case T_%s:\n", type)
      conds += format("  _out%s(str, obj);\n", type)
      conds += "  break;\n"
    end

    File.write('./src/pg_query_json_defs.c', defs)
    File.write('./src/pg_query_json_conds.c', conds)
  end
end

Generator.new.generate!
