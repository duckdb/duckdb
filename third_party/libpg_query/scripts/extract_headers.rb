#!/usr/bin/env ruby

# You need to call this with the PostgreSQL source directory as the first commandline agument
# ./scripts/extract_headers.rb ./my_postgres_src_dir

# rubocop:disable Style/PerlBackrefs, Metrics/AbcSize, Metrics/LineLength, Metrics/MethodLength, Style/WordArray, Metrics/ClassLength, Style/Documentation, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity, Style/TrailingComma, Style/RegexpLiteral

require 'bundler'
require 'json'

def underscore(str)
  str
    .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
    .gsub(/([a-z\d])([A-Z])/, '\1_\2')
    .tr('-', '_')
    .gsub(/\s/, '_')
    .gsub(/__+/, '_')
    .downcase
end

def classify(str)
  str
    .gsub(/([A-Z]+)/, '_\1')
    .split('_').collect(&:capitalize).join
end

class Extractor
  def initialize(pgdir)
    @pgdir = pgdir
  end

  def generate_nodetypes!
    inside = false
    @nodetypes = []

    lines = File.read(File.join(@pgdir, '/src/include/nodes/nodes.h'))
    lines.each_line do |line|
      if inside
        if line[/([A-z_]+)(\s+=\s+\d+)?,/]
          @nodetypes << $1[2..-1] # Without T_ prefix
        elsif line == "} NodeTag;\n"
          inside = false
        end
      elsif line == "typedef enum NodeTag\n"
        inside = true
      end
    end
  end

  IGNORE_LIST = [
    'Node', 'NodeTag', 'varlena', 'IntArray', 'nameData', 'bool',
    'sig_atomic_t', 'size_t', 'varatt_indirect',
  ]

  def generate_defs!
    @target_struct = nil
    @open_comment = false

    @all_known_enums = []
    @enum_defs = {}
    @struct_defs = {}
    @typedefs = []

    ['nodes/parsenodes', 'nodes/primnodes', 'nodes/lockoptions',
     'nodes/nodes', 'nodes/params', 'access/attnum', 'c', 'postgres', 'postgres_ext',
     'storage/block', 'access/sdir'].each do |group|
      @target_group = group
      @struct_defs[@target_group] = {}
      @enum_defs[@target_group] = {}
      @comment_text = nil

      lines = File.read(File.join(@pgdir, format('/src/include/%s.h', @target_group)))
      lines.each_line do |line|
        if !@current_struct_def.nil?
          handle_struct(line)
        elsif !@current_enum_def.nil?
          handle_enum(line)
        elsif line[/^typedef struct ([A-z]+)\s*(\/\*.+)?$/]
          next if IGNORE_LIST.include?($1)
          @current_struct_def = { fields: [], comment: @open_comment_text }
          @open_comment_text = nil
        elsif line[/^typedef enum( [A-z]+)?\s*(\/\*.+)?$/]
          next if IGNORE_LIST.include?($1)
          @current_enum_def = { values: [], comment: @open_comment_text }
          @open_comment_text = nil
        elsif line[/^typedef( struct)? ([A-z0-9\s_]+) \*?([A-z]+);/]
          next if IGNORE_LIST.include?($2) || IGNORE_LIST.include?($3)
          @typedefs << { new_type_name: $3, source_type: $2, comment: @open_comment_text }
          @open_comment_text = nil
        elsif line.strip.start_with?('/*')
          @open_comment_text = line
          @open_comment = !line.include?('*/')
        elsif @open_comment
          @open_comment_text += "\n" unless @open_comment_text.end_with?("\n")
          @open_comment_text += line
          @open_comment = !line.include?('*/')
        end
      end
    end
  end

  def handle_struct(line)
    if line[/^\s+(struct |const )?([A-z0-9]+)\s+(\*){0,2}([A-z_]+);\s*(\/\*.+)?/]
      name = $4
      c_type = $2 + $3.to_s
      comment = $5

      @current_struct_def[:fields] << { name: name, c_type: c_type, comment: comment }

      @open_comment = line.include?('/*') && !line.include?('*/')
    elsif line[/^\}\s+([A-z]+);/]
      @struct_defs[@target_group][$1] = @current_struct_def
      @current_struct_def = nil
    elsif line.strip.start_with?('/*')
      @current_struct_def[:fields] << { comment: line }
      @open_comment = !line.include?('*/')
    elsif @open_comment
      @current_struct_def[:fields].last[:comment] += "\n" unless @current_struct_def[:fields].last[:comment].end_with?("\n")
      @current_struct_def[:fields].last[:comment] += line
      @open_comment = !line.include?('*/')
    elsif !@current_struct_def[:fields].empty?
      @current_struct_def[:fields] << { comment: '' }
    end
  end

  def handle_enum(line)
    if line[/^\s+([A-z0-9_]+),?\s*([A-z0-9_]+)?(\/\*.+)?/]
      name = $1
      other_name = $2
      comment = $3

      @current_enum_def[:values] << { name: name, comment: comment }
      @current_enum_def[:values] << { name: other_name } if other_name

      @open_comment = line.include?('/*') && !line.include?('*/')
    elsif line[/^\}\s+([A-z]+);/]
      @all_known_enums << $1
      @enum_defs[@target_group][$1] = @current_enum_def
      @current_enum_def = nil
    elsif line.strip.start_with?('/*')
      @current_enum_def[:values] << { comment: line }
      @open_comment = !line.include?('*/')
    elsif @open_comment
      @current_enum_def[:values].last[:comment] += "\n" unless @current_enum_def[:values].last[:comment].end_with?("\n")
      @current_enum_def[:values].last[:comment] += line
      @open_comment = !line.include?('*/')
    elsif !@current_enum_def.empty?
      @current_enum_def[:values] << { comment: '' }
    end
  end

  # Top-of-struct comment special cases - we might want to merge these into the same output files at some point
  COMMENT_ENUM_TO_STRUCT = {
    'nodes/parsenodes' => {
      'SelectStmt' => 'SetOperation',
      'CreateRoleStmt' => 'RoleStmtType',
      'AlterRoleStmt' => 'RoleStmtType',
      'AlterRoleSetStmt' => 'RoleStmtType',
      'DropRoleStmt' => 'RoleStmtType',
      'A_Expr' => 'A_Expr_Kind',
      'DefElem' => 'DefElemAction',
      'DiscardStmt' => 'DiscardMode',
      'FetchStmt' => 'FetchDirection',
      'GrantStmt' => 'GrantTargetType',
      'RangeTblEntry' => 'RTEKind',
      'TransactionStmt' => 'TransactionStmtKind',
      'VacuumStmt' => 'VacuumOption',
      'ViewStmt' => 'ViewCheckOption',
    },
    'nodes/primnodes' => {
      'MinMaxExpr' => 'MinMaxOp',
      'Param' => 'ParamKind',
      'RowCompareExpr' => 'RowCompareType',
      'SubLink' => 'SubLinkType',
      'BooleanTest' => 'BoolTestType',
      'NullTest' => 'NullTestType',
    }
  }
  COMMENT_STRUCT_TO_STRUCT = {
    'nodes/parsenodes' => {
      'AlterDatabaseSetStmt' => 'AlterDatabaseStmt',
      # 'AlterExtensionStmt' => 'CreateExtensionStmt', # FIXME: This overrides an existing sub-comment
      'AlterExtensionContentsStmt' => 'CreateExtensionStmt',
      'AlterFdwStmt' => 'CreateFdwStmt',
      'AlterForeignServerStmt' => 'CreateForeignServerStmt',
      'AlterFunctionStmt' => 'CreateFunctionStmt',
      'AlterSeqStmt' => 'CreateSeqStmt',
      'AlterTableCmd' => 'AlterTableStmt',
      'ReplicaIdentityStmt' => 'AlterTableStmt',
      'AlterUserMappingStmt' => 'CreateUserMappingStmt',
      'DropUserMappingStmt' => 'CreateUserMappingStmt',
      'CreateOpClassItem' => 'CreateOpClassStmt',
      'DropTableSpaceStmt' => 'CreateTableSpaceStmt',
      'FunctionParameter' => 'CreateFunctionStmt',
      'InlineCodeBlock' => 'DoStmt',
    },
    'nodes/params' => {
      'ParamListInfoData' => 'ParamExternData',
    },
  }
  def transform_toplevel_comments!
    COMMENT_ENUM_TO_STRUCT.each do |file, mapping|
      mapping.each do |target, source|
        @struct_defs[file][target][:comment] = @enum_defs[file][source][:comment]
      end
    end

    COMMENT_STRUCT_TO_STRUCT.each do |file, mapping|
      mapping.each do |target, source|
        @struct_defs[file][target][:comment] = @struct_defs[file][source][:comment]
      end
    end
  end

  def extract!
    generate_nodetypes!
    generate_defs!
    transform_toplevel_comments!

    @struct_defs['nodes/value'] = {}
    @struct_defs['nodes/value']['Integer'] = { fields: [{ name: 'ival', c_type: 'long' }] }
    @struct_defs['nodes/value']['Float'] = { fields: [{ name: 'str', c_type: 'char*' }] }
    @struct_defs['nodes/value']['String'] = { fields: [{ name: 'str', c_type: 'char*' }] }
    @struct_defs['nodes/value']['BitString'] = { fields: [{ name: 'str', c_type: 'char*' }] }
    @struct_defs['nodes/pg_list'] = { 'List' => { fields: [{ name: 'items', c_type: '[]Node' }] } }
    @struct_defs['nodes/value']['Null'] = { fields: [] }
    @struct_defs['nodes/params']['ParamListInfoData'][:fields].reject! { |f| f[:c_type] == 'ParamExternData' }

    File.write('./srcdata/nodetypes.json', JSON.pretty_generate(@nodetypes))
    File.write('./srcdata/all_known_enums.json', JSON.pretty_generate(@all_known_enums))
    File.write('./srcdata/struct_defs.json', JSON.pretty_generate(@struct_defs))
    File.write('./srcdata/enum_defs.json', JSON.pretty_generate(@enum_defs))
    File.write('./srcdata/typedefs.json', JSON.pretty_generate(@typedefs))
  end
end

Extractor.new(ARGV[0]).extract!
