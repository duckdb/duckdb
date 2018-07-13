#!/usr/bin/env ruby

# rubocop:disable Metrics/AbcSize, Metrics/LineLength, Metrics/MethodLength, Style/WordArray, Metrics/ClassLength, Style/Documentation, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity, Style/MutableConstant, Style/TrailingCommaInLiteral

require 'bundler'
require 'json'

class Generator
  def initialize
    @nodetypes = JSON.parse(File.read('./srcdata/nodetypes.json'))
    @struct_defs = JSON.parse(File.read('./srcdata/struct_defs.json'))
    @enum_defs = JSON.parse(File.read('./srcdata/enum_defs.json'))
    @typedefs = JSON.parse(File.read('./srcdata/typedefs.json'))
    @all_known_enums = JSON.parse(File.read('./srcdata/all_known_enums.json'))
  end

  FINGERPRINT_RES_TARGET_NAME = <<-EOL
  if (node->name != NULL && (field_name == NULL || parent == NULL || !IsA(parent, SelectStmt) || strcmp(field_name, "targetList") != 0)) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }
EOL

  FINGERPRINT_NODE = <<-EOL
  if (true) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, &node->%<name>s, node, "%<name>s", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "%<name>s");
  }
EOL

  FINGERPRINT_NODE_PTR = <<-EOL
  if (node->%<name>s != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->%<name>s, node, "%<name>s", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "%<name>s");
  }
EOL

  FINGERPRINT_LIST = <<-EOL
  if (node->%<name>s != NULL && node->%<name>s->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->%<name>s, node, "%<name>s", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "%<name>s");
  }
EOL

  FINGERPRINT_INT = <<-EOL
  if (node->%<name>s != 0) {
    char buffer[50];
    sprintf(buffer, "%%d", node->%<name>s);
    _fingerprintString(ctx, "%<name>s");
    _fingerprintString(ctx, buffer);
  }

EOL

  FINGERPRINT_LONG_INT = <<-EOL
  if (node->%<name>s != 0) {
    char buffer[50];
    sprintf(buffer, "%%ld", node->%<name>s);
    _fingerprintString(ctx, "%<name>s");
    _fingerprintString(ctx, buffer);
  }

EOL

  FINGERPRINT_FLOAT = <<-EOL
if (node->%<name>s != 0) {
  char buffer[50];
  sprintf(buffer, "%%f", node->%<name>s);
  _fingerprintString(ctx, "%<name>s");
  _fingerprintString(ctx, buffer);
}

EOL

  FINGERPRINT_INT_ARRAY = <<-EOL
  if (true) {
    int x;
    Bitmapset	*bms = bms_copy(node->%<name>s);

    _fingerprintString(ctx, "%<name>s");

  	while ((x = bms_first_member(bms)) >= 0) {
      char buffer[50];
      sprintf(buffer, "%%d", x);
      _fingerprintString(ctx, buffer);
    }

    bms_free(bms);
  }
EOL

  # Fingerprinting additional code to be inserted
  FINGERPRINT_OVERRIDE_NODES = {
    'A_Const' => :skip,
    'Alias' => :skip,
    'ParamRef' => :skip,
    'SetToDefault' => :skip,
    'IntList' => :skip,
    'OidList' => :skip,
    'Null' => :skip,
  }
  FINGERPRINT_OVERRIDE_FIELDS = {
    [nil, 'location'] => :skip,
    ['ResTarget', 'name'] => FINGERPRINT_RES_TARGET_NAME,
    ['PrepareStmt', 'name'] => :skip,
    ['ExecuteStmt', 'name'] => :skip,
    ['DeallocateStmt', 'name'] => :skip,
    ['TransactionStmt', 'options'] => :skip,
    ['TransactionStmt', 'gid'] => :skip,
  }
  INT_TYPES = ['bits32', 'uint32', 'int', 'Oid', 'int32', 'Index', 'AclMode', 'int16', 'AttrNumber', 'uint16']
  LONG_INT_TYPES = ['long']
  INT_ARRAY_TYPES = ['Bitmapset*', 'Bitmapset', 'Relids']
  FLOAT_TYPES = ['Cost']

  IGNORE_FOR_GENERATOR = ['Integer', 'Float', 'String', 'BitString', 'List']

  def generate_fingerprint_defs!
    @fingerprint_defs = {}

    ['nodes/parsenodes', 'nodes/primnodes'].each do |group|
      @struct_defs[group].each do |type, struct_def|
        next if struct_def['fields'].nil?
        next if IGNORE_FOR_GENERATOR.include?(type)

        fp_override = FINGERPRINT_OVERRIDE_NODES[type]
        if fp_override
          fp_override = "  // Intentionally ignoring all fields for fingerprinting\n" if fp_override == :skip
          fingerprint_def = fp_override
        else
          fingerprint_def = format("  _fingerprintString(ctx, \"%s\");\n", type)
          struct_def['fields'].reject { |f| f['name'].nil? }.sort_by { |f| f['name'] }.each do |field|
            name = field['name']
            field_type = field['c_type']

            fp_override = FINGERPRINT_OVERRIDE_FIELDS[[type, field['name']]] || FINGERPRINT_OVERRIDE_FIELDS[[nil, field['name']]]
            if fp_override
              if fp_override == :skip
                fp_override = format("  // Intentionally ignoring node->%s for fingerprinting\n", name)
              end
              fingerprint_def += fp_override
              next
            end

            case field_type
            # when '[][]Node'
            #  fingerprint_def += format(FINGERPRINT_NODE_ARRAY_ARRAY, name: name)
            # when '[]Node'
            #  fingerprint_def += format(FINGERPRINT_NODE_ARRAY, name: name)
            when 'Node'
              fingerprint_def += format(FINGERPRINT_NODE, name: name)
            when 'Node*', 'Expr*'
              fingerprint_def += format(FINGERPRINT_NODE_PTR, name: name)
            when 'List*'
              fingerprint_def += format(FINGERPRINT_LIST, name: name)
            when 'CreateStmt'
              fingerprint_def += format("  _fingerprintString(ctx, \"%s\");\n", name)
              fingerprint_def += format("  _fingerprintCreateStmt(ctx, (const CreateStmt*) &node->%s, node, \"%s\", depth);\n", name, name)
            when 'char'
              fingerprint_def += format("  if (node->%s != 0) {\n", name)
              fingerprint_def += format("    char str[2] = {node->%s, '\\0'};\n", name)
              fingerprint_def += format("    _fingerprintString(ctx, \"%s\");\n", name)
              fingerprint_def += "    _fingerprintString(ctx, str);\n"
              fingerprint_def += "  }\n\n"
            when 'string'
              fingerprint_def += format("  if (strlen(node->%s) > 0) {\n", name)
              fingerprint_def += format("    _fingerprintString(ctx, \"%s\");\n", name)
              fingerprint_def += format("    _fingerprintString(ctx, node->%s);\n", name)
              fingerprint_def += "  }\n\n"
            when 'char*'
              fingerprint_def += format("\n  if (node->%s != NULL) {\n", name)
              fingerprint_def += format("    _fingerprintString(ctx, \"%s\");\n", name)
              fingerprint_def += format("    _fingerprintString(ctx, node->%s);\n", name)
              fingerprint_def += "  }\n\n"
            when 'bool'
              fingerprint_def += format("\n  if (node->%s) {", name)
              fingerprint_def += format("    _fingerprintString(ctx, \"%s\");\n", name)
              fingerprint_def += format("    _fingerprintString(ctx, \"true\");\n")
              fingerprint_def += "  }\n\n"
            when 'Datum', 'void*', 'Expr', 'NodeTag'
              # Ignore
            when *INT_TYPES
              fingerprint_def += format(FINGERPRINT_INT, name: name)
            when *LONG_INT_TYPES
              fingerprint_def += format(FINGERPRINT_LONG_INT, name: name)
            when *INT_ARRAY_TYPES
              fingerprint_def += format(FINGERPRINT_INT_ARRAY, name: name)
            when *FLOAT_TYPES
              fingerprint_def += format(FINGERPRINT_FLOAT, name: name)
            else
              if field_type.end_with?('*') && @nodetypes.include?(field_type[0..-2])
                fingerprint_def += format(FINGERPRINT_NODE_PTR, name: name)
              elsif @all_known_enums.include?(field_type)
                fingerprint_def += format(FINGERPRINT_INT, name: name)
              else
                # This shouldn't happen - if it does the above is missing something :-)
                puts type
                puts name
                puts field_type
                raise type
              end
            end
          end
        end

        @fingerprint_defs[type] = fingerprint_def
      end
    end
  end

  def generate!
    generate_fingerprint_defs!

    defs = ''
    conds = ''

    @nodetypes.each do |type|
      # next if IGNORE_LIST.include?(type)
      fingerprint_def = @fingerprint_defs[type]
      next unless fingerprint_def

      defs += "static void\n"
      defs += format("_fingerprint%s(FingerprintContext *ctx, const %s *node, const void *parent, const char *field_name, unsigned int depth)\n", type, type)
      defs += "{\n"
      defs += fingerprint_def
      defs += "}\n"
      defs += "\n"

      conds += format("case T_%s:\n", type)
      conds += format("  _fingerprint%s(ctx, obj, parent, field_name, depth);\n", type)
      conds += "  break;\n"
    end

    File.write('./src/pg_query_fingerprint_defs.c', defs)
    File.write('./src/pg_query_fingerprint_conds.c', conds)
  end
end

Generator.new.generate!
