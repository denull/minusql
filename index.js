function toCamelCase(k) {
  return k ? (k + '').split('_').map((word, i) => i > 0 ? word[0].toUpperCase() + word.substring(1) : word).join('') : k;
}

function toSnakeCase(k) {
  return k ? (k + '').replace(/(([a-z])(?=[A-Z]([a-zA-Z]|$))|([A-Z])(?=[A-Z][a-z]))/g,'$1_').toLowerCase() : k;
}

const CHARS_GLOBAL_REGEXP = /[\0\b\t\n\r\x1a\"\'\\]/g;
const CHARS_ESCAPE_MAP    = {
  '\0'   : '\\0',
  '\b'   : '\\b',
  '\t'   : '\\t',
  '\n'   : '\\n',
  '\r'   : '\\r',
  '\x1a' : '\\Z',
  '"'    : '\\"',
  '\''   : '\\\'',
  '\\'   : '\\\\'
};

function escapeMysqlIdent(val, forbidQualified) { // From sqlstring
  return forbidQualified ?
    '`' + String(val).replace(/`/g, '``') + '`' :
    '`' + String(val).replace(/`/g, '``').replace(/\./g, '`.`') + '`';
}

function escapeMysqlString(val) { // From sqlstring
  let chunkIndex = CHARS_GLOBAL_REGEXP.lastIndex = 0;
  let escapedVal = '';
  let match;

  while ((match = CHARS_GLOBAL_REGEXP.exec(val))) {
    escapedVal += val.slice(chunkIndex, match.index) + CHARS_ESCAPE_MAP[match[0]];
    chunkIndex = CHARS_GLOBAL_REGEXP.lastIndex;
  }

  if (chunkIndex === 0) {
    // Nothing was escaped
    return "'" + val + "'";
  }

  if (chunkIndex < val.length) {
    return "'" + escapedVal + val.slice(chunkIndex) + "'";
  }

  return "'" + escapedVal + "'";
}

function escapePostgresIdent(val, forbidQualified) {
  return forbidQualified ?
    '"' + String(val).replace(/"/g, '""') + '"' :
    '"' + String(val).replace(/"/g, '""').replace(/\./g, '"."') + '"';
}

function escapePostgresString(val) { // From pg-format
  let hasBackslash = false;
  let quoted = '\'';
  for (let i = 0; i < val.length; i++) {
    const c = val[i];
    if (c === '\'') {
      quoted += c + c;
    } else if (c === '\\') {
      quoted += c + c;
      hasBackslash = true;
    } else {
      quoted += c;
    }
  }
  quoted += '\'';
  if (hasBackslash) {
    quoted = 'E' + quoted;
  }
  return quoted;
}

function isVar(v) {
  return v && typeof v === 'object' && '$' in v;
}
function isMySQL(sql) {
  return sql.$config.flavor === 'mysql';
}
function isPostgres(sql) {
  return sql.$config.flavor === 'postgres';
}

const MaybeUnaryOperators = [
  '-', '~', '#', '@@', '@-@', '?-', '!!', ':', '|/', '||/', '@', '%',
];
const BinaryOperators = [
  '=', '!=', '<>', '>', '>=', '<', '<=', // Comparison
  '>>', '<<', '%', 'MOD', 'DIV', // Arithmetic
  'LIKE', 'NOT LIKE', 'ILIKE', 'NOT ILIKE', 'SIMILAR TO', 'NOT SIMILAR TO', // Pattern-matching
  'REGEXP', 'RLIKE', 'NOT REGEXP', 'NOT RLIKE', 'SOUNDS LIKE', // MySQL RegExps & Soundex
  '~~', '!~~', '~', '~*', '!~', '!~*', // Postgres patterns and RegExps
  '->', '->>', '#>', '#>>', '@>', '<@', '?', '?|', '?&', '#-', // Postgres JSON operators
  '@@', '&&', '<->', // Postgres full-text search operators
  '#', '@-@', '@@', '##', '<^', '>^', '?#', '?-', '?-|', '?||', // Postgres geometry
  '&&&', '&<', '&<|', '&>', '<<|', '@', '|&>', '|>>', '~=', '|=|', '<#>', '<<->>', // PostGIS operators
  '<%', '%>', '<<%', '%>>', '<<->', '<->>', '<<<->', '<->>>', // Postgres trigram operators
  'AT TIME ZONE', 'OVERLAPS', '>>=', '<<=', '!!=', '-|-', // Misc Postgres operators
];
const Operators = [
  ...BinaryOperators,
  '+', '-', '*', '/', '&', '|', '^',
  'AND', 'OR', 'XOR',
  '||', // Postgres concatenation
];
class QueryParts {
  constructor(sql, chunks, params = []) {
    this.sql = sql;
    this.chunks = chunks ? (Array.isArray(chunks) ? chunks : [chunks]) : [''];
    this.params = params;
  }

  toString() {
    return this.chunks.map(isPostgres(this.sql) ? (chunk, i) => {
      return (i > 0 ? '$' + i : '') + chunk;
    } : (chunk, i) => {
      return (i > 0 ? '?' : '') + chunk;
    }).join('');
  }
  
  append(...args) {
    if (Array.isArray(args[0])) {
      for (let i = 0; i < args[0].length; i++) {
        if (i > 0 && args[2]) {
          this.chunks[this.chunks.length - 1] += args[2];
        }
        args[1].call(this, args[0][i], i, args[0]);
      }
      return this;
    }
    this.chunks[this.chunks.length - 1] += args[0];
    for (let i = 1; i < args.length; i++) {
      if (i % 2 === 1) {
        this.params.push(args[i]);
      } else {
        this.chunks.push(args[i]);
      }
    }
    if (args.length % 2 === 0) { // Last chunk should always be a string
      this.chunks.push('');
    }
    return this; // Make chainable
  }

  ident(ident) {
    if (ident === '*') return '*';
    if (this.sql.$config.convertCase) {
      ident = toSnakeCase(ident);
    }
    return isMySQL(this.sql) ? escapeMysqlIdent(ident) : escapePostgresIdent(ident); 
  }

  keyword(name) {
    if (!/^[A-Za-z ]+/.test(name)) {
      throw new Error(`Keyword expected here, got "${name}" instead`);
    }
    return name;
  }

  regexp({ source, flags }, forceRegExp) {
    const isCaseSensitive = !flags.includes('i');
    let pattern;
    if (!forceRegExp) {
      pattern = source
        .replace(/^\^/, '').replace(/\$$/, '')
        .replace(/%/g, '\\%').replace(/_/g, '\\_')
        .replace(/\.\*/g, '%').replace(/\./g, '_');
      
      if (!source.startsWith('^') && !pattern.startsWith('%')) {
        pattern = '%' + pattern;
      }
      if (!source.endsWith('$') && (!pattern.endsWith('%') || pattern.endsWith('\\%'))) {
        pattern = pattern + '%';
      }

      const isComplex = 
        /[\^\$\(\)\[\]\{\}\?\+\*\|]/.test(pattern) || // Special RegExp chars
        /\\[dDsSwWbB]/.test(source) || // Character classes
        /\(\?[=!:]/.test(source);      // Lookahead/lookbehind
      if (!isComplex) {
        return {
          pattern,
          append: (lhs, inVar) =>
            isCaseSensitive ? this.append(`${this.ident(lhs)} LIKE `).value(pattern, inVar) :
              (isPostgres(this.sql) ? this.append(`${this.ident(lhs)} ILIKE `).value(pattern, inVar) :
                this.append(`LOWER(${this.ident(lhs)}) LIKE `).value(pattern.toLowerCase(), inVar)),
        }
      }
    }

    pattern = source;
    if (isPostgres(this.sql)) {
      pattern = pattern
        .replace(/\\b/g, '\\y');

      return {
        pattern,
        append: (lhs, inVar) =>
          this.append(`${this.ident(lhs)} ${isCaseSensitive ? '~' : '~*'} `).value(pattern, inVar),
      }
    }

    return {
      pattern,
      append: (lhs, inVar) =>
        this.append(`${this.ident(lhs)} REGEXP `).value(pattern, inVar).append(isCaseSensitive ? '' : ' COLLATE utf8_general_ci'),
    }
  }

  value(value, inVar = false) {
    if (isVar(value) || inVar) {
      let v = inVar ? value : value.$;
      let t = inVar ? false : value.type;
      if (Array.isArray(v)) {
        return this.append(v, el => this.value(el, true), ',');
      }
      if (v instanceof RegExp) {
        v = this.regexp(v, true).pattern;
      }
      if (value.type === 'unixtime') {
        return this.append(
          isPostgres(this.sql) ? 'TO_TIMESTAMP(' : 'FROM_UNIXTIME(',
          v instanceof Date ?
            v.getTime() / 1000 : (
              typeof v === 'string' && v.toUpperCase() === 'NOW' ?
              Date.now() / 1000 : v
            ),
          ')',
        );
      }
      if (t) {
        return isPostgres(this.sql) ? this.append('', v, `::${t}`) : this.append('CAST(', v, ` AS ${t})`);
      }
      return this.append('', v);
    }
    if (value === null || value === undefined) {
      return this.append('NULL');
    }
    switch (typeof value) {
      case 'symbol': return this.append(this.ident(value.description));
      case 'boolean': return this.append(isPostgres(this.sql) ? (value ? `'t'` : `'f'`) : (value ? 'true' : 'false'));
      case 'number': return this.append(value + '');
      case 'string': return this.append(isPostgres(this.sql) ? escapePostgresString(value) : escapeMysqlString(value));
      default:
        if (value instanceof RegExp) {
          const { pattern } = this.regexp(value, true);
          return this.append(isPostgres(this.sql) ? escapePostgresString(pattern) : escapeMysqlString(pattern));
        }
        throw new Error(`Unsupported type: ${typeof value}, ${JSON.stringify(value)}`);
    }
  }
  
  expr(e) {
    // Two variants: array (['func', ...args]) and object ({ field: value, ... })
    if (Array.isArray(e) && !isVar(e)) {
      if (typeof e[0] !== 'string') {
        throw new Error(`First element of array-style expression must a function/operator name, got "${e[0]}" instead`);
      }
      const fn = e.shift().toUpperCase();
      function checkArity(n) {
        if (e.length != n) throw new Error(`"${fn}" requires exactly ${n} operands (${e.length} supplied)`);
      }
  
      // Operators
      if (MaybeUnaryOperators.includes(fn) && (e.length === 1)) {
        return this.append(fn + ' ').expr(e[0]);
      }
      if (Operators.includes(fn)) {
        if (BinaryOperators.includes(fn)) {
          checkArity(2);
        }
        return this.append('(').append(e, this.expr, ` ${fn} `).append(')');
      }
  
      switch (fn) {
        case 'IN':
        case 'NOTIN':
        case 'NOT IN':
          checkArity(2);
          this.expr(e[0])
            .append(fn === 'IN' ? ' IN (' : ' NOT IN (');
          if (isVar(e[1])) {
            return this.value(e[1]).append(')');
          }
          if (!Array.isArray(e[1])) {
            throw new Error(`"${fn}" should take array as its second argument, ${typeof e[1]} supplied`);
          }
          return this.append(e[1], this.expr, ',').append(')');
        case 'IS NULL':
        case 'IS NOT NULL':
          checkArity(1);
          return this.expr(e[0]).append(` ${fn} `);
        case 'NOT':
          checkArity(1);
          return this.append(` ${fn} `).expr(e[0]);
        case 'BETWEEN':
        case 'NOT BETWEEN':
          checkArity(3);
          return this.expr(e[0]).append(` ${fn} `).expr(e[1]).append(' AND ').expr(e[2]);
        case 'TYPE':
          checkArity(2);
          return this.append(this.keyword(e[1]) + ' ').expr(e[0]);
        case 'CAST':
          checkArity(2);
          if (isPostgres(this.sql)) {
            return this.expr(e[0]).append(`::${this.keyword(e[1])}`);
          }
          return this.append('CAST(').expr(e[0]).append(` AS ${this.keyword(e[1])})`);
        case 'EXTRACT':
          checkArity(2);
          return this.append(`EXTRACT(${this.keyword(e[1])} FROM `).expr(e[0]).append(')');
        case 'CASE':
          return this.append('CASE ')
            .append((cond, i) => {
              if (cond.length > 1) {
                return this.append('WHEN ')
                  .expr(cond[0])
                  .append(' THEN ')
                  .expr(cond[1]);
              }
              if (i === 0) {
                return this.expr(cond[0]);
              }
              if (i === e.length - 1) {
                return this.append('ELSE ')
                  .expr(cond[0]);
              }
              throw new Error('Invalid case format');
            }, ' ')
            .append(' END');
        default:
          return this.append(`${this.keyword(fn)}(`)
            .append(e, this.expr, ',')
            .append(')');
      }
    }

    if (e && typeof e === 'object' && !isVar(e) && !(e instanceof RegExp)) {
      return this.append(Object.keys(e), k => {
        const v = e[k];
        if (Array.isArray(v)) {
          return this.expr([v[0], Symbol(k), ...v.slice(1)]);
        } else
        if (v === null) {
          return this.append(`${this.ident(k)} IS NULL`);
        } else
        if (v instanceof RegExp) {
          return this.regexp(v).append(k);
        } else
        if (v && typeof v === 'object' && '$' in v && v.$ instanceof RegExp) {
          return this.regexp(v.$).append(k, true);
        }
        return this.append(`${this.ident(k)}=`).value(v);
      }, ' AND ');
    }

    return this.value(e);
  }

  table(tables) {
    return this.append(tables, (t, i) => {
      if (typeof t === 'string') {
        return this.append(`${i > 0 ? 'LEFT JOIN ' : ''}${this.ident(t)}`);
      }
      if (i > 0) {
        this.append((t.join || 'LEFT') + ' JOIN ');
      }
      if (t.table instanceof Query) {
        this.append('(' + t.table.chunks[0]);
        this.params.push(...t.table.params);
        this.chunks.push(...t.table.chunks.slice(1));
        this.append(')');
      } else {
        this.append(this.ident(t.table));
      }
      if (t.as) {
        this.append(` AS ${this.ident(t.as)}`);
      }
      if (t.on) {
        this.append(' ON ').where(t.on);
      }
    }, ' ');
  }

  fields(fields) {
    if (typeof fields === 'string') {
      return this.append(fields);
    }
    if (Array.isArray(fields)) {
      return this.append(fields, (field) => this.append(this.ident(field)), ',');
    }
    return this.append(Object.keys(fields), (field) => {
      if (fields[field] === true) {
        return this.append(this.ident(field));
      }
      this.expr(fields[field]).append(` AS ${this.ident(field)}`);
    }, ',');
  }

  where(where) {
    if (!where) {
      return this;
    }
    return this.expr(where);
  }

  exprs(exprs) {
    if (typeof exprs === 'string') {
      return this.append(exprs);
    }
    if (Array.isArray(exprs)) {
      return this.append(exprs, (e) => typeof e === 'string' ?
        this.append(this.ident(e)) :
        this.expr(e), ',');
    }
    return this.expr(exprs);
  }

  order(exprs) {
    if (typeof exprs === 'string') {
      return this.append(exprs);
    }
    if (Array.isArray(exprs)) {
      return this.append(exprs, (e) => typeof e === 'string' ?
        this.append(this.ident(e)) :
        this.expr(e[0]).append(e[1] ? ` ${this.keyword(e[1])}` : ''), ',');
    }
    return this.expr(exprs);
  }

  updates(updates, transform) {
    if (typeof updates === 'string') {
      return this.append(updates);
    }
    return this.append(Object.keys(updates), (key) => {
      this.append(`${this.ident(key)}=`);
      const value = updates[key];
      if (typeof transform === 'function') {
        return this.expr(transform(value, key, updates));
      }
      if (transform === false) { // do not wrap any values at all
        return this.expr(value);
      } else
      if (typeof transform === 'object') {
        if (transform[key] === false) { // false = do not wrap (as a parameter)
          return this.expr(value);
        } else
        if (typeof transform[key] === 'string') { // string = wrap with type
          if (value && typeof value === 'object' && '$' in value) { // already wrapped, add type
            return this.expr(Object.assign({}, value, {type: transform[key]}));
          }
          return this.expr({$: value, type: transform[key]});
        } else
        if (typeof transform[key] === 'function') { // function = wrapper function
          return this.expr(transform[key](value, key, updates));
        }
      }

      if (value && typeof value === 'object' && '$' in value) { // Already wrapped
        return this.expr(value);
      }
      return this.expr({$: value});
    }, ',');
  }

  rows(rows, fields, transform) {
    if (typeof rows === 'number') {
      rows = Array(rows);
    } else
    if (typeof rows === 'function') {
      rows = [...rows()];
    } else
    if (Object.prototype.toString.call(rows) === '[object Generator]') {
      rows = [...rows];
    } else
    if (!Array.isArray(rows)) {
      rows = [rows];
    }

    if (!rows.length) {
      this.append('(SELECT NULL WHERE 1=0)');
      return null;
    }
    if (!fields) {
      fields = Object.keys(rows[0]);
    }

    this.append('(')
      .append(fields, (field) => this.append(this.ident(field)), ',')
      .append(') VALUES ')
      .append(rows, (row, i) =>
        this.append('(')
          .append(fields, (key) => {
            const value = row ? row[key] : null;
            if (typeof transform === 'function') {
              return this.value(transform(value, i, key, row, rows));
            }
            if (transform === false) { // do not wrap any values at all
              return this.expr(value);
            } else
            if (typeof transform === 'object') {
              if (transform[key] === false) { // false = do not wrap (as a parameter)
                return this.expr(value);
              } else
              if (typeof transform[key] === 'string') { // string = wrap with type
                if (value && typeof value === 'object' && '$' in value) { // already wrapped, add type
                  return this.expr(Object.assign({}, value, {type: transform[key]}));
                }
                return this.expr({$: value, type: transform[key]});
              } else
              if (typeof transform[key] === 'function') { // function = wrapper function
                return this.expr(transform[key](value, i, key, row, rows));
              }
            }
            if (value && typeof value === 'object' && '$' in value) { // Already wrapped
              return this.expr(value);
            }
            return this.expr({$: value});
          }, ',')
          .append(')'),
      ',');
    return rows[0];
  }

  conflict(conflict, table) {
    if (!conflict) {
      return this;
    }
    if (typeof conflict === 'string') {
      return this.append(conflict);
    }
    table = this.ident(table);
    return this.append(Object.keys(conflict), (key) => {
      const field = this.ident(key);
      const exclId = isPostgres(this.sql) ? `EXCLUDED.${field}` : `VALUES(${field})`;
      const value = conflict[key];
      if (value instanceof RegExp) {
        switch (value.source.toLowerCase()) {
          case 'update': return this.append(`${field}=${exclId}`);
          case 'fill':   return this.append(`${field}=COALESCE(${table}.${field}, ${exclId})`);
          case 'inc':    return this.append(`${field}=${table}.${field}+1`);
          case 'dec':    return this.append(`${field}=${table}.${field}-1`);
          case 'add':    return this.append(`${field}=${table}.${field}+${exclId}`);
          case 'sub':    return this.append(`${field}=${table}.${field}-${exclId}`);
          case 'max':    return this.append(`${field}=GREATEST(${table}.${field}, ${exclId})`);
          case 'min':    return this.append(`${field}=LEAST(${table}.${field}, ${exclId})`);
          default: throw new Error(`Unknown conflict rule: ${value.source}`);
        }
      } else {
        return this.append(`${field}=`).expr(value);
      }
    }, ',');
  }
}

class Query {
  constructor(parts, options = {}) {
    Object.defineProperties(this, {
      sql:      { value: parts.sql },
      parts:    { value: parts },
      options:  { value: options },
      text: {
        enumerable: true,
        get() {
          return this.parts.toString();
        }
      },
      params: {
        enumerable: true,
        get() {
          return this.parts.params;
        }
      }
    });
  }

  toString() {
    return this.parts.toString();
  }

  mapFn(field, row, index, rows) {
    if (typeof field === 'function') {
      if (/^\s*class\s+/.test(field.toString())) {
        const keys = Object.keys(row);
        return field.fromRow ? field.fromRow(row) : Object.create(
          field.prototype,
          Object.fromEntries(keys.map(k => [k,
            { value: row[k], enumerable: true, writable: true }
          ]))
        );
      }
      return field(row, index, rows);
    }
    if (Array.isArray(field)) {
      return field.map(f => this.mapFn(f, row, index, rows));
    }
    if (typeof field === 'string') {
      return row[field];
    }
    if (typeof field === 'object') {
      const keys = Object.keys(field);
      return Object.fromEntries(
        keys.map(k => [k,
          this.mapFn(field[k], row, index, rows)
        ])
      );
    }
    return row;
  }

  async toObject(key, value) {
    const rows = await this.sql.exec(this);
    if (typeof key === 'function') {
      return Object.fromEntries(rows.map((row, index) => [key(row, index, rows), this.mapFn(value, row, index, rows)]));
    }
    if (Array.isArray(key)) {
      return Object.fromEntries(rows.map((row, index) => [key.map(k => row[k]).join('_'), this.mapFn(value, row, index, rows)]));
    }
    return Object.fromEntries(rows.map((row, index) => [row[key], this.mapFn(value, row, index, rows)]));
  }

  async toObjectArray(key, value) {
    const rows = await this.sql.exec(this);
    const obj = {};
    for (let i = 0; i < rows.length; i++) {
      const k = typeof key === 'function' ? key(rows[i], i, rows) :
        (Array.isArray(key) ? key.map(k => row[k]).join('_') : rows[i][key]);
      (obj[k] ||= []).push(this.mapFn(value, rows[i], i, rows));
    }
    return obj;
  }

  async toMap(key, value) { // Better alternative for object
    const rows = await this.sql.exec(this);
    const map = new Map();
    if (typeof key === 'function') {
      for (let i = 0; i < rows.length; i++) {
        map.set(key(rows[i], i, rows), this.mapFn(value, rows[i], i, rows));
      }
    } else
    if (Array.isArray(key)) {
      for (let i = 0; i < rows.length; i++) {
        map.set(key.map(k => row[k]).join('_'), this.mapFn(value, rows[i], i, rows));
      }
    } else {
      for (let i = 0; i < rows.length; i++) {
        map.set(rows[i][key], this.mapFn(value, rows[i], i, rows));
      }
    }
    return map;
  }

  async toMapArray(key, value) {
    const rows = await this.sql.exec(this);
    const map = new Map();
    for (let i = 0; i < rows.length; i++) {
      const k = typeof key === 'function' ? key(rows[i], i, rows) :
        (Array.isArray(key) ? key.map(k => row[k]).join('_') : rows[i][key]);
      if (!map.has(k)) {
        map.set(k, []);
      }
      map.get(k).push(this.mapFn(value, rows[i], i, rows));
    }
    return map;
  }

  async toSet(value) {
    const rows = await this.sql.exec(this);
    const set = new Set();
    for (let i = 0; i < rows.length; i++) {
      set.add(this.mapFn(value, rows[i], i, rows));
    }
    return set;
  }

  async toArray(value) {
    const rows = await this.sql.exec(this);
    if (!value) {
      return rows;
    }
    return rows.map((row, i) => this.mapFn(value, row, i, rows));
  }

  async forEach(fn) {
    const rows = await this.sql.exec(this);
    rows.forEach(fn);
  }
  
  async one(value) {
    const rows = await this.sql.exec(this);
    if (!rows[0]) {
      return null;
    }
    if (!value) {
      return rows[0];
    }
    return this.mapFn(value, rows[0], 0, rows);
  }

  async withId() { // To be used in conjuction with insert({ ... }, { returnId: true }) - returns original object augmented with inserted id
    if (!this.options.firstRow) {
      throw new Error('withId() can only be called on query created using insert() method');
    }
    const rows = await this.sql.exec(this);
    if (isPostgres(this.sql)) {
      return Object.assign({}, this.options.firstRow, rows[0]);
    }
    return Object.assign({}, this.options.firstRow, { id: rows[0].insertId }); // TODO: Column may not always be called 'id'?
  }

  then(onFullfilled, onRejected) {
    return this.exec().then(onFullfilled, onRejected);
  }
  
  exec() {
    return this.sql.exec(this);
  }

  explain(opts = {}) {
    const prefix = `EXPLAIN${
      isPostgres(this.sql) && Object.keys(opts).length ? ` (${Object.keys(opts).map(opt => `${opt.toUpperCase()} ${opts[opt] + ''}`).join(',')})` : ''
    } `;
    return new Query(new QueryParts(this.sql, [
      prefix + this.parts.chunks[0], ...this.parts.chunks.slice(1)
    ], this.params));
  }
}

class Builder {
  constructor(sql) {
    this.sql = sql;
  }

  ident(ident) {
    if (ident === '*') return '*';
    if (this.sql.$config.convertCase) {
      ident = toSnakeCase(ident);
    }
    return isMySQL(this.sql) ? escapeMysqlIdent(ident) : escapePostgresIdent(ident); 
  }

  select(table, where, { fields = '*', distinct, group, having, order, limit, offset } = {}) {
    const parts = new QueryParts(this.sql, 'SELECT ');
    if (distinct) {
      parts.append('DISTINCT ');
      if (distinct !== true) {
        parts.append('ON (');
        parts.exprs(distinct);
        parts.append(') ');
      }
    }
    parts.fields(fields);
    table.length && parts.append(' FROM ').table(table);
    where && parts.append(' WHERE ').where(where);
    group && parts.append(' GROUP BY ').exprs(group);
    having && parts.append(' HAVING ').where(having);
    order && parts.append(' ORDER BY ').order(order);
    limit && parts.append(' LIMIT ').value(limit);
    offset && parts.append(' OFFSET ').value(offset);
    return new Query(parts);
  }

  update(table, updates, where, { transform } = {}) {
    const parts = new QueryParts(this.sql, 'UPDATE ');
    parts.table(table);
    parts.append(' SET ').updates(updates, transform);
    where && parts.append(' WHERE ').where(where);
    return new Query(parts);
  }

  insert(table, rows, { fields, transform, unique, conflict, returnId } = {}) {
    if (unique && conflict === undefined) {
      throw new Error(`"conflict" should be either false (to ignore conflicts) or an update object when "unique" is set`);
    }
    if (!unique && conflict !== undefined && isPostgres(this.sql)) {
      throw new Error(`Specifying "conflict" on Postgres requires also specifying "unique" fields (constraints)`);
    }
    

    const parts = new QueryParts(this.sql, 'INSERT ');
    if (isMySQL(this.sql) && conflict === false) {
      parts.append('IGNORE ');
    }
    parts.append('INTO ').table(table);
    const firstRow = parts.rows(rows, fields, transform);

    if (unique && Array.isArray(unique)) {
      unique = unique.map(field => parts.ident(field)).join(',');
    }
    if (isMySQL(this.sql)) {
      if (conflict) {
        parts.append(' ON DUPLICATE KEY UPDATE ').conflict(conflict, table);
      }
    } else
    if (isPostgres(this.sql)) {
      if (unique) {
        if (conflict) {
          parts.append(` ON CONFLICT (${unique}) DO UPDATE SET `).conflict(conflict, table);
        } else {
          parts.append(` ON CONFLICT (${unique}) DO NOTHING`);
        }
      }
    }

    returnId && isPostgres(this.sql) && parts.append(` RETURNING ${this.ident(returnId === true ? 'id' : returnId)}`);
    return new Query(parts, { firstRow });
  }

  delete(table, where) {
    const parts = new QueryParts(this.sql, 'DELETE FROM ');
    parts.table(table);
    where && parts.append(' WHERE ').where(where);
    return new Query(parts);
  }
}

class Tables {
  constructor(sql, list) {
    this.sql = sql;
    this.list = Array.isArray(list) ? list : (list ? [list] : []);
  }

  join(other, on) {
    if (on) {
      this.list.push({ table: other, on });
    } else
    if (Array.isArray(other)) {
      this.list.push(...other);
    } else {
      this.list.push(other);
    }
    return this;
  }

  toString() {
    return this.sql.$builder.table(this.list);
  }

  selectAll(options = {}) {
    return this.sql.$builder.select(this.list, null, options);
  }

  selectOne(where, options = {}) {
    return this.sql.$builder.select(this.list, where, options).one();
  }

  select(where, options = {}) {
    return this.sql.$builder.select(this.list, where, options);
  }

  update(update, where, options = {}) {
    return this.sql.$builder.update(this.list, update, where, options);
  }

  insert(rows, options = {}) {
    return this.sql.$builder.insert(this.list, rows, options);
  }

  delete(where) {
    return this.sql.$builder.delete(this.list, where);
  }
}

class Values {
  constructor(rows, fields) {
    this.rows = rows;
    this.fields = fields;
  }
}

class SQL extends Function {
  constructor(db, config = {}) {
    super();
    // Dollar-signs are used instead of "_" to designate private fields
    // This is to minimise risks of collisions with SQL table names (where _ is allowed as first character, but $ is not)
    this.$db = db;
    this.$config = config;
    if (this.$config.convertCase === undefined) {
      this.$config.convertCase = true;
    }
    this.$builder = new Builder(this);
    return new Proxy(this, {
      get(target, prop) {
        if (prop in target) {
          return target[prop];
        }
        return new Tables(target, prop);
      },
      apply(target, thisArg, argumentsList) {
        const params = [];
        return new Query(new QueryParts(target, argumentsList[0].map((chunk, i, chunks) => {
          if (i === chunks.length - 1) {
            return chunk;
          }
          const arg = argumentsList[i + 1];
          if (arg instanceof Values) {
            if (arg.rows.length === 0) {
              return chunk + '(SELECT NULL WHERE 1=0)'; // Workaround to insert 0 rows
            }
            let fields = arg.fields;
            let values = [];
            for (const row of arg.rows) {
              if (!fields) {
                fields = Object.keys(row);
              }
              values.push('(' + fields.map((key, i) => {
                params.push(Array.isArray(row) ? row[i] : row[key]);
                return '$' + params.length;
              }).join(',') + ')');
            };
            return chunk + `(${fields.map(field => target.$builder.ident(field)).join(',')}) VALUES ${values.join(',')}`;
          }
          params.push(arg);
          return chunk + '$' + (i + 1);
        }).join(''), params));
      },
    });
  }

  values(rows, fields) {
    return new Values((!Array.isArray(rows) && typeof rows !== 'function') ? [rows] : rows, fields);
  }

  // Raw query
  exec(query, params) {
    if (query instanceof Query) {
      params = query.params;
      query = query.text;
    }
    return new Promise(async (resolve, reject) => {
      const convertResults = (results) => {
        if (!Array.isArray(results)) { // MySQL behavior is a bit inconsistent with everything else
          return [results];
        }
        if (!this.$config.convertCase) {
          return results;
        }
        return results.map(row => Object.fromEntries(Object.keys(row).map(k => [toCamelCase(k), row[k]])));
      }
      switch (this.$config.flavor) {
        case 'mysql': 
          this.$db.query(query, params, (error, results, fields) => {
            if (error) {
              reject(error);
            } else {
              resolve(convertResults(results));
            }
          });
          break;
        case 'postgres':
          const results = (await this.$db.query(query, params)).rows;
          resolve(convertResults(results));
          break;
      }
    });
  }

  // Alternative to simply accessing db.tableName
  from(table) {
    return new Tables(this, table);
  }

  // Join multiple tables
  join(tables) {
    return new Tables(this, tables);
  }

  // Bun-inspired transactions support (TODO: support savepoints?)
  async begin(callback) {
    const tx = new SQL(await this.$db.connect(), this.$config);
    try {
      await tx.exec('BEGIN');
      await callback(tx);
      await tx.exec('COMMIT');
    } catch (err) {
      await tx.exec('ROLLBACK');
      throw err;
    } finally {
      tx.db.release();
    }
  }
}

SQL.Postgres = class extends SQL {
  constructor(db, params = {}) {
    super(db, { flavor: 'postgres', ...params });
  }
}

SQL.MySQL = class extends SQL {
  constructor(db, params = {}) {
    super(db, { flavor: 'mysql', ...params });
  }
}

module.exports = SQL;