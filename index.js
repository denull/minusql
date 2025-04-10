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

class Query {
  constructor(sql, text, params = []) {
    Object.defineProperty(this, 'sql', {
      enumerable: false,
      value: sql,
    });
    this.text = text;
    this.params = params;
  }

  toString() {
    return this.text;
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

  then(onFullfilled, onRejected) {
    return this.exec().then(onFullfilled, onRejected);
  }
  
  exec() {
    return this.sql.exec(this);
  }

  explain(opts = {}) {
    return new Query(this.sql, `EXPLAIN${
      isPostgres(this.sql) && Object.keys(opts).length ? ` (${Object.keys(opts).map(opt => `${opt.toUpperCase()} ${opts[opt] + ''}`).join(',')})` : ''
    } ` + this.text, this.params);
  }
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

class Builder {
  constructor(sql) {
    this.sql = sql;
  }

  tableCase(name) {
    return this.sql.$config.convertCase ? toSnakeCase(name) : name;
  }

  id(name) {
    if (name === '*') return '*';
    return this.tableCase(name).split('.').map(id => isMySQL(this.sql) ? `\`${id}\`` : `"${id}"`).join('.'); 
  }

  keyword(name) {
    if (/^[A-Za-z ]+/.match(name)) {
      throw new Error(`Keyword expected here, got "${name}" instead`);
    }
    return name;
  }

  value(value, params, inVar = false) {
    if (value === null || value === undefined) {
      return 'NULL';
    }
    if (isVar(value) || inVar) {
      if (!params) {
        throw new Error('Parameters not supported here');
      }
      let v = inVar ? value : value.$;
      let t = inVar ? false : value.type;
      if (Array.isArray(v)) {
        return v.map(el => this.value(el, params, true)).join(',');
      }
      if (value.type === 'timestamp') {
        // TODO: support MySQL?
        //v = typeof v === 'number' ? v : (v instanceof Date ? Math.floor(v.getTime() / 1000) : v);
        params.push(v);
        return `TO_TIMESTAMP($${params.length}) AT TIME ZONE 'UTC'`;
      }
      params.push(v);
      return isPostgres(this.sql) ? `$${params.length}${t ? `::${t}` : ''}` : '?';
    }
    switch (typeof value) {
      case 'symbol': return this.id(value.description);
      case 'boolean': return isPostgres(this.sql) ? (value ? `'t'` : `'f'`) : (value ? 'true' : 'false');
      case 'number': return value + '';
      case 'string': return isPostgres(this.sql) ? escapePostgresString(value) : escapeMysqlString(value);
      default: throw new Error(`Unsupported type: ${typeof value}, ${JSON.stringify(value)}`);
    }
  }
  
  expr(e, ps) {
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
        return `${fn} ${this.expr(e[0], ps)}`;
      }
      if (Operators.includes(fn)) {
        if (BinaryOperators.includes(fn)) {
          checkArity(2);
        }
        return `(${e.map(e => this.expr(e, ps)).join(` ${fn} `)})`;
      }
  
      switch (fn) {
        case 'IN':
        case 'NOTIN':
        case 'NOT IN':
          checkArity(2);
          const list = isVar(e[1]) ? this.value(e[1], ps) : e[1].map(e => this.expr(e, ps)).join(',');
          return `${this.expr(e[0], ps)}${fn === 'IN' ? '' : ' NOT'} IN (${list})`;
        case 'IS NULL':
        case 'IS NOT NULL':
          checkArity(1);
          return `${this.expr(e[0], ps)} ${fn}`;
        case 'NOT':
          checkArity(1);
          return `${fn} ${this.expr(e[0], ps)}`;
        case 'BETWEEN':
        case 'NOT BETWEEN':
          checkArity(3);
          return `${this.expr(e[0], ps)} ${fn} ${this.expr(e[1], ps)} AND ${this.expr(e[2], ps)}`;
        case 'TYPE':
          checkArity(2);
          return `${this.keyword(e[1])} ${this.expr(e[0], ps)}`;
        case 'CAST':
          checkArity(2);
          return isPostgres(this.sql) ? `${this.expr(e[0], ps)}::${e[1]}` : `CAST(${this.expr(e[0], ps)} AS ${this.keyword(e[1])})`;
        case 'EXTRACT':
          checkArity(2);
          return `EXTRACT(${e[1]} FROM ${this.expr(e[0], ps)})`;
        case 'CASE':
          return `CASE ${
          e.map((cond, i, e) =>
            cond.length > 1 ?
              `WHEN ${this.expr(cond[0], ps)} THEN ${this.expr(cond[1], ps)}` :
              (i === 0 ?
                this.expr(cond[0], ps) :
                (i === e.length - 1 ?
                  `ELSE ${this.expr(cond[0], ps)}` :
                  (() => { throw new Error('Invalid case format') })
                )
              )
          ).join(' ')
        } END`;
        default: return `${fn}(${e.map(e => this.expr(e, ps)).join(',')})`;
      }
    }

    if (e && typeof e === 'object' && !isVar(e)) {
      return Object.keys(e).map(k => {
        const v = e[k];
        const field = this.tableCase(k);
        if (Array.isArray(v)) {
          return this.expr([v[0], Symbol(field), ...v.slice(1)], ps);
        } else
        if (v === null) {
          return `${this.id(field)} IS NULL`;
        } else {
          return `${this.id(field)} = ${this.value(v, ps)}`;
        }
      }).join(' AND ');
    }

    return this.value(e, ps);
  }

  table(tables, params) {
    return tables.map((t, i) => {
      if (typeof t === 'string') {
        return `${i > 0 ? 'LEFT JOIN ' : ''}${this.tableCase(t)}`;
      }
      return `${i > 0 ? (t.join || 'LEFT') + ' JOIN ' : ''}${
        typeof t.table === 'string' ? this.tableCase(t.table) : `(${t.table})`
      }${
        t.as ? ' AS ' + t.as : ''
      }${
        t.on ? ' ON ' + this.where(t.on) : ''
      }`;
    }).join(' ');
  }

  fields(fields, params) {
    if (typeof fields === 'string') {
      return fields;
    }
    if (Array.isArray(fields)) {
      return this.sql.$config.convertCase ? fields.map(toSnakeCase).join(',') : fields.join(',');
    }
    return Object.keys(fields).map(field => {
      const id = this.tableCase(field);
      return (fields[field] === true) ? id : `${this.expr(fields[field], params)} AS ${id}`;
    }).join(',');
  }

  where(where, params) {
    if (!where) {
      return '';
    }
    return this.expr(where, params);
  }

  exprs(exprs, params) {
    if (typeof exprs === 'string') {
      return exprs;
    }
    if (Array.isArray(exprs)) {
      return exprs.map(e => typeof e === 'string' ? e : this.expr(e, params)).join(',');
    }
    return this.expr(e, params);
  }

  order(exprs, params) {
    if (typeof exprs === 'string') {
      return exprs;
    }
    if (Array.isArray(exprs)) {
      return exprs.map(e => typeof e === 'string' ? e : `${this.expr(e[0], params)}${e[1] ? ' ' + e[1] : ''}`).join(',');
    }
    return this.expr(e, params);
  }

  updates(updates, params) {
    if (typeof updates === 'string') {
      return updates;
    }
    return Object.keys(updates).map(key => `${
      this.tableCase(key)
    }=${
      this.expr(updates[key], params)
    }`).join(',');
  }

  rows(rows, fields, transform, params) {
    if (typeof rows === 'number') {
      rows = Array(rows);
    } else
    if (!Array.isArray(rows) && typeof rows !== 'function') {
      rows = [rows];
    }
    const result = [];
    for (const v of rows) {
      if (!fields) {
        fields = Object.keys(v);
      }
      result.push('(' + fields.map(key => {
        if (typeof transform === 'function') {
          return this.value(transform(key, v, result.length, rows), params);
        }
        
        const value = v[key];
        if (transform === false) { // do not wrap any values at all
          return this.expr(value, params);
        } else
        if (typeof transform === 'object') {
          if (transform[key] === false) { // false = do not wrap (as a parameter)
            return this.expr(value, params);
          } else
          if (typeof transform[key] === 'string') { // string = wrap with type
            if (value && typeof value === 'object' && '$' in value) { // already wrapped, add type
              return this.expr(Object.assign({}, value, {type: transform[key]}), params);
            }
            return this.expr({$: value, type: transform[key]}, params);
          } else
          if (typeof transform[key] === 'function') { // function = wrapper function
            return this.expr(transform[key](value, v, result.length, rows), params);
          }
        }

        if (value && typeof value === 'object' && '$' in value) { // Already wrapped
          return this.expr(value, params);
        }
        return this.expr({$: value}, params);
      }).join(',') + ')');
    }
    return [
      result.join(','),
      fields ? `(${fields.map(field => this.id(field)).join(',')})` : '',
    ];
  }

  conflict(conflict, table, params) {
    if (typeof conflict === 'string' || !conflict) {
      return conflict;
    }
    return Object.keys(conflict).map((key) => {
      const field = this.id(key);
      const value = conflict[key];
      const exclId = isPostgres(this.sql) ?
        `EXCLUDED.${field}` : 
        `VALUES(${field})`;
      if (value instanceof RegExp) {
        switch (value.source.toLowerCase()) {
          case 'update': return `${field} = ${exclId}`;
          case 'fill':   return `${field} = COALESCE(${table}.${field}, ${exclId})`;
          case 'inc':    return `${field} = ${table}.${field} + 1`;
          case 'dec':    return `${field} = ${table}.${field} - 1`;
          case 'add':    return `${field} = ${table}.${field} + ${exclId}`;
          case 'sub':    return `${field} = ${table}.${field} - ${exclId}`;
          case 'max':    return `${field} = GREATEST(${table}.${field}, ${exclId})`;
          case 'min':    return `${field} = LEAST(${table}.${field}, ${exclId})`;
          default: throw new Error(`Unknown conflict rule: ${value.source}`);
        }
      } else {
        return `${field} = ${this.expr(value, params)}`;
      }
    }).join(',');
  }

  select(table, where, { fields = '*', distinct, group, having, order = '', limit, offset } = {}) {
    const params = [];
    return new Query(this.sql, `SELECT ${
      distinct ? 'DISTINCT' + (distinct === true ? '' : ' ON (' + this.exprs(distinct, params) + ')') + ' ' : ''
    }${
      this.fields(fields, params)
    }${
      table.length ? ' FROM ' + this.table(table, params) : ''
    }${
      where ? ' WHERE ' + this.where(where, params) : ''
    }${
      group ? ' GROUP BY ' + this.exprs(group, params) : ''
    }${
      having ? ' HAVING ' + this.where(having, params) : ''
    }${
      order ? ' ORDER BY ' + this.order(order, params) : ''
    }${
      limit ? ' LIMIT ' + this.value(limit, params) : ''
    }${
      offset ? ' OFFSET ' + this.value(offset, params) : ''
    }`, params);
  }

  update(table, updates, where) {
    const params = [];
    return new Query(this.sql, `UPDATE ${
      this.table(table, params)
    } SET ${
      this.updates(updates, params)
    }${
      where ? ' WHERE ' + this.where(where, params) : ''
    }`, params);
  }

  insert(table, rows, { fields, transform, unique, conflict, returnId } = {}) {
    if (unique && conflict === undefined) {
      throw new Error(`"conflict" should be either false (to ignore conflicts) or an update object when "unique" is set`);
    }
    if (!unique && conflict !== undefined && isPostgres(this.sql)) {
      throw new Error(`Specifying "conflict" on Postgres requires also specifying "unique" fields (constraints)`);
    }

    const params = [];
    table = this.table(table, params);

    [rows, fields] = this.rows(rows, fields, transform, params);

    if (unique && Array.isArray(unique)) {
      unique = unique.map(field => this.id(field)).join(',');
    }

    if (isMySQL(this.sql)) {
      if (conflict) {
        conflict = ` ON DUPLICATE KEY UPDATE ${this.conflict(conflict, table, params)}`;
      }
    } else
    if (isPostgres(this.sql)) {
      if (unique) {
        if (conflict) {
          conflict = ` ON CONFLICT (${unique}) DO UPDATE SET ${this.conflict(conflict, table, params)}`;
        } else {
          conflict = ` ON CONFLICT (${unique}) DO NOTHING`;
        }
      }
    }
  
    return new Query(this.sql,
      `INSERT${
        conflict === false ? ' IGNORE' : ''
      } INTO ${
        table
      }${
        rows.length ? fields + ' VALUES ' + rows : '(SELECT NULL WHERE 1=0)'
      }${
        conflict || ''
      }${
        returnId && isPostgres(this.sql) ? ' RETURNING ' + (returnId === true ? 'id' : returnId) : ''
      }`, params);
  }

  delete(table, where) {
    const params = [];
    return new Query(this.sql, `DELETE FROM ${
      this.table(table, params)
    }${
      where ? ' WHERE ' + this.where(where, params) : ''
    }`, params);
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

  update(update, where) {
    return this.sql.$builder.update(this.list, update, where);
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
        return new Tables(target, target.$builder.tableCase(prop));
      },
      apply(target, thisArg, argumentsList) {
        const params = [];
        return new Query(target, argumentsList[0].map((chunk, i, chunks) => {
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
            return chunk + `(${fields.map(field => target.$builder.id(field)).join(',')}) VALUES ${values.join(',')}`;
          }
          params.push(arg);
          return chunk + '$' + (i + 1);
        }).join(''), params);
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
    return new Tables(this, this.$builder.tableCase(table));
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