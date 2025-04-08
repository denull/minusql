function toCamelCase(k) {
  return k.split('_').map((word, i) => i > 0 ? word[0].toUpperCase() + word.substring(1) : word).join('');
}

function toSnakeCase(k) {
  return k.replace(/(([a-z])(?=[A-Z]([a-zA-Z]|$))|([A-Z])(?=[A-Z][a-z]))/g,'$1_').toLowerCase();
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
function maybeSnakeCase(sql, k) {
  return sql.$config.convertCase ? toSnakeCase(k) : k;
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
      const k = typeof key === 'function' ?
        key(rows[i], i, rows) :
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
      const k = typeof key === 'function' ?
        key(rows[i], i, rows) :
        (Array.isArray(key) ? key.map(k => row[k]).join('_') : rows[i][key]);
      if (!map.has(k)) {
        map.set(k, []);
      }
      map.get(k).push(this.mapFn(value, rows[i], i, rows));
    }
    return map;
  }

  async toSet(field) {
    const rows = await this.sql.exec(this);
    const set = new Set();
    for (let i = 0; i < rows.length; i++) {
      map.add(this.mapFn(field, rows[i], i, rows));
    }
    return map;
  }

  async toArray(field) {
    const rows = await this.sql.exec(this);
    if (!field) {
      return rows;
    }
    return rows.map((row, i) => this.mapFn(field, row, i, rows));
  }

  async forEach(fn) {
    const rows = await this.sql.exec(this);
    rows.forEach(fn);
  }
  
  async one(field) {
    const rows = await this.sql.exec(this);
    if (!rows[0]) {
      return null;
    }
    if (!field) {
      return rows[0];
    }
    return this.mapFn(field, rows[0], 0, rows);
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

class Tables {
  constructor(sql, list) {
    this.sql = sql;
    this.list = Array.isArray(list) ? list : [list];
  }

  id(name) {
    if (name === '*') return '*';
    name = maybeSnakeCase(this.sql, name);
    return name.split('.').map(id => isMySQL(this.sql) ? `\`${id}\`` : `"${id}"`).join('.'); 
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
      const fn = e.shift();
  
      // Operators
      if (['+', '-', '*', '/', '&', '|', 'and', 'or', 'xor', '=', '!=', '<>', '>', '>=', '<', '<='].includes(fn)) {
        return `(${e.map(e => this.expr(e, ps)).join(` ${fn} `)})`;
      }
  
      switch (fn) {
        case 'in':
        case 'notIn':
          const list = isVar(e[1]) ? this.value(e[1], ps) : e[1].map(e => this.expr(e, ps)).join(',');
          return `${this.expr(e[0], ps)}${fn === 'notIn' ? ' NOT' : ''} IN (${list})`;
        case 'not':  return `NOT ${this.expr(e[0], ps)}`;
        case 'cast': return `${this.expr(e[0], ps)}::${e[1]}`;
        case 'case': return `CASE ${
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
        const field = maybeSnakeCase(this.sql, k);
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

  where(where, params) {
    if (!where) {
      return '';
    }
    return this.expr(where, params);
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
    let tables = '';
    for (const t of this.list) {
      if (typeof t === 'string') {
        tables += `${tables !== '' ? ' LEFT JOIN ' : ''}${maybeSnakeCase(this.sql, t)}`;
      } else {
        tables += `${tables !== '' ? ' ' + (t.join || 'LEFT') + ' JOIN ' : ''}${
          typeof t.table === 'string' ? maybeSnakeCase(this.sql, t.table) : `(${t.table})`
        }${
          t.as ? ' AS ' + t.as : ''
        }${
          t.on ? ' ON ' + this.where(t.on) : ''
        }`;
      }
    }
    return tables;
  }

  select(where, { fields = '*', group, having, order = '', limit, offset } = {}) {
    if (typeof fields !== 'string') {
      if (Array.isArray(fields)) {
        fields = this.sql.$config.convertCase ? fields.map(toSnakeCase).join(',') : fields.join(',');
      } else {
        fields = Object.keys(fields).map(field => {
          const id = maybeSnakeCase(this.sql, field);
          return (fields[field] === true) ? id : `${this.expr(fields[field])} AS ${id}`;
        }).join(',');
      }
    }
    const params = [];
    order = order && Array.isArray(order) ? order.join(',') : order;
    where = where && this.where(where, params);
    having = having && this.where(having, params);
    return new Query(this.sql, `SELECT ${fields} FROM ${
      this.toString()
    }${
      where ? ' WHERE ' + where : ''
    }${
      group ? ' GROUP BY ' + group : ''
    }${
      having ? ' HAVING ' + having : ''
    }${
      order ? ' ORDER BY ' + order : ''
    }${
      limit ? ' LIMIT ' + limit : ''
    }${
      offset ? ' OFFSET ' + offset : ''
    }`, params);
  }

  selectAll(opts = {}) {
    return this.select(null, opts);
  }

  selectOne(where, opts = {}) {
    return this.select(where, opts).one();
  }

  selectMany(where, opts = {}) {
    return this.select(where, opts).toArray();
  }

  update(update, where) {
    if (typeof update !== 'string') {
      update = Object.keys(update).map(key => `${
        maybeSnakeCase(this.sql, key)
      }=${
        this.expr(update[key])
      }`).join(',');
    }
    const params = [];
    where = where && this.where(where, params);
    return new Query(this.sql, `UPDATE ${this.toString()} SET ${update}${where ? ' WHERE ' + where : ''}`, params);
  }

  insert(values, { fields, unique, conflict, returnId } = {}) {
    if (!Array.isArray(values)) {
      values = [values];
    }

    if (values.length == 0) {
      return;
    }

    if (!fields) {
      fields = Object.keys(values[0]);
    }

    const updates = [];
    if (conflict) {
      for (const key in conflict) {
        const field = this.id(key);
        const value = conflict[key];
        const exclId = isPostgres(this.sql) ?
          `EXCLUDED.${field}` : 
          `VALUES(${field})`;
        if (value instanceof RegExp) {
          switch (value.source.toLowerCase()) {
            case 'update': updates.push(`${field} = ${exclId}`); break;
            case 'fill':   updates.push(`${field} = COALESCE(${this}.${field}, ${exclId})`); break;
            case 'inc':    updates.push(`${field} = ${this}.${field} + 1`); break;
            case 'dec':    updates.push(`${field} = ${this}.${field} - 1`); break;
            case 'add':    updates.push(`${field} = ${this}.${field} + ${exclId}`); break;
            case 'sub':    updates.push(`${field} = ${this}.${field} - ${exclId}`); break;
            case 'max':    updates.push(`${field} = GREATEST(${this}.${field}, ${exclId})`); break;
            case 'min':    updates.push(`${field} = LEAST(${this}.${field}, ${exclId})`); break;
            default: throw new Error(`Unknown conflict rule: ${value.source}`);
          }
        } else {
          updates.push(`${field} = ${this.expr(value)}`);
        }
      }
    }
  
    const params = [];
    const rows = [];
    for (const v of values) {
      const row = [];
      for (const key of fields) {
        row.push(this.value(v[key], params));
      }
      rows.push('(' + row.join(',') + ')');
    }

    if (unique && conflict === undefined) {
      throw new Error(`"conflict" should be either false (to ignore conflicts) or an update object when "unique" is set`);
    }
    if (!unique && conflict !== undefined && isPostgres(this.sql)) {
      throw new Error(`Specifying "conflict" on Postgres requires also specifying "unique" fields (constraints)`);
    }

    if (unique && Array.isArray(unique)) {
      unique = unique.map(field => this.id(field)).join(',');
    }

    if (isMySQL(this.sql)) {
      if (conflict) {
        conflict = ` ON DUPLICATE KEY UPDATE ${updates.join(',')}`;
      }
    } else
    if (isPostgres(this.sql)) {
      if (unique) {
        if (conflict) {
          conflict = ` ON CONFLICT (${unique}) DO UPDATE SET ${updates.join(',')}`;
        } else {
          conflict = ` ON CONFLICT (${unique}) DO NOTHING`;
        }
      }
    }
  
    return new Query(this.sql,
      `INSERT${
        conflict === false && isMySQL(this.sql) ? ' IGNORE' : ''
      } INTO ${
        this.toString()
      } (${
        fields.map(field => this.id(field)).join(',')
      }) VALUES ${
        rows.join(',')
      }${
        conflict || ''
      }${
        returnId && isPostgres(this.sql) ? ' RETURNING ' + (returnId === true ? 'id' : returnId) : ''
      }`, params);
  }

  delete(where) {
    const params = [];
    where = where && this.where(where, params);
    return new Query(this.sql, `DELETE FROM ${this.toString()}${where ? ' WHERE ' + where : ''}`, params);
  }
}

class SQL {
  constructor(db, config = {}) {
    // Dollar-signs are used instead of "_" to designate private fields
    // This is to minimise risks of collisions with SQL table names (where _ is allowed as first character, but $ is not)
    this.$db = db;
    this.$config = config;
    if (this.$config.convertCase === undefined) {
      this.$config.convertCase = true;
    }
    return new Proxy(this, {
      get(target, prop) {
        if (prop in target) {
          return target[prop];
        }
        return new Tables(target, maybeSnakeCase(target, prop));
      }
    });
  }

  // Raw query
  exec(query, params) {
    if (query instanceof Query) {
      params = query.params;
      query = query.text;
    }
    return new Promise(async (resolve, reject) => {
      const convertResults = (results) => {
        if (!Array.isArray(results)) { // MySQL behavior is a bit inconsistent with everythin else
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
    return new Tables(this, maybeSnakeCase(this, table));
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