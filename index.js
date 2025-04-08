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

class Var {
  constructor(value, type) {
    this.value = value;
    this.type = type;
  }
}

function isVar(v) {
  return v instanceof Var;
}

class Query {
  constructor(sql, query, params = []) {
    this.sql = sql;
    this.query = query;
    this.params = params;
  }

  toString() {
    return this.query;
  }

  mapFn(field, row, index, rows) {
    if (typeof field === 'function') {
      if (/^\s*class\s+/.test(field.toString())) {
        const keys = Object.keys(row);
        return field.fromRow ? field.fromRow(row) : Object.create(
          field.prototype,
          Object.fromEntries(keys.map(k => [
            this.sql.convertCase ? toCamelCase(k) : k,
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
      return row[this.sql.convertCase ? toSnakeCase(field) : field];
    }
    if (typeof field === 'object') {
      const keys = Object.keys(field);
      return Object.fromEntries(
        keys.map(k => [
          this.sql.convertCase ? toCamelCase(k) : k,
          this.mapFn(field[k], row, index, rows)
        ])
      );
    }
    if (this.sql.convertCase) {
      const keys = Object.keys(row);
      return Object.fromEntries(
        keys.map(k => [toCamelCase(k), row[k]])
      );
    }
    return row;
  }

  async toObject(key, value) {
    const rows = await this.sql.run(this.query, this.params);
    if (typeof key === 'function') {
      return Object.fromEntries(rows.map((row, index) => [key(row, index, rows), this.mapFn(value, row, index, rows)]));
    }
    if (Array.isArray(key)) {
      return Object.fromEntries(rows.map((row, index) => [key.map(k => row[k]).join('_'), this.mapFn(value, row, index, rows)]));
    }
    return Object.fromEntries(rows.map((row, index) => [row[key], this.mapFn(value, row, index, rows)]));
  }

  async toObjectArray(key, value) {
    const rows = await this.sql.run(this.query, this.params);
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
    const rows = await this.sql.run(this.query, this.params);
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
    const rows = await this.sql.run(this.query, this.params);
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
    const rows = await this.sql.run(this.query, this.params);
    const set = new Set();
    for (let i = 0; i < rows.length; i++) {
      map.add(this.mapFn(field, rows[i], i, rows));
    }
    return map;
  }

  async toArray(field) {
    const rows = await this.sql.run(this.query, this.params);
    if (!field && !this.sql.convertCase) {
      return rows;
    }
    return rows.map((row, i) => this.mapFn(field, row, i, rows));
  }

  async forEach(fn) {
    const rows = await this.sql.run(this.query, this.params);
    rows.forEach(fn);
  }
  
  async one(field) {
    const rows = await this.sql.run(this.query, this.params);
    if (!rows[0]) {
      return null;
    }
    if (!field && !this.sql.convertCase) {
      return rows[0];
    }
    return this.mapFn(field, rows[0], 0, rows);
  }
  
  run() {
    return this.sql.run(this.query, this.params);
  }

  async explain(opts = {}) {
    const rows = await this.sql.run(`EXPLAIN${
      this.sql.flavor === 'postgres' && Object.keys(opts).length ? ` (${Object.keys(opts).map(opt => `${opt.toUpperCase()} ${opts[opt] + ''}`).join(',')})` : ''
    } ` + this.query, this.params);
    return this.sql.flavor === 'postgres' ? rows.map(row => row['QUERY PLAN']) : rows[0];
  }
}

class Tables {
  constructor(sql, list) {
    this.sql = sql;
    this.list = Array.isArray(list) ? list : [list];
  }

  value(value, params, inVar = false) {
    if (value === null || value === undefined) {
      return 'NULL';
    }
    if (isVar(value) || inVar) {
      if (!params) {
        throw new Error('Parameters not supported here');
      }
      let v = inVar ? value : value.value;
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
      return this.sql.flavor === 'postgres' ? `$${params.length}${t ? `::${t}` : ''}` : '?';
    }
    switch (typeof value) {
      case 'symbol': return value.description.split('.').map(id => this.sql.id(id)).join('.');
      case 'boolean': return this.sql.flavor === 'postgres' ? (value ? `'t'` : `'f'`) : (value ? 'true' : 'false');
      case 'number': return value + '';
      case 'string': return this.sql.flavor === 'postgres' ? escapePostgresString(value) : escapeMysqlString(value);
      default: throw new Error(`Unsupported type: ${typeof value}, ${JSON.stringify(value)}`);
    }
    /*
    if (type === 'timestamp') {
      return `to_timestamp(${typeof value === 'number' ? value :
        (value instanceof Date ? Math.floor(value.getTime() / 1000) : 0)
      }) AT TIME ZONE 'UTC'`;
    }
    if ((type === 'json') || (typeof value == 'object' && !Array.isArray(value))) {
      return escapeLiteral(JSON.stringify(value));
    }
    return (this.sql.flavor === 'postgres') ? literal(value) : mysql.escape(value);
    */
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
        const field = this.sql.convertCase ? toSnakeCase(k) : k;
        if (Array.isArray(v)) {
          return this.expr([v[0], Symbol(field), ...v.slice(1)], ps);
        } else
        if (v === null) {
          return `${this.sql.id(field)} IS NULL`;
        } else {
          return `${this.sql.id(field)} = ${this.value(v, ps)}`;
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

  join(other) {
    this.list.push(...(Array.isArray(other) ? other : [other]));
    return this;
  }

  toString() {
    let tables = '';
    for (const t of this.list) {
      if (typeof t === 'string') {
        tables += `${tables !== '' ? ' LEFT JOIN ' : ''}${this.sql.convertCase ? toSnakeCase(t) : t}`;
      } else {
        tables += `${tables !== '' ? ' ' + (t.join || 'LEFT') + ' JOIN ' : ''}${
          typeof t.table === 'string' ? (this.sql.convertCase ? toSnakeCase(t.table) : t.table) : `(${t.table})`
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
        fields = this.sql.convertCase ? fields.map(toSnakeCase).join(',') : fields.join(',');
      } else {
        fields = Object.keys(fields).map(field => {
          const id = this.sql.convertCase ? toSnakeCase(field) : field;
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

  update(update, where, { run = true } = {}) {
    if (typeof update !== 'string') {
      update = Object.keys(update).map(key => `${
        this.sql.convertCase ? toSnakeCase(key) : key
      }=${
        this.expr(update[key])
      }`).join(',');
    }
    const params = [];
    where = where && this.where(where, params);
    const query = new Query(this.sql, `UPDATE ${this.toString()} SET ${update}${where ? ' WHERE ' + where : ''}`, params);
    return run ? query.run() : query;
  }

  insert(values, { fields, unique, conflict, returnId, map, run = true } = {}) {
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
        const field = this.sql.id(key);
        const value = conflict[key];
        const exclId = this.sql.flavor === 'postgres' ?
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
    for (let i = 0; i < values.length; i++) {
      const v = map ? map(values[i], i, values) : values[i];
      const row = [];
      for (const key of fields) {
        row.push(this.value(v[key], params));
      }
      rows.push('(' + row.join(',') + ')');
    }

    if (unique && conflict === undefined) {
      throw new Error(`"conflict" should be either false (to ignore conflicts) or an update object when "unique" is set`);
    }
    if (!unique && conflict !== undefined && this.sql.flavor === 'postgres') {
      throw new Error(`Specifying "conflict" on Postgres requires also specifying "unique" fields (constraints)`);
    }

    if (unique && Array.isArray(unique)) {
      unique = unique.map(field => this.sql.id(field)).join(',');
    }

    if (this.sql.flavor === 'mysql') {
      if (conflict) {
        conflict = ` ON DUPLICATE KEY UPDATE ${updates.join(',')}`;
      }
    } else
    if (this.sql.flavor === 'postgres') {
      if (unique) {
        if (conflict) {
          conflict = ` ON CONFLICT (${unique}) DO UPDATE SET ${updates.join(',')}`;
        } else {
          conflict = ` ON CONFLICT (${unique}) DO NOTHING`;
        }
      }
    }
  
    const query = new Query(this.sql,
      `INSERT${
        conflict === false && this.sql.flavor === 'mysql' ? ' IGNORE' : ''
      } INTO ${
        this.toString()
      } (${
        fields.map(field => this.sql.id(field)).join(',')
      }) VALUES ${
        rows.join(',')
      }${
        conflict || ''
      }${
        returnId && this.sql.flavor === 'postgres' ? ' RETURNING ' + (returnId === true ? 'id' : returnId) : ''
      }`, params);
    return run ? query.run() : query;
  }

  delete(where, { run = true } = {}) {
    const params = [];
    where = where && this.where(where, params);
    const query = new Query(this.sql, `DELETE FROM ${this.toString()}${where ? ' WHERE ' + where : ''}`, params);
    return run ? query.run() : query;
  }
}

class SQL {
  constructor(db, { flavor, convertCase = true } = {}) {
    this.db = db;
    this.flavor = flavor;
    this.convertCase = convertCase;

    return new Proxy(this, {
      get(target, prop) {
        if (prop in target) {
          return target[prop];
        }
        return new Tables(target, this.convertCase ? toSnakeCase(prop) : prop);
      }
    });
  }

  id(name) {
    if (name === '*') return '*';
    this.convertCase && (name = toSnakeCase(name));
    return this.flavor === 'mysql' ? `\`${name}\`` : `"${name}"`; 
  }

  // Raw query
  run(query, params) {
    return new Promise(async (resolve, reject) => {
      switch (this.flavor) {
        case 'mysql': 
          this.db.query(query, params, (error, results, fields) => {
            if (error) {
              reject(error);
            } else {
              resolve(results);
            }
          });
          break;
        case 'postgres':
          const results = (await this.db.query(query, params)).rows;
          resolve(results);
          break;
      }
    });
  }

  join(tables) {
    return new Tables(this, tables);
  }

  // Bun-inspired transactions support (TODO: support savepoints?)
  async begin(callback) {
    const tx = new SQL(await this.db.connect(), { flavor: this.flavor, convertCase: this.convertCase });
    try {
      await tx.run('BEGIN');
      await callback(tx);
      await tx.run('COMMIT');
    } catch (err) {
      await tx.run('ROLLBACK');
      throw err;
    } finally {
      tx.db.release();
    }
  }
}

SQL.prototype.Var = Var;

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

SQL.Var = (value, type) => new Var(value, type);

module.exports = SQL;