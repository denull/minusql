const assert = require('node:assert');
const { it } = require('node:test');
const Minusql = require('./index.js');

class MockedDb {
  constructor(results) {
    this.results = results;
  }
  reset() {
    this.lastQuery = null;
    this.lastParams = null;
  }
}
class MockedMysql extends MockedDb {
  query(query, params, callback) {
    this.lastQuery = query;
    this.lastParams = params;
    if (this.results) {
      callback(this.results, null);
    } else {
      callback(null, { code: 'ER_UNKNOWN', fatal: true });
    }
  }
}
class MockedPostgres extends MockedDb {
  query(query, params) {
    this.lastQuery = query;
    this.lastParams = params;
    if (this.results) {
      return { rows: this.results };
    }
    throw new Error('Unknown error');
  }
}

it('should create MySQL wrapper', () => {
  const mysql = new MockedMysql();
  const db = new Minusql.MySQL(mysql);
  assert(db instanceof Minusql);
  assert.strictEqual(db.$config.flavor, 'mysql');
  assert.strictEqual(db.$db, mysql);
});

it('should create Postgres wrapper', () => {
  const pg = new MockedPostgres();
  const db = new Minusql.Postgres(pg);
  assert(db instanceof Minusql);
  assert.strictEqual(db.$config.flavor, 'postgres');
  assert.strictEqual(db.$db, pg);
});

it('should execute raw queries', async () => {
  const pg = new MockedPostgres([]);
  const db = new Minusql.Postgres(pg);
  const query = 'SELECT * FROM "users" WHERE id = $1';
  const params = [123];

  await db.exec(query, params);
  assert.strictEqual(pg.lastQuery, query);
  assert.strictEqual(pg.lastParams, params);

  pg.reset();
  await db`SELECT * FROM "users" WHERE id = ${123}`;
  assert.deepEqual(pg.lastQuery, query);
  assert.deepEqual(pg.lastParams, params);
});

it('should escape Postgres strings', () => {
  const pg = new MockedPostgres();
  const db = new Minusql.Postgres(pg);
  assert.strictEqual(
    db.users.select({ name: `admin'--` }).text,
    `SELECT * FROM "users" WHERE "name"='admin''--'`,
  );
  assert.strictEqual(
    db.users.select({ [`name with "`]: `test` }).text,
    `SELECT * FROM "users" WHERE "name with """='test'`,
  );
});

it('should escape MySQL strings', () => {
  const mysql = new MockedMysql();
  const db = new Minusql.MySQL(mysql);
  assert.strictEqual(
    db.users.select({ name: `admin'--` }).text,
    `SELECT * FROM \`users\` WHERE \`name\`='admin\\'--'`,
  );
});

it('should construct SELECT queries', () => {
  const pg = new MockedPostgres();
  const db = new Minusql.Postgres(pg);
  
  assert.strictEqual(
    db.users.select().text,
    `SELECT * FROM "users"`,
  );
  assert.strictEqual(
    db.users.select({ id: 100, name: 'John' }).text,
    `SELECT * FROM "users" WHERE "id"=100 AND "name"='John'`,
  );
  assert.deepEqual(
    db.users.select({ id: {$: 100}, name: {$: 'John'} }),
    {
      text: `SELECT * FROM "users" WHERE "id"=$1 AND "name"=$2`,
      params: [100, 'John'],
    }
  );
  assert.strictEqual(
    db.users.selectAll({ fields: ['id', 'name'] }).text,
    `SELECT "id","name" FROM "users"`,
  );
  assert.strictEqual(
    db.users.selectAll({ distinct: true }).text,
    `SELECT DISTINCT * FROM "users"`,
  );
  assert.strictEqual(
    db.users.selectAll({ distinct: ['id', 'name']}).text,
    `SELECT DISTINCT ON ("id","name") * FROM "users"`,
  );
  assert.strictEqual(
    db.users.selectAll({ group: ['id', 'name'] }).text,
    `SELECT * FROM "users" GROUP BY "id","name"`,
  );
  assert.strictEqual(
    db.users.selectAll({ having: { id: 100 }}).text,
    `SELECT * FROM "users" HAVING "id"=100`,
  );
  assert.strictEqual(
    db.users.selectAll({ order: 'id DESC' }).text,
    `SELECT * FROM "users" ORDER BY id DESC`,
  );
  assert.strictEqual(
    db.users.selectAll({ order: [
      [Symbol('id'), 'DESC'],
      [Symbol('name'), 'ASC'],
    ] }).text,
    `SELECT * FROM "users" ORDER BY "id" DESC,"name" ASC`,
  );
  assert.strictEqual(
    db.users.selectAll({ limit: 100 }).text,
    `SELECT * FROM "users" LIMIT 100`,
  );
  assert.strictEqual(
    db.users.selectAll({ offset: 100 }).text,
    `SELECT * FROM "users" OFFSET 100`,
  );
  assert.deepEqual( // All options together
    db.users.select({
      id: {$: 100},
      name: {$: 'John'},
    }, {
      fields: ['id', 'name'],
      distinct: ['id', 'name'],
      group: ['id', 'name'],
      having: { id: {$: 200} },
      order: [
        [Symbol('id'), 'DESC'],
        [Symbol('name'), 'ASC'],
      ],
      limit: {$: 10},
      offset: {$: 20},
    }), {
      text: `SELECT DISTINCT ON ("id","name") "id","name" FROM "users" WHERE "id"=$1 AND "name"=$2 GROUP BY "id","name" HAVING "id"=$3 ORDER BY "id" DESC,"name" ASC LIMIT $4 OFFSET $5`,
      params: [100, 'John', 200, 10, 20],
    }
  );
});

it('should construct UPDATE queries', () => {
  const pg = new MockedPostgres();
  const db = new Minusql.Postgres(pg);

  assert.deepEqual(
    db.users.update({ name: 'John', age: 20 }, { id: 100 }),
    {
      text: `UPDATE "users" SET "name"=$1,"age"=$2 WHERE "id"=100`,
      params: ['John', 20],
    }
  );
  assert.deepEqual(
    db.users.update({ name: 'John', age: {$: 20} }, { id: 100 }, { transform: false }),
    {
      text: `UPDATE "users" SET "name"='John',"age"=$1 WHERE "id"=100`,
      params: [20],
    }
  );
  assert.deepEqual(
    db.users.update({
      name: 'John',
      age: 20,
      city: 'New York',
      alive: true,
      meta: { foo: 'bar' },
    }, { id: 100 }, {
      transform: {
        name: (name) => name.toUpperCase(),
        age: (age) => age + 10,
        city: false,
        meta: 'json',
      }
    }),
    {
      text: `UPDATE "users" SET "name"='JOHN',"age"=30,"city"='New York',"alive"=$1,"meta"=$2::json WHERE "id"=100`,
      params: [true, { foo: 'bar' }],
    }
  );
  assert.deepEqual(
    db.users.update({ name: 'John', age: 20 }, { id: 100 }, {
      transform: (value) => ({$: value + 10}),
    }),
    {
      text: `UPDATE "users" SET "name"=$1,"age"=$2 WHERE "id"=100`,
      params: ['John10', 30],
    }
  );
});

it('should construct INSERT queries', () => {
  const pg = new MockedPostgres();
  const db = new Minusql.Postgres(pg);

  assert.deepEqual(
    db.users.insert([{ id: 100, name: 'John' }, { id: 101, name: 'Mary' }]),
    {
      text: `INSERT INTO "users"("id","name") VALUES ($1,$2),($3,$4)`,
      params: [100, 'John', 101, 'Mary'],
    }
  );
  assert.deepEqual(
    db.users.insert({ id: 100, name: 'John' }, { fields: ['id', 'surname'] }),
    {
      text: `INSERT INTO "users"("id","surname") VALUES ($1,$2)`,
      params: [100, null],
    }
  );
  assert.deepEqual(
    db.users.insert({ id: 100, name: 'John' }, {
      unique: ['id'],
      conflict: false,
    }),
    {
      text: `INSERT INTO "users"("id","name") VALUES ($1,$2) ON CONFLICT ("id") DO NOTHING`,
      params: [100, 'John'],
    }
  );
  assert.deepEqual(
    db.users.insert({ id: 100, name: 'John' }, {
      unique: ['id'],
      conflict: {
        id: /inc/,
        name: {$: 'Paul'},
      },
    }),
    {
      text: `INSERT INTO "users"("id","name") VALUES ($1,$2) ON CONFLICT ("id") DO UPDATE SET "id"="users"."id"+1,"name"=$3`,
      params: [100, 'John', 'Paul'],
    }
  );
  assert.deepEqual(
    db.users.insert({ id: 100, name: 'John' }, { returnId: 'id' }),
    {
      text: `INSERT INTO "users"("id","name") VALUES ($1,$2) RETURNING "id"`,
      params: [100, 'John'],
    }
  );
  assert.deepEqual(
    db.users.insert({ name: 'John', age: {$: 20} }, { transform: false }),
    {
      text: `INSERT INTO "users"("name","age") VALUES ('John',$1)`,
      params: [20],
    }
  );
  assert.deepEqual(
    db.users.insert({
      name: 'John',
      age: 20,
      city: 'New York',
      alive: true,
      meta: { foo: 'bar' },
    }, {
      transform: {
        name: (name) => name.toUpperCase(),
        age: (age) => age + 10,
        city: false,
        meta: 'json',
      }
    }),
    {
      text: `INSERT INTO "users"("name","age","city","alive","meta") VALUES ('JOHN',30,'New York',$1,$2::json)`,
      params: [true, { foo: 'bar' }],
    }
  );
  assert.deepEqual(
    db.users.insert({ name: 'John', age: 20 }, {
      transform: (value) => ({$: value + 10}),
    }),
    {
      text: `INSERT INTO "users"("name","age") VALUES ($1,$2)`,
      params: ['John10', 30],
    }
  );
  assert.deepEqual(
    db.users.insert(2, {
      fields: ['id', 'name'],
      transform: {
        id: (id, i) => i,
        name: (name, i) => ['John', 'Mary'][i],
      }
    }),
    {
      text: `INSERT INTO "users"("id","name") VALUES (0,'John'),(1,'Mary')`,
      params: [],
    }
  );
  assert.deepEqual( // Using a generator function
    db.users.insert(function*() {
      yield 'A';
      yield 'B';
    }, {
      fields: ['id', 'name'],
      transform(value, i, key, row) {
        return {$: key + row};
      }
    }),
    {
      text: `INSERT INTO "users"("id","name") VALUES ($1,$2),($3,$4)`,
      params: ['idA', 'nameA', 'idB', 'nameB'],
    }
  )
});

it('should construct DELETE queries', () => {
  const pg = new MockedPostgres();
  const db = new Minusql.Postgres(pg);

  assert.deepEqual(
    db.users.delete({ id: {$: 1} }),
    {
      text: 'DELETE FROM "users" WHERE "id"=$1',
      params: [1],
    }
  );

  assert.deepEqual(
    db.users.join([{
      table: 'userStatuses',
      as: 'us',
      on: { 'users.id': Symbol('us.userId') },
    }, {
      table: 'photos',
      as: 'p',
      on: { 'users.id': Symbol('p.userId') },
    }]).delete({ 'us.status': {$: 'active'} }),
    {
      text: 'DELETE FROM "users" USING "user_statuses" AS "us","photos" AS "p" WHERE ("users"."id"="us"."user_id" AND "users"."id"="p"."user_id" AND "us"."status"=$1)',
      params: ['active'],
    }
  )
});

it('should apply output transformations', async () => {
  const input = [
    { id: 1, name: 'John', age: 20, role: 'admin' },
    { id: 2, name: 'Ivan', age: 21, role: 'admin' },
    { id: 3, name: 'Paul', age: 25, role: 'user' },
    { id: 4, name: 'Mary', age: 36, role: 'user' },
    { id: 5, name: 'Andrew', age: 36, role: 'user' },
  ];
  const pg = new MockedPostgres(input);
  const db = new Minusql.Postgres(pg);
  const query = db.users.selectAll();

  assert.deepEqual(
    await query,
    input,
  );
  assert.deepEqual(
    await query.exec(),
    input,
  );
  assert.deepEqual(
    await query.toArray(),
    input,
  );
  assert.deepEqual(
    await query.one(),
    input[0],
  );
  assert.deepEqual(
    await query.toArray('name'),
    ['John', 'Ivan', 'Paul', 'Mary', 'Andrew'],
  );
  assert.deepEqual(
    await query.toArray(['id', 'name', 'age']),
    [
      [1, 'John', 20],
      [2, 'Ivan', 21],
      [3, 'Paul', 25],
      [4, 'Mary', 36],
      [5, 'Andrew', 36],
    ]
  );
  assert.deepEqual(
    await query.toArray({
      fakeId: 'id',
      upperName: (row) => row.name.toUpperCase(),
      other: { age: 'age' },
    }),
    [
      { fakeId: 1, upperName: 'JOHN', other: { age: 20 }},
      { fakeId: 2, upperName: 'IVAN', other: { age: 21 }},
      { fakeId: 3, upperName: 'PAUL', other: { age: 25 }},
      { fakeId: 4, upperName: 'MARY', other: { age: 36 }},
      { fakeId: 5, upperName: 'ANDREW', other: { age: 36 }},
    ]
  );
  assert.deepEqual(
    await query.toObject('id'),
    {
      1: input[0],
      2: input[1],
      3: input[2],
      4: input[3],
      5: input[4],
    }
  );
  assert.deepEqual(
    await query.toObject(['name', 'age'], 'role'),
    {
      John_20: 'admin',
      Ivan_21: 'admin',
      Paul_25: 'user',
      Mary_36: 'user',
      Andrew_36: 'user',
    }
  );
  assert.deepEqual(
    await query.toObject((row) => `${row.id}:${row.name.toLowerCase()}`),
    {
      '1:john': input[0],
      '2:ivan': input[1],
      '3:paul': input[2],
      '4:mary': input[3],
      '5:andrew': input[4],
    }
  );
  assert.deepEqual(
    await query.toObjectArray('role', 'id'),
    {
      admin: [1, 2],
      user: [3, 4, 5],
    }
  );
  assert.deepEqual(
    await query.toMap('name', 'age'),
    new Map([
      ['John', 20],
      ['Ivan', 21],
      ['Paul', 25],
      ['Mary', 36],
      ['Andrew', 36],
    ])
  );
  assert.deepEqual(
    await query.toMapArray('age', 'name'),
    new Map([
      [20, ['John']],
      [21, ['Ivan']],
      [25, ['Paul']],
      [36, ['Mary', 'Andrew']],
    ])
  );
  assert.deepEqual(
    await query.toSet('name'),
    new Set(['John', 'Paul', 'Ivan', 'Andrew', 'Mary']),
  );
});

it('should not consume expressions while building', () => {
  const pg = new MockedPostgres();
  const db = new Minusql.Postgres(pg);

  // The same expression is routinely reused: a listing query and the COUNT(*)
  // that paginates it share one condition.
  const where = ['and',
    ['=', Symbol('status'), 'active'],
    ['>', Symbol('age'), { $: 18 }],
  ];
  const expected = `SELECT * FROM "users" WHERE (("status" = 'active') AND ("age" > $1))`;
  assert.strictEqual(db.users.select(where).text, expected);
  assert.strictEqual(db.users.select(where).text, expected);
  // The operator names are still at the front of every level.
  assert.strictEqual(where.length, 3);
  assert.strictEqual(where[0], 'and');
  assert.strictEqual(where[1][0], '=');
  assert.strictEqual(where[2][0], '>');

  const update = { visits: ['+', Symbol('visits'), 1] };
  assert.strictEqual(
    db.users.update(update, { id: 1 }).text,
    `UPDATE "users" SET "visits"=("visits" + 1) WHERE "id"=1`,
  );
  assert.strictEqual(
    db.users.update(update, { id: 2 }).text,
    `UPDATE "users" SET "visits"=("visits" + 1) WHERE "id"=2`,
  );
});

it('should construct CASE expressions', () => {
  const pg = new MockedPostgres();
  const db = new Minusql.Postgres(pg);

  // Searched form: ['case', [when, then], …, [else]]
  assert.strictEqual(
    db.users.selectAll({ fields: {
      adults: ['sum', ['case', [['>=', Symbol('age'), 18], 1], [0]]],
    } }).text,
    `SELECT SUM(CASE WHEN ("age" >= 18) THEN 1 ELSE 0 END) AS "adults" FROM "users"`,
  );

  // Simple form: a leading one-element branch is the subject.
  assert.strictEqual(
    db.users.selectAll({ fields: {
      label: ['case', [Symbol('status')], [1, 'active'], [2, 'banned'], ['unknown']],
    } }).text,
    `SELECT CASE "status" WHEN 1 THEN 'active' WHEN 2 THEN 'banned' ELSE 'unknown' END AS "label" FROM "users"`,
  );

  assert.throws(() => db.users.selectAll({ fields: { x: ['case', 'oops'] } }).text);
});

it('should join against a subquery', () => {
  const pg = new MockedPostgres();
  const db = new Minusql.Postgres(pg);

  const totals = db.orders.select(['>', Symbol('createdAt'), { $: '2026-01-01' }], {
    fields: { userId: true, total: ['sum', Symbol('amount')] },
    group: 'user_id',
    having: ['>', ['sum', Symbol('amount')], 100],
  });

  // The subquery's own parameters keep their place in the numbering.
  assert.deepEqual(
    db.join([
      { table: 'users' },
      { table: totals, as: 't', join: 'INNER', on: { 't.userId': Symbol('users.id') } },
    ]).select(['=', Symbol('users.city'), { $: 'Berlin' }], {
      fields: { id: Symbol('users.id'), total: Symbol('t.total') },
    }),
    {
      text: 'SELECT "users"."id" AS "id","t"."total" AS "total" FROM "users" INNER JOIN ' +
        '(SELECT "user_id",SUM("amount") AS "total" FROM "orders" WHERE ("created_at" > $1) ' +
        'GROUP BY user_id HAVING (SUM("amount") > 100)) AS "t" ON "t"."user_id"="users"."id" ' +
        'WHERE ("users"."city" = $2)',
      params: ['2026-01-01', 'Berlin'],
    },
  );
});

it('should parameterise tagged template queries per flavor', () => {
  const pg = new Minusql.Postgres(new MockedPostgres());
  const my = new Minusql.MySQL(new MockedMysql());

  assert.deepEqual(
    pg`SELECT * FROM "users" WHERE id = ${1} AND name = ${'John'}`,
    { text: 'SELECT * FROM "users" WHERE id = $1 AND name = $2', params: [1, 'John'] },
  );
  assert.deepEqual(
    my`SELECT * FROM \`users\` WHERE id = ${1} AND name = ${'John'}`,
    { text: 'SELECT * FROM `users` WHERE id = ? AND name = ?', params: [1, 'John'] },
  );

  // Values blocks contribute parameters too, so anything after them has to be
  // numbered from the parameter count rather than the interpolation index.
  const rows = [{ a: 1, b: 2 }, { a: 3, b: 4 }];
  assert.deepEqual(
    pg`INSERT INTO "t" ${pg.values(rows)} RETURNING id > ${9}`,
    {
      text: 'INSERT INTO "t" ("a","b") VALUES ($1,$2),($3,$4) RETURNING id > $5',
      params: [1, 2, 3, 4, 9],
    },
  );
  assert.deepEqual(
    my`INSERT INTO \`t\` ${my.values(rows)}`,
    { text: 'INSERT INTO `t` (`a`,`b`) VALUES (?,?),(?,?)', params: [1, 2, 3, 4] },
  );
  // Column names convert like they do everywhere else in the builder.
  assert.strictEqual(
    pg`INSERT INTO "t" ${pg.values([{ userId: 1, createdAt: 'now' }])}`.text,
    'INSERT INTO "t" ("user_id","created_at") VALUES ($1,$2)',
  );
  assert.strictEqual(
    pg`INSERT INTO "t" ${pg.values([])}`.text,
    'INSERT INTO "t" (SELECT NULL WHERE 1=0)',
  );
});
