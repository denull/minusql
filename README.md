# MinuSQL

MinuSQL (pronounced _minuscule_) is a lightweight, flexible SQL query builder and database abstraction layer for Node.js that supports both MySQL and PostgreSQL databases. It provides a minimalistic API for building SQL queries while maintaining type safety and security.

## Features

- Support for both MySQL and PostgreSQL
- No dependencies
- Fluent query builder interface
- Automatic case conversion (snake_case ↔ camelCase)
- Parameterized queries for security
- Transaction support
- Flexible result mapping
- Built-in support for common SQL operations (SELECT, INSERT, UPDATE, DELETE)
- JOIN operations with various join types
- Conflict resolution for INSERT operations
- Type-safe query building
- EXPLAIN query support

## Installation

```bash
npm install minusql
```

## Quick Start

This library acts as a wrapper around database drivers. To use it, you first create an instance of either a `MySQL` or a `Postgres` class, passing the database client (or a pool) from the `mysql` or `pg` libraries:

### MySQL

```javascript
const mysql = require('mysql');
const { MySQL } = require('minusql');

const pool = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: 'password',
  database: 'mydb'
});

const db = new MySQL(pool);

// Query example
const user = await db.users.selectOne({ id: 1 });
```

### PostgreSQL

```javascript
const { Pool } = require('pg');
const { Postgres } = require('minusql');

const pool = new Pool({
  host: 'localhost',
  user: 'postgres',
  password: 'password',
  database: 'mydb'
});

const db = new Postgres(pool);

// Query example
const user = await db.users.selectOne({ id: 1 });
```

By default, MinuSQL automatically converts all identifiers to snake_case when building queries, and back to camelCase when handling results. You can disable this behavior by passing `{ convertCase: false }` to the constructor.

## API Documentation

To perform queries on specific tables, simply access them as properties of the `db` instance: for example, `db.users` represents the `users` table. To join multiple tables, call `db.join()` (see below).

You can also explicitly call `db.from('users')` to specify a table.

To perform raw queries, use `db.exec(query, params)`.

### Query Building

To perform CRUD operations on a table, use one of the following methods:
- `db.table.select(where?, options?)`
- `db.table.insert(rows, options?)`
- `db.table.update(update, where?, options?)`
- `db.table.delete(where?)`

All of these methods return a `Query` object, which will be executed as soon as you `await` it (or call `exec()` on it). You can also use the Query object to inspect the built query (the `text` field) or call `explain()` to construct an EXPLAIN query from it.

There are also two convenient aliases for common select types:
- `selectAll(options?)` is equivalent to `select(null, options)`
- `selectOne(where?, options?)` is equivalent to `select(where, options).one()`

#### SELECT Queries

```javascript
// Select everything
const results = await db.users.selectAll();

// Simple query
const results = await db.users.select({ id: 1, name: 'John' });

// Complex query with operators and parameter substitution
const ageMin = 18;
const permissionList = ['edit', 'delete'];
const users = await db.users.select(['and',
  ['>', Symbol('age'), {$: ageMin}],
  ['=', Symbol('status'), 'active'],
  ['or',
    { role: 'admin' },
    { permissions: ['in', {$: permissionList}] },
  ],
]);

// With specific fields
const results = await db.users
  .select(null, { fields: ['id', 'name'] });

// With ordering and limits
const results = await db.users
  .select(null, { 
    order: 'createdAt DESC', // or [[Symbol('createdAt'), 'DESC']]
    limit: 10,
    offset: 0
  });
```

The `select(where?, options?)` method takes two arguments: the first defines the query condition and the second sets additional options.

You can use a raw query string for `where`, but it's strongly discouraged as it won't be parameterized or escaped. Almost any condition can be expressed in a structured form instead.

Structured condition is defined as a recursive expression:
- If it's an **array**, its first element should be a SQL function name (like `COALESCE`) or an operator (like `<` or `AND`). All following elements are its arguments (which are parsed as nested expressions).
- If it's an **object with a $ field**, it contains a parameter which will be passed along with the query. This prevents SQL injections and improves performance, and is recommended for all user-supplied data. You can also use the `type` field to add an explicit type cast.
- If it's an **object**, its key-value pairs are converted to expressions in the form of `key = value` and joined using the `AND` operator. This is the same as `['AND', ['=', key1, value1], ['=', key2, value2], ...]`, just less verbose. If a value is itself an array, it's interpreted as if the key was inserted after the first element: `{ x: ['>', y] }` is the same as `['>', Symbol('x'), y]`. Keys are escaped as identifiers, and values may contain nested expressions.
- If it's a **Symbol** instance, it refers to a database column and is therefore escaped as an identifier.
- Otherwise (if it's a primitive value, like a number or string), it's escaped and inserted into the query.

There are a few special behaviors for specific SQL operators:
- `['in', Symbol('x'), [1, 2, 3]]` is converted to `"x" IN (1, 2, 3)`
- `['not in', Symbol('x'), [1, 2, 3]]` is converted to `"x" NOT IN (1, 2, 3)`
- `['between', Symbol('x'), 1, 2]` is converted to `"x" BETWEEN 1 AND 2`
- `['not between', Symbol('x'), 1, 2]` is converted to `"x" NOT BETWEEN 1 AND 2`
- `['type', Symbol('x'), 'json']` is converted to `json "x"` (the type is NOT escaped)
- `['cast', Symbol('x'), 'json']` is converted to `"x"::json` (the type is NOT escaped)
- `['extract', Symbol('x'), 'month']` is converted to `EXTRACT(month FROM x)` (note the order change; also the last argument is NOT escaped)
- `['case', [cond1, then1], [cond2, then2], [default]]` is converted to `CASE WHEN cond1 THEN then1 WHEN cond2 THEN then2 ELSE default END`
- `['filter', cond]` is converted to `FILTER (WHERE cond)`

Supported options are (all optional):
- `fields`: a list of fields to select
  - if `fields` is a string, it's inserted as is, **without any escaping** (not recommended)
  - if `fields` is an array, each element is treated as a column name
  - if `fields` is an object, keys with `true` values are treated as column names, and all other values are treated as expressions and converted to `expr AS key` statements
- `group`: a raw string or an array of expressions to use in the `GROUP BY` clause
- `having`: a raw string or structured condition to use in the `HAVING` clause
- `order`: a raw string or an array of pairs [expression, 'ASC' | 'DESC'] to use in the `ORDER BY` clause
- `limit`: a number to use in the `LIMIT` clause
- `offset`: a number to use in the `OFFSET` clause

By default, the resulting query returns an array of rows. To re-map it to more suitable data structures, see "Result Mapping" below.

#### INSERT Queries

```javascript
// Single insert
await db.users.insert({
  name: 'John', // Values will be parametrized by default (you can change this behavior by supplying "tranform" option)
  age: 30
});

// Returning inserted ID
const result = await db.users.insert({
  name: 'John',
  age:  30,
}, { returnId: true }); // (PostgreSQL only, MySQL will always add insertId to output)
// result will contain the ID of the inserted row

// Batch insert with manually parametrized values
await db.users.insert(usersToInsert.map(user => ({
  name: {$: user.name},
  age:  {$: user.age},
})), { transform: false });

// Upsert (handling conflicts)
await db.users.insert({
  id:         888352,
  name:       'John',
  age:        30,
  revision:   0,
  joinedAt:   Date.now() / 1000,
}, {
  transform: {
    joinedAt: 'timestamp', // Unixtime can be easily converted to timestamps
  },
  unique: ['id'], // Needed only for PostgreSQL (upserts on MySQL will work without it)
  conflict: {
    name:     /update/, // Update name on conflict
    age:      /max/,    // Update to largest of old and new value
    revision: ['+', Symbol('revision'), 1], // Expressions are supported here as well
    joinedAt: /fill/,   // Update only if was null
  },
});

```

`insert` accepts two parameters: rows to insert (or a single row) and options.

Supported options:
- `transform`: describes transformations to be applied to fields before insert:
  - `false`: do not apply any transformations
  - function: apply specified function to each field with `(value, key, updates)` as arguments
  - object: each key describes how corresponding field should be transformed (`false` and functions treates as above, strings are used to cast values to specified type)
  - otherwise, fields with simple values are wrapped in `{$: value}`, and object/arrays are left as is
- `fields`: an array of columns; if omitted, the first element's keys will be used
- `unique` (PostgreSQL only): for upserts, you need to specify a list of unique fields
- `conflict`: for upserts, describes the conflict resolution strategy (see below)
- `returnId` (PostgreSQL only): which column to return after insertion (set to `true` to return column "id"). MySQL will always return id of the inserted row (along with some other information) as a `insertId` field in the resulting row.

Conflict resolution strategy is either `false` (ignore all conflicts) or an object. Its keys correspond to columns that should be updated on conflict, and values are structured expressions to set them to.

For convenience, you can pass the following predefined RegExp patterns as aliases for common strategies:
- `/update/`: update to the new value on conflict
- `/fill/`: only update if the old value is `NULL`
- `/inc/`: increment old value by 1
- `/dec/`: decrement old value by 1
- `/add/`: add new value to the old one
- `/sub/`: subtract new value from the old one
- `/max/`: select the maximum out of old value and the new one
- `/min/`: select the minimum out of old value and the new one

Postgres also support `merge` queries (the syntax is the same as in inserts).

#### UPDATE Queries

```javascript
// Basic update with a simple where condition
await db.users.update(
  { age: 31 },
  { name: 'John' }
);

// Update with a complex where condition
await db.users.update(
  { status: 'inactive', lastSeen: new Date() },
  ['and', 
    ['<', Symbol('lastLogin'), {$: oneMonthAgo}],
    ['=', Symbol('status'), 'active']
  ]
);

// Update with expressions
await db.users.update(
  { 
    loginCount: ['+', Symbol('loginCount'), 1],
    status: 'active'
  },
  { id: 42 }
);

// Update all rows (be careful!)
await db.users.update(
  { isArchived: true },
  null
);
```

The `update(update, where?, options?)` method takes three parameters:
1. `update`: An object where keys are column names and values are either direct values or expressions
2. `where`: A condition to determine which rows to update (same format as in `select`); if `null`, all rows will be updated
3. `options`: The only supported option is `transform` (see `insert` above)

The update values can be:
- Simple values (strings, numbers, booleans, etc.)
- Expressions using the same syntax as in `where` conditions
- SQL functions and operators in array format

#### DELETE Queries

```javascript
// Delete with a simple condition
await db.users.delete({ name: 'John' });

// Delete with a complex condition
await db.users.delete(['and',
  ['<', Symbol('lastLogin'), {$: sixMonthsAgo}],
  ['=', Symbol('status'), 'inactive']
]);

// Delete all rows (use with caution!)
await db.users.delete(null);
```

The `delete(where?)` method takes a single parameter:
- `where`: A condition to determine which rows to delete (same format as in `select`); if `null` or omitted, all rows will be deleted

### Result Mapping

MinuSQL provides various methods for transforming query results into different data structures:

```javascript
// Get an array of results (default behavior)
const users = await db.users.select().toArray();

// Get an array of single column's values
const names = await db.users.select().toArray('name');


// Get just the first result or null if none found
const user = await db.users.select({ id: 1 }).one();
// Equivalent to using selectOne()
const user = await db.users.selectOne({ id: 1 });

// Get first result with transformation
const userName = await db.users.select({ id: 1 }).one('name');

// Map results to an object using a key
const usersById = await db.users.select().toObject('id');
// Result: { '1': {id: 1, name: 'John'}, '2': {id: 2, name: 'Jane'}, ... }

// Map to object with specific value
const nameById = await db.users.select().toObject('id', 'name');
// Result: { '1': 'John', '2': 'Jane', ... }

// Map to object using custom key function
const usersByFullName = await db.users.select().toObject(
  user => `${user.firstName} ${user.lastName}`
);

// Group into arrays by a key
const usersByRole = await db.users.select().toObjectArray('role', 'name');
// Result: { 'admin': ['John', 'Jane'], 'user': ['Bob', 'Alice'], ... }

// Map instance
const userMap = await db.users.select().toMap('id');
// Result: Map { 1 => {id: 1, name: 'John'}, 2 => {id: 2, name: 'Jane'}, ... }

// Map with specific value
const nameMap = await db.users.select().toMap('id', 'name');
// Result: Map { 1 => 'John', 2 => 'Jane', ... }

// Group into Map of arrays
const roleMap = await db.users.select().toMapArray('role', 'name');
// Result: Map { 'admin' => ['John', 'Jane'], 'user' => ['Bob', 'Alice'], ... }

// Extract values to a Set
const allRoles = await db.users.select().toSet('role');
// Result: Set { 'admin', 'user', 'guest', ... }

// Process each row with a function
await db.users.select().forEach(user => {
  console.log(`User ${user.name} is ${user.age} years old`);
});

// Map to class instances
class User {
  static fromRow(row) {
    const user = new User();
    user.id = row.id;
    user.name = row.name;
    return user;
  }
  
  greet() {
    return `Hello, ${this.name}!`;
  }
}

const users = await db.users.select().toArray(User);
console.log(users[0].greet()); // "Hello, John!"
```

The Query object provides these mapping methods:
- `one(value?)`: Returns the first result or null if none found, optionally transformed
- `toArray(value?)`: Returns results as an array, optionally transforming each row using the field parameter
- `toObject(key, value?)`: Maps results to an object using the specified key, optionally transforming values
- `toObjectArray(key, value?)`: Groups results into arrays by key
- `toMap(key, value?)`: Maps results to a Map
- `toMapArray(key, value?)`: Groups results into arrays in a Map by key
- `toSet(value)`: Extracts unique values from the specified field into a Set
- `forEach(fn)`: Executes a function for each result row

The transformation parameter (`value`) can be:
- A **string**: extracts that property from each row
- A **function**: called with `(row, index, allRows)` for custom transformations
- A **class**: tries to instantiate objects from rows, using `fromRow()` static method if available
- An **object**: for extracting/transforming multiple properties (recursively)
- An **array**: for extracting multiple properties as an array (recursively)

`key` parameter supports a subset of those types:
- A **string**: property to be used as key
- A **function**: called with `(row, index, allRows)` and result is used as key
- An **array**: elements will be joined with `_` and used as key

Note that by default, all result keys are automatically converted from snake_case to camelCase unless `convertCase: false` was set.

### Transactions

MinuSQL provides a simple way to work with transactions:

```javascript
// Basic transaction
await db.begin(async (tx) => {
  // The tx object is a transaction-specific database instance
  await tx.users.insert({ name: 'John' });
  await tx.profiles.insert({ userId: 1, bio: 'Hello' });
  
  // If any query fails, the transaction will be automatically rolled back
  // If all succeed, it will be committed automatically
});
```

### JOIN Operations

```javascript
const results = await db.join([
  { table: 'users', as: 'u' },
  { table: 'profiles', as: 'p', on: { 'u.id': Symbol('p.userId') } },
]).selectAll();

// Same as
const results = await db.users
  .join('profiles p', { 'users.id': Symbol('p.userId') })
  .selectAll();
```

To join multiple tables, call `db.join` with the array of objects containing following fields:
- `table`: name of the table or another subquery
- `as` (Optional): alias to be used in `AS` clause
- `join` (Optional): join type to be used in `JOIN` clause (if omitted, defaults to `LEFT`)
- `on`: any expression in the structured format to be used in `ON` clause

Instead of object with join description, you can also use raw string, but it's discouraged.

Alternatively, you can call `join` directly on a table: `db.users.join({ table: 'profiles', ... })`. As a shorthand, you can also pass table name as the first argument and join condition as second.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT