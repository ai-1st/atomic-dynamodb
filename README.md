# Atomic DynamoDB

A TypeScript-first DynamoDB wrapper that provides atomic operations and simplified batch operations. Built on top of AWS SDK v3.

## Features

- 🔒 Built-in optimistic locking
- 🚀 Efficient batch operations (automatically handles DynamoDB limits)
- 📝 Full TypeScript support
- 🔄 Streaming support for large result sets
- ⚡ Simple and intuitive API

## Installation

```bash
npm install @ai-1st/atomic-dynamodb
```

## Quick Start

```typescript
import { AtomicDynamoDB } from '@ai-1st/atomic-dynamodb'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'

const client = new DynamoDBClient({
  region: 'us-east-1',
})
const db = new AtomicDynamoDB(
  client,
  'YourTableName'
)

// Basic CRUD
await db.set({
  pk: 'user:1',
  sk: 'profile',
  data: { name: 'Alice' },
})
const profile = await db.get({
  pk: 'user:1',
  sk: 'profile',
})

// Batch operations
const users = await db.getMany([
  { pk: 'user:1', sk: 'profile' },
  { pk: 'user:2', sk: 'profile' },
])

// Query operations
const userPosts = await db.query({
  pk: 'user:1',
  sk: 'post:', // Get all items starting with "post:"
})

// Streaming for large result sets
const stream = db.stream({ pk: 'user:1' })
stream.on('data', (item) => console.log(item))
```

## Atomic Operations

The package provides optimistic locking through the `setAtomic` method:

```typescript
// Get current version
const lock = await db.getLock({
  pk: 'counter',
  sk: 'visits',
})

// Update with version check
try {
  await db.setAtomic(
    {
      pk: 'counter',
      sk: 'visits',
      data: { count: 42 },
    },
    lock
  )
} catch (e) {
  if (e instanceof RaceCondition) {
    // Handle concurrent modification
  }
  throw e
}
```

## Batch Operations

Batch operations automatically handle DynamoDB's limits:

```typescript
// Batch write (automatically handles 25 item limit)
await db.set([
  {
    pk: 'user:1',
    sk: 'post:1',
    data: { title: 'Hello' },
  },
  {
    pk: 'user:1',
    sk: 'post:2',
    data: { title: 'World' },
  },
  // ... can handle any number of items
])

// Batch get (maintains order, handles missing items)
const items = await db.getMany([
  { pk: 'user:1', sk: 'post:1' },
  { pk: 'user:1', sk: 'missing' }, // Will be undefined in result
  { pk: 'user:1', sk: 'post:2' },
])
// items[1] will be undefined for missing item
```

## Design Decisions

### Optimistic Locking

1. Uses ULID for versioning to ensure monotonically increasing versions
2. Versions are automatically created when getting a lock for a non-existent item
3. Atomic updates use DynamoDB transactions to ensure consistency
4. Failed version checks throw a `RaceCondition` error

### Data Structure

1. Each item requires `pk` (partition key) and `sk` (sort key)
2. Data is stored in a separate `data` attribute as JSON
3. Optional `ttl` attribute for item expiration
4. Version is stored in a separate `version` attribute

### Batch Operations

1. Automatically handles DynamoDB's 25-item batch limit
2. Maintains order of results in batch gets
3. Returns undefined for missing items rather than omitting them
4. Uses efficient BatchWrite for non-atomic operations

### Query Operations

1. Supports optional sort key prefix filtering
2. Returns items in sort key order
3. Provides both promise and stream interfaces

## API Reference

### Constructor

```typescript
const db = new AtomicDynamoDB(client: DynamoDBClient, tableName: string)
```

### Methods

- `get<T>(key: AtomicDbItemKey): Promise<AtomicDbItem<T> | undefined>`
- `getMany<T>(keys: AtomicDbItemKey[]): Promise<(AtomicDbItem<T> | undefined)[]>`
- `set<T>(items: AtomicDbItem<T>[] | AtomicDbItem<T>): Promise<void>`
- `setAtomic<T>(items: AtomicDbItem<T>[] | AtomicDbItem<T>, locks: AtomicDbItemLock[] | AtomicDbItemLock): Promise<void>`
- `delete(keys: AtomicDbItemKey[] | AtomicDbItemKey): Promise<void>`
- `query<T>(query: AtomicDbQuery): Promise<AtomicDbItem<T>[]>`
- `stream<T>(query: AtomicDbQuery): NodeJS.ReadableStream`
- `getLock(key: AtomicDbItemKey): Promise<AtomicDbItemLock>`

## License

MIT
