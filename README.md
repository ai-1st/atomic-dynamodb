# Atomic DynamoDB

A TypeScript library for DynamoDB that provides atomic operations with optimistic locking.

## Features

- **Atomic Operations**: Perform atomic updates with optimistic locking to prevent race conditions
- **Separate Lock Objects**: Lock objects are stored separately from items, allowing for flexible locking strategies
- **Type Safety**: Full TypeScript support with generic types for item data
- **Streaming**: Stream query results for efficient processing of large datasets
- **Batch Operations**: Efficient batch operations for non-atomic updates

## Installation

```bash
npm install @ai-1st/atomic-dynamodb
```

## Usage

### Basic Setup

```typescript
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { AtomicDynamoDB } from '@ai-1st/atomic-dynamodb'

const client = new DynamoDBClient({
  region: 'us-west-2',
})

const db = new AtomicDynamoDB(client, 'my-table')
```

### Simple Operations

```typescript
// Set an item
await db.set({
  pk: 'user#123',
  sk: 'profile',
  data: { name: 'John', age: 30 },
})

// Get an item
const item = await db.get({
  pk: 'user#123',
  sk: 'profile',
})

// Delete an item
await db.delete({
  pk: 'user#123',
  sk: 'profile',
})
```

### Atomic Operations

```typescript
// Get or create a lock for atomic operations
const lock = await db.getLock({
  pk: 'user#123',
  sk: 'counter',
})

// Update item atomically
try {
  await db.setAtomic(
    {
      pk: 'user#123',
      sk: 'counter',
      data: { value: 42 },
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

### Batch Operations

```typescript
// Set multiple items
await db.set([
  {
    pk: 'user#123',
    sk: 'profile',
    data: { name: 'John' },
  },
  {
    pk: 'user#123',
    sk: 'settings',
    data: { theme: 'dark' },
  },
])

// Get multiple items
const items = await db.getMany([
  { pk: 'user#123', sk: 'profile' },
  { pk: 'user#123', sk: 'settings' },
])
```

### Query Operations

```typescript
// Query by partition key
const results = await db.query({
  pk: 'user#123',
})

// Query with sort key prefix
const results = await db.query({
  pk: 'user#123',
  sk: 'profile#',
})

// Stream results
const stream = db.stream({
  pk: 'user#123',
})
```

## Table Schema

Your DynamoDB table should have the following schema:

- Partition Key: `pk` (String)
- Sort Key: `sk` (String)

Optional attributes:

- `data` (String): JSON stringified data
- `version` (String): Used for optimistic locking
- `ttl` (Number): Time-to-live in epoch seconds

## Optimistic Locking

The library uses optimistic locking to prevent race conditions in atomic operations. Here's how it works:

1. Lock objects are stored separately from the actual items
2. Each lock object has a version that's updated on every atomic operation
3. The `setAtomic` method requires both the item to update and its corresponding lock
4. If the lock's version has changed since it was read, the operation fails with a `RaceCondition` error

This approach allows for:

- Separate versioning of locks and items
- Multiple items to be locked independently
- Atomic updates across multiple items

## Error Handling

The library throws the following errors:

- `RaceCondition`: Thrown when an atomic operation fails due to concurrent modifications
- `Error`: Standard error for invalid operations or DynamoDB errors

## License

MIT
