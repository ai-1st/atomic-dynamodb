# Atomic DynamoDB

A TypeScript library for DynamoDB that provides atomic operations with optimistic locking.

## Features

- **Atomic Operations**: Perform atomic updates with optimistic locking to prevent race conditions
- **Automatic Lock Management**: Lock objects are automatically managed with transparent key transformation, 24-hour expiry, and automatic refresh
- **FIFO Queue Operations**: Built-in support for First-In-First-Out queues with deduplication, visibility timeout, and optimistic locking
- **Type Safety**: Full TypeScript support with generic types for item data
- **Streaming**: Stream query results for efficient processing of large datasets
- **Batch Operations**: Efficient batch operations for non-atomic updates

## Installation

```bash
npm install atomic-dynamodb
```

## Usage

### Basic Setup

```typescript
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { AtomicDynamoDB } from 'atomic-dynamodb'

const client = new DynamoDBClient({
  region: 'us-west-2',
})

// Basic usage
const db = new AtomicDynamoDB(client, 'my-table')

// With data compression (reduces storage size and costs)
const dbWithCompression = new AtomicDynamoDB(
  client,
  'my-table',
  {
    compressData: true,
  }
)
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
// Define the key for your item
const itemKey = {
  pk: 'user#123',
  sk: 'counter',
}

// Get or create a lock (automatically expires after 24 hours)
// Uses the same key - lock transformation is handled internally
const lock = await db.getLock(itemKey)

// Update item atomically
try {
  await db.setAtomic(
    {
      pk: itemKey.pk,
      sk: itemKey.sk,
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

// Clean up (automatically removes both item and lock)
await db.delete(itemKey)
```

### Data Compression

The library supports optional data compression using gzip to reduce storage size and costs:

```typescript
// Enable compression when creating the instance
const db = new AtomicDynamoDB(
  client,
  'my-table',
  {
    compressData: true,
  }
)
```

**How it works:**

- When `compressData: true`, all data is compressed using gzip before storing
- Compressed data is stored as binary attributes (B) in DynamoDB for maximum efficiency
- Uncompressed data continues to be stored as string attributes (S)
- Compressed data is automatically decompressed when reading
- Backward compatibility: compressed instances can read uncompressed data and vice versa

**Storage efficiency:**

- Compressed data is stored as native binary attributes (no base64 overhead)
- Typical compression ratios: 60-90% size reduction for structured/repetitive data
- Reduces DynamoDB storage costs and improves read/write performance for large items

**When to use compression:**

- ✅ Large data objects (> 1KB)
- ✅ Repetitive or structured data
- ✅ Cost optimization for high-volume applications
- ❌ Small objects (< 100 bytes) - compression overhead may increase size
- ❌ Already compressed data (images, videos, etc.)

**Example with large data:**

```typescript
const largeData = {
  description: 'Lorem ipsum...'.repeat(100), // Large text
  metadata: {
    /* complex nested object */
  },
  tags: Array.from(
    { length: 50 },
    (_, i) => `tag-${i}`
  ),
}

// Automatically compresses before storing
await db.set({
  pk: 'document#123',
  sk: 'content',
  data: largeData,
})

// Automatically decompresses when reading
const result = await db.get({
  pk: 'document#123',
  sk: 'content',
})
// result.data === largeData (decompressed)
```

### Best Practices for Locks

1. **Use the Same Key**: You can use the same key for both the item and its lock

   `getLock` and `setAtomic` automatically handle lock key transformation by prepending `"__LOCK__"` to the primary key internally. You never need to manually create separate lock keys.

   ```typescript
   // Good - use the same key for both item and lock
   const itemKey = { pk: 'user#123', sk: 'data' }
   const lock = await db.getLock(itemKey) // Automatically uses __LOCK__user#123 internally
   await db.setAtomic(item, lock) // Works with the same key

   // The library automatically transforms the lock key internally:
   // Item stored at: { pk: 'user#123', sk: 'data' }
   // Lock stored at: { pk: '__LOCK__user#123', sk: 'data' }
   ```

2. **Lock Lifecycle**: Locks are automatically managed

   - New locks expire after 24 hours
   - Locks are automatically refreshed when accessed within their last hour
   - No manual TTL management required

3. **Clean Up**: The `delete` method automatically removes both the item and its lock
   ```typescript
   // Clean up both the data and lock automatically
   await db.delete(itemKey)
   // Internally deletes both:
   // - Item at: { pk: 'user#123', sk: 'data' }
   // - Lock at: { pk: '__LOCK__user#123', sk: 'data' }
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

### FIFO Queue Operations

The library provides built-in support for FIFO (First-In-First-Out) queues with deduplication, visibility timeout, and optimistic locking.

```typescript
// Push items to a queue
await db.queuePush([
  {
    pk: 'jobs',
    sk: 'process-image-123',
    data: {
      task: 'process-image',
      imageId: '123',
    },
  },
  {
    pk: 'jobs',
    sk: 'send-email-456',
    data: {
      task: 'send-email',
      to: 'user@example.com',
    },
  },
])

// Pull an item from the queue (with 5-minute visibility timeout)
const result = await db.queuePull({
  pk: 'jobs',
  ttlSeconds: 300, // 5 minutes
})

if (result.item) {
  const itemKey = {
    pk: result.item.pk,
    sk: result.item.sk,
  }

  try {
    // Process the item
    const task = result.item.data
    console.log('Processing:', task)

    // ... do work ...

    // Acknowledge completion (deletes item)
    await db.queueAcknowledge(itemKey)
  } catch (error) {
    // If processing fails, you can manually release it immediately
    // or let the timeout expire naturally
    console.error('Processing failed:', error)

    // Option 1: Manually release immediately
    await db.queueRelease(itemKey)

    // Option 2: Let timeout expire (no action needed)
    // The item will become available again after processingTimeout expires
  }
} else {
  console.log(
    'Queue is empty or all items are being processed'
  )
}
```

#### Queue Features

- **FIFO Ordering**: Items are pulled in the order they were added (using ULID for automatic ordering)
- **Deduplication**: Items with the same `sk` (sort key) are automatically deduplicated
- **Visibility Timeout**: Items being processed automatically become available again if the processing timeout expires
- **Item-Level Processing**: Each pulled item is marked as processing with a timeout, allowing multiple consumers to process different items concurrently
- **Manual Release**: You can manually release an item back to the queue before the timeout expires
- **Multiple Consumers**: Multiple consumers can pull and process different items simultaneously without conflicts

```typescript
// Multiple consumers can work concurrently
const consumer1 = async () => {
  const result = await db.queuePull({
    pk: 'jobs',
    ttlSeconds: 300,
  })
  if (result.item) {
    // Process item...
    await db.queueAcknowledge({
      pk: result.item.pk,
      sk: result.item.sk,
    })
  }
}

const consumer2 = async () => {
  const result = await db.queuePull({
    pk: 'jobs',
    ttlSeconds: 300,
  })
  if (result.item) {
    // Process different item...
    await db.queueAcknowledge({
      pk: result.item.pk,
      sk: result.item.sk,
    })
  }
}

// Both consumers can work concurrently
await Promise.all([consumer1(), consumer2()])
```

## Table Schema

Your DynamoDB table should have the following schema:

- Partition Key: `pk` (String)
- Sort Key: `sk` (String)

Optional attributes:

- `data` (String or Binary): JSON stringified data (String) or gzip compressed data (Binary)
- `version` (String): Used for optimistic locking (only on lock items)
- `ttl` (Number): Time-to-live in epoch seconds (automatically managed for locks)
- `enqueued` (String Set): ULID timestamps for queue items (queue operations only)
- `isProcessing` (Boolean): Processing state for queue items (queue operations only)
- `processingTimeout` (Number): Processing timeout epoch seconds (queue operations only)

The library automatically manages different collections using key prefixes:

- Regular items: stored with your provided keys
- Lock items: stored with `__LOCK__` prefix on the primary key
- Queue items: stored with `__FIFO__` prefix on the primary key
- Queue deduplication: stored with `__FIFO_DEDUP__` prefix on the primary key

## Lock Management

The library uses optimistic locking with automatic TTL management to prevent race conditions in atomic operations. Here's how it works:

1. Lock objects are automatically stored separately from items using transparent key transformation (prepending `"__LOCK__"`)
2. Each lock object has a version that's updated on every atomic operation
3. Locks automatically expire after 24 hours via DynamoDB's TTL feature
4. When a lock is accessed within its last hour of validity, it's automatically refreshed with a new 24-hour TTL
5. The `setAtomic` method requires both the item to update and its corresponding lock
6. If the lock's version has changed since it was read, the operation fails with a `RaceCondition` error
7. The `delete` method automatically removes both the item and its associated lock

This approach allows for:

- Atomic updates with simplified key management
- Automatic separation between data and lock storage
- Transparent lock key transformation
- Automatic cleanup of stale locks via TTL
- Zero-maintenance lock management

## Error Handling

The library throws the following errors:

- `RaceCondition`: Thrown when an atomic operation fails due to concurrent modifications
- `Error`: Standard error for invalid operations or DynamoDB errors

## License

MIT
