import test from 'ava'
import {
  AtomicDynamoDB,
  AtomicDbItemKey,
  AtomicDbItem,
  RaceCondition,
} from './index'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'

const client = new DynamoDBClient({
  region: 'us-east-1',
})
const TABLE_NAME = 'TEST'

const db = new AtomicDynamoDB(client, TABLE_NAME)

test('basic CRUD operations', async (t) => {
  const key = { pk: 'test-crud', sk: 'item1' }
  await db.delete(key)

  // Test set and get
  const item = {
    ...key,
    data: { message: 'hello' },
  }
  await db.set(item)
  const result = await db.get(key)
  t.deepEqual(result?.data, item.data)

  // Test delete
  await db.delete(key)
  const deleted = await db.get(key)
  t.is(deleted, undefined)
})

test('batch operations', async (t) => {
  const keys = [
    { pk: 'test-batch', sk: 'item1' },
    { pk: 'test-batch', sk: 'item2' },
    { pk: 'test-batch', sk: 'item3' },
  ]
  await db.delete(keys)

  // Test batch set
  const items = keys.map((key) => ({
    ...key,
    data: {
      index: parseInt(key.sk.replace('item', '')),
    },
  }))
  await db.set(items)

  // Test batch get - should maintain order
  const results = await db.getMany(keys)
  t.deepEqual(
    results.map((r) => r?.data),
    items.map((i) => i.data)
  )

  // Test get with missing items
  const mixedKeys = [
    keys[0],
    { pk: 'test-batch', sk: 'missing' },
    keys[2],
  ]
  const mixedResults = await db.getMany(mixedKeys)
  t.deepEqual(
    mixedResults.map((r) => r?.data),
    [items[0].data, undefined, items[2].data]
  )

  // Test batch delete
  await db.delete(keys)
  const deletedResults = await db.getMany(keys)
  t.deepEqual(deletedResults, [
    undefined,
    undefined,
    undefined,
  ])
})

test('query operations', async (t) => {
  const pk = 'test-query'
  const keys = [
    { pk, sk: 'a:item1' },
    { pk, sk: 'a:item2' },
    { pk, sk: 'b:item1' },
  ]
  await db.delete(keys)

  // Set up test data
  const items = keys.map((key) => ({
    ...key,
    data: { category: key.sk.split(':')[0] },
  }))
  await db.set(items)

  t.deepEqual(
    [
      // Test query with prefix
      (await db.query({ pk, sk: 'a:' })).length,
      (await db.query({ pk })).length,
      (await db.query({ pk, sk: 'c:' })).length,
    ],
    [2, 3, 0]
  )

  await db.delete(keys)
})

test('atomic operations', async (t) => {
  const itemKey = {
    pk: 'test',
    sk: 'counter',
  }
  const lockKey = {
    pk: 'test',
    sk: 'counter_lock',
  }

  // Get lock first
  const lock = await db.getLock(lockKey)
  t.truthy(lock.version)

  // Set item with lock
  await db.setAtomic(
    {
      pk: itemKey.pk,
      sk: itemKey.sk,
      data: { counter: 1 },
    },
    lock
  )

  // Try to update with old version (should fail)
  const error = await t.throwsAsync(
    db.setAtomic(
      {
        pk: itemKey.pk,
        sk: itemKey.sk,
        data: { counter: 2 },
      },
      lock
    )
  )
  t.true(error instanceof RaceCondition)

  // Verify item was updated but lock is separate
  const item = await db.get<{ counter: number }>(
    itemKey
  )
  t.is(item?.data?.counter, 1)
  const newLock = await db.getLock(lockKey)
  t.truthy(newLock.version)
  t.not(newLock.version, lock.version)

  await db.delete([lockKey, itemKey])
})

test('stream operations', async (t) => {
  const pk = 'test-stream'
  const keys = [
    { pk, sk: 'item1' },
    { pk, sk: 'item2' },
    { pk, sk: 'item3' },
  ]
  await db.delete(keys)

  // Set up test data
  const items = keys.map((key) => ({
    ...key,
    data: {
      index: parseInt(key.sk.replace('item', '')),
    },
  }))
  await db.set(items)

  // Test streaming
  const stream = db.stream({ pk })
  const streamedItems: any[] = []

  await new Promise((resolve, reject) => {
    stream.on('data', (item) => {
      streamedItems.push(item)
    })
    stream.on('end', resolve)
    stream.on('error', reject)
  })

  t.deepEqual(
    [
      streamedItems.length,
      streamedItems.map((i) => i.data),
    ],
    [items.length, items.map((i) => i.data)]
  )

  await db.delete(keys)
})

test('large batch operations', async (t) => {
  // Create 50 items (2 full batches)
  const keys = Array.from(
    { length: 50 },
    (_, i) => ({
      pk: 'test-large-batch',
      sk: `item${i.toString().padStart(2, '0')}`, // item00, item01, etc.
    })
  )
  await db.delete(keys)

  // Test batch set
  const items = keys.map((key) => ({
    ...key,
    data: {
      index: parseInt(key.sk.replace('item', '')),
    },
  }))
  await db.set(items)

  // Test batch get - should maintain order
  const results = await db.getMany(keys)
  t.deepEqual(
    results.map((r) => r?.data),
    items.map((i) => i.data)
  )

  // Test get with missing items
  const mixedKeys = [
    keys[0],
    { pk: 'test-large-batch', sk: 'missing1' },
    keys[25], // middle of second batch
    { pk: 'test-large-batch', sk: 'missing2' },
    keys[49], // last item
  ]
  const mixedResults = await db.getMany(mixedKeys)
  t.deepEqual(
    mixedResults.map((r) => r?.data),
    [
      items[0].data,
      undefined,
      items[25].data,
      undefined,
      items[49].data,
    ]
  )

  // Test batch delete
  await db.delete(keys)
  const deletedResults = await db.getMany(keys)
  t.deepEqual(
    deletedResults,
    Array(50).fill(undefined)
  )
})
