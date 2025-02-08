import test from 'ava'
import {
  AtomicDynamoDB,
  AtomicDbItemKey,
  AtomicDbItem,
  RaceCondition,
} from './index'
import {
  DynamoDBClient,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb'

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

test('atomic operations with race conditions', async (t) => {
  const db = new AtomicDynamoDB(
    client,
    TABLE_NAME
  )

  const itemKey = {
    pk: 'test',
    sk: 'profile',
  }
  const lockKey = {
    pk: 'test',
    sk: 'profile_lock',
  }

  // Get initial lock
  const lock = await db.getLock(lockKey)

  // Set initial value
  await db.setAtomic(
    {
      pk: itemKey.pk,
      sk: itemKey.sk,
      data: { name: '' },
    },
    lock
  )

  // Simulate two parallel operations trying to set different names
  const operation1 = async () => {
    const lock1 = await db.getLock(lockKey)

    // Add a longer delay for operation1 to ensure race condition
    await new Promise((resolve) =>
      setTimeout(resolve, 500)
    )

    await db.setAtomic(
      {
        pk: itemKey.pk,
        sk: itemKey.sk,
        data: { name: 'John' },
      },
      lock1
    )
    return 'John'
  }

  const operation2 = async () => {
    // Add a small delay before getting lock for operation2
    await new Promise((resolve) =>
      setTimeout(resolve, 100)
    )

    const lock2 = await db.getLock(lockKey)

    // Add a small delay before setting for operation2
    await new Promise((resolve) =>
      setTimeout(resolve, 400)
    )

    await db.setAtomic(
      {
        pk: itemKey.pk,
        sk: itemKey.sk,
        data: { name: 'Mary' },
      },
      lock2
    )
    return 'Mary'
  }

  // Run both operations concurrently
  const results = await Promise.allSettled([
    operation1(),
    operation2(),
  ])

  // Verify that exactly one operation succeeded and one failed
  const succeeded = results.filter(
    (r) => r.status === 'fulfilled'
  ).length
  const failed = results.filter(
    (r) => r.status === 'rejected'
  ).length
  t.is(
    succeeded,
    1,
    'Exactly one operation should succeed'
  )
  t.is(
    failed,
    1,
    'Exactly one operation should fail'
  )

  // Verify that the failed operation was due to RaceCondition
  const failedOp = results.find(
    (r) => r.status === 'rejected'
  ) as PromiseRejectedResult
  t.true(
    failedOp.reason instanceof RaceCondition,
    'Failed operation should throw RaceCondition'
  )

  // Get the name that succeeded
  const successOp = results.find(
    (r) => r.status === 'fulfilled'
  ) as PromiseFulfilledResult<string>
  const expectedName = successOp.value

  // Verify that exactly one name was set
  const finalItem = await db.get<{
    name: string
  }>(itemKey)
  t.is(
    finalItem?.data?.name,
    expectedName,
    'Name should match the successful operation'
  )
  t.true(
    ['John', 'Mary'].includes(
      finalItem?.data?.name || ''
    ),
    'Name should be either John or Mary'
  )

  await db.delete([lockKey, itemKey])
})

test('lock ttl updates', async (t) => {
  const db = new AtomicDynamoDB(
    client,
    TABLE_NAME
  )
  const key = { pk: 'test-ttl', sk: 'lock' }

  // First call creates lock with 24h TTL
  const lock1 = await db.getLock(key)
  t.truthy(lock1.ttl)
  const now = Math.floor(Date.now() / 1000)
  t.true(lock1.ttl! >= now + 23 * 60 * 60) // At least 23 hours in the future

  // Second call with same lock should not update TTL or version
  const lock2 = await db.getLock(key)
  t.is(lock2.ttl, lock1.ttl)
  t.is(lock2.version, lock1.version)

  // Manually set TTL to less than 1 hour from now to force refresh
  await client.send(
    new UpdateItemCommand({
      TableName: TABLE_NAME,
      Key: {
        pk: { S: key.pk },
        sk: { S: key.sk },
      },
      UpdateExpression: 'SET #ttlAttr = :ttl',
      ExpressionAttributeNames: {
        '#ttlAttr': 'ttl',
      },
      ExpressionAttributeValues: {
        ':ttl': { N: (now + 30 * 60).toString() }, // 30 minutes from now
      },
    })
  )

  // Third call should create new lock with fresh TTL and version
  const lock3 = await db.getLock(key)
  t.not(lock3.version, lock2.version)
  t.true(lock3.ttl! >= now + 23 * 60 * 60)

  await db.delete(key)
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
