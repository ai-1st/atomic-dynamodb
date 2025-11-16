import test from 'ava'
import { AtomicDynamoDB } from './index'
import {
  AtomicDbItemKey,
  AtomicDbItem,
  RaceCondition,
  AtomicDbQueueItemInput,
} from 'atomic-db-interface'
import {
  DynamoDBClient,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb'

const client = new DynamoDBClient({
  region: 'us-east-1',
})
const TABLE_NAME = 'TEST'

const db = new AtomicDynamoDB(client, TABLE_NAME)
const dbWithCompression = new AtomicDynamoDB(
  client,
  TABLE_NAME,
  { compressData: true }
)

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

  // Get initial lock - now using the same key as the item
  const lock = await db.getLock(itemKey)

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
    const lock1 = await db.getLock(itemKey)

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

    const lock2 = await db.getLock(itemKey)

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
  const finalItem = await db.get({
    pk: itemKey.pk,
    sk: itemKey.sk,
  })
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

  await db.delete(itemKey)
})

test('lock ttl updates', async (t) => {
  const db = new AtomicDynamoDB(
    client,
    TABLE_NAME
  )
  const key = { pk: 'test-ttl', sk: 'item' }

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
  // Note: We need to update the lock key (with __LOCK__ prefix)
  await client.send(
    new UpdateItemCommand({
      TableName: TABLE_NAME,
      Key: {
        pk: { S: `__LOCK__${key.pk}` },
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
  const streamedItems: AtomicDbItem[] = []

  await new Promise<void>((resolve, reject) => {
    stream.on('data', (item: AtomicDbItem) => {
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

test('data compression feature', async (t) => {
  const key = {
    pk: 'test-compression',
    sk: 'item1',
  }
  await db.delete(key)
  await dbWithCompression.delete(key)

  // Create a large data object that would benefit from compression
  const largeData = {
    message: 'hello world '.repeat(100), // Creates a string ~1200 characters long
    numbers: Array.from(
      { length: 100 },
      (_, i) => i
    ),
    repeatedData: {
      field1: 'value1'.repeat(50),
      field2: 'value2'.repeat(50),
      field3: 'value3'.repeat(50),
    },
  }

  const item = {
    ...key,
    data: largeData,
  }

  // Test that compressed data can be stored and retrieved correctly
  await dbWithCompression.set(item)
  const result = await dbWithCompression.get(key)
  t.deepEqual(result?.data, largeData)

  // Test that non-compressed data can still be read by compressed instance
  await db.set(item)
  const resultFromNonCompressed =
    await dbWithCompression.get(key)
  t.deepEqual(
    resultFromNonCompressed?.data,
    largeData
  )

  // Test that compressed data can be read by non-compressed instance
  // (this tests backward compatibility)
  await dbWithCompression.set(item)
  const resultFromCompressed = await db.get(key)
  t.deepEqual(
    resultFromCompressed?.data,
    largeData
  )

  // Test atomic operations with compression
  await dbWithCompression.delete(key)

  const lock = await dbWithCompression.getLock(
    key
  )
  const atomicItem = {
    ...key,
    data: {
      counter: 42,
      message: 'atomic with compression',
    },
  }

  await dbWithCompression.setAtomic(
    atomicItem,
    lock
  )
  const atomicResult =
    await dbWithCompression.get(key)
  t.deepEqual(atomicResult?.data, atomicItem.data)

  // Cleanup
  await db.delete(key)
  await dbWithCompression.delete(key)
})

test('compression enables storing large data that exceeds DynamoDB limits', async (t) => {
  const key = {
    pk: 'test-large-compression',
    sk: 'item1',
  }
  await db.delete(key)
  await dbWithCompression.delete(key)

  // Create a 1MB string of "0"s - this will compress extremely well
  // but exceed DynamoDB's 400KB item size limit when uncompressed
  const oneMegabyteOfZeros = '0'.repeat(
    1024 * 1024
  ) // 1MB string
  const largeItem = {
    ...key,
    data: {
      content: oneMegabyteOfZeros,
      metadata:
        'This is a 1MB string that should only work with compression',
    },
  }

  // Test 1: Compressed client should successfully store and retrieve the large data
  await t.notThrowsAsync(async () => {
    await dbWithCompression.set(largeItem)
  }, 'Compressed client should handle 1MB of data')

  const compressedResult =
    await dbWithCompression.get(key)
  t.truthy(
    compressedResult,
    'Should retrieve the compressed large data'
  )
  t.is(
    compressedResult?.data.content,
    oneMegabyteOfZeros,
    'Content should match exactly'
  )
  t.is(
    compressedResult?.data.metadata,
    'This is a 1MB string that should only work with compression'
  )

  // Test 2: Non-compressed client should fail to store the large data (exceeds 400KB limit)
  await t.throwsAsync(
    async () => {
      await db.set(largeItem)
    },
    { instanceOf: Error },
    'Non-compressed client should fail with large data exceeding DynamoDB limits'
  )

  // Cleanup
  await dbWithCompression.delete(key)
})

test('queue operations - basic push and pull', async (t) => {
  const queuePk = 'test-queue-basic'

  // Clean up any existing queue items
  const existing = await db.query({
    pk: `__FIFO__${queuePk}`,
  })
  if (existing.length > 0) {
    await db.delete(
      existing.map((item) => ({
        pk: item.pk,
        sk: item.sk,
      }))
    )
  }

  // Test pushing items to queue
  const items: AtomicDbQueueItemInput[] = [
    {
      pk: queuePk,
      sk: 'task-1',
      data: { message: 'first task' },
    },
    {
      pk: queuePk,
      sk: 'task-2',
      data: { message: 'second task' },
    },
    {
      pk: queuePk,
      sk: 'task-3',
      data: { message: 'third task' },
    },
  ]

  await db.queuePush(items)

  // Test pulling items from queue (should be FIFO order)
  // In FIFO queues, items must be processed in order - can't pull next until previous is acknowledged
  const result1 = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.truthy(result1.item, 'Should pull first item')
  t.is(
    result1.item?.data?.message,
    'first task',
    'Should get first task'
  )
  t.true(
    result1.item?.isProcessing,
    'Item should be marked as processing'
  )
  t.truthy(
    result1.item?.processingTimeout,
    'Should have processing timeout'
  )

  // Can't pull second item while first is being processed (FIFO behavior)
  const result2BeforeAck = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.falsy(
    result2BeforeAck.item,
    'Should not pull second item while first is processing'
  )

  // Acknowledge first item to allow next item to be pulled
  await db.queueAcknowledge({
    pk: result1.item!.pk,
    sk: result1.item!.sk,
  })

  // Now can pull second item
  const result2 = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.truthy(
    result2.item,
    'Should pull second item after first is acknowledged'
  )
  t.is(
    result2.item?.data?.message,
    'second task',
    'Should get second task'
  )

  // Can't pull third item while second is being processed
  const result3BeforeAck = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.falsy(
    result3BeforeAck.item,
    'Should not pull third item while second is processing'
  )

  // Acknowledge second item
  await db.queueAcknowledge({
    pk: result2.item!.pk,
    sk: result2.item!.sk,
  })

  // Now can pull third item
  const result3 = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.truthy(
    result3.item,
    'Should pull third item after second is acknowledged'
  )
  t.is(
    result3.item?.data?.message,
    'third task',
    'Should get third task'
  )

  // Acknowledge third item
  await db.queueAcknowledge({
    pk: result3.item!.pk,
    sk: result3.item!.sk,
  })

  // Queue should be empty now
  const result4 = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.falsy(
    result4.item,
    'Queue should be empty after all items acknowledged'
  )
})

test('queue operations - deduplication', async (t) => {
  const queuePk = 'test-queue-dedup'

  // Clean up any existing queue items
  const existing = await db.query({
    pk: `__FIFO__${queuePk}`,
  })
  if (existing.length > 0) {
    await db.delete(
      existing.map((item) => ({
        pk: item.pk,
        sk: item.sk,
      }))
    )
  }

  // Push same item multiple times (should be deduplicated by sk)
  const item: AtomicDbQueueItemInput = {
    pk: queuePk,
    sk: 'unique-task',
    data: { message: 'deduplicated task' },
  }

  await db.queuePush(item)
  await db.queuePush(item) // Should not create duplicate
  await db.queuePush(item) // Should not create duplicate

  // Should only pull one item
  const result1 = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.truthy(
    result1.item,
    'Should pull the deduplicated item'
  )
  t.is(
    result1.item?.data?.message,
    'deduplicated task'
  )

  // No more items should be available
  const result2 = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.falsy(
    result2.item,
    'No duplicate items should be available'
  )

  // Clean up
  if (result1.item) {
    await db.queueAcknowledge({
      pk: result1.item.pk,
      sk: result1.item.sk,
    })
  }
})

test('queue operations - acknowledge and release', async (t) => {
  const queuePk = 'test-queue-ack-release'

  // Clean up any existing queue items
  const existing = await db.query({
    pk: `__FIFO__${queuePk}`,
  })
  if (existing.length > 0) {
    await db.delete(
      existing.map((item) => ({
        pk: item.pk,
        sk: item.sk,
      }))
    )
  }

  // Push test items
  await db.queuePush([
    {
      pk: queuePk,
      sk: 'ack-task',
      data: { message: 'to acknowledge' },
    },
    {
      pk: queuePk,
      sk: 'release-task',
      data: { message: 'to release' },
    },
  ])

  // Pull first item and acknowledge it
  const ackResult = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.truthy(
    ackResult.item,
    'Should pull first item'
  )

  await db.queueAcknowledge({
    pk: ackResult.item!.pk,
    sk: ackResult.item!.sk,
  })

  // Pull second item and release it
  const releaseResult = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.truthy(
    releaseResult.item,
    'Should pull second item'
  )

  await db.queueRelease({
    pk: releaseResult.item!.pk,
    sk: releaseResult.item!.sk,
  })

  // The released item should be available again
  const releasedItem = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.truthy(
    releasedItem.item,
    'Released item should be available again'
  )
  t.is(
    releasedItem.item?.data?.message,
    'to release'
  )

  // The acknowledged item should not be available
  const noMoreItems = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.falsy(
    noMoreItems.item,
    'No more items after one was acknowledged and one pulled again'
  )

  // Clean up the remaining item
  if (releasedItem.item) {
    await db.queueAcknowledge({
      pk: releasedItem.item.pk,
      sk: releasedItem.item.sk,
    })
  }
})

test('queue operations - processing timeout expiration', async (t) => {
  const queuePk = 'test-queue-timeout'

  // Clean up any existing queue items
  const existing = await db.query({
    pk: `__FIFO__${queuePk}`,
  })
  if (existing.length > 0) {
    await db.delete(
      existing.map((item) => ({
        pk: item.pk,
        sk: item.sk,
      }))
    )
  }

  // Push a test item
  await db.queuePush({
    pk: queuePk,
    sk: 'timeout-task',
    data: { message: 'will timeout' },
  })

  // Pull with very short timeout (1 second)
  const result1 = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 1,
  })
  t.truthy(result1.item, 'Should pull item')
  t.true(
    result1.item!.processingTimeout <=
      Math.floor(Date.now() / 1000) + 2,
    'Should have short timeout'
  )

  // Wait for timeout to expire
  await new Promise((resolve) =>
    setTimeout(resolve, 1500)
  )

  // Item should be available again after timeout
  const result2 = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.truthy(
    result2.item,
    'Item should be available again after timeout'
  )
  t.is(
    result2.item?.data?.message,
    'will timeout'
  )

  // Clean up
  if (result2.item) {
    await db.queueAcknowledge({
      pk: result2.item.pk,
      sk: result2.item.sk,
    })
  }
})

test('queue operations - empty queue', async (t) => {
  const queuePk = 'test-queue-empty'

  // Ensure queue is empty
  const existing = await db.query({
    pk: `__FIFO__${queuePk}`,
  })
  if (existing.length > 0) {
    await db.delete(
      existing.map((item) => ({
        pk: item.pk,
        sk: item.sk,
      }))
    )
  }

  // Try to pull from empty queue
  const result = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.falsy(
    result.item,
    'Should return undefined for empty queue'
  )
})

test('queue operations - multiple consumers', async (t) => {
  const queuePk = 'test-queue-multi-consumer'

  // Clean up any existing queue items
  const existing = await db.query({
    pk: `__FIFO__${queuePk}`,
  })
  if (existing.length > 0) {
    await db.delete(
      existing.map((item) => ({
        pk: item.pk,
        sk: item.sk,
      }))
    )
  }

  // Push multiple items sequentially to ensure ULID ordering
  // (pushing in batch might cause ULIDs to be generated out of order)
  for (let i = 0; i < 5; i++) {
    await db.queuePush({
      pk: queuePk,
      sk: `task-${i}`,
      data: { taskId: i, message: `Task ${i}` },
    })
    // Small delay to ensure ULIDs are generated in order
    await new Promise((resolve) =>
      setTimeout(resolve, 1)
    )
  }

  // In a FIFO queue, only one consumer can process items at a time
  // Consumers must acknowledge items before the next one can be pulled
  // Simulate sequential processing (which is how FIFO queues work)
  const processedItems: any[] = []

  // Process all items sequentially - each must be acknowledged before next can be pulled
  for (let i = 0; i < 5; i++) {
    const result = await db.queuePull({
      pk: queuePk,
      ttlSeconds: 60,
    })
    t.truthy(result.item, `Should pull item ${i}`)
    processedItems.push(result.item)

    // Verify we can't pull next item while current is being processed
    if (i < 4) {
      const nextResult = await db.queuePull({
        pk: queuePk,
        ttlSeconds: 60,
      })
      t.falsy(
        nextResult.item,
        `Should not pull item ${
          i + 1
        } while item ${i} is processing`
      )
    }

    // Acknowledge current item to allow next one to be pulled
    await db.queueAcknowledge({
      pk: result.item!.pk,
      sk: result.item!.sk,
    })
  }

  t.is(
    processedItems.length,
    5,
    'All 5 items should be processed'
  )

  // Verify items were processed in order (FIFO)
  const taskIds = processedItems.map(
    (item) => item.data.taskId
  )
  t.deepEqual(
    taskIds,
    [0, 1, 2, 3, 4],
    'Items should be processed in FIFO order'
  )

  // Queue should be empty now
  const finalCheck = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 60,
  })
  t.falsy(
    finalCheck.item,
    'Queue should be completely empty'
  )
})
