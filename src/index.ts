import {
  BatchGetItemCommand,
  BatchWriteItemCommand,
  DeleteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  QueryCommand,
  QueryCommandInput,
  QueryCommandOutput,
  TransactWriteItemsCommand,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import {
  AttributeValue,
  TransactWriteItemsCommandInput,
} from '@aws-sdk/client-dynamodb'
import {
  marshall,
  unmarshall,
} from '@aws-sdk/util-dynamodb'
import { monotonicFactory } from 'ulid'
import { Readable } from 'stream'

// Error types for database operations
export class RaceCondition extends Error {
  constructor() {
    super('Race condition detected')
    this.name = 'RaceCondition'
  }
}

/**
 * Database item key structure
 */
export interface AtomicDbItemKey {
  /** Primary key */
  pk: string
  /** Sort key */
  sk: string
}

/**
 * Generic database item with TTL
 * @template T The type of data stored in the item
 */
export interface AtomicDbItem<T>
  extends AtomicDbItemKey {
  /** The actual data stored in the item */
  data?: T

  /**
   * Epoch time in seconds after which the item will be deleted by the database
   */
  ttl?: number
}

/**
 * Lock object for database items
 */
export interface AtomicDbItemLock
  extends AtomicDbItemKey {
  version: string
}

/**
 * Query options for database operations
 */
export interface AtomicDbQuery {
  /** Primary key to query */
  pk: string
  /** Optional sort key prefix */
  sk?: string
  /** If true, returns results in reverse order */
  reverse?: boolean
  /** Maximum number of items to return */
  limit?: number
}

/**
 * Base interface for database operations
 */
export interface AtomicDbInterface<T> {
  /**
   * Get a single item by its key
   * @template T The type of data stored in the item
   * @param key The database item key
   * @returns The found item or undefined if not found
   */
  get(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItem<T> | undefined>

  /**
   * Get multiple items by their keys
   * @template T The type of data stored in the item
   * @param keys The database item keys
   * @returns Array with same length as input keys array. Each element will be the corresponding item or undefined if not found.
   */
  getMany(
    keys: AtomicDbItemKey[]
  ): Promise<(AtomicDbItem<T> | undefined)[]>

  /**
   * Get a lock object by its key directly from the DB
   * If the item doesn't exist, creates a new one with a version.
   * Lock objects are separate from regular items and are used for optimistic locking.
   * @param key The database item key
   * @returns The found lock object or a new one with initial version
   */
  getLock(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItemLock>

  /**
   * Set one or multiple items using fast and cost-efficient BatchWriteItem command
   * This operation does not perform any version checks.
   * @param items The items to set
   */
  set(
    items: AtomicDbItem<T>[] | AtomicDbItem<T>
  ): Promise<void>

  /**
   * Set one or multiple items atomically with optimistic locking
   * Each item requires a corresponding lock object for version checking.
   * Lock objects are stored separately from the items and are updated with new versions after successful operations.
   * @param items The items to set
   * @param locks The lock objects to check versions against. Must match items one-to-one.
   * @throws {RaceCondition} If version check fails
   */
  setAtomic(
    items: AtomicDbItem<T>[] | AtomicDbItem<T>,
    locks: AtomicDbItemLock[] | AtomicDbItemLock
  ): Promise<void>

  /**
   * Delete one or multiple items
   * @param keys The keys of items to delete
   */
  delete(
    keys: AtomicDbItemKey[] | AtomicDbItemKey
  ): Promise<void>

  /**
   * Query items by primary key and optional sort key prefix
   * @param query The query parameters
   * @returns Array of matching items
   */
  query(
    query: AtomicDbQuery
  ): Promise<AtomicDbItem<T>[]>

  /**
   * Stream items by primary key and optional sort key prefix
   * @param query The query parameters
   * @returns Readable stream of matching items
   */
  stream(
    query: AtomicDbQuery
  ): NodeJS.ReadableStream
}

/**
 * DynamoDB implementation of the database interface
 */
export class AtomicDynamoDB
  implements AtomicDbInterface<unknown>
{
  private client: DynamoDBDocumentClient
  private tableName: string

  constructor(
    client: DynamoDBClient,
    tableName: string
  ) {
    this.client =
      DynamoDBDocumentClient.from(client)
    this.tableName = tableName
  }

  /**
   * Get a single item by its key
   */
  async get<T>(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItem<T> | undefined> {
    const results = await this.getMany<T>([key])
    return results[0]
  }

  /**
   * Get multiple items by their keys
   */
  async getMany<T>(
    keys: AtomicDbItemKey[]
  ): Promise<(AtomicDbItem<T> | undefined)[]> {
    if (keys.length === 0) return []

    // DynamoDB batch get has a limit of 100 items
    const batchSize = 100
    const results = new Map<
      string,
      AtomicDbItem<T>
    >()

    // Process in batches of 100
    for (
      let i = 0;
      i < keys.length;
      i += batchSize
    ) {
      const batch = keys.slice(i, i + batchSize)
      const batchRequests = batch.map((key) => ({
        Key: {
          pk: { S: key.pk },
          sk: { S: key.sk },
        },
      }))

      const command = new BatchGetItemCommand({
        RequestItems: {
          [this.tableName]: {
            Keys: batchRequests.map(
              (req) => req.Key
            ),
          },
        },
      })

      const response = await this.client.send(
        command
      )

      if (response.Responses?.[this.tableName]) {
        for (const item of response.Responses[
          this.tableName
        ]) {
          const converted =
            this.fromDynamoDBItem<T>(item)
          results.set(
            `${converted.pk}:${converted.sk}`,
            converted
          )
        }
      }
    }

    // Return items in the same order as requested, with undefined for missing items
    return keys.map((key) =>
      results.get(`${key.pk}:${key.sk}`)
    )
  }

  /**
   * Get a lock object by its key directly from the DB
   * If the item doesn't exist, creates a new one with a version
   */
  async getLock(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItemLock> {
    const command = new GetItemCommand({
      TableName: this.tableName,
      Key: {
        pk: { S: key.pk },
        sk: { S: key.sk },
      },
    })

    const response = await this.client.send(
      command
    )
    if (response.Item) {
      return this.fromDynamoDBItemToLock(
        response.Item
      )
    }

    // Create new lock with initial version
    const newLock: AtomicDbItemLock = {
      pk: key.pk,
      sk: key.sk,
      version: monotonicFactory()(),
    }

    await this.client.send(
      new PutItemCommand({
        TableName: this.tableName,
        Item: this.toDynamoDBLockItem(newLock),
      })
    )

    return newLock
  }

  /**
   * Set one or multiple items using fast and cost-efficient BatchWriteItem command
   */
  async set<T>(
    items: AtomicDbItem<T>[] | AtomicDbItem<T>
  ): Promise<void> {
    const itemArray = Array.isArray(items)
      ? items
      : [items]
    if (itemArray.length === 0) return

    // DynamoDB batch write has a limit of 25 items
    const batchSize = 25

    // Process in batches of 25
    for (
      let i = 0;
      i < itemArray.length;
      i += batchSize
    ) {
      const batch = itemArray.slice(
        i,
        i + batchSize
      )
      const batchRequests = batch.map((item) => ({
        PutRequest: {
          Item: {
            pk: { S: item.pk },
            sk: { S: item.sk },
            ...(item.data !== undefined
              ? {
                  data: {
                    S: JSON.stringify(item.data),
                  },
                }
              : {}),
            ...(item.ttl
              ? {
                  ttl: { N: item.ttl.toString() },
                }
              : {}),
          },
        },
      }))

      const command = new BatchWriteItemCommand({
        RequestItems: {
          [this.tableName]: batchRequests,
        },
      })

      try {
        await this.client.send(command)
      } catch (error) {
        console.error(
          'Error batch writing items:',
          error
        )
        throw error
      }
    }
  }

  /**
   * Set items atomically with version check
   * @template T The type of data stored in the item
   * @param items The items to set
   * @param locks The locks to check versions against
   * @throws {RaceCondition} If version check fails
   */
  async setAtomic<T>(
    items: AtomicDbItem<T>[] | AtomicDbItem<T>,
    locks: AtomicDbItemLock[] | AtomicDbItemLock
  ): Promise<void> {
    const itemsArray = Array.isArray(items)
      ? items
      : [items]
    const locksArray = Array.isArray(locks)
      ? locks
      : [locks]

    if (itemsArray.length !== locksArray.length) {
      throw new Error(
        'Number of items must match number of locks'
      )
    }

    const newVersion = monotonicFactory()()
    const transactItems: TransactWriteItemsCommandInput['TransactItems'] =
      []

    for (let i = 0; i < itemsArray.length; i++) {
      const item = itemsArray[i]
      const lock = locksArray[i]

      // Update lock with version check
      const updatedLock: AtomicDbItemLock = {
        ...lock,
        version: newVersion,
      }
      transactItems.push({
        Update: {
          TableName: this.tableName,
          Key: {
            pk: { S: lock.pk },
            sk: { S: lock.sk },
          },
          UpdateExpression:
            'SET version = :newVersion',
          ConditionExpression:
            'version = :oldVersion',
          ExpressionAttributeValues: {
            ':newVersion': { S: newVersion },
            ':oldVersion': { S: lock.version },
          },
        },
      })

      // Put the actual item
      transactItems.push({
        Put: {
          TableName: this.tableName,
          Item: this.toDynamoDBItem(item),
        },
      })
    }

    try {
      await this.client.send(
        new TransactWriteItemsCommand({
          TransactItems: transactItems,
        })
      )
    } catch (e) {
      if (
        e instanceof TransactionCanceledException
      ) {
        throw new RaceCondition()
      }
      throw e
    }
  }

  /**
   * Delete one or multiple items
   */
  async delete(
    keys: AtomicDbItemKey[] | AtomicDbItemKey
  ): Promise<void> {
    const keyArray = Array.isArray(keys)
      ? keys
      : [keys]
    if (keyArray.length === 0) return

    const transactItems = keyArray.map((key) => ({
      Delete: {
        TableName: this.tableName,
        Key: {
          pk: { S: key.pk },
          sk: { S: key.sk },
        },
      },
    }))

    try {
      await this.client.send(
        new TransactWriteItemsCommand({
          TransactItems: transactItems,
        })
      )
    } catch (error) {
      if (
        error instanceof Error &&
        error.name ===
          'TransactionCanceledException'
      ) {
        throw new RaceCondition()
      }
      throw error
    }
  }

  /**
   * Query items by primary key and optional sort key prefix
   */
  async query<T>(
    query: AtomicDbQuery
  ): Promise<AtomicDbItem<T>[]> {
    const results: AtomicDbItem<T>[] = []
    let LastEvaluatedKey:
      | Record<string, AttributeValue>
      | undefined

    const TableName = this.tableName

    do {
      const params: QueryCommandInput = {
        TableName,
        KeyConditionExpression: query.sk
          ? 'pk = :p and begins_with(sk, :s)'
          : 'pk = :p',
        ExpressionAttributeValues: {
          ':p': { S: query.pk },
          ...(query.sk
            ? { ':s': { S: query.sk } }
            : {}),
        },
        ...(LastEvaluatedKey
          ? {
              ExclusiveStartKey: LastEvaluatedKey,
            }
          : {}),
      }

      try {
        const command = new QueryCommand(params)
        const response = await this.client.send(
          command
        )
        const Items = response.Items || []
        LastEvaluatedKey =
          response.LastEvaluatedKey

        const pageItems = Items.map((item) =>
          this.fromDynamoDBItem<T>(item)
        )
        results.push(...pageItems)
      } catch (error) {
        throw error
      }
    } while (LastEvaluatedKey)

    return results
  }

  /**
   * Stream items by primary key and optional sort key prefix
   */
  stream<T>(query: AtomicDbQuery): Readable {
    const client = this.client
    const TableName = this.tableName
    const fromDynamoDBItem =
      this.fromDynamoDBItem.bind(this)

    return new Readable({
      objectMode: true,
      async read() {
        try {
          const params: QueryCommandInput = {
            TableName,
            KeyConditionExpression: query.sk
              ? 'pk = :p and begins_with(sk, :s)'
              : 'pk = :p',
            ExpressionAttributeValues: {
              ':p': { S: query.pk },
              ...(query.sk
                ? { ':s': { S: query.sk } }
                : {}),
            },
          }

          const command = new QueryCommand(params)
          const response = await client.send(
            command
          )
          const Items = response.Items || []

          const items = Items.map((item) =>
            fromDynamoDBItem<T>(item)
          )
          for (const item of items) {
            this.push(item)
          }
          this.push(null)
        } catch (error) {
          this.destroy(error as Error)
        }
      },
    })
  }

  /**
   * Convert DynamoDB item to our AtomicDbItem format
   */
  private fromDynamoDBItem<T>(
    item: Record<string, AttributeValue>
  ): AtomicDbItem<T> {
    if (!item.pk?.S || !item.sk?.S) {
      throw new Error(
        'Invalid DynamoDB item: missing required fields'
      )
    }
    return {
      pk: item.pk.S,
      sk: item.sk.S,
      ...(item.data?.S
        ? { data: JSON.parse(item.data.S) }
        : {}),
      ...(item.ttl?.N
        ? { ttl: parseInt(item.ttl.N) }
        : {}),
    }
  }

  /**
   * Convert DynamoDB item to lock format
   */
  private fromDynamoDBItemToLock(
    item: Record<string, AttributeValue>
  ): AtomicDbItemLock {
    if (
      !item.pk?.S ||
      !item.sk?.S ||
      !item.version?.S
    ) {
      throw new Error(
        'Invalid DynamoDB item: missing required fields'
      )
    }
    return {
      pk: item.pk.S,
      sk: item.sk.S,
      version: item.version.S,
    }
  }

  /**
   * Convert our AtomicDbItem to DynamoDB format
   */
  private toDynamoDBItem(
    item: AtomicDbItem<unknown>
  ): Record<string, AttributeValue> {
    return {
      pk: { S: item.pk },
      sk: { S: item.sk },
      ...(item.data !== undefined
        ? {
            data: {
              S: JSON.stringify(item.data),
            },
          }
        : {}),
      ...(item.ttl
        ? { ttl: { N: item.ttl.toString() } }
        : {}),
    }
  }

  /**
   * Convert our lock item to DynamoDB format
   */
  private toDynamoDBLockItem(
    item: AtomicDbItemLock
  ): Record<string, AttributeValue> {
    return {
      pk: { S: item.pk },
      sk: { S: item.sk },
      version: { S: item.version },
    }
  }
}
