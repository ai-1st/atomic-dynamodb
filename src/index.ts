import {
  DynamoDBClient,
  BatchGetItemCommand,
  QueryCommand,
  QueryCommandInput,
  TransactWriteItemsCommand,
  BatchWriteItemCommand,
  GetItemCommand,
  PutItemCommand,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import {
  marshall,
  unmarshall,
} from '@aws-sdk/util-dynamodb'
import { monotonicFactory } from 'ulid'
import { AttributeValue } from '@aws-sdk/client-dynamodb'
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
 * Generic database item with versioning and TTL
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
  version?: string
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
export interface AtomicDbInterface {
  /**
   * Get a single item by its key
   * @template T The type of data stored in the item
   * @param key The database item key
   * @returns The found item or undefined if not found
   */
  get<T>(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItem<T> | undefined>

  /**
   * Get multiple items by their keys
   * @template T The type of data stored in the item
   * @param keys The database item keys
   * @returns Array with same length as input keys array. Each element will be the corresponding item or undefined if not found.
   */
  getMany<T>(
    keys: AtomicDbItemKey[]
  ): Promise<(AtomicDbItem<T> | undefined)[]>

  /**
   * Get a lock object by its key directly from the DB
   * @param key The database item key
   * @returns The found lock object or undefined if not found
   */
  getLock(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItemLock>

  /**
   * Set one or multiple items using fast and cost-efficient BatchWriteItem command
   */
  set<T>(
    items: AtomicDbItem<T>[] | AtomicDbItem<T>
  ): Promise<void>

  /**
   * Set one or multiple items in a single transaction
   * If locks are provided, they will be used for optimistic locking
   */
  setAtomic<T>(
    items: AtomicDbItem<T>[] | AtomicDbItem<T>,
    locks?: AtomicDbItemLock[] | AtomicDbItemLock
  ): Promise<void>

  /**
   * Delete one or multiple items
   */
  delete(
    keys: AtomicDbItemKey[] | AtomicDbItemKey
  ): Promise<void>

  /**
   * Query items by primary key and optional sort key prefix
   */
  query<T>(
    query: AtomicDbQuery
  ): Promise<AtomicDbItem<T>[]>

  /**
   * Stream items by primary key and optional sort key prefix
   */
  stream<T>(
    query: AtomicDbQuery
  ): NodeJS.ReadableStream
}

/**
 * DynamoDB implementation of the database interface
 */
export class AtomicDynamoDB
  implements AtomicDbInterface
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
    const item = response.Item

    if (!item || !item.version?.S) {
      // Create new item with version
      const version = monotonicFactory()()
      const command = new PutItemCommand({
        TableName: this.tableName,
        Item: {
          pk: { S: key.pk },
          sk: { S: key.sk },
          version: { S: version },
          data: { S: JSON.stringify({}) },
        },
      })

      await this.client.send(command)
      return { ...key, version }
    }

    return {
      pk: item.pk.S!,
      sk: item.sk.S!,
      version: item.version.S!,
    }
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
            data: {
              S: JSON.stringify(item.data),
            },
            ...(item.ttl && {
              ttl: { N: item.ttl.toString() },
            }),
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
   * Set one or multiple items in a single transaction
   * If locks are provided, they will be used for optimistic locking
   */
  async setAtomic<T>(
    items: AtomicDbItem<T>[] | AtomicDbItem<T>,
    locks?: AtomicDbItemLock[] | AtomicDbItemLock
  ): Promise<void> {
    const itemArray = Array.isArray(items)
      ? items
      : [items]
    const lockArray = locks
      ? Array.isArray(locks)
        ? locks
        : [locks]
      : []

    if (itemArray.length === 0) return

    const transactItems = itemArray.map(
      (item, i) => {
        const lock = lockArray[i]
        return {
          Update: {
            TableName: this.tableName,
            Key: {
              pk: { S: item.pk },
              sk: { S: item.sk },
            },
            UpdateExpression:
              'SET #data = :data, #version = :newVersion' +
              (item.ttl ? ', #ttl = :ttl' : ''),
            ConditionExpression:
              '#version = :oldVersion',
            ExpressionAttributeNames: {
              '#data': 'data',
              '#version': 'version',
              ...(item.ttl !== undefined
                ? { '#ttl': 'ttl' }
                : {}),
            },
            ExpressionAttributeValues: {
              ':data': {
                S: JSON.stringify(item.data),
              },
              ':newVersion': {
                S: monotonicFactory()(),
              },
              ':oldVersion': { S: lock.version },
              ...(item.ttl !== undefined
                ? {
                    ':ttl': {
                      N: item.ttl.toString(),
                    },
                  }
                : {}),
            },
          },
        }
      }
    )

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
    const allItems: AtomicDbItem<T>[] = []
    let lastEvaluatedKey:
      | Record<string, any>
      | undefined
    let itemCount = 0
    const TableName = this.tableName

    do {
      const params: QueryCommandInput = {
        TableName,
        KeyConditionExpression: query.sk
          ? 'pk = :p and begins_with(sk, :s)'
          : 'pk = :p',
        ExpressionAttributeValues: marshall({
          ':p': query.pk,
          ...(query.sk && { ':s': query.sk }),
        }),
        ScanIndexForward: !(
          query.reverse ?? false
        ),
        ...(query.limit && {
          Limit: Math.min(
            query.limit - itemCount,
            1000
          ),
        }),
        ...(lastEvaluatedKey && {
          ExclusiveStartKey: marshall(
            lastEvaluatedKey
          ),
        }),
      }

      try {
        const command = new QueryCommand(params)
        const { Items = [], LastEvaluatedKey } =
          await this.client.send(command)

        const pageItems = Items.map((item) =>
          this.fromDynamoDBItem<T>(item)
        )

        allItems.push(...pageItems)
        itemCount += pageItems.length

        // Update the last evaluated key for the next page
        lastEvaluatedKey = LastEvaluatedKey
          ? unmarshall(LastEvaluatedKey)
          : undefined

        // Break if we've reached the requested limit
        if (
          query.limit &&
          itemCount >= query.limit
        ) {
          break
        }
      } catch (error) {
        console.error(
          'Error querying items:',
          error
        )
        throw error
      }
    } while (lastEvaluatedKey) // Continue until no more pages

    return allItems
  }

  /**
   * Stream items by primary key and optional sort key prefix
   */
  stream<T>(
    query: AtomicDbQuery
  ): NodeJS.ReadableStream {
    const TableName = this.tableName
    const fromDynamoDBItem =
      this.fromDynamoDBItem.bind(this)
    const client = this.client

    const stream = new Readable({
      objectMode: true,
      async read() {
        try {
          const params: QueryCommandInput = {
            TableName,
            KeyConditionExpression: query.sk
              ? 'pk = :p and begins_with(sk, :s)'
              : 'pk = :p',
            ExpressionAttributeValues: marshall({
              ':p': query.pk,
              ...(query.sk && { ':s': query.sk }),
            }),
            ScanIndexForward: !(
              query.reverse ?? false
            ),
            ...(query.limit && {
              Limit: query.limit,
            }),
          }

          const command = new QueryCommand(params)
          const { Items = [] } =
            await client.send(command)

          const items = Items.map((item) =>
            fromDynamoDBItem<T>(item)
          )

          for (const item of items) {
            this.push(item)
          }

          // Signal end of stream
          this.push(null)
        } catch (error) {
          console.error(
            'Error streaming items:',
            error
          )
          this.emit('error', error)
        }
      },
    })

    return stream
  }

  /**
   * Convert DynamoDB item to our AtomicDbItem format
   */
  private fromDynamoDBItem<T>(
    item: Record<string, AttributeValue>
  ): AtomicDbItem<T> {
    if (
      !item.pk?.S ||
      !item.sk?.S ||
      !item.data?.S
    ) {
      throw new Error(
        'Invalid DynamoDB item: missing required fields'
      )
    }
    return {
      pk: item.pk.S,
      sk: item.sk.S,
      data: JSON.parse(item.data.S) as T,
      ...(item.ttl?.N
        ? { ttl: parseInt(item.ttl.N) }
        : {}),
    }
  }

  /**
   * Convert our AtomicDbItem to DynamoDB format
   */
  private toDynamoDBItem<T>(
    item: AtomicDbItem<T>
  ): Record<string, AttributeValue> {
    const version = item.version
      ? { S: item.version }
      : { S: monotonicFactory()() }
    return {
      pk: { S: item.pk },
      sk: { S: item.sk },
      version,
      data: { S: JSON.stringify(item.data) },
      ...(item.ttl && {
        ttl: { N: item.ttl.toString() },
      }),
    }
  }
}
