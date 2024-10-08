import {
  ConditionalCheckFailedException,
  type DynamoDBClient,
  GetItemCommand,
  UpdateItemCommand,
} from "@aws-sdk/client-dynamodb";

type LockedValue =
  | {
      hashKey: string;
      value: string;
      ttl: number;
    }
  | {
      hashKey: null;
      value: null;
      ttl: null;
    };

type UnixTimestampMillis = Date | number;

async function getEventuallyConsistentValue(
  client: DynamoDBClient,
  tableName: string,
  hashKeyName: string,
  hashKey: string,
): Promise<LockedValue> {
  const item = await client.send(
    new GetItemCommand({
      TableName: tableName,
      Key: {
        [hashKeyName]: { S: hashKey },
      },
    }),
  );

  const value = item.Item?.value.S ?? null;
  const ttl = item.Item?.ttl.N ? Number.parseInt(item.Item.ttl.N) : null;

  return value !== null && ttl !== null
    ? {
        hashKey,
        value,
        ttl,
      }
    : {
        hashKey: null,
        value: null,
        ttl: null,
      };
}

async function updateValue(
  client: DynamoDBClient,
  tableName: string,
  hashKeyName: string,
  hashKey: string,
  value: string,
  ttl: number,
): Promise<void> {
  await client.send(
    new UpdateItemCommand({
      TableName: tableName,
      Key: {
        [hashKeyName]: { S: hashKey },
      },
      UpdateExpression: "SET #value = :value, #ttl = :ttl",
      ExpressionAttributeNames: {
        "#value": "value",
        "#ttl": "ttl",
      },
      ExpressionAttributeValues: {
        ":value": { S: value },
        ":ttl": { N: ttl.toString() },
      },
    }),
  );
}

type LockResult = "acquired" | "locked" | "expired";

async function getLock(
  client: DynamoDBClient,
  tableName: string,
  hashKeyName: string,
  hashKey: string,
  ttl: number,
  now: number,
): Promise<LockResult> {
  try {
    const lock = await client.send(
      new UpdateItemCommand({
        TableName: tableName,
        Key: {
          [hashKeyName]: { S: hashKey },
        },
        UpdateExpression: "SET #ttl = :ttl",
        ConditionExpression: "#ttl < :now OR attribute_not_exists(#ttl)",
        ExpressionAttributeNames: {
          "#ttl": "ttl",
        },
        ExpressionAttributeValues: {
          ":ttl": { N: ttl.toString() },
          ":now": { N: now.toString() },
        },
        ReturnValues: "ALL_OLD",
        ReturnValuesOnConditionCheckFailure: "ALL_OLD",
      }),
    );

    console.log(lock.Attributes);

    return "acquired";
  } catch (error) {
    if (error instanceof ConditionalCheckFailedException) {
      const ttl = error.Item?.ttl?.N ? Number.parseInt(error.Item.ttl.N) : null;

      if (ttl === null) {
        return "locked";
      }

      return ttl < now ? "expired" : "locked";
    }

    throw error;
  }
}

export async function getLockedValue(
  client: DynamoDBClient,
  {
    tableName,
    hashKeyAttributeName,
    hashKey,
    now,
    getNewValue,
    ignoreLock,
    tolerance = 5000,
  }: {
    tableName: string;
    hashKeyAttributeName: string;
    hashKey: string;
    now: UnixTimestampMillis;
    tolerance?: number;
    ignoreLock?: boolean;
    getNewValue: () => Promise<{ value: string; ttl: number }>;
  },
): Promise<string | null> {
  const currentValue = await getEventuallyConsistentValue(
    client,
    tableName,
    hashKeyAttributeName,
    hashKey,
  );

  if (currentValue.hashKey === null) {
    const nowUnixTime = typeof now === "number" ? now : now.getTime();
    const result = await getLock(
      client,
      tableName,
      hashKeyAttributeName,
      `${hashKey}:initial`,
      nowUnixTime + tolerance,
      nowUnixTime,
    );

    switch (result) {
      case "acquired": {
        const { value, ttl } = await getNewValue();
        await updateValue(
          client,
          tableName,
          hashKeyAttributeName,
          hashKey,
          value,
          ttl,
        );
        return value;
      }

      case "locked":
        if (ignoreLock) {
          return null;
        }

        throw new Error(`Should retry. ${hashKey} is locked`);

      case "expired":
        break;
    }
  } else {
    return currentValue.value;
  }
}
