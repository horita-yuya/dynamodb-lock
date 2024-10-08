import {
  ConditionalCheckFailedException,
  DeleteItemCommand,
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

async function getValue(
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

async function lockValue(
  client: DynamoDBClient,
  tableName: string,
  hashKeyName: string,
  hashKey: string,
  ttl: number,
  now: number,
): Promise<LockResult> {
  try {
    await client.send(
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

async function unlockValue(
  client: DynamoDBClient,
  tableName: string,
  hashKeyName: string,
  hashKey: string,
) {
  await client.send(
    new DeleteItemCommand({
      TableName: tableName,
      Key: {
        [hashKeyName]: { S: hashKey },
      },
    }),
  );
}

export async function getLockedValue(
  client: DynamoDBClient,
  {
    tableName,
    hashKeyAttributeName,
    hashKey,
    now,
    getNewValue,
    lockDuration = 5000,
  }: {
    tableName: string;
    hashKeyAttributeName: string;
    hashKey: string;
    now: UnixTimestampMillis;
    lockDuration?: number;
    getNewValue: () => Promise<{ value: string; ttl: number }>;
  },
): Promise<string> {
  // All copies of data usually reach consistency within a second.
  // https://aws.amazon.com/dynamodb/faqs/?nc1=h_ls
  const currentValue = await getValue(
    client,
    tableName,
    hashKeyAttributeName,
    hashKey,
  );
  const nowUnixTime = typeof now === "number" ? now : now.getTime();

  if (currentValue.hashKey === null) {
    const result = await lockValue(
      client,
      tableName,
      hashKeyAttributeName,
      `${hashKey}:initial`,
      nowUnixTime + lockDuration,
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
        throw new Error(
          `Another process is updating key:${hashKey} value. You should retry.`,
        );

      case "expired":
        await unlockValue(
          client,
          tableName,
          hashKeyAttributeName,
          `${hashKey}:initial`,
        );
        return await getLockedValue(client, {
          tableName,
          hashKeyAttributeName,
          hashKey,
          now,
          getNewValue,
        });
    }
  }

  const expired = currentValue.ttl < nowUnixTime;
  // Update the value before 3 minutes from the expiration time.
  if (currentValue.ttl < nowUnixTime + 3 * 60 * 1000) {
    const result = await lockValue(
      client,
      tableName,
      hashKeyAttributeName,
      `${hashKey}:${currentValue.ttl}`,
      nowUnixTime + lockDuration,
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
        if (expired) {
          throw new Error(
            `Another process is updating key:${hashKey} value. You should retry.`,
          );
        }

        return currentValue.value;

      case "expired":
        await unlockValue(client, tableName, hashKeyAttributeName, hashKey);

        if (expired) {
          return await getLockedValue(client, {
            tableName,
            hashKeyAttributeName,
            hashKey,
            now,
            getNewValue,
          });
        }

        return currentValue.value;
    }
  }

  return currentValue.value;
}
