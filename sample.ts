import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { getLockedValue } from "dynamodb-locked-value";

const client = new DynamoDBClient();

async function retry<T>(callback: () => Promise<T>): Promise<T> {
  // You can implement your own retry logic here
  return await callback();
}

export async function getValue(
  hashKey: string,
  now: Date,
  getNewValue: () => Promise<{ value: string; ttl: number }>,
): Promise<string> {
  return retry(() =>
    getLockedValue(client, {
      tableName: "tokens",
      hashKeyAttributeName: "key",
      hashKey,
      now,
      lockDuration: 3000,
      getNewValue,
    }),
  );
}
