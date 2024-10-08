import { CreateTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { GenericContainer } from "testcontainers";
import { expect, test, vi } from "vitest";
import { getLockedValue } from "./index";

async function createContainer(): Promise<DynamoDBClient> {
  const container = await GenericContainer.fromDockerfile(
    __dirname,
    "Dockerfile-test",
  ).build();

  const testContainer = await container.withExposedPorts(8000).start();

  return new DynamoDBClient({
    region: "ap-northeast-1",
    endpoint: {
      protocol: "http:",
      hostname: testContainer.getHost(),
      port: testContainer.getMappedPort(8000),
      path: "",
    },
    credentials: {
      accessKeyId: "dummy",
      secretAccessKey: "dummy",
    },
  });
}

async function createTable(client: DynamoDBClient) {
  await client.send(
    new CreateTableCommand({
      TableName: "TestTable",
      KeySchema: [
        {
          KeyType: "HASH",
          AttributeName: "key",
        },
      ],
      AttributeDefinitions: [
        {
          AttributeName: "key",
          AttributeType: "S",
        },
      ],
      BillingMode: "PAY_PER_REQUEST",
    }),
  );
}

test(
  "",
  async () => {
    const client = await createContainer();

    await createTable(client);
    const now = new Date().getTime();
    const value = await getLockedValue(client, {
      tableName: "TestTable",
      hashKeyAttributeName: "key",
      hashKey: "token",
      now,
      getNewValue: async () => ({ value: "value", ttl: now + 1000 }),
    });

    expect(value).toBe("value");
  },
  {
    timeout: 10000,
  },
);

// Add parallel calling getLockedValue test
test(
  "",
  async () => {
    const client = await createContainer();

    await createTable(client);
    const now = new Date().getTime();
    const getNewValueMock = vi.fn(async () => ({
      value: "value",
      ttl: now + 1000,
    }));
    const promises = Array.from({ length: 10 }).map(() =>
      getLockedValue(client, {
        tableName: "TestTable",
        hashKeyAttributeName: "key",
        hashKey: "token",
        now,
        ignoreLock: true,
        getNewValue: getNewValueMock,
      }),
    );

    const values = (await Promise.all(promises)).filter((it) => it !== null);

    expect(values).toHaveLength(1);
    expect(getNewValueMock).toHaveBeenCalledTimes(1);
  },
  {
    timeout: 10000,
  },
);
