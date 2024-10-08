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
  "getLockedValue returns value from getNewValue when the value is not found in the table",
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
    timeout: 100000,
  },
);

test(
  "Only one getNewValue call is made when multiple getLockedValue calls are made in parallel",
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
        getNewValue: getNewValueMock,
      }).catch(() => "test-locked"),
    );

    const values = (await Promise.all(promises)).filter(
      (it) => it !== "test-locked",
    );

    expect(values).toHaveLength(1);
    expect(getNewValueMock).toHaveBeenCalledTimes(1);
  },
  {
    timeout: 100000,
  },
);

test(
  "only one getNewValue call is made when multiple getLockedValue calls are made in parallel even if current token is expired",
  async () => {
    const client = await createContainer();

    await createTable(client);
    const now = new Date().getTime();
    const getNewValueMock = vi.fn(async () => ({
      value: "value",
      ttl: now + 1000,
    }));

    const currentValue = await getLockedValue(client, {
      tableName: "TestTable",
      hashKeyAttributeName: "key",
      hashKey: "token",
      now,
      getNewValue: async () => ({ value: "old-value", ttl: now - 1000 }),
    });

    const promises = Array.from({ length: 10 }).map(() =>
      getLockedValue(client, {
        tableName: "TestTable",
        hashKeyAttributeName: "key",
        hashKey: "token",
        now,
        getNewValue: getNewValueMock,
      }).catch(() => "test-locked"),
    );

    const values = (await Promise.all(promises)).filter(
      (it) => it !== "test-locked",
    );

    expect(currentValue).toBe("old-value");
    expect(values).toHaveLength(1);
    expect(values[0]).toBe("value");
    expect(getNewValueMock).toHaveBeenCalledTimes(1);
  },
  {
    timeout: 100000,
  },
);

test(
  "unlock value if lock is expired",
  async () => {
    const client = await createContainer();

    await createTable(client);
    const now = new Date().getTime();

    const currentValue = await getLockedValue(client, {
      tableName: "TestTable",
      hashKeyAttributeName: "key",
      hashKey: "token",
      now,
      getNewValue: async () => ({ value: "old-value", ttl: now - 1000 }),
    });

    const failedToUpdateValue = await getLockedValue(client, {
      tableName: "TestTable",
      hashKeyAttributeName: "key",
      hashKey: "token",
      now,
      lockDuration: 1000,
      getNewValue: async () => {
        throw new Error("test-failed-to-update");
      },
    }).catch((e) => e.message);

    const getNewValueMock = vi.fn(async () => ({
      value: "force-update",
      ttl: now + 3000,
    }));
    const forceUpdateValues = Array.from({ length: 10 }).map(() => {
      return getLockedValue(client, {
        tableName: "TestTable",
        hashKeyAttributeName: "key",
        hashKey: "token",
        now: now + 2000,
        getNewValue: getNewValueMock,
      }).catch(() => "test-locked");
    });

    const forceUpdatedResult = (await Promise.all(forceUpdateValues)).filter(
      (it) => it !== "test-locked",
    );

    expect(currentValue).toBe("old-value");
    expect(failedToUpdateValue).toBe("test-failed-to-update");
    expect(forceUpdatedResult).toHaveLength(1);
    expect(forceUpdatedResult[0]).toBe("force-update");
    expect(getNewValueMock).toHaveBeenCalledTimes(1);
  },
  {
    timeout: 100000,
  },
);
