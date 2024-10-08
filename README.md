# dynamodb-lock

Utility to create and manage locks in DynamoDB.
There are times to share some values between multiple instances of a service. 
For example, when a token is issued by AWS Lambda functions, you might want to care about rate limits or costs issuing the token.

## Usage

```typescript
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { getLockedValue } from "dynamodb-lock";

const client = new DynamoDBClient({ region: "us-west-2" });

await getLockedValue(client, {
  tableName: "TestTable",
  hashKeyAttributeName: "key",
  hashKey: "token",
  now: new Date(),
  getNewValue: async () => fetch("https://example.com/token"),
});
```
