## Create a new SP identity

In Azure CLI
`az ad sp create-for-rbac`

The output of this command should look like this
{
  "appId": "adfa4575-2584-4714-bdd1-ed5f1300ef36",
  "displayName": "azure-cli-2021-03-18-01-18-48",
  "name": "http://azure-cli-2021-03-18-01-18-48",
  "password": "",
  "tenant": "72f988bf-86f1-41af-91ab-2d7cd011db47"
}

## Create a new CosmosDB read/write role to SQL DB containers

Take the following permission configuration and store it to a json file
```
{
    "RoleName": "CosmosReadWriteRole",
    "Type": "CustomRole",
    "AssignableScopes": ["/"],
    "Permissions": [{
        "DataActions": [
            "Microsoft.DocumentDB/databaseAccounts/readMetadata",
            "Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/*",
            "Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/*"
        ]
    }]
}
```
Run the following command in the Azure CLI
`az cosmosdb sql role definition create --account-name "<Cosmos DB Account>" --resource-group "<Resource Group>" --body @<filename with rbac>`

The output of this command should look like this
```
{- Finished ..
  "assignableScopes": [
    "/subscriptions/e42acc2d-8462-4fb5-bf0d-d983c0017584/resourceGroups/identity/providers/Microsoft.DocumentDB/databaseAccounts/msi-auth-test"
  ],
  "id": "/subscriptions/e42acc2d-8462-4fb5-bf0d-d983c0017584/resourceGroups/identity/providers/Microsoft.DocumentDB/databaseAccounts/msi-auth-test/sqlRoleDefinitions/9cf8a95b-1830-4fb5-b89e-a4d0586c10f4",
  "name": "9cf8a95b-1830-4fb5-b89e-a4d0586c10f4",
  "permissions": [
    {
      "dataActions": [
        "Microsoft.DocumentDB/databaseAccounts/readMetadata",
        "Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/*",
        "Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/*"
      ],
      "notDataActions": []
    }
  ],
  "resourceGroup": "identity",
  "roleName": "CosmosReadWriteRole",
  "sqlRoleDefinitionGetResultsType": "CustomRole",
  "type": "Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions"
}
```

## Assign the role to the Service Principal
The last step is to assign this new Cosmos DB RBAC permission to our Service Principal
az cosmosdb sql role assignment create --account-name "msi-auth-test" --resource-group "identity"  --scope "/" --principal-id "41b6a0bc-5f0f-41a7-9dea-0c405c7e3f07" --role-definition-name "CosmosReadWriteRole" --role-assignment-id cb8ed2d7-2371-4e3c-bd31-6cc1560e84f9

> Note: the Principal Id should be the Service Principal **Object ID** - Yes, that's right

The output of the command should look like this:

```
{- Finished ..
  "id": "/subscriptions/e42acc2d-8462-4fb5-bf0d-d983c0017584/resourceGroups/identity/providers/Microsoft.DocumentDB/databaseAccounts/msi-auth-test/sqlRoleAssignments/cb8ed2d7-2371-4e3c-bd31-6cc1560e84f9",
  "name": "cb8ed2d7-2371-4e3c-bd31-6cc1560e84f9",
  "principalId": "41b6a0bc-5f0f-41a7-9dea-0c405c7e3f07",
  "resourceGroup": "identity",
  "roleDefinitionId": "/subscriptions/e42acc2d-8462-4fb5-bf0d-d983c0017584/resourceGroups/identity/providers/Microsoft.DocumentDB/databaseAccounts/msi-auth-test/sqlRoleDefinitions/9cf8a95b-1830-4fb5-b89e-a4d0586c10f4",
  "scope": "/subscriptions/e42acc2d-8462-4fb5-bf0d-d983c0017584/resourceGroups/identity/providers/Microsoft.DocumentDB/databaseAccounts/msi-auth-test",
  "type": "Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments"
}
```
Next, we need to update our code to use the Azure.Identity library for authentication

```
var credential = new DefaultAzureCredential();
var cosmosClient = new CosmosClient(config[connectionString], credential);
```
With these two lines, we can now connect to our Cosmos DB database and perform CRUD operations against our DB collections

> Note: This permission model only covers database operations that let you read and write data. It does not cover any kind of management operations, like creating containers or changing their throughput. To authenticate management operations with an AAD identity, use Azure RBAC instead.

## Referenced docs and blogs
- https://devblogs.microsoft.com/cosmosdb/role-based-access-control-preview/#no-more-primary-keys
- https://docs.microsoft.com/en-us/cli/azure/ext/cosmosdb-preview/cosmosdb/sql/role?view=azure-cli-latest
- https://docs.microsoft.com/en-us/dotnet/api/overview/azure/identity-readme
- https://github.com/Azure/azure-cli-extensions/tree/master/src/cosmosdb-preview
- https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-setup-rbac#permission-model