# test-data-loader
RMB based module used to load test data

RMB based data loader. Currently supports loading binary marc records into the mod-inventory-storage instance table.

Exposes three APIs
1. POST `/load/marc-rules` - uploads a [rules json](https://github.com/folio-org/test-data-loader/blob/master/ramls/rules.json) file to use when mapping marc fields to instance fields. The rules file is only stored in memory and will be associated with the tenant passed in the x-okapi-tenant header
2.  GET `/load/marc-rules`
3. POST `/load/marc-data?storageURL=http://localhost:8888` - posts the attached binary marc file. This will convert the marc records into instances and bulk load them into postgres.

The RAML can be found here:
https://github.com/folio-org/test-data-loader/blob/master/ramls/loader.raml

Some notes:

 1. A tenant must be passed in the x-okapi-tenant header.
 2. A rules files must be set for that tenant
 3. The inventory-storage module must be available at the host / port indicated via the storageURL query parameter (this is checked before processing begins)

It is best to attach marc files with the same amount of records as the batch size - this is not mandatory (default batch size is 50,000 records and can be changed via the `batchSize` query parameter)

You can call the `/load/marc-data` API multiple times on different marc files - this should improve loading performance (the amount of concurrent calls depends on the amount of hardware on the server)

A records position in the uploaded file will be present in the `X-Unprocessed` header if the marc record was not parsed correctly.

Currently, if the database is down, or the tenant in the x-okapi-tenant does not exist, the api will return success but will do nothing. This is an issue in the RMB framework used by mod-inventory-storage (errors will be logged in the mod-inventory-storage log, but the message is not propogated at this time)

