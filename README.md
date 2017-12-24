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

MarcEdit can be used to split very large Marc records.

You can call the `/load/marc-data` API multiple times on different marc files - this should improve loading performance (the amount of concurrent calls depends on the amount of hardware on the server)

A records position in the uploaded file will be present in the `X-Unprocessed` header for each marc record that was not parsed correctly.

Available functions:

1. Control fields can be used to insert constant values into instance fields. For example, the below will insert the value Books into the instanceTypeId field if all conditions of this rule are met. Multiple rule may be declared.

```json
      "rules": [
        {
          "conditions": [
            {
              "type": "char_select",
              "parameter": "0",
              "value": "7"
            },
            {
              "type": "char_select",
              "parameter": "1",
              "value": "8"
            },
            {
              "type": "char_select",
              "parameter": "0",
              "value": "0",
              "LDR": true
            }
          ],
          "value": "Books"
        }
      ]
```

Available functions:

`char_select` - select a specific char (parameter) from the field and compare it to the indicated value (value). `LDR` indicates that the data from the leader field should be used for this condition and not the data of the field itself
`remove_ending_punc` remove punctuation at the end of the data field
`trim` remove leading and trailing spaces from the data field

Example:
```
      "rules": [
        {
          "conditions": [
            {
              "type": "remove_ending_punc,trim"
            }
          ]
        }
      ]
```
Note that you can indicate the use of multiple functions using the comma delimiter. This is only possible for functions that do not receive parameters

Currently, if the database is down, or the tenant in the x-okapi-tenant does not exist, the api will return success but will do nothing. This is an issue in the RMB framework used by mod-inventory-storage (errors will be logged in the mod-inventory-storage log, but the message is not propogated at this time)
