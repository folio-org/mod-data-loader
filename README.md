# test-data-loader
RMB based module used to load test data

RMB based data loader. Currently supports loading binary Marc records into the mod-inventory-storage instance table.

### APIs 
Exposes four APIs
1. POST `/load/marc-rules` - uploads a [rules json](https://github.com/folio-org/test-data-loader/blob/master/ramls/rules.json) file to use when mapping marc fields to instance fields. The rules file is only stored in memory and will be associated with the tenant passed in the x-okapi-tenant header
2.  GET `/load/marc-rules`
3. POST `/load/marc-data?storageURL=http://localhost:8888` - posts the attached binary Marc file. This will convert the Marc records into instances and bulk load them into Postgres.
4. POST `/load/marc-data/test` - normalizes the attached binary Marc file (should be small) and returns json instance object as the response to the API. Can be used to check mappings from Marc to instances. The file attached should be kept small so that there are no memory issues for the client (up to 500 entries)

The RAML can be found here:
https://github.com/folio-org/test-data-loader/blob/master/ramls/loader.raml

Some notes:

 1. A tenant must be passed in the x-okapi-tenant header.
 2. A rules files must be set for that tenant
 3. The inventory-storage module must be available at the host / port indicated via the storageURL query parameter (this is checked before processing begins)

It is best to attach Marc files with the same amount of records as the batch size - this is not mandatory (default batch size is 50,000 records and can be changed via the `batchSize` query parameter)

[MarcEdit](http://marcedit.reeset.net/) can be used to split very large Marc records.

You can call the `/load/marc-data` API multiple times on different Marc files - this should improve loading performance (the amount of concurrent calls depends on the amount of hardware on the server)

A records position in the uploaded file will be present in the `X-Unprocessed` header for each Marc record that was not parsed correctly.

Control fields can be used to insert constant values into instance fields. For example, the below will insert the value Books into the instanceTypeId field if all conditions of this rule are met. Multiple rule may be declared.

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

#### Available functions

 - `char_select` - select a specific char (parameter) from the field and compare it to the indicated value (value) - ranges can be passed in as well (ex. 1-3). `LDR` indicates that the data from the leader field should be used for this condition and not the data of the field itself
 - `remove_ending_punc` remove punctuation at the end of the data field
 - `trim_period` if the last char in the field is a period it is removed
 - `trim` remove leading and trailing spaces from the data field

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
- `custom` - (for Marc data fields only, does not support control fields) - define a custom JavaScript function to run on the field's data (passed in as DATA to the JavaScript function as a binded variable)
Ex.
```
"target": "publication.dateOfPublication",
"rules": [
  {
    "conditions": [
      {
        "type": "custom",
        "value": "DATA.replace(/\\D/g,'');"
      }
    ]
  }
]
```

#### Multiple subfields

Indicating multiple subfields will concat the values of each subfield into the target instance field
```
"690": [
    {
      "subfield": [
        "a",
        "y",
        "5"
      ],
      "description": "local subjects",
      "target": "subjects"
    }
  ]
```

#### Grouping fields into an object

Normally, all mappings in a single field that refer to the same object type will be mapped to a single object. For example, the below will map two subfields found in 001 to two different fields in the same identifier object within the instance.

```
  "001": [
    {
      "target": "identifiers.identifierTypeId",
      "description": "Type for Control Number (001)",
      ....
    },
    {
      "target": "identifiers.value",
      "description": "Control Number (001)",
  ...
    }
  ],
```
However, sometimes there is a need to create multiple objects (for example, multiple identifier objects) from subfields within a single field. 
Consider the following Marc field:

`020    ##$a0877790019$qblack leather$z0877780116 :$c$14.00`

which should map to:
```
"identifiers": [
  { "value": "0877790019", "identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422"},
  { "value": "0877780116", "identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422"}
]
```

To achieve this, you can wrap multiple subfield definitions into an `entity` field. In the below, both subfields will be mapped to the same object (which would happen normally), however, any additional entries outside of the `"entity"` definitions will be mapped to a new object, hence allowing you to create multiple objects from a single Marc field.
```
"020": [
    {
      "entity": [
        {
        "target": "identifiers.identifierTypeId",
        "description": "Type for Control Number (020)",
        "subfield": ["a"],
        ...
        },
        {
          "target": "identifiers.value",
          "description": "Control Number (020)",
          "subfield": ["b"],
          ...
        }
      ]
    },
```    


**Note**:

Currently, if the database is down, or the tenant in the x-okapi-tenant does not exist, the API will return success but will do nothing. This is an issue in the RMB framework used by mod-inventory-storage (errors will be logged in the mod-inventory-storage log, but the message is not propagated at this time)


**Performance**

A single call to the API with a binary Marc file with 50,000 records should take approximately 40 seconds. You can run multiple API calls concurrently with different files to speed up loading. A 4-core server should support at least 4 concurrent calls (approximately 200,000 records within a minute).

Adding Javascript custom functions (while allowing maximum normalization flexibility) does slow down processing. Each call takes approximately 0.2 milliseconds, meaning, for example, attaching custom Javascript functions to 6 fields in a 50,000 record Marc file means 300,000 javascript calls at 0.2 milliseconds per call -> 60,000 milliseconds (60 seconds) overhead.


