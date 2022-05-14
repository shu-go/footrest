footrest - A REST API server from Go sql.DB

# go get

```
go get github.com/shu-go/footrest
```


# Start from SQLite

## Generate a config file

```
footrest gen
```

The command generates footrest.config.


## Edit the config file

In this case, you use sqlite, test.db.

```json
{
  "Format": {
    "QueryOK": "{\"result\": [%]}",
    "ExecOK": "{\"result\": %}",
    "Error": "{\"error\": %}"
  },
  "Params": {
    "Select": "select",
    "Where": "where",
    "Order": "order",
    "Rows": "rows",
    "Page": "page"
  },
  "Timeout": 5000,         <-- ms, you should edit
  "Addr": ":12345",        <-- host:port, you should edit
  "Root": "/",             <-- you should edit
  "DBType": "sqlite",      <-- driverName in sql.Open, you MUST edit
  "Connection": "test.db", <-- dataSourceName in sql.Open, you MUST edit
  "ShiftJIS": false,       <-- you should edit
  "Debug": false
}
```


## Prepare test.db

Create test.db with your favarite tool.

You will use a table `"table1" ("ID" INTEGER, "Text1" TEXT)` with some records in it.


## Run

```
footrest
```


## URI

`http://{config.addr}{config.root}/{object_in_the_rdbms}`


## REST (GET)

Now, go to `http://localhost:12345/table1`.

All records in the table `table1` are output as JSON form.


## REST (GET with query params)

GET requests accepts some parameters.

The column conditions form is `{column_name}={operator}{arg}`.

### `ID > 1`

`http://localhost:12345/table1?id=>1`

* operator: >
* arg: 1


### `ID >= 1`

`http://localhost:12345/table1?id=>=1`

* operator: >=
* arg: 1


### `ID != 1`

`http://localhost:12345/table1?id=!1`

* operator: !
* arg: 1


### `Text1 like aaa%`

`http://localhost:12345/table1?text1=%25aaa%25`

* operator: %25
* arg: aaa%25


### `ID = 1`

`http://localhost:12345/table1?id=1`

* operator: = if omitted
* arg: 1


## REST (GET with special `where` query param)

You use S-expr to describe conditions.

### `(ID >= 2) AND (Text1 LIKE 'aaa%')`

`http://localhost:12345/table1?where=(and (>= .id 2) (like .text1 aaa%25))`

Refer to souce file `dialect.go` to see what operators are defined.

NOTE: COLUMN NAME IS DESCRIBED AS `.{COLUMN_NAME}`.


## REST (GET with special `order` query param)

Pass `order` a comma separated list.

### `ORDER BY Text1, ID DESC`

`http://localhost:12345/table1?order=text1,-id`


## REST (GET with special `select` query param)

Pass `select` a comma separated list.

### `SELECT ID`

`http://localhost:12345/table1?select=id`


## REST (GET paginated with special `rows` and `page` query params)

Both `rows` and `page` are required to paginated.

`http://localhost:12345/table1?rows=50&page=1`


## REST (POST)

No query params.

Pass JSON in a request body.


## REST (PUT)

Qeury params:

* column conditions
* special `where` query param

Pass JSON to update in a request body.


## REST (DELETE)

Qeury params:

* column conditions
* special `where` query param


# It is designed to be customized.

## Adding a supoorted RDBMS

* Copy a file `dialect/sqlite/sqlite.go` and customize.
* Edit a file `cmd/footrest/main.go`
  * Import the dialect and driver.


# Remarks

## Security

No security verifications.

Do not use this package for public or commercial purposes or in any other situation where security is required.


## DBMS

This package depends on `Rows.ColumnTypes()` returns appropriate result.
