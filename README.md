# sql-to-mongodb-query

[![Latest Version on Packagist](https://img.shields.io/packagist/v/nddcoder/sql-to-mongodb-query.svg?style=flat-square)](https://packagist.org/packages/nddcoder/sql-to-mongodb-query)
[![GitHub Tests Action Status](https://img.shields.io/github/actions/workflow/status/dangdungcntt/sql-to-mongodb-query/run-tests.yml?branch=master)](https://github.com/dangdungcntt/sql-to-mongodb-query/actions?query=workflow%3Atests+branch%3Amaster)
[![Total Downloads](https://img.shields.io/packagist/dt/nddcoder/sql-to-mongodb-query.svg?style=flat-square)](https://packagist.org/packages/nddcoder/sql-to-mongodb-query)

## Installation

You can install the package via composer:

```bash
composer require nddcoder/sql-to-mongodb-query
```

## Online Demo

[SQL To MongoDB Query](https://nddapp.com/sql-to-mongodb-query-converter.html)

## GUI phpMongoAdmin

[phpMongoAdmin](https://github.com/dangdungcntt/phpmongoadmin)

## Usage

Parse Find query

```php
$parser = new Nddcoder\SqlToMongodbQuery\SqlToMongodbQuery();
$query = $parser->parse("
    SELECT id, username, email, created_at 
    FROM users
    USE INDEX active_1_created_at_1
    WHERE active = true and created_at >= date('2021-01-01') 
    ORDER BY created_at desc 
    LIMIT 10, 20
");

/*
Nddcoder\SqlToMongodbQuery\Model\FindQuery {#473
  +filter: array:2 [
    "active" => true
    "created_at" => array:1 [
      "$gte" => MongoDB\BSON\UTCDateTime {#926
        +"milliseconds": "1609459200000"
      }
    ]
  ]
  +projection: array:4 [
    "id" => 1
    "username" => 1
    "email" => 1
    "created_at" => 1
  ]
  +sort: array:1 [
    "created_at" => -1
  ]
  +limit: 20
  +skip: 10
  +collection: "users"
  +hint: "active_1_created_at_1"
}

*/
```

Parse Aggregate query

```php
$parser = new Nddcoder\SqlToMongodbQuery\SqlToMongodbQuery();
$query = $parser->parse("
    SELECT date, count(*)
    FROM clicks
    USE INDEX status_1_created_at_1
    WHERE status = 1 and created_at >= date('2021-07-01') 
    GROUP BY date
    HAVING count(*) > 100
");

/*
Nddcoder\SqlToMongodbQuery\Model\Aggregate {#493
  +pipelines: array:4 [
    0 => array:1 [
      "$match" => array:2 [
        "status" => 1
        "created_at" => array:1 [
          "$gte" => MongoDB\BSON\UTCDateTime {#926
            +"milliseconds": "1625097600000"
          }
        ]
      ]
    ]
    1 => array:1 [
      "$group" => array:2 [
        "_id" => array:1 [
          "date" => "$date"
        ]
        "count(*)" => array:1 [
          "$sum" => 1
        ]
      ]
    ]
    2 => array:1 [
      "$project" => array:3 [
        "date" => "$_id.date"
        "count(*)" => "$count(*)"
        "_id" => 0
      ]
    ]
    3 => array:1 [
      "$match" => array:1 [
        "count(*)" => array:1 [
          "$gt" => 100
        ]
      ]
    ]
  ]
  +collection: "clicks"
  +hint: "status_1_created_at_1"
}
*/
```

## Testing

``` bash
composer test
```

## Changelog

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed recently.

## Contributing

Please see [CONTRIBUTING](.github/CONTRIBUTING.md) for details.

## Security Vulnerabilities

Please review [our security policy](../../security/policy) on how to report security vulnerabilities.

## Credits

- [Dung Nguyen Dang](https://github.com/dangdungcntt)
- [All Contributors](../../contributors)

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
