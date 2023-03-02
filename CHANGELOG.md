# Changelog

All notable changes to `sql-to-mongodb-query` will be documented in this file

## 1.2.0 - 2023-03-02

- Support merge condition on same field.

## 1.1.3 - 2022-06-16

- Support group by, select field with quote, double quote or backtick : ```SELECT `key` FROM logs group by 'key', "key1"```

## 1.1.1 - 2022-06-16

- Fix empty step `$project` when parse query like this: `SELECT * FROM users group by country_code`. With this release, no `$project` will be added.

## 1.1.0 - 2022-06-07

- Support select complex expression

```
SELECT date, sum(cost) / sum(clicks) as avg_cpc
FROM  reports
```

- Support `count(field)`: count number of records with this `field` is not null
- Support where `is`, `is not`
- Support filter with field name equal function name: `where date >= date('...')`

## 1.0.0 - 2021-10-07

- initial release
