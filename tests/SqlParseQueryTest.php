<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use Nddcoder\SqlToMongodbQuery\Exceptions\InvalidSelectStatementException;
use Nddcoder\SqlToMongodbQuery\Exceptions\InvalidSqlQueryException;
use Nddcoder\SqlToMongodbQuery\Exceptions\NotSupportStatementException;
use Nddcoder\SqlToMongodbQuery\SqlToMongodbQuery;

beforeEach(function () {
    $this->parser = new SqlToMongodbQuery();
});

it('should return options', function () {
    expect(
        $this->parser->parse(
            'SELECT id, name FROM users use index index_name where id = 1 order by created_at desc limit 20'
        )->getOptions()
    )
        ->toEqual(
            [
                'skip'       => null,
                'limit'      => 20,
                'hint'       => 'index_name',
                'projection' => [
                    'id'   => 1,
                    'name' => 1,
                ],
                'sort'       => [
                    'created_at' => -1
                ]
            ]
        );
});

it('should throw exception for non statement', function () {
    $this->parser->parse('random sql query');
})->throws(InvalidSqlQueryException::class);

it('should throw exception when invalid select statement', function () {
    $this->parser->parse('SELECT FROM users');
})->throws(InvalidSelectStatementException::class);

it('should throw exception when use having without group by', function () {
    $this->parser->parse('SELECT * FROM users having user_id = 1');
})->throws(InvalidSelectStatementException::class);

it('should throw exception for not support statement', function () {
    $this->parser->parse('DELETE FROM USERS where id = 1');
})->throws(NotSupportStatementException::class);

it('should return null for select statement without from', function () {
    expect($this->parser->parse('SELECT * FROM where id = 1'))->toBeNull();
});

it('should parse collection name', function () {
    expect($this->parser->parse("SELECT * FROM users")->collection)
        ->toBe('users');
});

it('should parse select field', function () {
    expect($this->parser->parse("SELECT * FROM users")->projection)
        ->toBeNull()
        ->and($this->parser->parse("SELECT id, name FROM users")->projection)
        ->toEqual(['id' => 1, 'name' => 1]);
});

it('should parse order', function () {
    expect($this->parser->parse("SELECT * FROM users order by created_at")->sort)
        ->toEqual(['created_at' => 1])
        ->and($this->parser->parse("SELECT * FROM users order by created_at asc, modified_at desc")->sort)
        ->toEqual(['created_at' => 1, 'modified_at' => -1])
        ->and($this->parser->parse("SELECT * FROM users order by created_at desc")->sort)
        ->toEqual(['created_at' => -1])
        ->and($this->parser->parse("SELECT * FROM users order by info.created_at desc")->sort)
        ->toEqual(['info.created_at' => -1]);
});

it('should parse limit', function () {
    expect($this->parser->parse("SELECT * FROM users"))
        ->limit->toEqual(0)
        ->skip->toEqual(0)
        ->and($this->parser->parse("SELECT * FROM users limit 10"))
        ->limit->toEqual(10)
        ->skip->toEqual(0)
        ->and($this->parser->parse("SELECT * FROM users limit 20, 10"))
        ->limit->toEqual(10)
        ->skip->toEqual(20);
});

it('should parse hint', function () {
    expect($this->parser->parse("SELECT * FROM users")->hint)
        ->toBeNull()
        ->and($this->parser->parse("SELECT * FROM users use index index_name")->hint)
        ->toEqual('index_name');
});

it('should parse single where', function () {
    expect($this->parser->parse("SELECT * FROM users WHERE active = TRUE and banned = false")->filter)
        ->toEqual(['active' => true, 'banned' => false])
        ->and($this->parser->parse("SELECT * FROM users WHERE name = 'nddcoder'")->filter)
        ->toEqual(['name' => 'nddcoder'])
        ->and($this->parser->parse("SELECT * FROM users WHERE age > 12")->filter)
        ->toEqual(['age' => ['$gt' => 12]])
        ->and($this->parser->parse("SELECT * FROM users WHERE age >= 12")->filter)
        ->toEqual(['age' => ['$gte' => 12]])
        ->and($this->parser->parse("SELECT * FROM users WHERE amount < 50.2")->filter)
        ->toEqual(['amount' => ['$lt' => 50.2]])
        ->and($this->parser->parse("SELECT * FROM users WHERE amount <= 50.2")->filter)
        ->toEqual(['amount' => ['$lte' => 50.2]])
        ->and($this->parser->parse("SELECT * FROM users WHERE name <> 'nddcoder'")->filter)
        ->toEqual(['name' => ['$ne' => 'nddcoder']])
        ->and($this->parser->parse("SELECT * FROM users WHERE name != 'nddcoder'")->filter)
        ->toEqual(['name' => ['$ne' => 'nddcoder']])
        ->and($this->parser->parse("SELECT * FROM users WHERE name LIKE 'nddcoder'")->filter)
        ->toEqual(['name' => new Regex('nddcoder', 'i')])
        ->and($this->parser->parse("SELECT * FROM users WHERE name not LIKE 'nddcoder'")->filter)
        ->toEqual(['name' => ['$not' => new Regex('nddcoder', 'i')]])
        ->and($this->parser->parse("SELECT * FROM users WHERE role in (1, 'sdfsdf , sdfsdf', 3, TRUE)")->filter)
        ->toEqual(['role' => ['$in' => [1, 'sdfsdf , sdfsdf', 3, true]]])
        ->and($this->parser->parse("SELECT * FROM users WHERE role not in (1, 'sdfsdf , sdfsdf', 3.2, false)")->filter)
        ->toEqual(['role' => ['$nin' => [1, 'sdfsdf , sdfsdf', 3.2, false]]]);
});

it('should parse single where reverse', function () {
    expect($this->parser->parse("SELECT * FROM users WHERE 'nddcoder' = name")->filter)
        ->toEqual(['name' => 'nddcoder'])
        ->and($this->parser->parse("SELECT * FROM users WHERE 12 < age")->filter)
        ->toEqual(['age' => ['$gt' => 12]])
        ->and($this->parser->parse("SELECT * FROM users WHERE 12 <= age")->filter)
        ->toEqual(['age' => ['$gte' => 12]])
        ->and($this->parser->parse("SELECT * FROM users WHERE 50.2 > amount")->filter)
        ->toEqual(['amount' => ['$lt' => 50.2]])
        ->and($this->parser->parse("SELECT * FROM users WHERE 50.2 >= amount")->filter)
        ->toEqual(['amount' => ['$lte' => 50.2]])
        ->and($this->parser->parse("SELECT * FROM users WHERE 'nddcoder'<>name")->filter)
        ->toEqual(['name' => ['$ne' => 'nddcoder']])
        ->and($this->parser->parse("SELECT * FROM users WHERE 'nddcoder'!=name")->filter)
        ->toEqual(['name' => ['$ne' => 'nddcoder']])
        ->and($this->parser->parse("SELECT * FROM users WHERE 'nddcoder' LIKE name")->filter)
        ->toEqual(['name' => new Regex('nddcoder', 'i')]);
});

it('should parse inline function', function () {
    expect($this->parser->parse("SELECT * FROM users WHERE _id = ObjectId('5d3937af498831003e9f6f2a')")->filter)
        ->toEqual(['_id' => new ObjectId('5d3937af498831003e9f6f2a')])
        ->and($this->parser->parse("SELECT * FROM users WHERE _id in (Id('5d3937af498831003e9f6f2a'))")->filter)
        ->toEqual(['_id' => ['$in' => [new ObjectId('5d3937af498831003e9f6f2a')]]])
        ->and(
            $this->parser->parse("SELECT * FROM users WHERE _id not in (ObjectId('5d3937af498831003e9f6f2a'))")->filter
        )
        ->toEqual(['_id' => ['$nin' => [new ObjectId('5d3937af498831003e9f6f2a')]]])
        ->and($this->parser->parse("SELECT * FROM users WHERE created_at = date('2020-12-12')")->filter)
        ->toEqual(['created_at' => new UTCDateTime(date_create('2020-12-12'))])
        ->and(
            $this->parser->parse("SELECT * FROM users WHERE created_at >= date('2020-12-12T10:00:00.000+0700')")->filter
        )
        ->toEqual(['created_at' => ['$gte' => new UTCDateTime(date_create('2020-12-12T10:00:00.000+0700'))]]);
});

it('should parse multi inline function inside in condition', function () {
    expect(
        $this->parser->parse(
            "SELECT * FROM users WHERE _id in (Id('5d3937af498831003e9f6f2a'), ObjectId('5d3937af498831003e9f6f2b'), date('2020-12-12'))"
        )->filter
    )
        ->toEqual([
            '_id' => [
                '$in' => [
                    new ObjectId('5d3937af498831003e9f6f2a'),
                    new ObjectId('5d3937af498831003e9f6f2b'),
                    new UTCDateTime(date_create('2020-12-12'))
                ]
            ]
        ]);
});

it('should parse complex and condition', function () {
    expect($this->parser->parse("SELECT * FROM users WHERE name = 'dung' and email = 'dangdungcntt@gmail.com'")->filter)
        ->toEqual(['name' => 'dung', 'email' => 'dangdungcntt@gmail.com'])
        ->and(
            $this->parser->parse(
                "SELECT * FROM users WHERE name = 'dung' and email = 'dangdungcntt@gmail.com' and (phone like '^0983')"
            )->filter
        )
        ->toEqual(['name' => 'dung', 'email' => 'dangdungcntt@gmail.com', 'phone' => new Regex('^0983', 'i')]);
});

it('should parse complex or condition', function () {
    expect($this->parser->parse("SELECT * FROM users WHERE name = 'dung' or email = 'dangdungcntt@gmail.com'")->filter)
        ->toEqual(['$or' => [['name' => 'dung'], ['email' => 'dangdungcntt@gmail.com']]])
        ->and(
            $this->parser->parse(
                "SELECT * FROM users WHERE name = 'dung' or (email = 'dangdungcntt@gmail.com' or age > 12)"
            )->filter
        )
        ->toEqual(['$or' => [['name' => 'dung'], ['email' => 'dangdungcntt@gmail.com'], ['age' => ['$gt' => 12]]]])
        ->and(
            $this->parser->parse(
                "SELECT * FROM users WHERE name = 'dung' or (email = 'dangdungcntt@gmail.com' or (age > 12 or age < 6))"
            )->filter
        )
        ->toEqual([
            '$or' => [
                ['name' => 'dung'],
                ['email' => 'dangdungcntt@gmail.com'],
                ['age' => ['$gt' => 12]],
                ['age' => ['$lt' => 6]]
            ]
        ])
        ->and(
            $this->parser->parse(
                "SELECT * FROM users WHERE (name = 'dung' or email = 'dangdungcntt@gmail.com') or (age > 12 or age < 6))"
            )->filter
        )
        ->toEqual([
            '$or' => [
                ['name' => 'dung'],
                ['email' => 'dangdungcntt@gmail.com'],
                ['age' => ['$gt' => 12]],
                ['age' => ['$lt' => 6]]
            ]
        ]);
});

it('should parse complex and or condition', function () {
    expect(
        $this->parser->parse(
            "
SELECT * FROM users
where role = 'admin' or (username like 'admin$' and (created_at < date('2020-01-01') or email LIKE '@nddcoder.com$')) or ip in ('10.42.0.1', '192.168.0.1')"
        )->filter
    )
        ->toEqual([
            '$or' => [
                ['role' => 'admin'],
                [
                    'username' => new Regex('admin$', 'i'),
                    '$or'      => [
                        ['created_at' => ['$lt' => new UTCDateTime(date_create('2020-01-01'))]],
                        ['email' => new Regex('@nddcoder.com$', 'i')]
                    ]
                ],
                ['ip' => ['$in' => ['10.42.0.1', '192.168.0.1']]]
            ]
        ])
        ->and(
            $this->parser->parse(
                "
SELECT * FROM users
where id = 1 and name = 'dung' and email = 'dangdungcntt@gmail.com' and (date('2020-12-14') >= created_at or age > 20) and (date('2020-12-16') < created_at or 22 >= age) or ip not in ('192.168.1.1', '192.168.2.2')"
            )->filter
        )
        ->toEqual([
            '$or' => [
                [
                    '$and' => [
                        [
                            'id'    => 1,
                            'name'  => 'dung',
                            'email' => 'dangdungcntt@gmail.com',
                            '$or'   => [
                                ['created_at' => ['$lte' => new UTCDateTime(date_create('2020-12-14'))]],
                                ['age' => ['$gt' => 20]]
                            ]
                        ],
                        [
                            '$or' => [
                                ['created_at' => ['$gt' => new UTCDateTime(date_create('2020-12-16'))]],
                                ['age' => ['$lte' => 22]]
                            ]
                        ]
                    ]
                ],
                ['ip' => ['$nin' => ['192.168.1.1', '192.168.2.2']]]
            ]
        ])
        ->and(
            $this->parser->parse(
                "
SELECT * FROM users
where ((role = 'admin' or username like 'admin$') and (created_at < date('2020-01-01') or created_at >= date('2021-01-01'))) and ((email LIKE '@nddcoder.com$' or email like '^admin@') and (age < 16 or age > 20)) and active = true"
            )->filter
        )
        ->toEqual([
            '$and' => [
                [
                    '$or' => [
                        ['role' => 'admin'],
                        ['username' => new Regex('admin$', 'i')]
                    ],
                ],
                [
                    '$or' => [
                        ['created_at' => ['$lt' => new UTCDateTime(date_create('2020-01-01'))]],
                        ['created_at' => ['$gte' => new UTCDateTime(date_create('2021-01-01'))]],
                    ]
                ],
                [
                    '$or' => [
                        ['email' => new Regex('@nddcoder.com$', 'i')],
                        ['email' => new Regex('^admin@', 'i')],
                    ]
                ],
                [
                    '$or' => [
                        ['age' => ['$lt' => 16]],
                        ['age' => ['$gt' => 20]],
                    ]
                ],
                ['active' => true]
            ]
        ]);
});

it('should parse value contain special char', function () {
    expect(
        $this->parser->parse(
            "SELECT * FROM clicks WHERE user_agent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0'"
        )->filter
    )
        ->toEqual(['user_agent' => 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0'])
        ->and($this->parser->parse("SELECT * FROM users WHERE 'nddcoder (dung nguyen dang)' = name")->filter)
        ->toEqual(['name' => 'nddcoder (dung nguyen dang)']);
});

it('should parse value contain empty string', function () {
    expect($this->parser->parse("SELECT * FROM users WHERE name != '' and address = ''")->filter)
        ->toEqual(['name' => ['$ne' => ''], 'address' => ''])
        ->and($this->parser->parse("SELECT * FROM users WHERE name not in ('', 'dungnd', true)")->filter)
        ->toEqual(['name' => ['$nin' => ['', 'dungnd', true]]]);
});

it('should parse null condition', function () {
    expect($this->parser->parse("SELECT * FROM clicks WHERE user_agent != NULL")->filter)
        ->toEqual(['user_agent' => ['$ne' => null]])
        ->and($this->parser->parse("SELECT * FROM clicks WHERE user_agent = null")->filter)
        ->toEqual(['user_agent' => null])
        ->and($this->parser->parse("SELECT * FROM clicks WHERE user_agent in (null, 1, 'abc')")->filter)
        ->toEqual([
            'user_agent' => [
                '$in' => [
                    null,
                    1,
                    'abc'
                ]
            ]
        ]);
});

it('should parse nested condition', function () {
    expect($this->parser->parse("SELECT * FROM clicks WHERE device_info.device_type = 'smartphone'")->filter)
        ->toEqual(['device_info.device_type' => 'smartphone'])
        ->and($this->parser->parse("SELECT * FROM clicks WHERE device_info.device_type != NULL")->filter)
        ->toEqual(['device_info.device_type' => ['$ne' => null]]);
});

it('should parse identifier many times', function () {
    expect(
        $this->parser->parse(
            "SELECT * FROM clicks WHERE _id in (1, true, date('2020-12-12T10:00:00.000+0700'), null, '5d3937af498831003e9f6f2a', ObjectId('5d3937af498831003e9f6f2a'), Id('5d3937af498831003e9f6f2a')) and created_at >= date('2020-12-12T10:00:00.000+0700') and modified_at <= date('2020-12-12T10:00:00.000+0700')"
        )->filter
    )
        ->toEqual([
            '_id'         => [
                '$in' => [
                    1,
                    true,
                    new UTCDateTime(date_create('2020-12-12T10:00:00.000+0700')),
                    null,
                    '5d3937af498831003e9f6f2a',
                    new ObjectId('5d3937af498831003e9f6f2a'),
                    new ObjectId('5d3937af498831003e9f6f2a'),
                ],
            ],
            'created_at'  => [
                '$gte' => new UTCDateTime(date_create('2020-12-12T10:00:00.000+0700'))
            ],
            'modified_at' => [
                '$lte' => new UTCDateTime(date_create('2020-12-12T10:00:00.000+0700'))
            ]
        ]);
});

it('should parse string numeric where in', function () {
    $filter = $this->parser->parse(
        "SELECT * FROM clicks WHERE id in ('123', '456', 789, '',  'abc', true, date('2020-12-12T10:00:00.000+0700'), null, '5d3937af498831003e9f6f2a', ObjectId('5d3937af498831003e9f6f2a'), Id('5d3937af498831003e9f6f2a'))"
    )->filter;
    expect(gettype($filter['id']['$in'][0]) == 'string')
        ->toBeTrue()
        ->and(gettype($filter['id']['$in'][1]) == 'string')
        ->toBeTrue()
        ->and(gettype($filter['id']['$in'][2]) == 'integer')
        ->toBeTrue()
        ->and($filter)
        ->toEqual(
            [
                'id' => [
                    '$in' => [
                        '123',
                        '456',
                        789,
                        '',
                        'abc',
                        true,
                        new UTCDateTime(date_create('2020-12-12T10:00:00.000+0700')),
                        null,
                        '5d3937af498831003e9f6f2a',
                        new ObjectId('5d3937af498831003e9f6f2a'),
                        new ObjectId('5d3937af498831003e9f6f2a'),
                    ],
                ],
            ]
        );
});

it('should merge and condition', function () {
    expect(
        $this->parser->parse(
            "SELECT * FROM clicks WHERE (id >= 10 or id >= 1 and id < 6) and (count >= 5 and count <= 10)"
        )->filter
    )
        ->toEqual([
            '$or'   => [
                [
                    'id' => [
                        '$gte' => 10
                    ]
                ],
                [
                    'id' => [
                        '$gte' => 1,
                        '$lt'  => 6

                    ]
                ]
            ],
            'count' => [
                '$gte' => 5,
                '$lte' => 10
            ]
        ]);
});

it('support where is not', function () {
    expect($this->parser->parse("SELECT * FROM clicks WHERE created_at is not null")->filter)
        ->toEqual([
            'created_at' => [
                '$ne' => null
            ]
        ])
        ->and($this->parser->parse("SELECT * FROM clicks WHERE created_at is not date('2022-01-01')")->filter)
        ->toEqual([
            'created_at' => [
                '$ne' => new UTCDateTime(date_create('2022-01-01'))
            ]
        ]);
});

it('support where is', function () {
    expect($this->parser->parse("SELECT * FROM clicks WHERE created_at is null")->filter)
        ->toEqual([
            'created_at' => null
        ])
        ->and($this->parser->parse("SELECT * FROM clicks WHERE created_at is date('2022-01-01')")->filter)
        ->toEqual([
            'created_at' => new UTCDateTime(date_create('2022-01-01'))
        ]);
});

it('support where field name equal function name', function () {
    expect($this->parser->parse("SELECT * FROM clicks WHERE date >= date('2022-01-01')")->filter)
        ->toEqual([
            'date' => [
                '$gte' => new UTCDateTime(date_create('2022-01-01'))
            ]
        ])
        ->and($this->parser->parse("SELECT * FROM clicks WHERE date('2022-01-01') <= date")->filter)
        ->toEqual([
            'date' => [
                '$gte' => new UTCDateTime(date_create('2022-01-01'))
            ]
        ]);
});

it('support merge gte and lte condition', function () {
    expect(
        $this->parser->parse(
            "SELECT * FROM clicks WHERE created_at <= date('2023-01-01') and (status = 1 and created_at >= date('2022-01-01'))"
        )->filter
    )
        ->toEqual([
            'status'     => 1,
            'created_at' => [
                '$gte' => new UTCDateTime(date_create('2022-01-01')),
                '$lte' => new UTCDateTime(date_create('2023-01-01'))
            ]
        ])
        ->and(
            $this->parser->parse(
                "SELECT * FROM clicks WHERE status = 1 and created_at <= date('2023-01-01') and created_at >= date('2022-01-01'))"
            )->filter
        )
        ->toEqual([
            'status'     => 1,
            'created_at' => [
                '$gte' => new UTCDateTime(date_create('2022-01-01')),
                '$lte' => new UTCDateTime(date_create('2023-01-01'))
            ]
        ]);
});
