<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use Nddcoder\SqlToMongodbQuery\Exceptions\InvalidSelectStatementException;
use Nddcoder\SqlToMongodbQuery\Exceptions\InvalidSqlQueryException;
use Nddcoder\SqlToMongodbQuery\Exceptions\NotSupportStatementException;
use Nddcoder\SqlToMongodbQuery\Model\FindQuery;
use Nddcoder\SqlToMongodbQuery\SqlToMongodbQuery;

class SqlParseQueryTest extends TestCase
{
    protected SqlToMongodbQuery $parser;

    public function setUp(): void
    {
        $this->parser = new SqlToMongodbQuery();
    }

    protected function parse(string $sql): ?FindQuery
    {
        return $this->parser->parse($sql);
    }

    /** @test */
    public function it_should_return_options()
    {
        $this->assertEquals(
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
            ],
            $this->parse(
                'SELECT id, name FROM users use index index_name where id = 1 order by created_at desc limit 20'
            )->getOptions()
        );
    }

    /** @test */
    public function it_should_throw_exception_for_non_statement()
    {
        $this->expectException(InvalidSqlQueryException::class);
        $this->assertNull($this->parse('random sql query'));
    }

    /** @test */
    public function it_should_throw_exception_when_invalid_select_statement()
    {
        $this->expectException(InvalidSelectStatementException::class);
        $this->parse('SELECT FROM users');
    }

    /** @test */
    public function it_should_throw_exception_when_use_having_without_group_by()
    {
        $this->expectException(InvalidSelectStatementException::class);
        $this->parse('SELECT * FROM users having user_id = 1');
    }

    /** @test */
    public function it_should_throw_exception_for_not_support_statement()
    {
        $this->expectException(NotSupportStatementException::class);
        $this->assertNull($this->parse('DELETE FROM USERS where id = 1'));
    }

    /** @test */
    public function it_should_return_null_for_select_statement_without_from()
    {
        $this->assertNull($this->parse('SELECT * FROM where id = 1'));
    }

    /** @test */
    public function it_should_parse_collection_name()
    {
        $query = $this->parse("SELECT * FROM users");

        $this->assertEquals('users', $query->collection);
    }

    /** @test */
    public function it_should_parse_select_field()
    {
        $query = $this->parse("SELECT * FROM users");

        $this->assertNull($query->projection);

        $query1 = $this->parse("SELECT id, name FROM users");

        $this->assertEquals(['id' => 1, 'name' => 1], $query1->projection);
    }

    /** @test */
    public function it_should_parse_order()
    {
        $this->assertEquals(['created_at' => 1], $this->parse("SELECT * FROM users order by created_at")->sort);
        $this->assertEquals(['created_at' => 1, 'modified_at' => -1],
            $this->parse("SELECT * FROM users order by created_at asc, modified_at desc")->sort);
        $this->assertEquals(['created_at' => -1], $this->parse("SELECT * FROM users order by created_at desc")->sort);
        $this->assertEquals(['info.created_at' => -1],
            $this->parse("SELECT * FROM users order by info.created_at desc")->sort);
    }

    /** @test */
    public function it_should_parse_limit()
    {
        $query = $this->parse("SELECT * FROM users");

        $this->assertEquals(0, $query->limit);
        $this->assertEquals(0, $query->skip);

        $query1 = $this->parse("SELECT * FROM users limit 10");

        $this->assertEquals(10, $query1->limit);
        $this->assertEquals(0, $query1->skip);

        $query2 = $this->parse("SELECT * FROM users limit 20, 10");

        $this->assertEquals(10, $query2->limit);
        $this->assertEquals(20, $query2->skip);
    }

    /** @test */
    public function it_should_parse_hint()
    {
        $query = $this->parse("SELECT * FROM users");

        $this->assertNull($query->hint);

        $query1 = $this->parse("SELECT * FROM users use index index_name");

        $this->assertEquals('index_name', $query1->hint);
    }

    /** @test */
    public function it_should_parse_single_where()
    {
        $this->assertEquals(
            ['active' => true, 'banned' => false],
            $this->parse("SELECT * FROM users WHERE active = TRUE and banned = false")->filter
        );

        $this->assertEquals(
            ['name' => 'nddcoder'],
            $this->parse("SELECT * FROM users WHERE name = 'nddcoder'")->filter
        );

        $this->assertEquals(
            ['age' => ['$gt' => 12]],
            $this->parse("SELECT * FROM users WHERE age > 12")->filter
        );

        $this->assertEquals(
            ['age' => ['$gte' => 12]],
            $this->parse("SELECT * FROM users WHERE age >= 12")->filter
        );

        $this->assertEquals(
            ['amount' => ['$lt' => 50.2]],
            $this->parse("SELECT * FROM users WHERE amount < 50.2")->filter
        );

        $this->assertEquals(
            ['amount' => ['$lte' => 50.2]],
            $this->parse("SELECT * FROM users WHERE amount <= 50.2")->filter
        );

        $this->assertEquals(
            ['name' => ['$ne' => 'nddcoder']],
            $this->parse("SELECT * FROM users WHERE name <> 'nddcoder'")->filter
        );

        $this->assertEquals(
            ['name' => ['$ne' => 'nddcoder']],
            $this->parse("SELECT * FROM users WHERE name != 'nddcoder'")->filter
        );

        $this->assertEquals(
            ['name' => new Regex('nddcoder', 'i')],
            $this->parse("SELECT * FROM users WHERE name LIKE 'nddcoder'")->filter
        );

        $this->assertEquals(
            ['name' => ['$not' => new Regex('nddcoder', 'i')]],
            $this->parse("SELECT * FROM users WHERE name not LIKE 'nddcoder'")->filter
        );

        $this->assertEquals(
            ['role' => ['$in' => [1, 'sdfsdf , sdfsdf', 3, true]]],
            $this->parse("SELECT * FROM users WHERE role in (1, 'sdfsdf , sdfsdf', 3, TRUE)")->filter
        );

        $this->assertEquals(
            ['role' => ['$nin' => [1, 'sdfsdf , sdfsdf', 3.2, false]]],
            $this->parse("SELECT * FROM users WHERE role not in (1, 'sdfsdf , sdfsdf', 3.2, false)")->filter
        );
    }

    /** @test */
    public function it_should_parse_single_where_reverse()
    {
        $this->assertEquals(
            ['name' => 'nddcoder'],
            $this->parse("SELECT * FROM users WHERE 'nddcoder' = name")->filter
        );

        $this->assertEquals(
            ['age' => ['$gt' => 12]],
            $this->parse("SELECT * FROM users WHERE 12 < age")->filter
        );

        $this->assertEquals(
            ['age' => ['$gte' => 12]],
            $this->parse("SELECT * FROM users WHERE 12 <= age")->filter
        );

        $this->assertEquals(
            ['amount' => ['$lt' => 50.2]],
            $this->parse("SELECT * FROM users WHERE 50.2 > amount")->filter
        );

        $this->assertEquals(
            ['amount' => ['$lte' => 50.2]],
            $this->parse("SELECT * FROM users WHERE 50.2 >= amount")->filter
        );

        $this->assertEquals(
            ['name' => ['$ne' => 'nddcoder']],
            $this->parse("SELECT * FROM users WHERE 'nddcoder'<>name")->filter
        );

        $this->assertEquals(
            ['name' => ['$ne' => 'nddcoder']],
            $this->parse("SELECT * FROM users WHERE 'nddcoder'!=name")->filter
        );

        $this->assertEquals(
            ['name' => new Regex('nddcoder', 'i')],
            $this->parse("SELECT * FROM users WHERE 'nddcoder' LIKE name")->filter
        );
    }

    /** @test */
    public function it_should_parse_inline_function()
    {
        $this->assertEquals(
            ['_id' => new ObjectId('5d3937af498831003e9f6f2a')],
            $this->parse("SELECT * FROM users WHERE _id = ObjectId('5d3937af498831003e9f6f2a')")->filter
        );

        $this->assertEquals(
            ['_id' => ['$in' => [new ObjectId('5d3937af498831003e9f6f2a')]]],
            $this->parse("SELECT * FROM users WHERE _id in (Id('5d3937af498831003e9f6f2a'))")->filter
        );

        $this->assertEquals(
            ['_id' => ['$nin' => [new ObjectId('5d3937af498831003e9f6f2a')]]],
            $this->parse("SELECT * FROM users WHERE _id not in (ObjectId('5d3937af498831003e9f6f2a'))")->filter
        );

        $this->assertEquals(
            ['created_at' => new UTCDateTime(date_create('2020-12-12'))],
            $this->parse("SELECT * FROM users WHERE created_at = date('2020-12-12')")->filter
        );

        $this->assertEquals(
            ['created_at' => ['$gte' => new UTCDateTime(date_create('2020-12-12T10:00:00.000+0700'))]],
            $this->parse("SELECT * FROM users WHERE created_at >= date('2020-12-12T10:00:00.000+0700')")->filter
        );
    }

    /** @test */
    public function it_should_parse_multi_inline_function_inside_in_condition()
    {
        $this->assertEquals(
            [
                '_id' => [
                    '$in' => [
                        new ObjectId('5d3937af498831003e9f6f2a'), new ObjectId('5d3937af498831003e9f6f2b'),
                        new UTCDateTime(date_create('2020-12-12'))
                    ]
                ]
            ],
            $this->parse("SELECT * FROM users WHERE _id in (Id('5d3937af498831003e9f6f2a'), ObjectId('5d3937af498831003e9f6f2b'), date('2020-12-12'))")->filter
        );
    }

    /** @test */
    public function it_should_parse_complex_and_condition()
    {
        $this->assertEquals(
            ['name' => 'dung', 'email' => 'dangdungcntt@gmail.com'],
            $this->parse("SELECT * FROM users WHERE name = 'dung' and email = 'dangdungcntt@gmail.com'")->filter
        );

        $this->assertEquals(
            ['name' => 'dung', 'email' => 'dangdungcntt@gmail.com', 'phone' => new Regex('^0983', 'i')],
            $this->parse("SELECT * FROM users WHERE name = 'dung' and email = 'dangdungcntt@gmail.com' and (phone like '^0983')")->filter
        );
    }

    /** @test */
    public function it_should_parse_complex_or_condition()
    {
        $this->assertEquals(
            ['$or' => [['name' => 'dung'], ['email' => 'dangdungcntt@gmail.com']]],
            $this->parse("SELECT * FROM users WHERE name = 'dung' or email = 'dangdungcntt@gmail.com'")->filter
        );

        $this->assertEquals(
            ['$or' => [['name' => 'dung'], ['email' => 'dangdungcntt@gmail.com'], ['age' => ['$gt' => 12]]]],
            $this->parse("SELECT * FROM users WHERE name = 'dung' or (email = 'dangdungcntt@gmail.com' or age > 12)")->filter
        );

        $this->assertEquals(
            [
                '$or' => [
                    ['name' => 'dung'], ['email' => 'dangdungcntt@gmail.com'], ['age' => ['$gt' => 12]],
                    ['age' => ['$lt' => 6]]
                ]
            ],
            $this->parse("SELECT * FROM users WHERE name = 'dung' or (email = 'dangdungcntt@gmail.com' or (age > 12 or age < 6))")->filter
        );

        $this->assertEquals(
            [
                '$or' => [
                    ['name' => 'dung'], ['email' => 'dangdungcntt@gmail.com'], ['age' => ['$gt' => 12]],
                    ['age' => ['$lt' => 6]]
                ]
            ],
            $this->parse("SELECT * FROM users WHERE (name = 'dung' or email = 'dangdungcntt@gmail.com') or (age > 12 or age < 6))")->filter
        );
    }

    /** @test */
    public function it_should_parse_complex_and_or_condition()
    {
        $this->assertEquals(
            [
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
            ],
            $this->parse("
SELECT * FROM users
where role = 'admin' or (username like 'admin$' and (created_at < date('2020-01-01') or email LIKE '@nddcoder.com$')) or ip in ('10.42.0.1', '192.168.0.1')")->filter
        );

        $this->assertEquals(
            [
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
            ],
            $this->parse("
SELECT * FROM users
where id = 1 and name = 'dung' and email = 'dangdungcntt@gmail.com' and (date('2020-12-14') >= created_at or age > 20) and (date('2020-12-16') < created_at or 22 >= age) or ip not in ('192.168.1.1', '192.168.2.2')")->filter
        );

        $this->assertEquals(
            [
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
            ],
            $this->parse("
SELECT * FROM users
where ((role = 'admin' or username like 'admin$') and (created_at < date('2020-01-01') or created_at >= date('2021-01-01'))) and ((email LIKE '@nddcoder.com$' or email like '^admin@') and (age < 16 or age > 20)) and active = true")->filter
        );
    }

    /** @test */
    public function it_should_parse_value_contain_special_char()
    {
        $this->assertEquals(
            ['user_agent' => 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0'],
            $this->parse("SELECT * FROM clicks WHERE user_agent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0'")->filter
        );

        $this->assertEquals(
            ['name' => 'nddcoder (dung nguyen dang)'],
            $this->parse("SELECT * FROM users WHERE 'nddcoder (dung nguyen dang)' = name")->filter
        );
    }

    /** @test */
    public function it_should_parse_value_contain_empty_string()
    {
        $this->assertEquals(
            ['name' => ['$ne' => ''], 'address' => ''],
            $this->parse("SELECT * FROM users WHERE name != '' and address = ''")->filter
        );

        $this->assertEquals(
            ['name' => ['$nin' => ['', 'dungnd', true]]],
            $this->parse("SELECT * FROM users WHERE name not in ('', 'dungnd', true)")->filter
        );
    }

    /** @test */
    public function it_should_parse_null_condition()
    {
        $this->assertEquals(
            ['user_agent' => ['$ne' => null]],
            $this->parse("SELECT * FROM clicks WHERE user_agent != NULL")->filter
        );

        $this->assertEquals(
            ['user_agent' => null],
            $this->parse("SELECT * FROM clicks WHERE user_agent = null")->filter
        );

        $this->assertEquals(
            [
                'user_agent' => [
                    '$in' => [
                        null, 1, 'abc'
                    ]
                ]
            ],
            $this->parse("SELECT * FROM clicks WHERE user_agent in (null, 1, 'abc')")->filter
        );
    }

    /** @test */
    public function it_should_parse_nested_condition()
    {
        $this->assertEquals(
            ['device_info.device_type' => 'smartphone'],
            $this->parse("SELECT * FROM clicks WHERE device_info.device_type = 'smartphone'")->filter
        );

        $this->assertEquals(
            ['device_info.device_type' => ['$ne' => null]],
            $this->parse("SELECT * FROM clicks WHERE device_info.device_type != NULL")->filter
        );
    }

    /** @test */
    public function it_should_parse_identifier_many_times()
    {
        $this->assertEquals(
            [
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
            ],
            $this->parse("SELECT * FROM clicks WHERE _id in (1, true, date('2020-12-12T10:00:00.000+0700'), null, '5d3937af498831003e9f6f2a', ObjectId('5d3937af498831003e9f6f2a'), Id('5d3937af498831003e9f6f2a')) and created_at >= date('2020-12-12T10:00:00.000+0700') and modified_at <= date('2020-12-12T10:00:00.000+0700')")->filter
        );
    }

    /** @test */
    public function it_should_parse_string_numeric_where_in()
    {
        $filter = $this->parse("SELECT * FROM clicks WHERE id in ('123', '456', 789, '',  'abc', true, date('2020-12-12T10:00:00.000+0700'), null, '5d3937af498831003e9f6f2a', ObjectId('5d3937af498831003e9f6f2a'), Id('5d3937af498831003e9f6f2a'))")->filter;
        $this->assertTrue(gettype($filter['id']['$in'][0]) == 'string');
        $this->assertTrue(gettype($filter['id']['$in'][1]) == 'string');
        $this->assertTrue(gettype($filter['id']['$in'][2]) == 'integer');
        $this->assertEquals(
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
            ],
            $filter
        );
    }

    /** @test */
    public function it_should_merge_and_condition()
    {
        $this->assertEquals(
            [
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
            ],
            $this->parse("SELECT * FROM clicks WHERE (id >= 10 or id >= 1 and id < 6) and (count >= 5 and count <= 10)")->filter
        );
    }
}
