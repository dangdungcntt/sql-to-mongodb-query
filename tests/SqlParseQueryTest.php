<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use Nddcoder\SqlToMongodbQuery\Object\FindQuery;
use Nddcoder\SqlToMongodbQuery\SqlToMongodbQuery;

class SqlParseQueryTest extends TestCase
{
    protected SqlToMongodbQuery $parser;

    public function __construct()
    {
        parent::__construct();
        $this->parser = new SqlToMongodbQuery();
    }

    protected function parse(string $sql): ?FindQuery
    {
        return $this->parser->parse($sql);
    }

    /** @test */
    public function it_should_return_null_for_non_statement()
    {
        $this->assertNull($this->parse('random sql query'));
    }

    /** @test */
    public function it_should_return_null_for_non_select_statement()
    {
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
        $query = $this->parse("SELECT * FROM users order by created_at");

        $this->assertEquals(['created_at' => 1], $query->sort);

        $query1 = $this->parse("SELECT * FROM users order by created_at desc");

        $this->assertEquals(['created_at' => -1], $query1->sort);
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
            $this->parse("SELECT * FROM users WHERE _id in (ObjectId('5d3937af498831003e9f6f2a'))")->filter
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
            ['$or' => [['name' => 'dung'], ['email' => 'dangdungcntt@gmail.com'], ['age' => ['$gt' => 12]], ['age' => ['$lt' => 6]]]],
            $this->parse("SELECT * FROM users WHERE name = 'dung' or (email = 'dangdungcntt@gmail.com' or (age > 12 or age < 6))")->filter
        );

        $this->assertEquals(
            ['$or' => [['name' => 'dung'], ['email' => 'dangdungcntt@gmail.com'], ['age' => ['$gt' => 12]], ['age' => ['$lt' => 6]]]],
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
                        '$or' => [
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
                                'id' => 1,
                                'name' => 'dung',
                                'email' => 'dangdungcntt@gmail.com',
                                '$or' => [
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
}
