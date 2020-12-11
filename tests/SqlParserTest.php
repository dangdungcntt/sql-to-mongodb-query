<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Regex;
use Nddcoder\SqlToMongodbQuery\SqlToMongodbQuery;

class SqlParserTest extends TestCase
{
    protected SqlToMongodbQuery $parser;

    public function __construct()
    {
        parent::__construct();
        $this->parser = new SqlToMongodbQuery();
    }

    /** @test */
    public function it_should_parse_collection_name()
    {
        $query = $this->parser->parse("SELECT * FROM users");

        $this->assertEquals('users', $query->collection);
    }

    /** @test */
    public function it_should_parse_select_field()
    {
        $query = $this->parser->parse("SELECT * FROM users");

        $this->assertNull($query->projection);

        $query1 = $this->parser->parse("SELECT id, name FROM users");

        $this->assertEquals(['id' => 1, 'name' => 1], $query1->projection);
    }

    /** @test */
    public function it_should_parse_order()
    {
        $query = $this->parser->parse("SELECT * FROM users order by created_at");

        $this->assertEquals(['created_at' => 1], $query->sort);

        $query1 = $this->parser->parse("SELECT * FROM users order by created_at desc");

        $this->assertEquals(['created_at' => -1], $query1->sort);
    }

    /** @test */
    public function it_should_parse_limit()
    {
        $query = $this->parser->parse("SELECT * FROM users");

        $this->assertEquals(0, $query->limit);
        $this->assertEquals(0, $query->skip);

        $query1 = $this->parser->parse("SELECT * FROM users limit 10");

        $this->assertEquals(10, $query1->limit);
        $this->assertEquals(0, $query1->skip);

        $query2 = $this->parser->parse("SELECT * FROM users limit 20, 10");

        $this->assertEquals(10, $query2->limit);
        $this->assertEquals(20, $query2->skip);
    }

    /** @test */
    public function it_should_parse_hint()
    {
        $query = $this->parser->parse("SELECT * FROM users");

        $this->assertNull($query->hint);

        $query1 = $this->parser->parse("SELECT * FROM users use index index_name");

        $this->assertEquals('index_name', $query1->hint);
    }

    /** @test */
    public function it_should_parse_single_where()
    {
        $this->assertEquals(
            ['name' => 'nddcoder'],
            $this->parser->parse("SELECT * FROM users WHERE name = 'nddcoder'")->filter
        );

        $this->assertEquals(
            ['age' => ['$gt' => 12]],
            $this->parser->parse("SELECT * FROM users WHERE age > 12")->filter
        );

        $this->assertEquals(
            ['age' => ['$gte' => 12]],
            $this->parser->parse("SELECT * FROM users WHERE age >= 12")->filter
        );

        $this->assertEquals(
            ['amount' => ['$lt' => 50.2]],
            $this->parser->parse("SELECT * FROM users WHERE amount < 50.2")->filter
        );

        $this->assertEquals(
            ['amount' => ['$lte' => 50.2]],
            $this->parser->parse("SELECT * FROM users WHERE amount <= 50.2")->filter
        );

        $this->assertEquals(
            ['name' => ['$ne' => 'nddcoder']],
            $this->parser->parse("SELECT * FROM users WHERE name <> 'nddcoder'")->filter
        );

        $this->assertEquals(
            ['name' => ['$ne' => 'nddcoder']],
            $this->parser->parse("SELECT * FROM users WHERE name != 'nddcoder'")->filter
        );

        $this->assertEquals(
            ['name' => new Regex('nddcoder', 'i')],
            $this->parser->parse("SELECT * FROM users WHERE name LIKE 'nddcoder'")->filter
        );

        $this->assertEquals(
            ['role' => ['$in' => [new ObjectId('5d3937af498831003e9f6f2a'), 'sdfsdf , sdfsdf', 3]]],
            $this->parser->parse("SELECT * FROM users WHERE role in (ObjectId('5d3937af498831003e9f6f2a'), 'sdfsdf , sdfsdf', 3)")->filter
        );

        $this->assertEquals(
            ['role' => ['$nin' => [new ObjectId('5d3937af498831003e9f6f2a'), 'sdfsdf , sdfsdf', 3.2]]],
            $this->parser->parse("SELECT * FROM users WHERE role not in (ObjectId('5d3937af498831003e9f6f2a'), 'sdfsdf , sdfsdf', 3.2)")->filter
        );
    }

    /** @test */
    public function it_should_parse_single_where_reverse()
    {
        $this->assertEquals(
            ['name' => 'nddcoder'],
            $this->parser->parse("SELECT * FROM users WHERE 'nddcoder' = name")->filter
        );

        $this->assertEquals(
            ['age' => ['$gt' => 12]],
            $this->parser->parse("SELECT * FROM users WHERE 12 < age")->filter
        );

        $this->assertEquals(
            ['age' => ['$gte' => 12]],
            $this->parser->parse("SELECT * FROM users WHERE 12 <= age")->filter
        );

        $this->assertEquals(
            ['amount' => ['$lt' => 50.2]],
            $this->parser->parse("SELECT * FROM users WHERE 50.2 > amount")->filter
        );

        $this->assertEquals(
            ['amount' => ['$lte' => 50.2]],
            $this->parser->parse("SELECT * FROM users WHERE 50.2 >= amount")->filter
        );

        $this->assertEquals(
            ['name' => ['$ne' => 'nddcoder']],
            $this->parser->parse("SELECT * FROM users WHERE 'nddcoder'<>name")->filter
        );

        $this->assertEquals(
            ['name' => ['$ne' => 'nddcoder']],
            $this->parser->parse("SELECT * FROM users WHERE 'nddcoder'!=name")->filter
        );

        $this->assertEquals(
            ['name' => new Regex('nddcoder', 'i')],
            $this->parser->parse("SELECT * FROM users WHERE 'nddcoder' LIKE name")->filter
        );
    }

//    /** @test */
    public function it_should_parse_where()
    {
//        dd($this->assertEquals(
//            ['age' => ['$gte' => 12]],
//            dd($this->parser->parse("SELECT * FROM users WHERE 12<=age and age>=13 and age<>25 and age!=25")->filter)
//        ));
//        dd($this->parser->parse("
//            SELECT * FROM users
//            where name='nddcoder'
//            order by created_at
//            desc limit 20,10")->filter);
//
//        dd(json_encode($this->parser->parse("
//                    SELECT * FROM users
//                    where _id = ObjectId('5d3937af498831003e9f6f2a') and name = 'dung' and email = 'dangdungcntt@gmail.com' and (date('2020-12-14') < created_at or age > 20) and (date('2020-12-16') < created_at or age > 22) or ip not in ('192.168.1.1', '192.168.2.2')
//                    order by created_at
//                    desc limit 20,10")->filter));
    }


}
