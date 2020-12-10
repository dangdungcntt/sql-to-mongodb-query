<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

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

        $this->assertEquals(null, $query->hint);

        $query1 = $this->parser->parse("SELECT * FROM users use index index_name");

        $this->assertEquals('index_name', $query1->hint);
    }

    /** @test */
    public function it_should_parse_where()
    {
//        dd(json_encode($this->parser->parse("
//            SELECT * FROM users
//            where name = 'dung' and email = 'email' or (name = 'hung' or email = 'hudng') or (email = 'dat')
//            order by created_at
//            desc limit 20,10")->filter));
//
//        dd(json_encode($this->parser->parse("
//                    SELECT * FROM users
//                    where _id = ObjectId('5d3937af498831003e9f6f2a') and name = 'dung' and email = 'dangdungcntt@gmail.com' and (date('2020-12-14') < created_at or age > 20) and (date('2020-12-16') < created_at or age > 22) or ip not in ('192.168.1.1', '192.168.2.2')
//                    order by created_at
//                    desc limit 20,10")->filter));
    }


}
