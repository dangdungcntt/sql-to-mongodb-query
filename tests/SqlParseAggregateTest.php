<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use Nddcoder\SqlToMongodbQuery\Exceptions\InvalidSelectFieldException;
use Nddcoder\SqlToMongodbQuery\Model\Aggregate;
use Nddcoder\SqlToMongodbQuery\Model\FindQuery;
use Nddcoder\SqlToMongodbQuery\SqlToMongodbQuery;

class SqlParseAggregateTest extends TestCase
{
    protected SqlToMongodbQuery $parser;

    public function __construct()
    {
        parent::__construct();
        $this->parser = new SqlToMongodbQuery();
    }

    protected function parse(string $sql): ?Aggregate
    {
        return $this->parser->parse($sql);
    }

    /** @test */
    public function it_should_parse_group_by()
    {
        $aggregate = $this->parse('
            SELECT user_id, count(*), sum(time)
            FROM logs
            use index index_name
            where created_at >= date("2020-12-12")
            group by user_id
            order by count(*) desc
            limit 20, 10
            having count(*) > 2 and sum(time) > 1000
        ');

        $this->assertEquals('logs', $aggregate->collection);
        $this->assertEquals('index_name', $aggregate->hint);
        $this->assertCount(7, $aggregate->pipelines);
    }

    /** @test */
    public function it_should_parse_group_by_with_empty_select_functions()
    {
        $aggregate = $this->parse('
            SELECT user_id
            FROM logs
            where created_at >= date("2020-12-12")
            group by user_id
        ');

        $this->assertEquals('logs', $aggregate->collection);
        $this->assertCount(3, $aggregate->pipelines);
    }

    /** @test */
    public function it_should_throw_exception_for_invalid_select_field_when_group_by()
    {
        $this->expectException(InvalidSelectFieldException::class);
        $this->parse('
            SELECT user_id, name
            FROM logs
            where created_at >= date("2020-12-12")
            group by user_id
        ');
    }
}
