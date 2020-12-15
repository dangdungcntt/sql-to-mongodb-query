<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use Nddcoder\SqlToMongodbQuery\Exceptions\InvalidSelectFieldException;
use Nddcoder\SqlToMongodbQuery\Model\Aggregate;
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
    public function it_should_return_options()
    {
        $this->assertEquals(
            [
                'hint' => 'index_name'
            ],
            $this->parse(
                'SELECT count(*), sum(time)
            FROM logs
            use index index_name'
            )->getOptions()
        );
    }

    /** @test */
    public function it_should_parse_group_by()
    {
        $aggregate = $this->parse(
            '
            SELECT user_id, count(*), sum(time)
            FROM logs
            use index index_name
            where created_at >= date("2020-12-12")
            group by user_id
            order by count(*) desc
            limit 20, 10
            having count(*) > 2 and sum(time) > 1000
        '
        );

        $this->assertEquals('logs', $aggregate->collection);
        $this->assertEquals('index_name', $aggregate->hint);
        $this->assertCount(7, $aggregate->pipelines);
        $this->assertEquals(
            [
                '$group' => [
                    '_id'       => [
                        'user_id' => '$user_id'
                    ],
                    'count(*)'  => [
                        '$sum' => 1
                    ],
                    'sum(time)' => [
                        '$sum' => '$time'
                    ],
                ]
            ],
            $aggregate->pipelines[1]
        );
        $this->assertEquals(
            [
                '$project' => [
                    'user_id'   => '$_id.user_id',
                    'count(*)'  => '$count(*)',
                    'sum(time)' => '$sum(time)',
                    '_id'       => 0
                ]
            ],
            $aggregate->pipelines[2]
        );
        $this->assertEquals(
            [
                '$match' => [
                    'count(*)'  => [
                        '$gt' => 2
                    ],
                    'sum(time)' => [
                        '$gt' => 1000
                    ]
                ]
            ],
            $aggregate->pipelines[3]
        );
    }

    /** @test */
    public function it_should_group_by_id_null()
    {
        $aggregate = $this->parse(
            '
            SELECT count(*)
            FROM logs
        '
        );

        $this->assertCount(3, $aggregate->pipelines);
        $this->assertEquals(
            [
                '$group' => [
                    '_id'      => null,
                    'count(*)' => [
                        '$sum' => 1
                    ]
                ]
            ],
            $aggregate->pipelines[1]
        );
    }

    /** @test */
    public function it_should_parse_group_by_with_empty_select_functions()
    {
        $aggregate = $this->parse(
            '
            SELECT user_id
            FROM logs
            where created_at >= date("2020-12-12")
            group by user_id
        '
        );

        $this->assertEquals('logs', $aggregate->collection);
        $this->assertCount(3, $aggregate->pipelines);
    }

    /** @test */
    public function it_should_throw_exception_for_invalid_select_field_when_group_by()
    {
        $this->expectException(InvalidSelectFieldException::class);
        $this->parse(
            '
            SELECT user_id, name
            FROM logs
            where created_at >= date("2020-12-12")
            group by user_id
        '
        );
    }
}
