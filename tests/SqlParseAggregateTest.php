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
            SELECT user_id, count(*), sum(sum)
            FROM logs
            use index index_name
            where created_at >= date("2020-12-12")
            group by user_id
            order by count(*) desc
            limit 20, 10
            having count(*) > 2 and sum(sum) > 1000
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
                    'sum(sum)' => [
                        '$sum' => '$sum'
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
                    'sum(sum)' => '$sum(sum)',
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
                    'sum(sum)' => [
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

    /** @test */
    public function it_should_parse_select_functions()
    {
        $aggregate = $this->parse("SELECT avg(displays), max(clicks), min(ctr), sum(views) FROM clicks");
        $this->assertCount(3, $aggregate->pipelines);
        $this->assertEquals(
            ['$match' => (object) []],
            $aggregate->pipelines[0]
        );
        $this->assertEquals(
            [
                '$group' => [
                    "_id"           => null,
                    "avg(displays)" => [
                        '$avg' => '$displays'
                    ],
                    "max(clicks)"   => [
                        '$max' => '$clicks'
                    ],
                    "min(ctr)"      => [
                        '$min' => '$ctr'
                    ],
                    "sum(views)"    => [
                        '$sum' => '$views'
                    ]
                ]
            ]
            ,
            $aggregate->pipelines[1]
        );
        $this->assertEquals(
            [
                '$project' => [
                    "avg(displays)" => '$avg(displays)',
                    "max(clicks)"   => '$max(clicks)',
                    "min(ctr)"      => '$min(ctr)',
                    "sum(views)"    => '$sum(views)',
                    "_id"           => 0
                ]
            ],
            $aggregate->pipelines[2]
        );
    }

    /** @test */
    public function it_should_group_by_nested()
    {
        $aggregate = $this->parse("SELECT abc.device.device_info.device_type FROM clicks WHERE abc.device.device_info.device_type != NULL group by abc.device.device_info.device_type");
        $this->assertCount(3, $aggregate->pipelines);
        $this->assertEquals(
            [
                '$match' => [
                    'abc.device.device_info.device_type' => [
                        '$ne' => null
                    ]
                ]
            ],
            $aggregate->pipelines[0]
        );
        $this->assertEquals(
            [
                '$group' => [
                    '_id' => [
                        'abc'.SqlToMongodbQuery::SPECIAL_DOT_CHAR .'device'.SqlToMongodbQuery::SPECIAL_DOT_CHAR.'device_info'.SqlToMongodbQuery::SPECIAL_DOT_CHAR.'device_type' => '$abc.device.device_info.device_type'
                    ]
                ]
            ],
            $aggregate->pipelines[1]
        );
        $this->assertEquals(
            [
                '$project' => [
                    'abc.device.device_info.device_type' => '$_id.abc'.SqlToMongodbQuery::SPECIAL_DOT_CHAR .'device'.SqlToMongodbQuery::SPECIAL_DOT_CHAR.'device_info'.SqlToMongodbQuery::SPECIAL_DOT_CHAR.'device_type',
                    '_id'                     => 0
                ]
            ],
            $aggregate->pipelines[2]
        );
    }
}
