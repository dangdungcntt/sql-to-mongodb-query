<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use Nddcoder\SqlToMongodbQuery\Exceptions\InvalidSelectFieldException;
use Nddcoder\SqlToMongodbQuery\SqlToMongodbQuery;


beforeEach(function () {
    $this->parser = new SqlToMongodbQuery();
});

it('should return options', function () {
    expect($this->parser->parse(
        'SELECT count(*), sum(time)
            FROM logs
            use index index_name')->getOptions()
    )
        ->toEqual(
            [
                'hint' => 'index_name'
            ]
        );
});

it('should parse group by', function () {
    $aggregate = $this->parser->parse(
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

    expect($aggregate)
        ->collection->toEqual('logs')
        ->hint->toEqual('index_name')
        ->pipelines->toHaveCount(7);

    expect($aggregate->pipelines[1])
        ->toEqual([
            '$group' => [
                '_id'      => [
                    'user_id' => '$user_id'
                ],
                'count(*)' => [
                    '$sum' => 1
                ],
                'sum(sum)' => [
                    '$sum' => '$sum'
                ],
            ]
        ]);

    expect($aggregate->pipelines[2])
        ->toEqual([
            '$project' => [
                'user_id'  => '$_id.user_id',
                'count(*)' => '$count(*)',
                'sum(sum)' => '$sum(sum)',
                '_id'      => 0
            ]
        ]);

    expect($aggregate->pipelines[3])
        ->toEqual([
            '$match' => [
                'count(*)' => [
                    '$gt' => 2
                ],
                'sum(sum)' => [
                    '$gt' => 1000
                ]
            ]
        ]);
});

it('should group by id null', function () {
    $aggregate = $this->parser->parse(
        '
            SELECT count(*)
            FROM logs
        '
    );

    expect($aggregate->pipelines)
        ->toHaveCount(3);

    expect($aggregate->pipelines[1])
        ->toEqual([
            '$group' => [
                '_id'      => null,
                'count(*)' => [
                    '$sum' => 1
                ]
            ]
        ]);
});

it('should parse group by with empty select functions', function () {
    $aggregate = $this->parser->parse(
        '
            SELECT user_id
            FROM logs
            where created_at >= date("2020-12-12")
            group by user_id
        '
    );

    expect($aggregate->collection)
        ->toEqual('logs');

    expect($aggregate->pipelines)
        ->toHaveCount(3);
});

it('should throw exception for invalid select field when group by', function () {
    $this->parser->parse(
        '
            SELECT user_id, name
            FROM logs
            where created_at >= date("2020-12-12")
            group by user_id
        '
    );
})->throws(InvalidSelectFieldException::class);

it('should parse select functions', function () {
    $aggregate = $this->parser->parse("SELECT avg(displays), max(clicks), min(ctr), sum(views) FROM clicks");
    expect($aggregate->pipelines)
        ->toHaveCount(3);
    expect($aggregate->pipelines[0])
        ->toEqual(['$match' => (object) []]);
    expect($aggregate->pipelines[1])
        ->toEqual([
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
        ]);
    expect($aggregate->pipelines[2])
        ->toEqual([
            '$project' => [
                "avg(displays)" => '$avg(displays)',
                "max(clicks)"   => '$max(clicks)',
                "min(ctr)"      => '$min(ctr)',
                "sum(views)"    => '$sum(views)',
                "_id"           => 0
            ]
        ]);
});

it('should group by nested', function () {
    $aggregate = $this->parser->parse("SELECT abc.device.device_info.device_type FROM clicks WHERE abc.device.device_info.device_type != NULL group by abc.device.device_info.device_type");
    expect($aggregate->pipelines)
        ->toHaveCount(3);

    expect($aggregate->pipelines[0])
        ->toEqual([
            '$match' => [
                'abc.device.device_info.device_type' => [
                    '$ne' => null
                ]
            ]
        ]);

    expect($aggregate->pipelines[1])
        ->toEqual([
            '$group' => [
                '_id' => [
                    'abc'.SqlToMongodbQuery::SPECIAL_DOT_CHAR.'device'.SqlToMongodbQuery::SPECIAL_DOT_CHAR.'device_info'.SqlToMongodbQuery::SPECIAL_DOT_CHAR.'device_type' => '$abc.device.device_info.device_type'
                ]
            ]
        ]);

    expect($aggregate->pipelines[2])
        ->toEqual([
            '$project' => [
                'abc.device.device_info.device_type' => '$_id.abc'.SqlToMongodbQuery::SPECIAL_DOT_CHAR.'device'.SqlToMongodbQuery::SPECIAL_DOT_CHAR.'device_info'.SqlToMongodbQuery::SPECIAL_DOT_CHAR.'device_type',
                '_id'                                => 0
            ]
        ]);
});

it('should parse expression in select fields', function () {
    $aggregate = $this->parser->parse("
        SELECT date, sum(cost) / (sum(impressions) / 1000 + sum(clicks)) * 100000 as est_rev, sum(clicks) as total_clicks, sum(impressions) as total_impressions
        FROM  reports
        where  date >= 220516
        group by date
    ");

    expect($aggregate->pipelines)
        ->toHaveCount(3);

    expect($aggregate->pipelines[0])
        ->toEqual([
            '$match' => [
                'date' => [
                    '$gte' => 220516
                ]
            ]
        ]);

    expect($aggregate->pipelines[1])
        ->toEqual([
            '$group' => [
                '_id'              => [
                    'date' => '$date'
                ],
                'sum(clicks)'      => [
                    '$sum' => '$clicks'
                ],
                'sum(impressions)' => [
                    '$sum' => '$impressions'
                ],
                'sum(cost)'        => [
                    '$sum' => '$cost'
                ],
            ]
        ]);

    expect($aggregate->pipelines[2])
        ->toEqual([
            '$project' => [
                'date'              => '$_id.date',
                'total_clicks'      => '$sum(clicks)',
                'total_impressions' => '$sum(impressions)',
                'est_rev'           => [
                    '$multiple' => [
                        [
                            '$divide' => [
                                '$sum(cost)',
                                [
                                    '$add' => [
                                        [
                                            '$divide' => [
                                                '$sum(impressions)',
                                                1000
                                            ]
                                        ],
                                        '$sum(clicks)'
                                    ]

                                ]
                            ]
                        ],
                        100000
                    ]
                ],
                '_id' => 0
            ]
        ]);
});
