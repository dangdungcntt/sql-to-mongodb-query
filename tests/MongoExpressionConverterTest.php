<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use Nddcoder\SqlToMongodbQuery\Lib\MongoExpressionConverter;

it('mongo expression convert simple expression', function () {
    $expression = '(a + (b - c)) * d / e % f';
    $result     = MongoExpressionConverter::convert($expression);

    expect($result)
        ->toEqual([
            '$mod' => [
                [
                    '$divide' => [
                        [
                            '$multiply' => [
                                [
                                    '$add' => [
                                        'a',
                                        [
                                            '$subtract' => [
                                                'b',
                                                'c'
                                            ]
                                        ]
                                    ]
                                ],
                                'd'
                            ]
                        ],
                        'e'
                    ]
                ],
                'f'
            ]
        ]);
});

it('mongo expression convert expression contains functions', function () {
    $expression = 'sum(cost) / (sum(impressions) / 1000 + sum(clicks)) * 100000';
    $result     = MongoExpressionConverter::convert($expression);

    expect($result)
        ->toEqual([
            '$multiply' => [
                [
                    '$divide' => [
                        'sum(cost)',
                        [
                            '$add' => [
                                [
                                    '$divide' => [
                                        'sum(impressions)',
                                        '1000'
                                    ]
                                ],
                                'sum(clicks)'
                            ]
                        ]
                    ]
                ],
                '100000'
            ]
        ]);
});

it('mongo expression convert expression contains functions with expression inside', function () {
    $expression = 'sum(cost) / (sum(impressions + clicks) / 1000 + sum(displays)) * 100000';
    $result     = MongoExpressionConverter::convert($expression);

    expect($result)
        ->toEqual([
            '$multiply' => [
                [
                    '$divide' => [
                        'sum(cost)',
                        [
                            '$add' => [
                                [
                                    '$divide' => [
                                        [
                                            '$sum' => [
                                                '$add' => [
                                                    'impressions',
                                                    'clicks'
                                                ]
                                            ]
                                        ],
                                        '1000'
                                    ]
                                ],
                                'sum(displays)'
                            ]
                        ]
                    ]
                ],
                '100000'
            ]
        ]);
});
