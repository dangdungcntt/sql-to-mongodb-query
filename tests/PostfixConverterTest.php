<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use Nddcoder\SqlToMongodbQuery\Lib\PostfixConverter;

it('postfix convert simple expression', function () {
    $expression = '(a + (b - c)) * d / e % f';
    $result     = PostfixConverter::convert($expression);

    expect($result)
        ->toHaveCount(strlen(strtr($expression, [
            ' ' => '',
            '(' => '',
            ')' => ''
        ])))
        ->toEqual([
            'a', 'b', 'c', '-', '+', 'd', '*', 'e', '/', 'f', '%'
        ]);
});

it('postfix convert expression contains functions', function () {
    $expression = 'sum(cost) / (sum(impressions) / 1000 + sum(clicks)) * 100000';
    $result     = PostfixConverter::convert($expression);

    expect($result)
        ->toHaveCount(9)
        ->toEqual([
            'sum(cost)', 'sum(impressions)', '1000', '/', 'sum(clicks)', '+', '/', '100000', '*'
        ]);
});

it('postfix convert expression contains functions with expression inside', function () {
    $expression = '(sum(cost)) / (sum(cost)) * (sum(impressions + clicks) / 1000 + sum(displays)) * 100000';
    $result     = PostfixConverter::convert($expression);

    expect($result)
        ->toHaveCount(15)
        ->toEqual([
            'sum(cost)', 'sum(cost)', '/', 'sum', 'impressions', 'clicks', '+', '__call__', '1000', '/', 'sum(displays)', '+', '*', '100000', '*'
        ]);
});
