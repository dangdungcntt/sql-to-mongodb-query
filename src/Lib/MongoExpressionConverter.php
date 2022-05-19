<?php

namespace Nddcoder\SqlToMongodbQuery\Lib;

class MongoExpressionConverter
{
    public const OPERATORS = [
        '+' => '$add',
        '-' => '$subtract',
        '*' => '$multiple',
        '/' => '$divide',
        '%' => '$mod'
    ];

    protected static function isOperator($char): bool
    {
        return array_key_exists($char, self::OPERATORS);
    }

    public static function convert($str)
    {
        $postfixTokens = PostfixConverter::convert($str);
        $stack = [];

        foreach ($postfixTokens as $token) {
            if (self::isOperator($token)) {
                $arg2    = array_pop($stack);
                $arg1    = array_pop($stack);
                $stack[] = [
                    (self::OPERATORS[$token]) => [
                        $arg1,
                        $arg2,
                    ]
                ];
                continue;
            }
            $stack[] = $token;
        }

        return array_pop($stack);
    }
}
