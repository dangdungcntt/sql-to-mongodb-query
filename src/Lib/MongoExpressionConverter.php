<?php

namespace Nddcoder\SqlToMongodbQuery\Lib;

use Nddcoder\SqlToMongodbQuery\Lib\DataStruct\Stack;

class MongoExpressionConverter
{
    public const OPERATORS = [
        PostfixConverter::__CALL__ => '',
        '+'                        => '$add',
        '-'                        => '$subtract',
        '*'                        => '$multiply',
        '/'                        => '$divide',
        '%'                        => '$mod'
    ];

    protected static function isOperator($char): bool
    {
        return array_key_exists($char, self::OPERATORS);
    }

    public static function convert($str)
    {
        $postfixTokens = PostfixConverter::convert($str);

        $stack = new Stack();

        foreach ($postfixTokens as $token) {
            if (self::isOperator($token)) {
                $arg2 = $stack->pop();
                $arg1 = $stack->pop();

                if (PostfixConverter::__CALL__ == $token) {
                    $stack->push([
                        ('$'.$arg1) => $arg2
                    ]);
                    continue;
                }

                $stack->push([
                    (self::OPERATORS[$token]) => [
                        $arg1,
                        $arg2,
                    ]
                ]);
                continue;
            }
            $stack->push($token);
        }

        return $stack->pop();
    }
}
