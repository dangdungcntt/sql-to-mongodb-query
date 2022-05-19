<?php

namespace Nddcoder\SqlToMongodbQuery\Lib;

class PostfixConverter
{
    protected static function isOperator($char): bool
    {
        return in_array($char, ['+', '-', '*', '/', '%']);
    }

    protected static function isBracket($char): bool
    {
        return in_array($char, ['(', ')']);
    }

    protected static function getPriority(string $operator): int
    {
        if ($operator == "*" || $operator == "/" || $operator == "%") {
            return 2;
        }
        if ($operator == "+" || $operator == "-") {
            return 1;
        }
        return 0;
    }

    protected static function normalizeExpression(string $expression): string
    {
        $expression = preg_replace('/ /', '', $expression);
        $expression = preg_replace('/(\+|-|\*|\/|\)|\(|\))/', ' $1 ', $expression);
        $expression = preg_replace('!\s+!', ' ', $expression);
        return trim($expression);
    }

    protected static function convertExpressionToListToken(string $expression): array
    {
        return explode(' ', self::normalizeExpression($expression));
    }

    protected static function normalizeTokens(array $tokens): array
    {
        $stack = [];

        foreach ($tokens as $token) {
            if ($token == ')') {
                $foundOperator = false;
                $tmpArray      = [$token];
                while (!empty($stack)) {
                    $item       = array_pop($stack);
                    $tmpArray[] = $item;
                    if ($item == '(') {
                        break;
                    }
                    if (self::isOperator($item)) {
                        $foundOperator = true;
                    }
                }

                if (!$foundOperator) {
                    $previousItem = array_pop($stack);
                    if (self::isOperator($previousItem) || self::isBracket($previousItem)) {
                        array_shift($tmpArray);
                        array_pop($tmpArray);
                        $stack[] = $previousItem;
                    } else {
                        $tmpArray[] = $previousItem;
                    }
                    $tmpArray = join(array_reverse($tmpArray));
                    $stack[]  = $tmpArray;
                } else {
                    $stack = array_merge($stack, array_reverse($tmpArray));
                }
                continue;
            }

            $stack[] = $token;
        }

        return $stack;
    }

    public static function convert($expression): array
    {
        $tokens = self::convertExpressionToListToken($expression);
        $tokens = self::normalizeTokens($tokens);
        $stack  = [];
        $output = [];
        foreach ($tokens as $token) {
            if (self::isOperator($token)) {
                while (!empty($stack) && self::getPriority($token) <= self::getPriority($stack[0])) {
                    $output[] = array_shift($stack);
                }
                array_unshift($stack, $token);
                continue;
            }

            if ($token == '(') {
                array_unshift($stack, $token);
                continue;
            }

            if ($token == ')') {
                while (($item = array_shift($stack)) != '(') {
                    $output[] = $item;
                }
                continue;
            }

            $output[] = $token;
        }

        return array_merge($output, $stack);
    }
}
