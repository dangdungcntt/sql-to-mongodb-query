<?php

namespace Nddcoder\SqlToMongodbQuery\Lib;

use Nddcoder\SqlToMongodbQuery\Lib\DataStruct\Stack;

class PostfixConverter
{
    public const __CALL__ = '__call__';

    protected static function isOperator($char, $exclude = null): bool
    {
        if ($exclude == $char) {
            return false;
        }
        return in_array($char, ['+', '-', '*', '/', '%', self::__CALL__]);
    }

    protected static function isBracket($char): bool
    {
        return in_array($char, ['(', ')']);
    }

    protected static function getPriority(string $operator): int
    {
        if ($operator == self::__CALL__) {
            return 3;
        }

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
        $expression = preg_replace('/([+\-*\/%()])/', ' $1 ', $expression);
        $expression = preg_replace('!\s+!', ' ', $expression);
        return trim($expression);
    }

    protected static function convertExpressionToListToken(string $expression): array
    {
        return explode(' ', self::normalizeExpression($expression));
    }

    protected static function normalizeTokens(array $tokens): array
    {
        $stack = new Stack();

        foreach (array_values($tokens) as $index => $token) {
            if ($token == ')') {
                $foundOperator = false;
                $foundOperand  = false;
                $tmpArray      = [$token];
                while ($stack->isNotEmpty()) {
                    $item       = $stack->pop();
                    $tmpArray[] = $item;
                    if ($item == '(') {
                        break;
                    }
                    if (self::isOperator($item, self::__CALL__)) {
                        $foundOperator = true;
                    } else {
                        if ($item != self::__CALL__) {
                            $foundOperand = true;
                        }
                    }
                }

                if (!$foundOperator || !$foundOperand) {
                    $previousItem = $stack->top();
                    if ($previousItem == null || self::isOperator($previousItem,
                            self::__CALL__) || self::isBracket($previousItem)) {
                        array_shift($tmpArray);
                        array_pop($tmpArray);
                    } else {
                        $tmpArray[] = $stack->pop();
                    }

                    $joinedToken = join(array_reverse($tmpArray));
                    if (!str_starts_with($joinedToken, self::__CALL__)) {
                        $joinedToken = str_replace(self::__CALL__.'(', '(', $joinedToken);
                    }
                    $stack->push($joinedToken);
                } else {
                    $stack->pushAll(array_reverse($tmpArray));
                }
                continue;
            }

            if (!self::isOperator($token) && !self::isBracket($token)) {
                $nextToken = $tokens[$index + 1] ?? null;
                if ($nextToken == '(') {
                    $stack->push($token);
                    $stack->push(self::__CALL__);
                    continue;
                }
            }

            $stack->push($token);
        }

        $tokens = array_reverse($stack->getData());
        $stack->clear();

        $skipToIndex = 0;

        foreach (array_values($tokens) as $index => $token) {
            if ($index < $skipToIndex) {
                continue;
            }

            if (str_starts_with($token, self::__CALL__) && $token != self::__CALL__) {
                $token = str_replace_first(self::__CALL__, $tokens[$index + 1], $token);
                $stack->push($token);
                $skipToIndex = $index + 2;
                continue;
            }

            $stack->push($token);
        }

        return array_reverse($stack->getData());
    }

    public static function convert($expression): array
    {
        $tokens = self::convertExpressionToListToken($expression);
        $tokens = self::normalizeTokens($tokens);

        $stack  = new Stack();
        $output = [];
        foreach ($tokens as $token) {
            if (self::isOperator($token)) {
                while ($stack->isNotEmpty() && self::getPriority($token) <= self::getPriority($stack->top())) {
                    $output[] = $stack->pop();
                }
                $stack->push($token);
                continue;
            }

            if ($token == '(') {
                $stack->push($token);
                continue;
            }

            if ($token == ')') {
                while (($item = $stack->pop()) != '(') {
                    $output[] = $item;
                }
                continue;
            }

            $output[] = $token;
        }

        return array_merge($output, $stack->getData());
    }
}
