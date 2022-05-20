<?php

namespace Nddcoder\SqlToMongodbQuery\Lib;

class GroupByArithmeticConverter
{
    protected static function getKey0(array $array)
    {
        return array_keys($array)[0];
    }

    protected static function getValue0(array $array)
    {
        return array_values($array)[0];
    }

    protected static function isGroupByFunction($function): bool
    {
        return in_array($function, [
            '$sum',
            '$avg',
            '$min',
            '$max',
            '$count'
        ]);
    }

    public static function convert($expression, \Closure $onConvertGroupBy = null)
    {
        if (!is_array($expression)) {
            return $expression;
        }

        if (count($expression) == 1) {
            $key = strtolower(self::getKey0($expression));

            if (self::isGroupByFunction($key)) {
                $functionString = $key == '$count' ? 'count(*)' : '__tmp_expression_'.md5(json_encode($expression));

                if ($onConvertGroupBy) {
                    $onConvertGroupBy($functionString, $key == '$count' ? ['$sum' => 1] : $expression);
                }

                return $functionString;
            }

            return [
                $key => self::convert(self::getValue0($expression), $onConvertGroupBy)
            ];
        }

        $output = [];

        foreach ($expression as $arg) {
            $output[] = self::convert($arg, $onConvertGroupBy);
        }

        return $output;
    }
}
