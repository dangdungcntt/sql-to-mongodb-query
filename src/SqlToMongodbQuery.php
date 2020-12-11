<?php

namespace Nddcoder\SqlToMongodbQuery;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use Nddcoder\SqlToMongodbQuery\Object\Query;
use PhpMyAdmin\SqlParser\Components\Condition;
use PhpMyAdmin\SqlParser\Parser;
use PhpMyAdmin\SqlParser\Statements\SelectStatement;

class SqlToMongodbQuery
{
    public function parse(string $sql): ?Query
    {
        $sqlParser = new Parser($sql);

        if (!isset($sqlParser->statements[0])) {
            return null;
        }

        $statement = $sqlParser->statements[0];

        return match (true) {
            $statement instanceof SelectStatement => $this->parseSelectStatement($statement),
            default => null
        };
    }

    protected function parseSelectStatement(SelectStatement $statement): ?Query
    {
        if (empty($statement->from[0])) {
            return null;
        }

        $filter = [];

        if (!empty($statement->where)) {
            $filter = $this->parseWhereConditions($statement->where);
        }

        $projection = null;

        if (!empty($statement->expr)) {
            $projection = [];
            foreach ($statement->expr as $expression) {
                if ($expression->column) {
                    $projection[$expression->column] = 1;
                }
            }
            if (empty($projection)) {
                $projection = null;
            }
        }

        $sort = null;

        if (!empty($statement->order)) {
            $sort = [];
            foreach ($statement->order as $orderKeyword) {
                $sort[$orderKeyword->expr->column] = $orderKeyword->type == 'DESC' ? -1 : 1;
            }
        }

        $limit = 0;
        $skip  = 0;
        if (!empty($statement->limit)) {
            $limit = $statement->limit->rowCount;
            $skip  = $statement->limit->offset;
        }

        $hint = null;

        if (!empty($statement->index_hints)) {
            $hintValue = $statement->index_hints[0]->indexes[0] ?? null;
            $hint      = $hintValue?->column ?? $hintValue;
        }

        return new Query(
            collection: $statement->from[0]->table,
            filter: $filter,
            projection: $projection,
            sort: $sort,
            limit: $limit,
            skip: $skip,
            hint: $hint
        );
    }

    /**
     * @param  Condition[]  $conditions
     * @return array
     */
    protected function parseWhereConditions(array $conditions): array
    {
        $filter = [];

        $nextToIndex = 0;

        for ($index = 0; $index < count($conditions); $index++) {
            $condition = $conditions[$index];

            if ($nextToIndex > 0 && $nextToIndex > $index) {
                continue;
            }

            $nextToIndex = 0;

            if ($condition->isOperator) {
                if ($condition->expr == 'OR') {
                    $subConditions = [];
                    $bracketsDiff  = 0;
                    for ($i = $index + 1; $i < count($conditions); $i++) {
                        $nextToIndex = $i;

                        if ($conditions[$i]->isOperator) {
                            if ($conditions[$i]->expr == 'OR' && $bracketsDiff == 0) {
                                break;
                            }
                        } else {
                            $bracketsDiff += $this->getBracketsDiff($conditions[$i]);
                        }

                        $subConditions[] = $conditions[$i];
                    }

                    if ($nextToIndex == count($conditions) - 1) {
                        $nextToIndex++;
                    }

                    $subFilter = $this->parseWhereConditions($subConditions);

                    if ($this->hasOnlyFilter($filter, '$or')) {
                        if ($this->hasOnlyFilter($subFilter, '$or')) {
                            $filter = [
                                '$or' => array_merge($filter['$or'], $subFilter['$or'])
                            ];
                            continue;
                        }
                        $filter['$or'][] = $subFilter;
                        continue;
                    }

                    if ($this->hasOnlyFilter($subFilter, '$or')) {
                        $filter = [
                            '$or' => [
                                $filter,
                                ...$subFilter['$or']
                            ]
                        ];
                        continue;
                    }

                    $filter = [
                        '$or' => [$filter, $subFilter]
                    ];
                }
                continue;
            }

            $bracketsDiff = $this->getBracketsDiff($condition);

            if ($bracketsDiff == 0) {
                $condition->expr = trim($condition->expr);
                if (str_starts_with($condition->expr, '(')) {
                    $condition->expr = trim($condition->expr, ' ()');
                }
                $filter = $this->mergeSubFilterForAndOperator(
                    $filter,
                    $this->convertOperator($condition->identifiers, $condition->expr)
                );
                continue;
            }

            $cloneCondition       = clone $condition;
            $cloneCondition->expr = substr($cloneCondition->expr, 1);
            $subConditions        = [
                $cloneCondition
            ];

            for ($i = $index + 1; $i < count($conditions); $i++) {
                $nextToIndex  = $i + 1;
                $bracketsDiff += $this->getBracketsDiff($conditions[$i]);
                if (!$conditions[$i]->isOperator && $bracketsDiff == 0) {
                    $clone           = clone $conditions[$i];
                    $clone->expr     = substr($clone->expr, 0, strlen($clone->expr) - 1);
                    $subConditions[] = $clone;
                    break;
                }
                $subConditions[] = $conditions[$i];
            }

            $subFilter = $this->parseWhereConditions($subConditions);

            $filter = $this->mergeSubFilterForAndOperator($filter, $subFilter);
        }

        return $filter;
    }

    protected function mergeSubFilterForAndOperator(array $filter, array $subFilter): array
    {
        if (empty($filter)) {
            return $subFilter;
        }

        if (isset($filter['$and'])) {
            if ($this->hasOnlyFilter($subFilter, '$and')) {
                $filter['$and'] = array_merge($filter['$and'], $subFilter['$and']);
            } else {
                $filter['$and'][] = $subFilter;
            }
            return $filter;
        }

        if (count(array_intersect_key($filter, $subFilter)) == 0) {
            return array_merge($filter, $subFilter);
        }

        return [
            '$and' => [
                $filter,
                $subFilter
            ]
        ];
    }

    protected function convertOperator(array $identifiers, string $expr): array
    {
        $array = explode(' ', $this->normalizeExpr($identifiers, $expr));

        $reverseOperator = false;
        $not             = false;

        if (str_contains($identifiers[0], ' ')) {
            $field           = trim(array_pop($array));
            $operator        = trim(array_pop($array));
            $reverseOperator = true;
        } else {
            $field    = trim(array_shift($array));
            $operator = trim(array_shift($array));
        }

        $operator = strtolower($operator);

        if ($operator == 'not') {
            $not      = true;
            $operator = strtolower(array_shift($array));
        }

        $value = join(' ', $array);

        if (is_numeric($field) || $this->isStringValue($field) || $this->isInlineFunction($field)) {
            $tmp             = $value;
            $value           = $field;
            $field           = $tmp;
            $reverseOperator = true;
        }

        $identifiers = array_values(array_filter($identifiers, fn($string) => $string != $field));

        switch (true) {
            case $this->isStringValue($value):
                $value = $identifiers[0];
                break;
            case in_array(strtolower($value), ['true', 'false']):
                $value = $value === 'true';
                break;
            case is_numeric($value):
                settype($value, str_contains($value, '.') ? 'float' : 'int');
                break;
            case !in_array($operator, ['in', 'not']):
                $value = $this->convertInlineFunction($value, $identifiers);
                break;
            default:
                break;
        }

        return match ($operator) {
            '<' => [$field => [($reverseOperator ? '$gt' : '$lt') => $value]],
            '<=' => [$field => [($reverseOperator ? '$gte' : '$lte') => $value]],
            '>' => [$field => [($reverseOperator ? '$lt' : '$gt') => $value]],
            '>=' => [$field => [($reverseOperator ? '$lte' : '$gte') => $value]],
            '<>', '!=' => [$field => ['$ne' => $value]],
            '=' => [$field => $value],
            'like' => [$field => $not ? ['$not' => new Regex($value, 'i')] : new Regex($value, 'i')],
            'in' => [$field => [($not ? '$nin' : '$in') => $this->parseValueForInQuery($value, $identifiers)]],
            default => []
        };
    }

    protected function parseValueForInQuery($value, $identifiers): array
    {
        $value = trim($value, '() ');

        $replaces = [];

        foreach ($identifiers as $index => $identifier) {
            $key            = "__tmp_identifier_{$index}";
            $value          = str_replace_first($identifier, $key, $value);
            $replaces[$key] = $identifier;
        }

        return array_map(
            function ($item) use ($replaces) {
                $item           = trim($item);
                $subIdentifiers = [];
                foreach ($replaces as $key => $identifier) {
                    if (str_contains($item, $key)) {
                        $subIdentifiers[] = $identifier;
                        if ($this->isStringValue($item)) {
                            $item = $identifier;
                        } else {
                            $item = str_replace_first($key, $identifier, $item);
                        }
                    }
                }

                if (is_numeric($item)) {
                    settype($item, str_contains((string)$item, '.') ? 'float' : 'int');
                }

                return $this->convertInlineFunction($item, $subIdentifiers);
            },
            explode(',', $value)
        );
    }

    protected function isStringValue(string $value): bool
    {
        return str_starts_with($value, '"') || str_starts_with($value, '\'');
    }

    protected function isInlineFunction(string $value): bool
    {
        return !$this->isStringValue($value) && (str_contains($value, '"') || str_contains($value, '\''));
    }

    protected function convertInlineFunction(mixed $value, array $identifiers): mixed
    {
        if (empty($identifiers)) {
            return $value;
        }

        return match ($identifiers[0]) {
            'date' => new UTCDateTime(date_create($identifiers[1])),
            'ObjectId' => new ObjectId($identifiers[1]),
            default => $value
        };
    }

    protected function getBracketsDiff(Condition $condition): int
    {
        $bracketOpen  = 0;
        $bracketClose = 0;

        foreach ($condition->identifiers as $identifier) {
            $bracketOpen  += substr_count($identifier, '(');
            $bracketClose += substr_count($identifier, ')');
        }

        return (substr_count($condition->expr, '(') - $bracketOpen)
            - (substr_count($condition->expr, ')') - $bracketClose);
    }

    protected function hasOnlyFilter(
        array $filter,
        $filterKey
    ): bool {
        return count($filter) == 1 && isset($filter[$filterKey]);
    }

    protected function normalizeExpr($identifiers, string $expr): string
    {
        $replaces = [];

        foreach ($identifiers as $index => $identifier) {
            $key            = "__tmp_identifier_{$index}";
            $expr           = str_replace_first($identifier, $key, $expr);
            $replaces[$key] = $identifier;
        }

        $expr = $this->replaceOperators($expr, ['<', '=', '>']);

        foreach (['<  =', '>  =', '! =', '<  >'] as $operator) {
            $expr = str_replace($operator, str_replace(' ', '', $operator), $expr);
        }

        $expr = $this->replaceOperators(
            $expr,
            [
                '<=',
                '>=',
                '<>',
                '!=',
                'like',
                'LIKE',
                'not in',
                'not IN',
                'NOT in',
                'NOT IN',
                'in',
                'IN',
            ]
        );

        $expr = preg_replace('!\s+!', ' ', $expr);

        return strtr($expr, $replaces);
    }

    protected function replaceOperators(string $string, array $operators): string
    {
        foreach ($operators as $operator) {
            if (str_contains($string, $operator)) {
                $string = str_replace($operator, " $operator ", $string);
            }
        }

        return $string;
    }
}
