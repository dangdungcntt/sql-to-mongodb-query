<?php

namespace Nddcoder\SqlToMongodbQuery;

use JetBrains\PhpStorm\Pure;
use MongoDB\BSON\ObjectId;
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
     * @param  Condition[]  $where
     * @return array
     */
    protected function parseWhereConditions(array $where): array
    {
        $filter = [];

        $nextToIndex = 0;

//        dd($where);

        foreach ($where as $index => $condition) {
            if ($nextToIndex > 0 && $nextToIndex > $index) {
                continue;
            }

            $nextToIndex = 0;

            if ($condition->isOperator) {
                if ($condition->expr == 'OR') {
                    $subWhere     = [];
                    $bracketsDiff = 0;
                    for ($i = $index + 1; $i < count($where); $i++) {
                        $nextToIndex = $i;

                        if ($where[$i]->isOperator) {
                            if ($where[$i]->expr == 'OR' && $bracketsDiff == 0) {
                                break;
                            }
                        } else {
                            $bracketsDiff += $this->getBracketsDiff($where[$i]->expr);
                        }

                        $subWhere[] = $where[$i];
                    }

                    if ($nextToIndex == count($where) - 1) {
                        $nextToIndex++;
                    }

                    $subFilter = $this->parseWhereConditions($subWhere);

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

                    $filter = [
                        '$or' => [$filter, $subFilter]
                    ];
                }
                continue;
            }

            $bracketsDiff = $this->getBracketsDiff($condition->expr);

            if ($bracketsDiff == 0) {
                $condition->expr = trim(trim($condition->expr, ' ()'));
                $filter          = array_merge(
                    $filter,
                    $this->convertOperator($condition->identifiers, $condition->expr)
                );
                continue;
            }

            $cloneCondition       = clone $condition;
            $cloneCondition->expr = substr($cloneCondition->expr, 1);
            $subWhere             = [
                $cloneCondition
            ];

            for ($i = $index + 1; $i < count($where); $i++) {
                $nextToIndex  = $i + 1;
                $bracketsDiff += $this->getBracketsDiff($where[$i]->expr);
                if (!$where[$i]->isOperator && $bracketsDiff == 0) {
                    $clone       = clone $where[$i];
                    $clone->expr = substr($clone->expr, 0, strlen($clone->expr) - 1);
                    $subWhere[]  = $clone;
                    break;
                }
                $subWhere[] = $where[$i];
            }

            $subFilter = $this->parseWhereConditions($subWhere);

            if (isset($filter['$and'])) {
                if ($this->hasOnlyFilter($subFilter, '$and')) {
                    $filter['$and'] = array_merge($filter['$and'], $subFilter['$and']);
                } else {
                    $filter['$and'][] = $subFilter;
                }
                continue;
            }

            if (!empty($filter) && (isset($subFilter['$and']) || isset($subFilter['$or']))) {
                $filter = [
                    '$and' => [
                        $filter,
                        $subFilter
                    ]
                ];
                continue;
            }

            $filter = array_merge($filter, $subFilter);
        }

        return $filter;
    }

    protected function convertOperator(array $identifiers, string $expr): array
    {
        $array = explode(' ', $expr);

        $reverseOperator = false;

        $field = trim(array_shift($array));

        $operator = trim(array_shift($array));

        $value = trim(join(' ', $array));

        if (is_numeric($field) || $this->isStringValue($field) || $this->isInlineFunction($field)) {
            $tmp             = $value;
            $value           = $field;
            $field           = $tmp;
            $reverseOperator = true;
        }

        $identifiers = array_values(array_filter($identifiers, fn($string) => $string != $field));

        if ($this->isStringValue($value)) {
            $value = str_replace(['"', '\''], '', $value);
        } else {
            if (is_numeric($value)) {
                settype($value, str_contains($value, '.') ? 'float' : 'int');
            } else {
                $value = $this->convertInlineFunction($value, $identifiers);
            }
        }

        return match ($operator) {
            '<' => [$field => [($reverseOperator ? '$gt' : '$lt') => $value]],
            '<=' => [$field => [($reverseOperator ? '$gte' : '$lte') => $value]],
            '>' => [$field => [($reverseOperator ? '$lt' : '$gt') => $value]],
            '>=' => [$field => [($reverseOperator ? '$lte' : '$gte') => $value]],
            '<>', '!=' => [$field => ['$ne' => $value]],
            '=' => [$field => $value],
            'LIKE' => [$field => "/$value/i"],
            'in' => [$field => ['$in' => $identifiers]],
            'not' => $array[0] == 'in' ? [$field => ['$nin' => $identifiers]] : [],
            default => []
        };
    }

    protected function isStringValue(string $value): bool
    {
        return str_starts_with($value, '"') || str_starts_with($value, '\'');
    }

    protected function isInlineFunction(string $value): bool
    {
        return !$this->isStringValue($value) && (str_contains($value, '"') || str_contains($value, '\''));
    }

    protected function convertInlineFunction(string $value, array $identifiers): mixed
    {
        return match ($identifiers[0]) {
            'date' => new UTCDateTime(date_create($identifiers[1])),
            'ObjectId' => new ObjectId($identifiers[1]),
            default => $value
        };
    }

    #[Pure] protected function getBracketsDiff($string): int
    {
        return substr_count($string, '(') - substr_count($string, ')');
    }

    #[Pure]
    protected function hasOnlyFilter(
        array $filter,
        $filterKey
    ): bool {
        return count($filter) == 1 && isset($filter[$filterKey]);
    }
}
