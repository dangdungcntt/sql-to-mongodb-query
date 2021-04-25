<?php

namespace Nddcoder\SqlToMongodbQuery;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use Nddcoder\SqlToMongodbQuery\Exceptions\InvalidSelectFieldException;
use Nddcoder\SqlToMongodbQuery\Exceptions\InvalidSelectStatementException;
use Nddcoder\SqlToMongodbQuery\Exceptions\InvalidSqlQueryException;
use Nddcoder\SqlToMongodbQuery\Exceptions\NotSupportStatementException;
use Nddcoder\SqlToMongodbQuery\Model\Aggregate;
use Nddcoder\SqlToMongodbQuery\Model\FindQuery;
use Nddcoder\SqlToMongodbQuery\Model\Query;
use PhpMyAdmin\SqlParser\Components\Condition;
use PhpMyAdmin\SqlParser\Components\Expression;
use PhpMyAdmin\SqlParser\Parser;
use PhpMyAdmin\SqlParser\Statements\SelectStatement;

class SqlToMongodbQuery
{
    public const SPECIAL_DOT_CHAR = '__';

    /**
     * @param  string  $sql
     * @return Query|null
     * @throws InvalidSelectFieldException
     * @throws InvalidSelectStatementException
     * @throws NotSupportStatementException
     * @throws InvalidSqlQueryException
     */
    public function parse(string $sql): ?Query
    {
        $sqlParser = new Parser($sql);

        if (!isset($sqlParser->statements[0])) {
            throw new InvalidSqlQueryException(sprintf('Invalid sql query for string: %s', $sql));
        }

        $statement = $sqlParser->statements[0];

        return match (true) {
            $statement instanceof SelectStatement => $this->parseSelectStatement($statement),
            default => throw new NotSupportStatementException(
                sprintf('Not support statement type %s', $statement::class)
            )
        };
    }

    /**
     * @param  SelectStatement  $statement
     * @return Query|null
     * @throws InvalidSelectFieldException
     * @throws InvalidSelectStatementException
     */
    protected function parseSelectStatement(SelectStatement $statement): ?Query
    {
        if (empty($statement->from[0])) {
            return null;
        }

        $filter = $this->parseWhere($statement);

        [$projection, $projectionFunctions] = $this->parseSelectFields($statement);

        $sort = $this->parseSort($statement);

        [$skip, $limit] = $this->parseLimit($statement);

        $hint = $this->parseHint($statement);

        if (empty($statement->group) && empty($projectionFunctions)) {
            if (!empty($statement->having)) {
                throw new InvalidSelectStatementException('Cannot use having without group by');
            }
            return new FindQuery(
                collection: $statement->from[0]->table,
                filter: $filter,
                projection: $projection,
                sort: $sort,
                limit: $limit,
                skip: $skip,
                hint: $hint
            );
        }

        $groupBy = $this->parseGroupBy($statement);

        $invalidSelect = $this->validateSelect($projection, $groupBy ?? []);

        if (count($invalidSelect) > 0 && !(count($invalidSelect) == 1 && isset($invalidSelect['_id']))) {
            throw new InvalidSelectFieldException(
                'Cannot select field(s) not in group by clause: '.join(', ', array_keys($invalidSelect))
            );
        }

        $selectFunctions = $this->parseSelectFunctions($projectionFunctions);

        $project = [];
        foreach (array_keys($projection ?? []) as $field) {
            $project[$field] = '$_id.'.strtr($field, ['.' => self::SPECIAL_DOT_CHAR]);
        }

        foreach ($selectFunctions as $field => $_) {
            $project[$field] = '$'.$field;
        }

        if (!isset($project['_id']) && !empty($project)) {
            $project['_id'] = 0;
        }

        $pipelines = [
            [
                '$match' => empty($filter) ? (object) $filter : $filter
            ],
            [
                '$group' => array_merge(
                    [
                        '_id' => $groupBy
                    ],
                    $selectFunctions
                )
            ],
            [
                '$project' => $project
            ]
        ];

        $having = $this->parseHaving($statement);

        if (!empty($having)) {
            $havingStep  = [
                '$match' => $having
            ];
            $pipelines[] = $havingStep;
        }

        if ($sort) {
            $sortStep    = [
                '$sort' => $sort
            ];
            $pipelines[] = $sortStep;
        }

        if ($skip) {
            $skipStep    = [
                '$skip' => $skip
            ];
            $pipelines[] = $skipStep;
        }

        if ($limit) {
            $limitStep   = [
                '$limit' => $limit
            ];
            $pipelines[] = $limitStep;
        }

        return new Aggregate(
            collection: $statement->from[0]->table,
            pipelines: $pipelines,
            hint: $hint
        );
    }

    /**
     * @param  Condition[]  $conditions
     * @return array
     * @throws \Exception
     */
    protected function parseWhereConditions(array $conditions): array
    {
        $filter = [];

        $nextToIndex = 0;

        $conditionCount = count($conditions);

        for ($index = 0; $index < $conditionCount; $index++) {
            $condition = $conditions[$index];

            if ($nextToIndex > 0 && $nextToIndex > $index) {
                continue;
            }

            $nextToIndex = 0;

            if ($condition->isOperator) {
                if ($condition->expr == 'OR') {
                    $subConditions = [];
                    $bracketsDiff  = 0;
                    for ($i = $index + 1; $i < $conditionCount; $i++) {
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

                    if ($nextToIndex == $conditionCount - 1) {
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

            for ($i = $index + 1; $i < $conditionCount; $i++) {
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

    /**
     * @throws \Exception
     */
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
                $value = substr($value, 1, strlen($value) - 2);
                break;
            case in_array(strtolower($value), ['true', 'false']):
                $value = strtolower($value) === 'true';
                break;
            case strtolower($value) == 'null':
                $value = null;
                break;
            case is_numeric($value):
                settype($value, str_contains($value, '.') ? 'float' : 'int');
                break;
            case $operator != 'in':
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

    /**
     * @throws \Exception
     */
    protected function parseValueForInQuery($value, $identifiers): array
    {
        $value = trim($value, '() ');

        [$replaces, $value] = $this->buildReplacers($identifiers, $value);

        return array_map(
            function ($item) use ($replaces) {
                $item           = trim($item);
                $subIdentifiers = [];
                foreach ($replaces as $key => $identifier) {
                    $identifier = substr($identifier, 1, strlen($identifier) - 2);
                    if (str_contains($item, $key)) {
                        $subIdentifiers[] = trim($identifier, '\'"');
                        $item             = str_replace($key, $identifier, $item);
                    }
                }

                //Support parse multi inline function
                //Ex: where _id in (ObjectId('6014c5e76bc47532c6f4dc7f'), ObjectId('60150e876bc47532c6058dc2'))
                if (count($subIdentifiers) == 1 && !$this->isStringValue($item) && str_contains($item, '(')) {
                    array_unshift($subIdentifiers, substr($item, 0, strpos($item, '(')));
                }

                if ($this->isStringValue($item) && strlen($item) == 2) {
                    $item = '';
                }

                if (is_numeric($item)) {
                    settype($item, str_contains($item, '.') ? 'float' : 'int');
                }

                if (in_array(strtolower($item), ['true', 'false'])) {
                    $item = strtolower($item) === 'true';
                }

                if (is_string($item) && strtolower($item) == 'null') {
                    $item = null;
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
            'ObjectId', 'Id' => new ObjectId($identifiers[1]),
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

        return substr_count($condition->expr, '(') - $bracketOpen
            - (substr_count($condition->expr, ')') - $bracketClose);
    }

    protected function hasOnlyFilter(
        array $filter,
        $filterKey
    ): bool {
        return count($filter) === 1 && isset($filter[$filterKey]);
    }

    /**
     * @throws \Exception
     */
    protected function normalizeExpr($identifiers, string $expr): string
    {
        [$replaces, $expr] = $this->buildReplacers($identifiers, $expr);

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
            ]
        );

        $expr = preg_replace('!\s+!', ' ', $expr);

        return strtr($expr, $replaces);
    }

    /**
     * @throws \Exception
     */
    protected function buildReplacers(array $identifiers, $value): array
    {
        $replaces = [];

        $salt = time().random_int(1000, 10000);

        foreach ($identifiers as $index => $identifier) {
            if (empty($identifier)) {
                continue;
            }

            $key1 = "__tmp_identifier_{$salt}_{$index}_1";
            $key2 = "__tmp_identifier_{$salt}_{$index}_2";

            $value           = strtr($value, [
                "'$identifier'"   => $key1,
                "\"$identifier\"" => $key2,
            ]);
            $replaces[$key1] = "'$identifier'";
            $replaces[$key2] = "\"$identifier\"";
        }

        return [
            $replaces, $value
        ];
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

    /**
     * @param  Expression[]|null  $projectionFunctions
     * @return array
     */
    protected function parseSelectFunctions(?array $projectionFunctions): array
    {
        if (empty($projectionFunctions)) {
            return [];
        }

        $result = [];

        foreach ($projectionFunctions as $projectionFunction) {
            $field                             = '$'.trim(
                    str_replace_first($projectionFunction->function, '', $projectionFunction->expr),
                    '() '
                );
            $result[$projectionFunction->expr] = match (strtolower($projectionFunction->function)) {
                'count' => ['$sum' => 1],
                'sum' => [
                    '$sum' => $field
                ],
                'avg' => [
                    '$avg' => $field
                ],
                'min' => [
                    '$min' => $field
                ],
                'max' => [
                    '$max' => $field
                ]
            };
        }

        return $result;
    }

    /**
     * @param  SelectStatement  $statement
     * @return array
     * @throws InvalidSelectStatementException
     */
    protected function parseSelectFields(SelectStatement $statement): array
    {
        if (empty($statement->expr)) {
            throw new InvalidSelectStatementException('Invalid SELECT statement');
        }
        $projection          = [];
        $projectionFunctions = [];
        foreach ($statement->expr as $expression) {
            if ($expression->function) {
                $projectionFunctions[] = $expression;
                continue;
            }
            $field = $expression->expr;
            if ($field && $field !== '*') {
                $projection[$field] = 1;
            }
        }

        if (empty($projection)) {
            $projection = null;
        }

        if (empty($projectionFunctions)) {
            $projectionFunctions = null;
        }

        return [$projection, $projectionFunctions];
    }

    protected function parseGroupBy(SelectStatement $statement): ?array
    {
        $groupBy = [];
        foreach ($statement->group ?? [] as $groupKeyword) {
            $field = $groupKeyword->expr->expr;
            if ($field) {
                $groupBy[strtr($field, ['.' => self::SPECIAL_DOT_CHAR])] = "\$$field";
            }
        }
        return empty($groupBy) ? null : $groupBy;
    }

    protected function parseSort(SelectStatement $statement): ?array
    {
        if (empty($statement->order)) {
            return null;
        }
        $sort = [];
        foreach ($statement->order as $orderKeyword) {
            $sort[$orderKeyword->expr->column ?? $orderKeyword->expr->expr] = $orderKeyword->type === 'DESC' ? -1 : 1;
        }
        return $sort;
    }

    protected function parseLimit(SelectStatement $statement): array
    {
        if (empty($statement->limit)) {
            return [0, 0];
        }
        return [$statement->limit->offset, $statement->limit->rowCount];
    }

    protected function parseHint(SelectStatement $statement): ?string
    {
        if (empty($statement->index_hints)) {
            return null;
        }
        $hintValue = $statement->index_hints[0]->indexes[0] ?? null;
        return $hintValue?->column ?? $hintValue;
    }

    protected function parseWhere(SelectStatement $statement): array
    {
        if (empty($statement->where)) {
            return [];
        }
        return $this->parseWhereConditions($statement->where);
    }

    protected function parseHaving(SelectStatement $statement): array
    {
        if (empty($statement->having)) {
            return [];
        }
        return $this->parseWhereConditions($statement->having);
    }

    protected function validateSelect(mixed $projection, array $groupBy): array
    {
        $invalidSelect = [];
        foreach ($projection ?? [] as $field => $_) {
            if (str_contains($field, '.')) {
                $field = strtr($field, ['.' => self::SPECIAL_DOT_CHAR]);
            }
            if (!array_key_exists($field, $groupBy)) {
                $invalidSelect[$field] = 1;
            }
        }

        return $invalidSelect;
    }
}
