<?php

namespace Nddcoder\SqlToMongodbQuery\Object;

class FindQuery extends Query
{
    // @codeCoverageIgnoreStart
    public function __construct(
        string $collection,
        public array $filter,
        public ?array $projection = null,
        public ?array $sort = null,
        public int $limit = 0,
        public int $skip = 0,
        ?string $hint = null
    ) {
        parent::__construct($collection, $hint);
    }
    // @codeCoverageIgnoreEnd
}
