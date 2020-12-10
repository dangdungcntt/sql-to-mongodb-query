<?php

namespace Nddcoder\SqlToMongodbQuery\Object;

class Query
{
    public function __construct(
        public string $collection,
        public array $filter,
        public ?array $projection = null,
        public ?array $sort = null,
        public int $limit = 0,
        public int $skip = 0,
        public ?string $hint = null
    ) {
    }
}
