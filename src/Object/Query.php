<?php

namespace Nddcoder\SqlToMongodbQuery\Object;

abstract class Query
{
    public function __construct(
        public string $collection,
        public ?string $hint = null
    ) {
    }
}
