<?php

namespace Nddcoder\SqlToMongodbQuery\Model;

abstract class Query
{
    // @codeCoverageIgnoreStart
    public function __construct(
        public string $collection,
        public ?string $hint = null
    ) {
    }
    // @codeCoverageIgnoreEnd
}
