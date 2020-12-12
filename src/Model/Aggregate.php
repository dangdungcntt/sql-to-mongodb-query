<?php

namespace Nddcoder\SqlToMongodbQuery\Model;

class Aggregate extends Query
{
    // @codeCoverageIgnoreStart
    public function __construct(
        string $collection,
        public array $pipelines,
        ?string $hint = null
    ) {
        parent::__construct($collection, $hint);
    }
    // @codeCoverageIgnoreEnd
}
