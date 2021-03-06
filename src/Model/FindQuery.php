<?php

namespace Nddcoder\SqlToMongodbQuery\Model;

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
    public function getOptions(): array
    {
        return [
            'skip'       => $this->skip ?: null,
            'limit'      => $this->limit ?: null,
            'projection' => $this->projection,
            'sort'       => $this->sort,
            'hint'       => $this->hint
        ];
    }
}
