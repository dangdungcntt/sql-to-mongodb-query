<?php

namespace Nddcoder\SqlToMongodbQuery\Lib\DataStruct;

class Stack
{
    public function __construct(
        protected $data = []
    ) {
    }

    public function push($item): void
    {
        $this->data[] = $item;
    }

    public function pushAll(array $items): void
    {
        foreach ($items as $item) {
            $this->push($item);
        }
    }

    public function pop(): mixed
    {
        return array_pop($this->data);
    }

    public function top(): mixed
    {
        return $this->data[count($this->data) - 1] ?? null;
    }

    public function size(): int
    {
        return count($this->data);
    }

    public function isNotEmpty(): bool
    {
        return $this->size() > 0;
    }

    public function clear(): void
    {
        $this->data = [];
    }

    public function getData(): array
    {
        return $this->data;
    }
}
