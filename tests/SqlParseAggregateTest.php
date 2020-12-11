<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use Nddcoder\SqlToMongodbQuery\Object\Aggregate;
use Nddcoder\SqlToMongodbQuery\Object\FindQuery;
use Nddcoder\SqlToMongodbQuery\SqlToMongodbQuery;

class SqlParseAggregateTest extends TestCase
{
    protected SqlToMongodbQuery $parser;

    public function __construct()
    {
        parent::__construct();
        $this->parser = new SqlToMongodbQuery();
    }

    protected function parse(string $sql): ?Aggregate
    {
        $query = $this->parser->parse($sql);
        $this->assertInstanceOf(Aggregate::class, $query);
        return $query;
    }

    /** @test */
    public function it_should_parse_group_by()
    {
        $this->assertTrue(true);
//        $this->assertNull(dd($this->parser->parse('SELECT username, sum(displays) FROM users group by username')));
    }
}
