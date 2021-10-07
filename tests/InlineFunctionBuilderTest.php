<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\UTCDateTime;
use Nddcoder\SqlToMongodbQuery\SqlToMongodbQuery;

class InlineFunctionBuilderTest extends TestCase
{
    /** @test */
    public function it_add_default_functions()
    {
        $parser = new SqlToMongodbQuery();
        $this->assertEquals(
            ['created_at' => new UTCDateTime(date_create('2020-12-12'))],
            $parser->parse("SELECT * FROM users WHERE created_at = date('2020-12-12')")->filter
        );

        $this->assertEquals(
            ['_id' => new ObjectId('5d3937af498831003e9f6f2a')],
            $parser->parse("SELECT * FROM users WHERE _id = ObjectId('5d3937af498831003e9f6f2a')")->filter
        );

        $this->assertEquals(
            ['_id' => new ObjectId('5d3937af498831003e9f6f2a')],
            $parser->parse("SELECT * FROM users WHERE _id = Id('5d3937af498831003e9f6f2a')")->filter
        );
    }

    /** @test */
    public function it_not_use_default_function_when_existed_function()
    {
        SqlToMongodbQuery::addInlineFunctionBuilder('date', fn($str) => date_create($str));
        SqlToMongodbQuery::addInlineFunctionBuilder('ObjectId', fn($str) => $str);
        SqlToMongodbQuery::addInlineFunctionBuilder('Id', fn($str) => $str);

        $parser = new SqlToMongodbQuery();
        $this->assertEquals(
            ['created_at' => date_create('2020-12-12')],
            $parser->parse("SELECT * FROM users WHERE created_at = date('2020-12-12')")->filter
        );

        $this->assertEquals(
            ['_id' => '5d3937af498831003e9f6f2a'],
            $parser->parse("SELECT * FROM users WHERE _id = ObjectId('5d3937af498831003e9f6f2a')")->filter
        );

        $this->assertEquals(
            ['_id' => '5d3937af498831003e9f6f2a'],
            $parser->parse("SELECT * FROM users WHERE _id = Id('5d3937af498831003e9f6f2a')")->filter
        );

        SqlToMongodbQuery::removeInlineFunctionBuilder('date');
        SqlToMongodbQuery::removeInlineFunctionBuilder('ObjectId');
        SqlToMongodbQuery::removeInlineFunctionBuilder('Id');
    }
}
