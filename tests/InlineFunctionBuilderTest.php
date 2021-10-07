<?php

namespace Nddcoder\SqlToMongodbQuery\Tests;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\UTCDateTime;
use Nddcoder\SqlToMongodbQuery\SqlToMongodbQuery;

it('add_default_functions', function () {
    $parser = new SqlToMongodbQuery();

    expect($parser->parse("SELECT * FROM users WHERE created_at = date('2020-12-12')")->filter)
        ->toEqual(['created_at' => new UTCDateTime(date_create('2020-12-12'))])
        ->and($parser->parse("SELECT * FROM users WHERE _id = ObjectId('5d3937af498831003e9f6f2a')")->filter)
        ->toEqual(['_id' => new ObjectId('5d3937af498831003e9f6f2a')])
        ->and($parser->parse("SELECT * FROM users WHERE _id = Id('5d3937af498831003e9f6f2a')")->filter)
        ->toEqual(['_id' => new ObjectId('5d3937af498831003e9f6f2a')]);
});

it('not_use_default_function_when_existed_function', function () {
    SqlToMongodbQuery::addInlineFunctionBuilder('date', fn($str) => date_create($str));
    SqlToMongodbQuery::addInlineFunctionBuilder('ObjectId', fn($str) => $str);
    SqlToMongodbQuery::addInlineFunctionBuilder('Id', fn($str) => $str);

    $parser = new SqlToMongodbQuery();

    expect($parser->parse("SELECT * FROM users WHERE created_at = date('2020-12-12')")->filter)
        ->toEqual(['created_at' => date_create('2020-12-12')])
        ->and($parser->parse("SELECT * FROM users WHERE _id = ObjectId('5d3937af498831003e9f6f2a')")->filter)
        ->toEqual(['_id' => '5d3937af498831003e9f6f2a'])
        ->and($parser->parse("SELECT * FROM users WHERE _id = Id('5d3937af498831003e9f6f2a')")->filter)
        ->toEqual(['_id' => '5d3937af498831003e9f6f2a']);

    SqlToMongodbQuery::removeInlineFunctionBuilder('date');
    SqlToMongodbQuery::removeInlineFunctionBuilder('ObjectId');
    SqlToMongodbQuery::removeInlineFunctionBuilder('Id');
});

