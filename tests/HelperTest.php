<?php

it('str replace first return same $subject when $search empty', function () {
    expect(str_replace_first('', '', 'abc'))->toEqual('abc');
});

it('str replace first return same $subject when not found $search', function () {
    expect(str_replace_first('def', '', 'abc'))->toEqual('abc');
});

it('str before return same $subject when $search empty', function () {
    expect(str_before('abc', ''))->toEqual('abc');
});
