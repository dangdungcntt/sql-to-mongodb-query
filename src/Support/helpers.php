<?php

if (!function_exists('str_replace_first')) {
    function str_replace_first(string $search, string $replace, string $subject): string
    {
        return implode($replace, explode($search, $subject, 2));
    }
}
