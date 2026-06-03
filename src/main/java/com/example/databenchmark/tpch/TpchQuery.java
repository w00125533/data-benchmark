package com.example.databenchmark.tpch;

import java.util.Set;

public record TpchQuery(String name, String template, Set<String> querySets) {}
