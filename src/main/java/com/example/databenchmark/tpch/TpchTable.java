package com.example.databenchmark.tpch;

import java.util.List;

public record TpchTable(String name, long baseRows, List<TpchColumn> columns) {}
