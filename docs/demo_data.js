window.DEMO = [
  {
    "name": "Broadcast join",
    "query": "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue\nFROM lineitem\nJOIN supplier ON l_suppkey = s_suppkey\nJOIN nation ON s_nationkey = n_nationkey\nGROUP BY n_name ORDER BY revenue DESC",
    "optimized_sql": "SELECT /*+ BROADCAST(supplier, nation) */\n  n_name,\n  SUM(l_extendedprice * (\n    1 - l_discount\n  )) AS revenue\nFROM lineitem\nJOIN supplier\n  ON l_suppkey = s_suppkey\nJOIN nation\n  ON s_nationkey = n_nationkey\nGROUP BY\n  n_name\nORDER BY\n  revenue DESC",
    "applied_rules": [
      "broadcast_join"
    ],
    "speedup": 2.33,
    "status": "optimized",
    "explanation": "The query joined a large table to small lookup tables with a shuffle sort-merge join; broadcasting the small tables avoids shuffling the large one across the network. Net result: a 2.33x speedup with identical output.",
    "plan_before": {
      "nodes": [
        {
          "id": 1,
          "label": "Sort"
        },
        {
          "id": 2,
          "label": "Exchange"
        },
        {
          "id": 3,
          "label": "HashAggregate"
        },
        {
          "id": 4,
          "label": "Exchange"
        },
        {
          "id": 5,
          "label": "HashAggregate"
        },
        {
          "id": 6,
          "label": "Project"
        },
        {
          "id": 7,
          "label": "SortMergeJoin"
        },
        {
          "id": 8,
          "label": "Sort"
        },
        {
          "id": 9,
          "label": "Exchange"
        },
        {
          "id": 10,
          "label": "Project"
        },
        {
          "id": 11,
          "label": "SortMergeJoin"
        },
        {
          "id": 12,
          "label": "Sort"
        },
        {
          "id": 13,
          "label": "Exchange"
        },
        {
          "id": 14,
          "label": "Filter"
        },
        {
          "id": 15,
          "label": "ColumnarToRow"
        },
        {
          "id": 16,
          "label": "FileScan"
        },
        {
          "id": 17,
          "label": "Sort"
        },
        {
          "id": 18,
          "label": "Exchange"
        },
        {
          "id": 19,
          "label": "Filter"
        },
        {
          "id": 20,
          "label": "ColumnarToRow"
        },
        {
          "id": 21,
          "label": "FileScan"
        },
        {
          "id": 22,
          "label": "Sort"
        },
        {
          "id": 23,
          "label": "Exchange"
        },
        {
          "id": 24,
          "label": "Filter"
        },
        {
          "id": 25,
          "label": "ColumnarToRow"
        },
        {
          "id": 26,
          "label": "FileScan"
        }
      ],
      "edges": [
        {
          "from": 1,
          "to": 2
        },
        {
          "from": 2,
          "to": 3
        },
        {
          "from": 3,
          "to": 4
        },
        {
          "from": 4,
          "to": 5
        },
        {
          "from": 5,
          "to": 6
        },
        {
          "from": 6,
          "to": 7
        },
        {
          "from": 7,
          "to": 8
        },
        {
          "from": 8,
          "to": 9
        },
        {
          "from": 9,
          "to": 10
        },
        {
          "from": 10,
          "to": 11
        },
        {
          "from": 11,
          "to": 12
        },
        {
          "from": 12,
          "to": 13
        },
        {
          "from": 13,
          "to": 14
        },
        {
          "from": 14,
          "to": 15
        },
        {
          "from": 15,
          "to": 16
        },
        {
          "from": 11,
          "to": 17
        },
        {
          "from": 17,
          "to": 18
        },
        {
          "from": 18,
          "to": 19
        },
        {
          "from": 19,
          "to": 20
        },
        {
          "from": 20,
          "to": 21
        },
        {
          "from": 7,
          "to": 22
        },
        {
          "from": 22,
          "to": 23
        },
        {
          "from": 23,
          "to": 24
        },
        {
          "from": 24,
          "to": 25
        },
        {
          "from": 25,
          "to": 26
        }
      ]
    },
    "plan_after": {
      "nodes": [
        {
          "id": 1,
          "label": "Sort"
        },
        {
          "id": 2,
          "label": "Exchange"
        },
        {
          "id": 3,
          "label": "HashAggregate"
        },
        {
          "id": 4,
          "label": "Exchange"
        },
        {
          "id": 5,
          "label": "HashAggregate"
        },
        {
          "id": 6,
          "label": "Project"
        },
        {
          "id": 7,
          "label": "BroadcastHashJoin"
        },
        {
          "id": 8,
          "label": "Project"
        },
        {
          "id": 9,
          "label": "BroadcastHashJoin"
        },
        {
          "id": 10,
          "label": "Filter"
        },
        {
          "id": 11,
          "label": "ColumnarToRow"
        },
        {
          "id": 12,
          "label": "FileScan"
        },
        {
          "id": 13,
          "label": "BroadcastExchange"
        },
        {
          "id": 14,
          "label": "Filter"
        },
        {
          "id": 15,
          "label": "ColumnarToRow"
        },
        {
          "id": 16,
          "label": "FileScan"
        },
        {
          "id": 17,
          "label": "BroadcastExchange"
        },
        {
          "id": 18,
          "label": "Filter"
        },
        {
          "id": 19,
          "label": "ColumnarToRow"
        },
        {
          "id": 20,
          "label": "FileScan"
        }
      ],
      "edges": [
        {
          "from": 1,
          "to": 2
        },
        {
          "from": 2,
          "to": 3
        },
        {
          "from": 3,
          "to": 4
        },
        {
          "from": 4,
          "to": 5
        },
        {
          "from": 5,
          "to": 6
        },
        {
          "from": 6,
          "to": 7
        },
        {
          "from": 7,
          "to": 8
        },
        {
          "from": 8,
          "to": 9
        },
        {
          "from": 9,
          "to": 10
        },
        {
          "from": 10,
          "to": 11
        },
        {
          "from": 11,
          "to": 12
        },
        {
          "from": 9,
          "to": 13
        },
        {
          "from": 13,
          "to": 14
        },
        {
          "from": 14,
          "to": 15
        },
        {
          "from": 15,
          "to": 16
        },
        {
          "from": 7,
          "to": 17
        },
        {
          "from": 17,
          "to": 18
        },
        {
          "from": 18,
          "to": 19
        },
        {
          "from": 19,
          "to": 20
        }
      ]
    }
  },
  {
    "name": "Predicate pushdown",
    "query": "SELECT SUM(l_extendedprice) AS revenue\nFROM lineitem\nWHERE YEAR(l_shipdate) = 1994",
    "optimized_sql": "SELECT\n  SUM(l_extendedprice) AS revenue\nFROM lineitem\nWHERE\n  l_shipdate BETWEEN '1994-01-01' AND '1994-12-31'",
    "applied_rules": [
      "sargable_year",
      "llm_escalation"
    ],
    "speedup": 1.27,
    "status": "optimized",
    "explanation": "A YEAR() function wrapped a date column, which blocks predicate pushdown; rewriting it to a date range lets Spark push the filter into the Parquet scan. No deterministic rule matched, so an LLM proposed a novel rewrite; it was accepted only after the Validator proved the output is identical. Net result: a 1.27x speedup with identical output.",
    "plan_before": {
      "nodes": [
        {
          "id": 1,
          "label": "HashAggregate"
        },
        {
          "id": 2,
          "label": "Exchange"
        },
        {
          "id": 3,
          "label": "HashAggregate"
        },
        {
          "id": 4,
          "label": "Project"
        },
        {
          "id": 5,
          "label": "Filter"
        },
        {
          "id": 6,
          "label": "ColumnarToRow"
        },
        {
          "id": 7,
          "label": "FileScan"
        }
      ],
      "edges": [
        {
          "from": 1,
          "to": 2
        },
        {
          "from": 2,
          "to": 3
        },
        {
          "from": 3,
          "to": 4
        },
        {
          "from": 4,
          "to": 5
        },
        {
          "from": 5,
          "to": 6
        },
        {
          "from": 6,
          "to": 7
        }
      ]
    },
    "plan_after": {
      "nodes": [
        {
          "id": 1,
          "label": "HashAggregate"
        },
        {
          "id": 2,
          "label": "Exchange"
        },
        {
          "id": 3,
          "label": "HashAggregate"
        },
        {
          "id": 4,
          "label": "Project"
        },
        {
          "id": 5,
          "label": "Filter"
        },
        {
          "id": 6,
          "label": "ColumnarToRow"
        },
        {
          "id": 7,
          "label": "FileScan"
        }
      ],
      "edges": [
        {
          "from": 1,
          "to": 2
        },
        {
          "from": 2,
          "to": 3
        },
        {
          "from": 3,
          "to": 4
        },
        {
          "from": 4,
          "to": 5
        },
        {
          "from": 5,
          "to": 6
        },
        {
          "from": 6,
          "to": 7
        }
      ]
    }
  }
];
