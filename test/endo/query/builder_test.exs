defmodule Endo.Query.BuilderTest do
  use Endo.TestCase, async: true

  import Endo.Query
  alias Endo.Query.Builder

  describe "build_sql/2" do
    test "query that has everything" do
      threshold = 123
      offset = fn -> 456 end
      pct = %{percentage: 0.9}
      d_field = "\"What?\""
      d_values = ~w[This is just ridiculous]

      subquery = from("table \"9\"") |> select([x], x["foo"])

      query = from("table 1")
              |> select([a], [a["f 1"], count_distinct(a["f 2"])])
              |> join(:inner, [a], b in "table 2", on: a["f 3"] == b["f 4"] and not is_nil(a["f 5"]))
              |> select([..., b], percentile_cont(^pct.percentage, b["f6"]))
              |> join(:left, [a, ...], c in "table3", on: a["f 6"] == c["f 7"])
              |> join(:right, [..., c], d in ^subquery, on: c["foo1"] == d["foo"])
              |> where([a, ..., d], a["AAA"] == "bbb" and d[^d_field] in ^d_values)
              |> group_by([a, ..., d], [a["f 8"] * d["f 9"], d["aa\"bb\"cc"]])
              |> having([a, b, ...], median(a["f 10"] + b["f 11"]) * 2 >= ^threshold)
              |> order_by([..., c, d], asc: c["f 14"], desc: d["f 15"], asc: c["f 16"])
              |> limit(100)
              |> offset(^offset.())

      {sql, args} = Builder.build_sql(query, schema: "data")

      expected_sql =
        """
        SELECT
          t0."f 1",
          count(DISTINCT t0."f 2"),
          (percentile_cont ($1) WITHIN GROUP (ORDER BY t1."f6"))
        FROM "data"."table 1" AS t0
        INNER JOIN "data"."table 2" AS t1 ON ((t0."f 3" = t1."f 4") AND (NOT (t0."f 5" IS NULL)))
        LEFT JOIN "data"."table3" AS t2 ON (t0."f 6" = t2."f 7")
        RIGHT JOIN (
          SELECT st0."foo"
          FROM "data"."table ""9""" AS st0
        ) AS t3 ON (t2."foo1" = t3."foo")
        WHERE ((t0."AAA" = $2) AND (t3."""What?""" IN $3))
        GROUP BY (t0."f 8" * t3."f 9"), t3."aa""bb""cc"
        HAVING (((percentile_cont (0.5) WITHIN GROUP (ORDER BY (t0."f 10" + t1."f 11"))) * 2) >= $4)
        ORDER BY t2."f 14" ASC, t3."f 15" DESC, t2."f 16" ASC
        LIMIT 100
        OFFSET $5
        """
        |> String.replace(~r/\r?\n/, " ", global: true)
        |> String.replace(~r/\s{2,}/, " ", global: true)
        |> String.replace(~r/(?<=\()\s/, "", global: true)
        |> String.replace(~r/\s(?=\))/, "", global: true)
        |> String.trim()

      assert sql == expected_sql
      assert args == [0.9, "bbb", d_values, 123, 456]
    end
  end
end
