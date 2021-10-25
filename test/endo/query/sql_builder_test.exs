defmodule Endo.Query.SQLBuilderTest do
  use Endo.TestCase, async: true

  import Endo.Query
  alias Endo.Query.SQLBuilder

  describe "build_sql/2" do
    test "query that has everything" do
      threshold = 123
      offset = fn -> 456 end
      pct = %{percentage: 0.9}
      d_field = "\"What?\""
      d_values = ~w[some values]
      deadline = ~D[2021-10-31]

      subquery = from("table \"9\"") |> select([x], x["foo"])

      query = from("table 1")
              |> select([a], [a["f 1"], count_distinct(a["f 2"])])
              |> join(:inner, [a], b in "table 2", on: a["f 3"] == b["f 4"] and not is_nil(a["f 5"]))
              |> select([..., b], percentile_cont(^pct.percentage, b["f6"]))
              |> join(:left, [a, ...], c in "table3", on: a["f 6"] == c["f 7"])
              |> select([..., c], max(extract("year", c["order date"]) <> "-" <> extract("month", c["order date"])))
              |> join(:right, [..., c], d in ^subquery, on: c["foo1"] == d["foo"])
              |> select([a, ..., d], agg("weighted_sum(?, ?)", d["likes"], a["weight"]))
              |> where([a, ..., d], a["AAA"] == "bbb" and d[^d_field] in ^d_values)
              |> where([a, ...], a["deadline"] <= ^deadline)
              |> group_by([a, ..., d], [a["f 8"] * d["f 9"], d["aa\"bb\"cc"]])
              |> having([a, b, ...], median(a["f 10"] + b["f 11"]) * 2 >= ^threshold)
              |> order_by([..., c, d], asc: c["f 14"], desc: d["f 15"], asc: c["f 16"])
              |> limit(100)
              |> offset(^offset.())

      {sql, args} = SQLBuilder.build_sql(query, schema: "data")

      expected_sql = ~Q{
        SELECT
          t0."f 1",
          count(DISTINCT t0."f 2"),
          (percentile_cont ($1) WITHIN GROUP (ORDER BY t1."f6")),
          max((extract("year" FROM t2."order date") || ($2 || extract("month" FROM t2."order date")))),
          weighted_sum(t3."likes", t0."weight")
        FROM "data"."table 1" AS t0
        INNER JOIN "data"."table 2" AS t1 ON ((t0."f 3" = t1."f 4") AND (NOT (t0."f 5" IS NULL)))
        LEFT JOIN "data"."table3" AS t2 ON (t0."f 6" = t2."f 7")
        RIGHT JOIN (
          SELECT st0."foo"
          FROM "data"."table ""9""" AS st0
        ) AS t3 ON (t2."foo1" = t3."foo")
        WHERE (((t0."AAA" = $3) AND (t3."""What?""" IN $4)) AND (t0."deadline" <= $5))
        GROUP BY (t0."f 8" * t3."f 9"), t3."aa""bb""cc"
        HAVING (((percentile_cont (0.5) WITHIN GROUP (ORDER BY (t0."f 10" + t1."f 11"))) * 2) >= $6)
        ORDER BY t2."f 14" ASC, t3."f 15" DESC, t2."f 16" ASC
        LIMIT 100
        OFFSET $7
      }

      assert sql == expected_sql
      assert args == [0.9, "-", "bbb", d_values, deadline, 123, 456]
    end

    test "from only" do
      {sql, []} = from("table1")
                  |> to_sql()

      assert sql == ~Q{
        SELECT *
        FROM "public"."table1" AS t0
      }
    end

    test "dynamic composition" do
      {sql, args} =
        from("table 1")
        |> add_where()
        |> add_select()
        |> SQLBuilder.build_sql(schema: ~S["weirdo"])

      expected_sql = ~Q{
        SELECT t0."""dynamic field 1"""
        FROM """weirdo"""."table 1" AS t0
        WHERE (t0."""dynamic field 2""" ~* $1)
      }

      assert sql == expected_sql
      assert args == ["foo"]
    end

    defp add_select(query) do
      field = ~S["dynamic field 1"]
      select(query, [..., x], x[^field])
    end

    defp add_where(query) do
      pattern = "foo"
      field = ~S["dynamic field 2"]
      where(query, [..., x], x[^field] =~ ^pattern)
    end
  end
end
