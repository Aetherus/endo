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

      subquery = from("table 2") |> select([x], x["foo"])

      query = from("table 1")
              |> select([a], [a["f 1"], count_distinct(a["f 2"])])
              |> join(:inner, [a], b in "table 2", on: a["f 3"] == b["f 4"] and not is_nil(a["f 5"]))
              |> select([..., b], percentile_cont(^pct.percentage, b["f6"]))
              |> join(:left, [a, ...], c in "table3", on: a["f 6"] == c["f 7"])
              |> join(:right, [..., c], d in ^subquery, on: c["foo1"] == d["foo"])
              |> where([a, ..., d], a["AAA"] == "bbb" and d[^d_field] in ^d_values)
              |> group_by([a, ..., c], [a["f 8"] * c["f 9"], c["aa\"bb\"cc"]])
              |> having([a, b, ...], median(a["f 10"] + b["f 11"]) * 2 >= ^threshold)
              |> order_by([..., b, c], asc: b["f 14"], desc: c["f 15"], asc: b["f 16"])
              |> limit(100)
              |> offset(^offset.())

      {sql, args} = Builder.build_sql(query, schema: "data")

      IO.puts sql
      IO.inspect(args, limit: :infinity, structs: false, charlists: :as_lists, label: "Args")
    end
  end
end
