defmodule Endo.Query.BuilderTest do
  use Endo.TestCase, async: true

  import Endo.Query
  alias Endo.Query.Builder

  describe "build_sql/2" do
    test "query that has everything" do
      v1 = 123

      query = from("table 1")
              |> select([a], [a["f 1"], count_distinct(a["f 2"])])
              |> join(:inner, [a], b in "table 2", on: a["f 3"] == b["f 4"] and not is_nil(a["f 5"]))
              |> select([..., b], percentile_cont(^0.9, b["f6"]))
              |> join(:left, [a, ...], c in "table3", on: a["f 6"] == c["f 7"])
              |> group_by([a, ..., c], [a["f 8"] * c["f 9"], c["aa\"bb\"cc"]])
              |> having([a, b, ...], median(a["f 10"] + b["f 11"]) * 2 >= ^v1)
              |> order_by([..., b, c], asc: b["f 14"], desc: c["f 15"], asc: b["f 16"])
              |> limit(100)
              |> offset(123)

      {sql, args} = Builder.build_sql(query, schema: "data")

      IO.puts sql
      IO.inspect(args, limit: :infinity, structs: false, charlists: :as_lists, label: "Args")
    end
  end
end
