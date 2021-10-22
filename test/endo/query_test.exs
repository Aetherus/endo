defmodule Endo.QueryTest do
  use Endo.TestCase, async: true

  import Endo.Query
  alias Endo.Query

  describe "from/1" do
    test "with string literal arg" do
      query = from("foo")
      assert %Query{from: "foo", aliases_count: 1} = query
    end

    test "with variable as the arg" do
      table = "foo"
      query = from(^table)
      assert %Query{from: {:unsafe, "foo"}, aliases_count: 1} = query
    end
  end

  describe "select/3" do
    test "single fixed field" do
      query = from("table1") |> select([q], q["bar"])
      assert %Query{select: %{1 => select}, aggregate?: false} = query
      assert List.flatten(select) ==  [
        {:field, [{:bind, 0}, "bar"]}
      ]
    end

    test "single dynamic field" do
      f = fn -> "bar" end
      query = from("table1") |> select([q], q[^f.()])
      assert %Query{select: %{1 => select}, aggregate?: false} = query
      assert List.flatten(select) == [
        {:field, [{:bind, 0}, {:unsafe, "bar"}]}
      ]
    end

    test "multiple fields" do
      f = fn -> "qux" end
      query = from("table1") |> select([q], [q["bar"], q[^f.()]])
      assert %Query{select: %{1 => select}, aggregate?: false} = query
      assert List.flatten(select) == [
        {:field, [{:bind, 0}, "bar"]},
        {:field, [{:bind, 0}, {:unsafe, "qux"}]}
      ]
    end

    test "multiple bindings" do
      query = from("table1") |> select([p, q], [p["foo"], q["bar"]])
      assert %Query{select: %{1 => select}, aggregate?: false} = query
      assert List.flatten(select) == [
        {:field, [{:bind, 0}, "foo"]},
        {:field, [{:bind, 1}, "bar"]}
      ]
    end

    test "binary operator" do
      query = from("table1") |> select([p], p["foo"] + p["bar"])
      assert %Query{select: %{1 => select}, aggregate?: false} = query
      assert List.flatten(select) == [
        {{:non_agg, :+}, [
          {:field, [{:bind, 0}, "foo"]},
          {:field, [{:bind, 0}, "bar"]}
        ]}
      ]
    end

    test "binary operator with literal on one side" do
      query = from("table1") |> select([p], p["foo"] + 123)
      assert %Query{select: %{1 => select}, aggregate?: false} = query
      assert List.flatten(select) == [
        {{:non_agg, :+}, [
          {:field, [{:bind, 0}, "foo"]},
          123
        ]}
      ]
    end

    test "binary operator with literal on one side and aggregation on the other" do
      query = from("table1") |> select([p], avg(p["foo"]) * 123)
      assert %Query{select: %{1 => select}, aggregate?: true} = query
      assert List.flatten(select) == [
        {{:agg, :*}, [
          {{:agg, :avg}, [
            {:field, [{:bind, 0}, "foo"]}
          ]},
          123
        ]}
      ]
    end

    test "binary operator with aggregation on both sides" do
      query = from("table1") |> select([p], sum(p["foo"]) - avg(p["bar"]))
      assert %Query{select: %{1 => select}, aggregate?: true} = query
      assert List.flatten(select) == [
        {{:agg, :-}, [
          {{:agg, :sum}, [
            {:field, [{:bind, 0}, "foo"]}
          ]},
          {{:agg, :avg}, [
            {:field, [{:bind, 0}, "bar"]}
          ]},
        ]}
      ]
    end

    test "binary operator with aggregation on one side and non-aggregation non-literal expression on the other" do
      assert_compile_time_raise ArgumentError, "Can't mix aggregation and non-aggregation in one formula.", fn ->
        import Endo.Query
        from("table1") |> select([p], avg(p["foo"]) + p["bar"] * 10)
      end
    end

    test "aggregation on a field" do
      query = from("table1") |> select([p], sum(p["foo"]))
      assert %Query{select: %{1 => select}, aggregate?: true} = query
      assert List.flatten(select) == [
        {{:agg, :sum}, [
          {:field, [{:bind, 0}, "foo"]}
        ]}
      ]
    end

    test "aggregation on calculation" do
      query = from("table1") |> select([p], sum(p["foo"] + p["bar"]))
      assert %Query{select: %{1 => select}, aggregate?: true} = query
      assert List.flatten(select) == [
        {{:agg, :sum}, [
          {{:non_agg, :+}, [
            {:field, [{:bind, 0}, "foo"]},
            {:field, [{:bind, 0}, "bar"]}
          ]}
        ]}
      ]
    end

    test "aggregation on aggregation" do
      assert_compile_time_raise ArgumentError, "Can't apply aggregation on aggregation.", fn ->
        import Endo.Query
        from("table1") |> select([p], sum(sum(p["foo"])))
      end
    end

    test "aggregation with extra args" do
      query = from("table1") |> select([p], percentile_cont(0.95, p["foo"]))
      assert %Query{select: %{1 => select}, aggregate?: true} = query
      assert List.flatten(select) == [
        {{:agg, :percentile_cont}, [
          0.95,
          {:field, [{:bind, 0}, "foo"]}
        ]}
      ]
    end

    test "aggregation with wrong number of args" do
      assert_compile_time_raise ArgumentError, "sum takes 1 argument(s) but 2 is given.", fn ->
        import Endo.Query
        from("table1") |> select([p], sum(0.95, p["foo"]))
      end
    end

    test "chaining" do
      query = from("table1")
              |> select([q], q["foo"])
              |> select([r], [r["bar"], r["baz"]])

      assert %Query{select: %{1 => select}, aggregate?: false} = query
      assert List.flatten(select) == [
        {:field, [{:bind, 0}, "foo"]},
        {:field, [{:bind, 0}, "bar"]},
        {:field, [{:bind, 0}, "baz"]}
      ]
    end
  end

  describe "where/3" do
    test "happy case" do
      query = from("table1") |> where([q], q["foo"] > 10)
      assert %Query{
        where: %{1 => {{:non_agg, :>}, [
          {:field, [{:bind, 0}, "foo"]},
          10
        ]}}
      } = query
    end

    test "aggregation" do
      assert_compile_time_raise ArgumentError, "Where condition must not contain aggregation.", fn ->
        import Endo.Query
        from("table1") |> where([q], stddev_samp(q["foo"]) > 0.5)
      end
    end

    test "chaining" do
      query = from("table1")
              |> where([q], q["foo"] > 10)
              |> where([r], r["bar"] <= 100)

      assert %Query{
        where: %{1 => {{:non_agg, :and}, [
          {{:non_agg, :>}, [{:field, [{:bind, 0}, "foo"]}, 10]},
          {{:non_agg, :<=}, [{:field, [{:bind, 0}, "bar"]}, 100]}
        ]}}
      } = query
    end
  end

  describe "order_by/3" do
    test "non-aggregation" do
      f = "qux"
      query = from("table1") |> order_by([q], [q["foo"], asc: q["bar"], desc: q["baz"], asc: q[^f] + q["xxx"]])
      assert %Query{
        order_by: %{1 => [
          asc: {:field, [{:bind, 0}, "foo"]},
          asc: {:field, [{:bind, 0}, "bar"]},
          desc: {:field, [{:bind, 0}, "baz"]},
          asc: {{:non_agg, :+}, [
            {:field, [{:bind, 0}, {:unsafe, "qux"}]},
            {:field, [{:bind, 0}, "xxx"]}
          ]}
        ]}
      } = query
    end

    test "aggregation" do
      assert_compile_time_raise ArgumentError, "Aggregations are not allowed in order_by.", fn ->
        import Endo.Query
        from("table1") |> order_by([q], sum(q["foo"]))
      end
    end
  end

  describe "limit/2" do
    test "literal" do
      query = from("table1") |> limit(10)
      assert %Query{limit: 10} = query
    end

    test "pinned expression" do
      f = fn -> 10 end
      query = from("table1") |> limit(^f.())
      assert %Query{limit: {:unsafe, 10}} = query
    end
  end

  describe "offset/2" do
    test "literal" do
      query = from("table1") |> offset(10)
      assert %Query{offset: 10} = query
    end

    test "pinned expression" do
      f = fn -> 10 end
      query = from("table1") |> offset(^f.())
      assert %Query{offset: {:unsafe, 10}} = query
    end
  end

  describe "group_by/3" do
    test "non-aggregate" do
      query = from("table1") |> group_by([q], q["foo"] + q["bar"])
      assert %Query{group_by: {1, [{{:non_agg, :+}, [
        {:field, [{:bind, 0}, "foo"]},
        {:field, [{:bind, 0}, "bar"]}
      ]}]}} = query
    end

    test "aggregate" do
      assert_compile_time_raise ArgumentError, "Aggregations must not appear in group_by.", fn ->
        import Endo.Query
        from("table1") |> group_by([q], percentile_cont(0.95, q["foo"]))
      end
    end

    test "multiple expressions" do
      query = from("table1") |> group_by([q], [q["foo"], q["bar"]])
      assert %Query{group_by: {1, [
        {:field, [{:bind, 0}, "foo"]},
        {:field, [{:bind, 0}, "bar"]}
      ]}} = query
    end
  end

  describe "having/3" do
    test "aggregate" do
      query = from("table1") |> having([q], count_distinct(q["foo"]) >= 123)
      assert%Query{having:  {1, {{:agg, :>=}, [
        {{:agg, :count_distinct}, [
          {:field, [{:bind, 0}, "foo"]}
        ]},
        123
      ]}}} = query
    end

    test "non-aggregate" do
      assert_compile_time_raise ArgumentError, "Only an aggregation expression is allowed in having clause.", fn ->
        import Endo.Query
        from("table1") |> having([q], q["foo"] > q["bar"])
      end
    end
  end

  describe "join/5" do
    test "join 1 table" do
      query = from("table1")
              |> join(:inner, [p], q in "table2", on: p["foo"] == q["bar"])
      assert %Query{
        from: "table1",
        aliases_count: 2,
        join: %{2 => join}
      } = query

      assert join ==
        {:inner, {
          "table2",
          {{:non_agg, :==}, [
            {:field, [{:bind, 0}, "foo"]},
            {:field, [{:bind, 1}, "bar"]}
          ]}
        }}
    end

    test "join multiple tables" do
      query = from("table1")
              |> join(:inner, [p], q in "table2", on: p["foo"] == q["bar"])
              |> join(:left, [p, ...], r in "table3", on: p["baz"] == r["qux"])
              |> join(:right, [..., x], y in "table4", on: x["xxx"] == y["yyy"])
      assert %Query{
        from: "table1",
        aliases_count: 4,
        join: %{
          2 => join2,
          3 => join3,
          4 => join4
        }
      } = query

      assert join2 ==
        {:inner, {
          "table2",
          {{:non_agg, :==}, [
            {:field, [{:bind, 0}, "foo"]},
            {:field, [{:bind, 1}, "bar"]}
          ]}
        }}

      assert join3 ==
        {:left, {
          "table3",
          {{:non_agg, :==}, [
            {:field, [{:bind, 0}, "baz"]},
            {:field, [{:bind, -1}, "qux"]}
          ]}
        }}

      assert join4 ==
        {:right, {
          "table4",
          {{:non_agg, :==}, [
            {:field, [{:bind, -2}, "xxx"]},
            {:field, [{:bind, -1}, "yyy"]}
          ]}
        }}
    end

    test "join a query" do
      query1 = from("table1")
      query2 = from("table1")
               |> join(:inner, [p], q in ^query1, on: p["foo"] == q["bar"])

      assert %Query{
        aliases_count: 2,
        join: %{2 => join}
      } = query2

      assert join ==
        {:inner, {
          {:unsafe, from("table1")},
          {{:non_agg, :==}, [
            {:field, [{:bind, 0}, "foo"]},
            {:field, [{:bind, 1}, "bar"]}
          ]}
        }}
    end

    test "add selection after join" do
      query = from("table1")
              |> select([r1], r1["foo"])
              |> join(:left, [r1], r2 in "table2", on: r1["bar"] == r2["baz"])
              |> select([..., r2], r2["qux"])
              |> join(:right, [r1, ...], r3 in "table3", on: r1["abc"] == r3["efg"])
              |> select([..., r3], r3["hij"])

      assert %Query{
        aliases_count: 3,
        join: %{
          2 => _join2,
          3 => _join3
        },
        select: %{
          1 => select1,
          2 => select2,
          3 => select3
        }
      } = query

      assert select1 == [
        {:field, [{:bind, 0}, "foo"]}
      ]

      assert select2 == [
        {:field, [{:bind, -1}, "qux"]}
      ]

      assert select3 == [
        {:field, [{:bind, -1}, "hij"]}
      ]
    end
  end
end
