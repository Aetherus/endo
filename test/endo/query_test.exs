defmodule Endo.QueryTest do
  use Endo.TestCase, async: true

  import Endo.Query
  alias Endo.Query

  describe "from/1" do
    test "with string literal arg" do
      query = from("foo")
      assert %Query{from: "foo"} = query
    end

    test "with variable as the arg" do
      table = "foo"
      query = from(table)
      assert %Query{from: "foo"} = query
    end
  end

  describe "select/3" do
    test "single fixed field" do
      query = %Query{} |> select([q], q["bar"])
      assert %Query{select: [
        {:field, [{:bind, 0}, "bar"]}
      ]} = query
    end

    test "single dynamic field" do
      f = fn -> "bar" end
      query = %Query{} |> select([q], q[^f.()])
      assert %Query{select: [
        {:field, [{:bind, 0}, "bar"]}
      ]} = query
    end

    test "multiple fields" do
      f = fn -> "qux" end
      query = %Query{} |> select([q], [q["bar"], q[^f.()]])
      assert %Query{select: [
        {:field, [{:bind, 0}, "bar"]},
        {:field, [{:bind, 0}, "qux"]}
      ]} = query
    end

    test "multiple bindings" do
      query = %Query{} |> select([p, q], [p["foo"], q["bar"]])
      assert %Query{select: [
        {:field, [{:bind, 0}, "foo"]},
        {:field, [{:bind, 1}, "bar"]}
      ]} = query
    end

    test "multiple bindings with ..." do
      query = %Query{} |> select([p, ..., q], [p["foo"], q["bar"]])
      assert %Query{select: [
        {:field, [{:bind, 0}, "foo"]},
        {:field, [{:bind, -1}, "bar"]}
      ]} = query
    end

    test "binary operator" do
      query = %Query{} |> select([p], p["foo"] + p["bar"])
      assert %Query{select: [
        {{:arith, :+}, [
          {:field, [{:bind, 0}, "foo"]},
          {:field, [{:bind, 0}, "bar"]}
        ]}
      ]} = query
    end

    test "binary operator with literal on one side" do
      query = %Query{} |> select([p], p["foo"] + 123)
      assert %Query{select: [
        {{:arith, :+}, [
          {:field, [{:bind, 0}, "foo"]},
          123
        ]}
      ]} = query
    end

    test "binary operator with literal on one side and aggregation on the other" do
      query = %Query{} |> select([p], avg(p["foo"]) * 123)
      assert %Query{select: [
        {{:agg, :*}, [
          {{:agg, :avg}, [
            {:field, [{:bind, 0}, "foo"]}
          ]},
          123
        ]}
      ]} = query
    end

    test "binary operator with aggregation on both sides" do
      query = %Query{} |> select([p], sum(p["foo"]) - avg(p["bar"]))
      assert %Query{select: [
        {{:agg, :-}, [
          {{:agg, :sum}, [
            {:field, [{:bind, 0}, "foo"]}
          ]},
          {{:agg, :avg}, [
            {:field, [{:bind, 0}, "bar"]}
          ]},
        ]}
      ]} = query
    end

    test "binary operator with aggregation on one side and non-aggregation non-literal expression on the other" do
      assert_compile_time_raise ArgumentError, "Can't mix aggregation and non-aggregation in one formula.", fn ->
        import Endo.Query
        %Endo.Query{} |> select([p], avg(p["foo"]) + p["bar"] * 10)
      end
    end

    test "aggregation on a field" do
      query = %Query{} |> select([p], sum(p["foo"]))
      assert %Query{select: [
        {{:agg, :sum}, [
          {:field, [{:bind, 0}, "foo"]}
        ]}
      ]} = query
    end

    test "aggregation on calculation" do
      query = %Query{} |> select([p], sum(p["foo"] + p["bar"]))
      assert %Query{select: [
        {{:agg, :sum}, [
          {{:arith, :+}, [
            {:field, [{:bind, 0}, "foo"]},
            {:field, [{:bind, 0}, "bar"]}
          ]}
        ]}
      ]} = query
    end

    test "aggregation on aggregation" do
      assert_compile_time_raise ArgumentError, "Can't apply aggregation on aggregation.", fn ->
        import Endo.Query
        %Endo.Query{} |> select([p], sum(sum(p["foo"])))
      end
    end

    test "aggregation with extra args" do
      query = %Query{} |> select([p], percentile_cont(0.95, p["foo"]))
      assert %Query{select: [
        {{:agg, :percentile_cont}, [
          0.95,
          {:field, [{:bind, 0}, "foo"]}
        ]}
      ]} = query
    end

    test "aggregation with wrong number of args" do
      assert_compile_time_raise ArgumentError, "sum takes 1 argument(s) but 2 is given.", fn ->
        import Endo.Query
        query = %Endo.Query{} |> select([p], sum(0.95, p["foo"]))
      end
    end

    test "chaining" do
      query = %Query{}
              |> select([q], q["foo"])
              |> select([r], [r["bar"], r["baz"]])

      assert %Query{select: [
        {:field, [{:bind, 0}, "foo"]},
        {:field, [{:bind, 0}, "bar"]},
        {:field, [{:bind, 0}, "baz"]}
      ]} = query
    end
  end

  describe "where/3" do
    test "happy case" do
      query = %Query{} |> where([q], q["foo"] > 10)
      assert %Query{
        where: {{:arith, :>}, [
          {:field, [{:bind, 0}, "foo"]},
          10
        ]}
      } = query
    end

    test "aggregation" do
      assert_compile_time_raise ArgumentError, "Where condition must not contain aggregation.", fn ->
        import Endo.Query
        %Endo.Query{} |> where([q], stddev_samp(q["foo"]) > 0.5)
      end
    end

    test "chaining" do
      query = %Query{}
              |> where([q], q["foo"] > 10)
              |> where([r], r["bar"] <= 100)

      assert %Query{
        where: {{:arith, :and}, [
          {{:arith, :>}, [
            {:field, [{:bind, 0}, "foo"]},
            10
          ]},
          {{:arith, :<=}, [
            {:field, [{:bind, 0}, "bar"]},
            100
          ]}
        ]
      }} = query
    end
  end

  describe "order_by/3" do
    test "non-aggregation" do
      query = %Query{} |> order_by([q], q["foo"])
      assert %Query{
        order_by: [
          {:field, [{:bind, 0}, "foo"]}
        ]
      } = query
    end

    test "aggregation" do
      assert_compile_time_raise ArgumentError, "Aggregations are not allowed in order_by.", fn ->
        import Endo.Query
        %Endo.Query{} |> order_by([q], sum(q["foo"]))
      end
    end
  end
end
