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
      query = from("foo") |> select([q], q["bar"])
      assert %Query{select: [
        {{:bind, 0}, "bar"}
      ]} = query
    end

    test "single dynamic field" do
      f = fn -> "bar" end
      query = from("foo") |> select([q], q[^f.()])
      assert %Query{select: [
        {{:bind, 0}, "bar"}
      ]} = query
    end

    test "multiple fields" do
      f = fn -> "qux" end
      query = from("foo") |> select([q], [q["bar"], q[^f.()]])
      assert %Query{select: [
        {{:bind, 0}, "bar"},
        {{:bind, 0}, "qux"}
      ]} = query
    end

    test "multiple bindings" do
      query = from("foo") |> select([p, q], [p["foo"], q["bar"]])
      assert %Query{select: [
        {{:bind, 0}, "foo"},
        {{:bind, 1}, "bar"}
      ]} = query
    end

    test "multiple bindings with ..." do
      query = from("foo") |> select([p, ..., q], [p["foo"], q["bar"]])
      assert %Query{select: [
        {{:bind, 0}, "foo"},
        {{:bind, -1}, "bar"}
      ]} = query
    end

    test "binary operator" do
      query = from("foo") |> select([p], p["foo"] + p["bar"])
      assert %Query{select: [
        {{:arith, :+}, [
          {{:bind, 0}, "foo"},
          {{:bind, 0}, "bar"}
        ]}
      ]} = query
    end

    test "binary operator with literal on one side" do
      query = from("foo") |> select([p], p["foo"] + 123)
      assert %Query{select: [
        {{:arith, :+}, [
          {{:bind, 0}, "foo"},
          123
        ]}
      ]} = query
    end

    test "binary operator with literal on one side and aggregation on the other" do
      query = from("foo") |> select([p], avg(p["foo"]) * 123)
      assert %Query{select: [
        {{:agg, :*}, [
          {{:agg, :avg}, [
            {{:bind, 0}, "foo"}
          ]},
          123
        ]}
      ]} = query
    end

    test "binary operator with aggregation on both sides" do
      query = from("foo") |> select([p], sum(p["foo"]) - avg(p["bar"]))
      assert %Query{select: [
        {{:agg, :-}, [
          {{:agg, :sum}, [
            {{:bind, 0}, "foo"}
          ]},
          {{:agg, :avg}, [
            {{:bind, 0}, "bar"}
          ]},
        ]}
      ]} = query
    end

    test "binary operator with aggregation on one side and non-aggregation non-literal expression on the other" do
      assert_compile_time_raise ArgumentError, "Can't mix aggregation and non-aggregation in one formula.", fn ->
        import Endo.Query
        from("foo") |> select([p], avg(p["foo"]) + p["bar"] * 10)
      end
    end

    test "aggregation on a field" do
      query = from("foo") |> select([p], sum(p["foo"]))
      assert %Query{select: [
        {{:agg, :sum}, [
          {{:bind, 0}, "foo"}
        ]}
      ]} = query
    end

    test "aggregation on calculation" do
      query = from("foo") |> select([p], sum(p["foo"] + p["bar"]))
      assert %Query{select: [
        {{:agg, :sum}, [
          {{:arith, :+}, [
            {{:bind, 0}, "foo"},
            {{:bind, 0}, "bar"}
          ]}
        ]}
      ]} = query
    end

    test "aggregation on aggregation" do
      assert_compile_time_raise ArgumentError, "Can't apply aggregation on aggregation.", fn ->
        import Endo.Query
        from("foo") |> select([p], sum(sum(p["foo"])))
      end
    end

    test "aggregation with extra args" do
      query = from("foo") |> select([p], percentile_cont(0.95, p["foo"]))
      assert %Query{select: [
        {{:agg, :percentile_cont}, [
          0.95,
          {{:bind, 0}, "foo"}
        ]}
      ]} = query
    end

    test "aggregation with wrong number of args" do
      assert_compile_time_raise ArgumentError, "sum takes 1 argument(s) but 2 is given.", fn ->
        import Endo.Query
        query = from("foo") |> select([p], sum(0.95, p["foo"]))
      end
    end
  end
end
