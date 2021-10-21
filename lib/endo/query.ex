defmodule Endo.Query do
  @opaque t :: %__MODULE__{}
  defstruct [
    from: nil,
    select: [],
    where: nil,
    order_by: nil,
    limit: nil,
    offset: nil,
    group_by: nil,
    having: nil,
    join: [],
    tables_count: 0  # from +1, every join +1, subquery starts from 0
  ]

  defdelegate to_sql(query, opts \\ []), to: Endo.Query.Builder, as: :build_sql

  defguardp is_literal(x)
    when is_number(x)
    or is_binary(x)
    or is_boolean(x)

  @doc """
  # Example

      from("my_table")

  """
  defmacro from(table) do
    table = resolve_bind(table, [], nil)
    quote bind_quoted: [table: table] do
      %Endo.Query{
        from: table,
        tables_count: 1
      }
    end
  end

  @doc """
  # Example

  ## Single field

      select(query, [q], q["foo"])

  ## Dynamic field name

      field = "foo"
      select(query, [q], q[^field])

  ## Multiple fields

      select(query, [q], [ q["foo"], q["bar"] ])

  ## Expression

      select(query, [q], q["foo"] * 100)

  ## Aggregation

      select(query, [q], sum(q["foo"] * q["bar"]))

  or

      select(query, [q], percentile_cont(q["foo"], 0.9) / count(q["bar"]) * 100)
  """
  defmacro select(query, binds, selections) do
    # binds: [{:q, [], Elixir}, {:..., [], Elixir}, {:r, [], Elixir}]
    # selections: expr | [expr]
    # expr: { {:., [], [Access, :get]}, [], [bind, field_name] }
    #    or {operator, _, [expr, expr]}
    #    or {:agg, _, [String.t(), expr*]}  (none of the sub exprs should be an aggregation)
    #    or {:fragment, _, [String.t(), expr*]}

    binds_with_index = index_binds(binds)

    selections =
      selections
      |> List.wrap()
      |> Enum.map(&resolve_bind(&1, binds_with_index, get_tables_count(query, __CALLER__)))

    quote bind_quoted: [query: query, selections: selections] do
      %{query | select: [query.select, selections]}
    end
  end

  @doc """
  # Example

      from("my_table") |> where([q], q["foo"] > 123)

  """
  defmacro where(query, binds, condition) do
    binds_with_index = index_binds(binds)
    condition = resolve_bind(condition, binds_with_index, get_tables_count(query, __CALLER__))

    if aggregation?(condition) do
      raise ArgumentError, "Where condition must not contain aggregation."
    end

    quote bind_quoted: [query: query, condition: condition] do
      combined_condition =
        case query.where do
          nil -> condition
          existing_condition -> {{:non_agg, :and}, [existing_condition, condition]}
        end

      %{query | where: combined_condition}
    end
  end

  @doc """
  # Example

      from("my_table") |> order_by([q], asc: q["foo"], desc: q["bar"])

  """
  defmacro order_by(query, binds, exprs) do
    binds_with_index = index_binds(binds)
    exprs = exprs
            |> List.wrap()
            |> Enum.map(&build_order(&1, binds_with_index, get_tables_count(query, __CALLER__)))

    if exprs |> Enum.map(&elem(&1, 1)) |> has_aggregation?() do
      raise ArgumentError, "Aggregations are not allowed in order_by."
    end

    quote bind_quoted: [query: query, exprs: exprs] do
      %{query | order_by: exprs}
    end
  end

  defp build_order({direction, expr}, binds_with_index, tables_count) when direction in [:asc, :desc] do
    expr = resolve_bind(expr, binds_with_index, tables_count)
    {direction, expr}
  end

  defp build_order(expr, binds_with_index, tables_count) do
    expr = resolve_bind(expr, binds_with_index, tables_count)
    {:asc, expr}
  end


  @doc """
  # Example

      from("my_table") |> limit(10)

  """
  defmacro limit(query, expr) do
    expr = resolve_bind(expr, [], nil)
    quote bind_quoted: [query: query, expr: expr] do
      %{query | limit: expr}
    end
  end

  @doc """
  # Example

      from("my_table") |> offset(10)

  """
  defmacro offset(query, expr) do
    expr = resolve_bind(expr, [], nil)
    quote bind_quoted: [query: query, expr: expr] do
      %{query | offset: expr}
    end
  end

  @doc """
  # Example

      from("my_table")
      |> select([q], [q["foo"], q["bar"], sum(q["qux"])])
      |> group_by([q], [ q["foo"], q["bar"] ])

  """
  defmacro group_by(query, binds, exprs) do
    binds_with_index = index_binds(binds)
    exprs = exprs
            |> List.wrap()
            |> Enum.map(&resolve_bind(&1, binds_with_index, get_tables_count(query, __CALLER__)))

    if has_aggregation?(exprs) do
      raise ArgumentError, "Aggregations must not appear in group_by."
    end

    quote bind_quoted: [query: query, exprs: exprs] do
      %{query | group_by: exprs}
    end
  end

  defmacro having(query, binds, expr) do
    binds_with_index = index_binds(binds)
    expr = resolve_bind(expr, binds_with_index, get_tables_count(query, __CALLER__))

    unless aggregation?(expr) do
      raise ArgumentError, "Only an aggregation expression is allowed in having clause."
    end

    quote bind_quoted: [query: query, expr: expr] do
      %{query | having: expr}
    end
  end

  @doc """
  # Example

      from("table1")
      |> join(:inner, [p, ...], q in "table2", on: p["foo"] == q["bar"])
  """
  defmacro join(query, qual, parent_binds, {:in, _, [child_bind, subquery]}, on: expr) do
    binds_with_index = index_binds(parent_binds ++ [child_bind])
    tables_count = get_tables_count(query, __CALLER__)
    expr = resolve_bind(expr, binds_with_index, tables_count + 1)
    subquery = resolve_bind(subquery, [], nil)
    quote bind_quoted: [query: query, expr: expr, qual: qual, subquery: subquery, tables_count: tables_count] do
      %{query | join: [query.join, {qual, {subquery, expr}}], tables_count: query.tables_count + 1}
    end
  end

  defp index_binds(binds) do
    {leading, trailing} = Enum.split_while(binds, &!match?({:..., _, _}, &1))

    leading =
      leading
      |> Enum.with_index()
      |> Map.new()

    trailing =
      trailing
      |> Enum.reverse()
      |> Enum.with_index(fn ast, i -> {ast, -i - 1} end)
      |> Map.new()

    Map.merge(leading, trailing)
  end

  # literals
  defp resolve_bind(literal, _binds_with_index, _tables_count) when is_literal(literal) do
    literal
  end

  # ^expr
  defp resolve_bind({:^, _, [child]}, _binds_with_index, _tables_count) do
    {:unsafe, child}
  end

  # q[something]
  defp resolve_bind({
    {:., _, [Access, :get]},
    _,
    [bind, things_inside_brackets]
  }, binds_with_index, tables_count) do
    {:field, [
      {:bind, get_bind_index!(binds_with_index, bind, tables_count)},
      resolve_bind(things_inside_brackets, binds_with_index, tables_count)
    ]}
  end

  @aggregations [
    sum: 1,
    avg: 1,
    min: 1,
    max: 1,
    count: 1,
    count_distinct: 1,
    stddev_samp: 1,
    stddev_pop: 1,
    var_samp: 1,
    var_pop: 1,
    percentile_cont: 2,
    median: 1
  ]

  @aggregation_names Keyword.keys(@aggregations)

  # sum(q["foo"])
  defp resolve_bind({fun, _, children}, binds_with_index, tables_count) when fun in @aggregation_names do
    if length(children) != @aggregations[fun] do
      raise ArgumentError, "#{fun} takes #{@aggregations[fun]} argument(s) but #{length(children)} is given."
    end
    children = Enum.map(children, &resolve_bind(&1, binds_with_index, tables_count))
    if has_aggregation?(children) do
      raise ArgumentError, "Can't apply aggregation on aggregation."
    end
    {{:agg, fun}, children}
  end

  @non_aggregations [
    like: 2,
    ilike: 2,
    is_nil: 1,
    "+": 2,
    "-": 2,
    "*": 2,
    "/": 2,
    "==": 2,
    "!=": 2,
    "<": 2,
    "<=": 2,
    ">": 2,
    ">=": 2,
    "=~": 2,
    "<>": 2,  # String concatenation
    in: 2,
    and: 2,
    or: 2,
    not: 1
  ]

  @non_aggregation_names Keyword.keys(@non_aggregations)

  defp resolve_bind({fun, _, children}, binds_with_index, tables_count) when fun in @non_aggregation_names do
    if length(children) != @non_aggregations[fun] do
      raise ArgumentError, "#{fun} takes #{@non_aggregations[fun]} argument(s) but #{length(children)} is given."
    end
    children = Enum.map(children, &resolve_bind(&1, binds_with_index, tables_count))
    cond do
      static_and_aggregations_only?(children) ->
        {{:agg, fun}, children}
      has_aggregation?(children) ->
        raise ArgumentError, "Can't mix aggregation and non-aggregation in one formula."
      true ->
        {{:non_agg, fun}, children}
    end
  end

  defp get_bind_index!(binds_with_index, bind, tables_count) do
    case binds_with_index[bind] do
      nil -> raise ArgumentError, "Binding #{elem(bind, 0)} is not defined"
      index when index < 0 -> tables_count + index
      index -> index
    end
  end

  defp aggregation?(node) do
    match?({{:agg, _}, _}, node)
  end

  defp literal?(node) when is_literal(node), do: true
  defp literal?(_node), do: false

  defp static_and_aggregations_only?(nodes) do
    IO.puts("============")
    IO.inspect(nodes)
    Enum.all?(nodes, & static?(&1) or aggregation?(&1))
  end

  defp has_aggregation?(nodes) do
    Enum.any?(nodes, &aggregation?/1)
  end

  defp pinned_expression?({:^, _, [_]}), do: true
  defp pinned_expression?({:unsafe, _}), do: true
  defp pinned_expression?(_), do: false

  defp static?(expression) do
    literal?(expression) or pinned_expression?(expression)
  end

  defp get_tables_count(quoted_query, env) do
    %{tables_count: tables_count} =
      quoted_query
      |> Code.eval_quoted([], env)
      |> elem(0)
    tables_count
  end
end
