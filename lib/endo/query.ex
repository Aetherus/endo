defmodule Endo.Query do
  @opaque t :: {pos_integer(), %__MODULE__{}}
  defstruct [
    from: nil,
    select: [],
    where: true,
    order_by: nil,
    limit: nil,
    offset: nil,
    group_by: nil,
    having: nil,
    join: []
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
    quote do
      {
        1,
        %Endo.Query{
          from: unquote(table)
        }
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

    {tables_count, quoted_query} = expand_until_tables_count_and_quoted_query(query)

    binds_with_index = index_binds(binds)

    selections =
      selections
      |> List.wrap()
      |> Enum.map(&resolve_bind(&1, binds_with_index, tables_count))

    quote do
      {unquote(tables_count), %{unquote(quoted_query) | select: [unquote(quoted_query).select, unquote(selections)]}}
    end
  end

  @doc """
  # Example

      from("my_table") |> where([q], q["foo"] > 123)

  """
  defmacro where(query, binds, condition) do
    binds_with_index = index_binds(binds)

    {tables_count, quoted_query} = expand_until_tables_count_and_quoted_query(query)

    condition = resolve_bind(condition, binds_with_index, tables_count)

    if aggregation?(condition) do
      raise ArgumentError, "Where condition must not contain aggregation."
    end

    quote do
      {
        unquote(tables_count),
        %{unquote(quoted_query) | where:
          {{:non_agg, :and}, [unquote(quoted_query).where, unquote(condition)]}
        }
      }
    end
  end

  @doc """
  # Example

      from("my_table") |> order_by([q], asc: q["foo"], desc: q["bar"])

  """
  defmacro order_by(query, binds, exprs) do
    binds_with_index = index_binds(binds)

    {tables_count, quoted_query} = expand_until_tables_count_and_quoted_query(query)

    exprs = exprs
            |> List.wrap()
            |> Enum.map(&build_order(&1, binds_with_index, tables_count))

    if exprs |> Enum.map(&elem(&1, 1)) |> has_aggregation?() do
      raise ArgumentError, "Aggregations are not allowed in order_by."
    end

    quote do
      {unquote(tables_count), %{unquote(quoted_query) | order_by: unquote(exprs)}}
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
    {tables_count, quoted_query} = expand_until_tables_count_and_quoted_query(query)
    expr = resolve_bind(expr, [], nil)
    quote do
      {unquote(tables_count), %{unquote(quoted_query) | limit: unquote(expr)}}
    end
  end

  @doc """
  # Example

      from("my_table") |> offset(10)

  """
  defmacro offset(query, expr) do
    {tables_count, quoted_query} = expand_until_tables_count_and_quoted_query(query)
    expr = resolve_bind(expr, [], nil)
    quote do
      {unquote(tables_count), %{unquote(quoted_query) | offset: unquote(expr)}}
    end
  end

  @doc """
  # Example

      from("my_table")
      |> select([q], [q["foo"], q["bar"], sum(q["qux"])])
      |> group_by([q], [ q["foo"], q["bar"] ])

  """
  defmacro group_by(query, binds, exprs) do
    {tables_count, quoted_query} = expand_until_tables_count_and_quoted_query(query)
    binds_with_index = index_binds(binds)
    exprs = exprs
            |> List.wrap()
            |> Enum.map(&resolve_bind(&1, binds_with_index, tables_count))
    if has_aggregation?(exprs) do
      raise ArgumentError, "Aggregations must not appear in group_by."
    end

    quote do
      {unquote(tables_count), %{unquote(quoted_query) | group_by: unquote(exprs)}}
    end
  end

  defmacro having(query, binds, expr) do
    {tables_count, quoted_query} = expand_until_tables_count_and_quoted_query(query)
    binds_with_index = index_binds(binds)
    expr = resolve_bind(expr, binds_with_index, tables_count)

    unless aggregation?(expr) do
      raise ArgumentError, "Only an aggregation expression is allowed in having clause."
    end

    quote do
      {unquote(tables_count), %{unquote(quoted_query) | having: unquote(expr)}}
    end
  end

  @doc """
  # Example

      from("table1")
      |> join(:inner, [p, ...], q in "table2", on: p["foo"] == q["bar"])
  """
  defmacro join(query, qual, parent_binds, {:in, _, [child_bind, subquery]}, on: expr) do
    {tables_count, quoted_query} = expand_until_tables_count_and_quoted_query(query)
    binds_with_index = index_binds(parent_binds ++ [child_bind])
    tables_count = tables_count + 1
    expr = resolve_bind(expr, binds_with_index, tables_count)
    subquery = resolve_bind(subquery, [], nil)
    quote do
      {unquote(tables_count), %{unquote(quoted_query) | join: [
        unquote(quoted_query).join, {unquote(qual), {unquote(subquery), unquote(expr)}}
      ]}}
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

  defp expand_until_tables_count_and_quoted_query(quoted) do
    quoted
    |> Stream.unfold(&{&1, Macro.expand_once(&1, __ENV__)})
    |> Enum.find(&match?({count, _} when is_integer(count), &1))
  end
end
