defmodule Endo.Query do
  @type ast :: number() |
               boolean() |
               String.t() |
               {:unsafe, ast()} |
               {:field, [{:bind, pos_integer()} | ast()]} |
               {{:agg, atom()}, [ast()]} |
               {{:non_agg, atom()}, [ast()]}

  @type qual :: :inner | :left | :right

  @type t :: %__MODULE__{
    from: String.t() | {:unsafe, String.t()},
    select: %{optional(pos_integer()) => [ast() | [ast()]]},
    where: %{optional(pos_integer()) => [ast() | [ast()]]},
    order_by: %{optional(pos_integer()) => [ast() | [ast()]]},
    limit: nil | non_neg_integer() | {:unsafe, non_neg_integer()},
    offset: nil | non_neg_integer() | {:unsafe, non_neg_integer()},
    group_by: {pos_integer(), ast()},
    having: {pos_integer(), ast()},
    join: %{optional(pos_integer()) => {qual(), {String.t() | t() | {:unsafe, t()}, ast()}}},
    aliases_count: non_neg_integer(),
    aggregate?: boolean()
  }

  defstruct [
    from: nil,
    select: %{},
    where: %{},
    order_by: %{},
    limit: nil,
    offset: nil,
    group_by: nil,
    having: nil,
    join: %{},
    aliases_count: 0,
    aggregate?: false
  ]

  defdelegate to_sql(query, opts \\ []), to: Endo.Query.SQLBuilder, as: :build_sql

  defguardp is_literal(x)
    when is_number(x)
    or is_binary(x)
    or is_boolean(x)

  @doc """
  # Example

      from("my_table")

  """
  defmacro from(table) do
    table = resolve(table, [])
    quote do
      %Endo.Query{
        from: unquote(table),
        aliases_count: 1
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
      |> Enum.map(&resolve(&1, binds_with_index))

    is_aggregate = Enum.any?(selections, &aggregation?/1)

    quote bind_quoted: [query: query, selections: selections, is_aggregate: is_aggregate] do
      aliases_count = query.aliases_count
      select = query.select
      existing_selections = select[aliases_count]
      new_selections =
        if existing_selections do
          [existing_selections | selections]
        else
          selections
        end
      new_select = Map.put(select, aliases_count, new_selections)
      %{query | select: new_select, aggregate?: is_aggregate or query.aggregate?}
    end
  end

  @doc """
  # Example

      from("my_table") |> where([q], q["foo"] > 123)

  """
  defmacro where(query, binds, condition) do
    binds_with_index = index_binds(binds)

    condition = resolve(condition, binds_with_index)

    if aggregation?(condition) do
      raise ArgumentError, "Where condition must not contain aggregation."
    end

    quote bind_quoted: [query: query, condition: condition] do
      aliases_count = query.aliases_count
      where = query.where
      existing_condition = where[aliases_count]
      condition = if existing_condition do
        {{:non_agg, :and}, [existing_condition, condition]}
      else
        condition
      end
      %{query | where: Map.put(where, aliases_count, condition)}
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
            |> Enum.map(&build_order(&1, binds_with_index))

    if exprs |> Enum.map(&elem(&1, 1)) |> has_aggregation?() do
      raise ArgumentError, "Aggregations are not allowed in order_by."
    end

    quote bind_quoted: [query: query, exprs: exprs] do
      aliases_count = query.aliases_count
      order_by = query.order_by
      existing_orders = order_by[aliases_count]
      orders = if existing_orders, do: [existing_orders | exprs], else: exprs
      %{query | order_by: Map.put(order_by, aliases_count, orders)}
    end
  end

  defp build_order({direction, expr}, binds_with_index) when direction in [:asc, :desc] do
    expr = resolve(expr, binds_with_index)
    {direction, expr}
  end

  defp build_order(expr, binds_with_index) do
    expr = resolve(expr, binds_with_index)
    {:asc, expr}
  end


  @doc """
  # Example

      from("my_table") |> limit(10)

  """
  defmacro limit(query, expr) do
    expr = resolve(expr, [])
    quote bind_quoted: [query: query, expr: expr] do
      %{query | limit: expr}
    end
  end

  @doc """
  # Example

      from("my_table") |> offset(10)

  """
  defmacro offset(query, expr) do
    expr = resolve(expr, [])
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
            |> Enum.map(&resolve(&1, binds_with_index))
    if has_aggregation?(exprs) do
      raise ArgumentError, "Aggregations must not appear in group_by."
    end

    quote bind_quoted: [query: query, exprs: exprs] do
      if query.group_by do
        raise ArgumentError, "GROUP BY clause has already been set."
      end
      %{query | group_by: {query.aliases_count, exprs}}
    end
  end

  defmacro having(query, binds, expr) do
    binds_with_index = index_binds(binds)
    expr = resolve(expr, binds_with_index)

    unless aggregation?(expr) do
      raise ArgumentError, "Only an aggregation expression is allowed in having clause."
    end

    quote bind_quoted: [query: query, expr: expr] do
      if query.having do
        raise ArgumentError, "HAVING clause has already been set."
      end
      %{query | having: {query.aliases_count, expr}}
    end
  end

  @doc """
  # Example

      from("table1")
      |> join(:inner, [p, ...], q in "table2", on: p["foo"] == q["bar"])
  """
  defmacro join(query, qual, parent_binds, {:in, _, [child_bind, subquery]}, on: expr) when qual in [:left, :right, :inner] do
    binds_with_index = index_binds(parent_binds ++ [child_bind])
    expr = resolve(expr, binds_with_index)
    subquery = resolve(subquery, [])
    quote bind_quoted: [query: query, qual: qual, subquery: subquery, expr: expr] do
      aliases_count = query.aliases_count + 1
      %{query | aliases_count: aliases_count, join: Map.put(query.join, aliases_count, {qual, {subquery, expr}})}
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
  defp resolve(literal, _binds_with_index) when is_literal(literal) do
    literal
  end

  # ^expr
  defp resolve({:^, _, [child]}, _binds_with_index) do
    {:unsafe, child}
  end

  # q[something]
  defp resolve({
    {:., _, [Access, :get]},
    _,
    [bind, things_inside_brackets]
  }, binds_with_index) do
    {:field, [
      {:bind, Map.fetch!(binds_with_index, bind)},
      resolve(things_inside_brackets, binds_with_index)
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
  defp resolve({fun, _, children}, binds_with_index) when fun in @aggregation_names do
    if length(children) != @aggregations[fun] do
      raise ArgumentError, "#{fun} takes #{@aggregations[fun]} argument(s) but #{length(children)} is given."
    end
    children = Enum.map(children, &resolve(&1, binds_with_index))
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
    not: 1,
    extract: 2
  ]

  @non_aggregation_names Keyword.keys(@non_aggregations)

  defp resolve({fun, _, children}, binds_with_index) when fun in @non_aggregation_names do
    if length(children) != @non_aggregations[fun] do
      raise ArgumentError, "#{fun} takes #{@non_aggregations[fun]} argument(s) but #{length(children)} is given."
    end
    children = Enum.map(children, &resolve(&1, binds_with_index))
    cond do
      static_and_aggregations_only?(children) ->
        {{:agg, fun}, children}
      has_aggregation?(children) ->
        raise ArgumentError, "Can't mix aggregation and non-aggregation in one formula."
      true ->
        {{:non_agg, fun}, children}
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

  defp unsafe_expression?({:unsafe, _}), do: true
  defp unsafe_expression?(_), do: false

  defp static?(expression) do
    literal?(expression) or unsafe_expression?(expression)
  end
end
