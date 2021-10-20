defmodule Endo.Query do
  defstruct [
    from: nil,
    select: [],
    where: nil,
    order_by: nil,
    limit: nil,
    offset: nil
  ]

  @doc """
  # Example

      from("my_table")

  """
  defmacro from(table) do
    quote do
      %Endo.Query{
        from: unquote(table)
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
      |> Enum.map(&resolve_bind(&1, binds_with_index))

    quote do
      %{unquote(query) | select: unquote(query).select ++ unquote(selections)}
    end
  end

  @doc """
  # Example

      from("my_table") |> where([q], q["foo"] > 123)

  """
  defmacro where(query, binds, condition) do
    binds_with_index = index_binds(binds)
    condition = resolve_bind(condition, binds_with_index)

    if match?({{:agg, _}, _}, condition) do
      raise ArgumentError, "Where condition must not contain aggregation."
    end

    quote bind_quoted: [query: query, condition: condition] do
      combined_condition =
        case query.where do
          nil -> condition
          existing_condition -> {{:arith, :and}, [existing_condition, condition]}
        end

      %{query | where: combined_condition}
    end
  end

  defmacro order_by(query, binds, exprs) do
    binds_with_index = index_binds(binds)
    exprs = exprs
            |> List.wrap()
            |> Enum.map(&build_order(&1, binds_with_index))

    if exprs |> Enum.map(&elem(&1, 1)) |> has_aggregation?() do
      raise ArgumentError, "Aggregations are not allowed in order_by."
    end

    quote bind_quoted: [query: query, exprs: exprs] do
      %{query | order_by: exprs}
    end
  end

  defp build_order({direction, expr}, binds_with_index) when direction in [:asc, :desc] do
    expr = resolve_bind(expr, binds_with_index)
    {direction, expr}
  end

  defp build_order(expr, binds_with_index) do
    expr = resolve_bind(expr, binds_with_index)
    {:asc, expr}
  end

  defmacro limit(query, expr) do
    quote bind_quoted: [query: query, expr: expr] do
      %{query | limit: expr}
    end
  end

  defmacro offset(query, expr) do
    quote bind_quoted: [query: query, expr: expr] do
      %{query | offset: expr}
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

  defguardp is_literal(x)
    when is_number(x)
    or is_binary(x)
    or is_boolean(x)

  # literals
  defp resolve_bind(literal, _binds_with_index) when is_literal(literal) do
    literal
  end

  # ^expr
  defp resolve_bind({:^, _, [child]}, _binds_with_index) do
    child
  end

  # q[something]
  defp resolve_bind({
    {:., _, [Access, :get]},
    _,
    [bind, things_inside_brackets]
  }, binds_with_index) do
    {:field, [
      {:bind, get_bind_index!(binds_with_index, bind)},
      resolve_bind(things_inside_brackets, binds_with_index)
    ]}
  end

  @operators ~w[
    + - * /
    < <= > >=
    == != =~
    and or not in
  ]a

  # q["foo"] + q["bar"]
  defp resolve_bind({operator, _, children}, binds_with_index) when operator in @operators do
    children = Enum.map(children, &resolve_bind(&1, binds_with_index))
    cond do
      static_and_aggregations_only?(children) ->
        {{:agg, operator}, children}
      has_aggregation?(children) ->
        raise ArgumentError, "Can't mix aggregation and non-aggregation in one formula."
      true ->
        {{:arith, operator}, children}
    end
  end

  @aggregations [
    sum: 1,
    avg: 1,
    min: 1,
    max: 1,
    count: 1,
    countd: 1,
    stddev_samp: 1,
    stddev_pop: 1,
    var_samp: 1,
    var_pop: 1,
    percentile_cont: 2
  ]

  @aggregation_names Keyword.keys(@aggregations)

  # sum(q["foo"])
  defp resolve_bind({fun, _, children}, binds_with_index) when fun in @aggregation_names do
    if length(children) != @aggregations[fun] do
      raise ArgumentError, "#{fun} takes #{@aggregations[fun]} argument(s) but #{length(children)} is given."
    end
    children = Enum.map(children, &resolve_bind(&1, binds_with_index))
    if has_aggregation?(children) do
      raise ArgumentError, "Can't apply aggregation on aggregation."
    end
    {{:agg, fun}, children}
  end

  @non_aggregations [
    like: 2,
    ilike: 2,
    is_nil: 1
  ]

  @non_aggregation_names Keyword.keys(@non_aggregations)

  defp resolve_bind({fun, _, children}, binds_with_index) when fun in @non_aggregation_names do
    if length(children) != @non_aggregations[fun] do
      raise ArgumentError, "#{fun} takes #{@non_aggregations[fun]} argument(s) but #{length(children)} is given."
    end
    children = Enum.map(children, &resolve_bind(&1, binds_with_index))
    {{:non_agg, fun}, children}
  end

  defp get_bind_index!(binds_with_index, bind) do
    case binds_with_index[bind] do
      nil -> raise ArgumentError, "Binding #{elem(bind, 0)} is not defined"
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
  defp pinned_expression?(_), do: false

  defp static?(expression) do
    literal?(expression) or pinned_expression?(expression)
  end
end
