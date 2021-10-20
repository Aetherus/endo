defmodule Endo.Query do
  defstruct [
    from: nil,
    select: []
  ]

  @doc """
  # Example

      from("my_table")

  """
  def from(table) do
    %Endo.Query{
      from: table
    }
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

    binds_with_index = Map.merge(leading, trailing)

    selections =
      selections
      |> List.wrap()
      |> Enum.map(&resolve_bind(&1, binds_with_index))

    quote do
      %{unquote(query) | select: unquote(query).select ++ unquote(selections)}
    end
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
    quote do
      unquote(child)
    end
  end

  # q[something]
  defp resolve_bind({
    {:., _, [Access, :get]},
    _,
    [bind, things_inside_brackets]
  }, binds_with_index) do
    {{:bind, get_bind_index!(binds_with_index, bind)}, resolve_bind(things_inside_brackets, binds_with_index)}
  end

  @binary_operators ~w[
    + - * /
    < <= > >=
    == !=
    and or
  ]a

  # q["foo"] + q["bar"]
  defp resolve_bind({operator, _, children}, binds_with_index) when operator in @binary_operators do
    children = Enum.map(children, &resolve_bind(&1, binds_with_index))
    if Enum.any?(children, &match?({{:agg, _}, _}, &1)) do
      if Enum.any?(children, &match?({{type, _}, _} when type != :agg, &1)) do
        raise ArgumentError, "Can't mix aggregation and non-aggregation in one formula."
      end
      {{:agg, operator}, children}
    else
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
    if Enum.any?(children, &match?({{:agg, _}, _}, &1)) do
      raise ArgumentError, "Can't apply aggregation on aggregation."
    end
    {{:agg, fun}, children}
  end

  defp get_bind_index!(binds_with_index, bind) do
    case binds_with_index[bind] do
      nil -> raise ArgumentError, "Binding #{elem(bind, 0)} is not defined"
      index -> index
    end
  end
end
