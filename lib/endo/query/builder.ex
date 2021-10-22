defmodule Endo.Query.Builder do
  alias Endo.Query

  import Endo.Utils
  import Endo.Query.FormulaBuilder

  @doc """
  # Example

      %Endo.Query{} |> build_sql(schema: "data")

  # Options

    - `:schema` the PostgreSQL schema that the query will run on.
  """
  def build_sql({_, %Query{} = query}, opts) do
    do_build_sql(query, "t", true, [], opts)
  end

  defp do_build_sql(query, alias_prefix, is_root, args, opts) do
    schema = quote_ident(opts[:schema] || "public")
    {select_clause, args} = build_select_clause(query.select, schema, alias_prefix, args, opts)
    {from_clause, args} = build_from_clause(query.from, schema, alias_prefix, args, opts)

    {join_clauses, args} =
      query.join
      |> List.flatten()
      |> Enum.with_index(1)
      |> Enum.reduce({[], args}, fn {join, i}, {acc, args} ->
        {join_clause, args} = build_join_clause(join, schema, alias_prefix, i, args, opts)
        {[join_clause | acc], args}
      end)
    join_clauses = Enum.reverse(join_clauses)

    {where_clause, args} = build_where_clause(query.where, schema, alias_prefix, args, opts)
    {group_by_clause, args} = build_group_by_clause(query.group_by, schema, alias_prefix, args, opts)
    {having_clause, args} = build_having_clause(query.having, schema, alias_prefix, args, opts)
    {order_by_clause, args} = build_order_by_clause(query.order_by, schema, alias_prefix, args, opts)
    {limit_clause, args} = build_limit_clause(query.limit, schema, alias_prefix, args, opts)
    {offset_clause, args} = build_offset_clause(query.offset, schema, alias_prefix, args, opts)

    args =
      if is_root do
        args |> Enum.reverse() |> Enum.map(&elem(&1, 1))
      else
        args
      end

    sql = [
      select_clause,
      from_clause,
      join_clauses,
      where_clause,
      group_by_clause,
      having_clause,
      order_by_clause,
      limit_clause,
      offset_clause
    ]
    |> List.flatten()
    |> Enum.join(" ")

    {sql, args}
  end

  defguardp is_empty(value) when value in [nil, []]

  defp build_select_clause(selects, _schema, alias_prefix, args, _opts) when is_empty(selects) do
    {"SELECT #{alias_prefix}.*", args}
  end

  defp build_select_clause(selects, _schema, alias_prefix, args, _opts) do
    {selects, args} =
      selects
      |> List.flatten()
      |> Enum.reduce({[], args}, fn select, {acc, args} ->
        {clause, args} = build_formula(select, alias_prefix, args)
        {[clause | acc], args}
      end)

    selects =
      selects
      |> Enum.reverse()
      |> Enum.join(", ")

    {"SELECT #{selects}", args}
  end

  defp build_from_clause(table, _schema, _alias_prefix, _args, _opts) when is_empty(table) do
    raise ArgumentError, "Base table is not set."
  end

  defp build_from_clause(table, schema, alias_prefix, args, _opts) do
    table = table |> safe!() |> quote_ident()
    clause = "FROM #{schema}.#{table} AS #{alias_prefix}0"
    {clause, args}
  end

  defp build_join_clause({qual, {{:unsafe, {_, %Query{} = subquery}}, on}}, _schema, alias_prefix, i, args, opts) do
    qual = qual |> to_string() |> String.upcase()
    {subquery, args} = do_build_sql(subquery, "s#{alias_prefix}", false, args, opts)
    {on, args} = build_formula(on, alias_prefix, args)
    clause = "#{qual} JOIN (#{subquery}) AS #{alias_prefix}#{i} ON #{on}"
    {clause, args}
  end

  defp build_join_clause({qual, {table, on}}, schema, alias_prefix, i, args, _opts) do
    qual = qual |> to_string() |> String.upcase()
    table = table |> safe!() |> quote_ident()
    {on, args} = build_formula(on, alias_prefix, args)
    clause = "#{qual} JOIN #{schema}.#{table} AS #{alias_prefix}#{i} ON #{on}"
    {clause, args}
  end

  defp build_where_clause(where, _schema, _alias_prefix, args, _opts) when is_empty(where), do: {[], args}
  defp build_where_clause(where, _schema, alias_prefix, args, _opts) do
    {where, args} = build_formula(where, alias_prefix, args)
    clause = "WHERE #{where}"
    {clause, args}
  end

  defp build_group_by_clause(group_by, _schema, _alias_prefix, args, _opts) when is_empty(group_by), do: {[], args}
  defp build_group_by_clause(group_by, _schema, alias_prefix, args, _opts) do
    {group_by, args} =
      group_by
      |> Enum.reduce({[], args}, fn grouping, {acc, args} ->
        {grouping, args} = build_formula(grouping, alias_prefix, args)
        {[grouping | acc], args}
      end)

    group_by = group_by |> Enum.reverse() |> Enum.join(", ")
    clause = "GROUP BY #{group_by}"
    {clause, args}
  end

  defp build_having_clause(having, _schema, _alias_prefix, args, _opts) when is_empty(having), do: {[], args}
  defp build_having_clause(having, _schema, alias_prefix, args, _opts) do
    {having, args} = build_formula(having, alias_prefix, args)
    clause = "HAVING #{having}"
    {clause, args}
  end

  defp build_order_by_clause(order_by, _schema, _alias_prefix, args, _opts) when is_empty(order_by), do: {[], args}
  defp build_order_by_clause(order_by, _schema, alias_prefix, args, _opts) do
    {orders, args} =
      order_by
      |> List.flatten()
      |> Enum.reduce({[], args}, fn {direction, expr}, {acc, args} ->
        {expr, args} = build_formula(expr, alias_prefix, args)
        {[{direction, expr} | acc], args}
      end)
    orders = orders
             |> Enum.reverse()
             |> Enum.map(fn {direction, expr} -> "#{expr} #{dir(direction)}" end)
             |> Enum.join(", ")

    clause = "ORDER BY #{orders}"
    {clause, args}
  end

  defp build_limit_clause(limit, _schema, _alias_prefix, args, _opts) when is_empty(limit), do: {[], args}
  defp build_limit_clause(limit, _schema, alias_prefix, args, _opts) do
    {limit, args} = build_formula(limit, alias_prefix, args)
    clause = "LIMIT #{limit}"
    {clause, args}
  end

  defp build_offset_clause(offset, _schema, _alias_prefix, args, _opts) when is_empty(offset), do: {[], args}
  defp build_offset_clause(offset, _schema, alias_prefix, args, _opts) do
    {offset, args} = build_formula(offset, alias_prefix, args)
    clause = "OFFSET #{offset}"
    {clause, args}
  end

  defp dir(:asc), do: "ASC"
  defp dir(:desc), do: "DESC"

  defp safe!({:unsafe, value}), do: value
  defp safe!(value), do: value
end
