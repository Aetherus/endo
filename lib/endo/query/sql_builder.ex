defmodule Endo.Query.SQLBuilder do
  alias Endo.Query

  import Endo.Utils
  import Endo.Query.FormulaBuilder

  @doc """
  # Example

      %Endo.Query{} |> build_sql(schema: "data")

  # Options

    - `:schema` the PostgreSQL schema that the query will run on.
  """
  def build_sql(%Query{} = query, opts) do
    schema = quote_ident(opts[:schema] || "public")
    do_build_sql(query, schema, "t", true, [], opts)
  end

  defp do_build_sql(query, schema, alias_prefix, is_root, args, opts) do
    {select_clause, args} = build_select_clause(query.select, schema, alias_prefix, args, opts)
    {from_clause, args} = build_from_clause(query.from, schema, alias_prefix, args, opts)

    {join_clauses, args} = build_join_clauses(query.join, schema, alias_prefix, args, opts)

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

  defguardp is_empty(value) when value in [nil, [], %{}]

  defp build_select_clause(selects, _schema, _alias_prefix, args, _opts) when is_empty(selects) do
    {"SELECT *", args}
  end

  defp build_select_clause(selects, _schema, alias_prefix, args, _opts) do
    {selects, args} =
      selects
      |> Enum.sort()
      |> Enum.reduce({[], args}, fn {aliases_count, sub_selects}, {selects, args} ->
        sub_selects
        |> List.flatten()
        |> Enum.reduce({selects, args}, fn ast, {selects, args} ->
          {selection, args} = build_formula(ast, alias_prefix, aliases_count, args)
          {[selection | selects], args}
        end)
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

  defp build_join_clauses(joins, schema, alias_prefix, args, opts) do
    {joins, args} =
      joins
      |> Enum.sort()
      |> Enum.reduce({[], args}, fn {i, join}, {acc, args} ->
        {join, args} = build_join_clause(join, schema, alias_prefix, i, args, opts)
        {[join | acc], args}
      end)

    joins =
      joins
      |> Enum.reverse()

    {joins, args}
  end

  defp build_join_clause({qual, {%Query{} = subquery, on}}, schema, alias_prefix, i, args, opts) do
    qual = qual |> to_string() |> String.upcase()
    {subquery, args} = do_build_sql(subquery, schema, "s#{alias_prefix}", false, args, opts)
    {on, args} = build_formula(on, alias_prefix, i, args)
    clause = "#{qual} JOIN (#{subquery}) AS #{alias_prefix}#{i - 1} ON #{on}"
    {clause, args}
  end

  defp build_join_clause({qual, {{:unsafe, %Query{} = subquery}, on}}, schema, alias_prefix, i, args, opts) do
    build_join_clause({qual, {subquery, on}}, schema, alias_prefix, i, args, opts)
  end

  defp build_join_clause({qual, {table, on}}, schema, alias_prefix, i, args, _opts) do
    qual = qual |> to_string() |> String.upcase()
    table = table |> safe!() |> quote_ident()
    {on, args} = build_formula(on, alias_prefix, i, args)
    clause = "#{qual} JOIN #{schema}.#{table} AS #{alias_prefix}#{i - 1} ON #{on}"
    {clause, args}
  end

  defp build_where_clause(where, _schema, _alias_prefix, args, _opts) when is_empty(where), do: {[], args}
  defp build_where_clause(where, _schema, alias_prefix, args, _opts) do
    {conditions, args} =
      where
      |> Enum.sort()
      |> Enum.reduce({[], args}, fn {aliases_count, condition}, {acc, args} ->
        {condition, args} = build_formula(condition, alias_prefix, aliases_count, args)
        {[condition | acc], args}
      end)

    conditions =
      conditions
      |> Enum.reverse()
      |> Enum.join(" AND ")

    clause = "WHERE #{conditions}"
    {clause, args}
  end

  defp build_group_by_clause(group_by, _schema, _alias_prefix, args, _opts) when is_empty(group_by), do: {[], args}
  defp build_group_by_clause({aliases_count, group_by}, _schema, alias_prefix, args, _opts) do
    {group_by, args} =
      group_by
      |> Enum.map(&safe!/1)
      |> List.flatten()
      |> Enum.reduce({[], args}, fn grouping, {acc, args} ->
        {grouping, args} = build_formula(grouping, alias_prefix, aliases_count, args)
        {[grouping | acc], args}
      end)

    group_by = group_by |> Enum.reverse() |> Enum.join(", ")
    clause = "GROUP BY #{group_by}"
    {clause, args}
  end

  defp build_having_clause(having, _schema, _alias_prefix, args, _opts) when is_empty(having), do: {[], args}
  defp build_having_clause({aliases_count, having}, _schema, alias_prefix, args, _opts) do
    {having, args} = build_formula(having, alias_prefix, aliases_count, args)
    clause = "HAVING #{having}"
    {clause, args}
  end

  defp build_order_by_clause(order_by, _schema, _alias_prefix, args, _opts) when is_empty(order_by), do: {[], args}
  defp build_order_by_clause(order_by, _schema, alias_prefix, args, _opts) do
    {orders, args} =
      order_by
      |> Enum.sort()
      |> Enum.reduce({[], args}, fn {aliases_count, orders}, {acc, args} ->
        orders
        |> List.flatten()
        |> Enum.reduce({acc, args}, fn {direction, expr}, {acc, args} ->
          {expr, args} = build_formula(expr, alias_prefix, aliases_count, args)
          {["#{expr} #{dir(direction)}" | acc], args}
        end)
      end)
    orders =
      orders
      |> Enum.reverse()
      |> Enum.join(", ")

    {"ORDER BY #{orders}", args}
  end

  defp build_limit_clause(limit, _schema, _alias_prefix, args, _opts) when is_empty(limit), do: {[], args}
  defp build_limit_clause(limit, _schema, alias_prefix, args, _opts) do
    {limit, args} = build_formula(limit, alias_prefix, nil, args)
    clause = "LIMIT #{limit}"
    {clause, args}
  end

  defp build_offset_clause(offset, _schema, _alias_prefix, args, _opts) when is_empty(offset), do: {[], args}
  defp build_offset_clause(offset, _schema, alias_prefix, args, _opts) do
    {offset, args} = build_formula(offset, alias_prefix, nil, args)
    clause = "OFFSET #{offset}"
    {clause, args}
  end

  defp dir(:asc), do: "ASC"
  defp dir(:desc), do: "DESC"
end
