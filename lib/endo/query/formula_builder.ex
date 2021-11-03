defmodule Endo.Query.FormulaBuilder do
  import Endo.Utils

  @binary_operator_mapping %{
    +: :+,
    -: :-,
    *: :*,
    /: :/,
    ==: :=,
    !=: :<>,
    =~: :"~*",
    <: :<,
    <=: :<=,
    >: :>,
    >=: :>=,
    # String concatenation
    <>: :||,
    and: :AND,
    or: :OR,
    like: :LIKE,
    ilike: :ILIKE
  }

  @binary_operators Map.keys(@binary_operator_mapping)

  # Number and Boolean Literals
  def build_formula(literal, _alias_prefix, _aliases_count, args, _opts)
      when is_number(literal) or is_boolean(literal) do
    {to_string(literal), args}
  end

  # String Literals should be considered unsafe even if it's hard-coded.
  def build_formula(string, _alias_prefix, _aliases_count, args, _opts) when is_binary(string) do
    placeholder = next_placeholder_index(args)
    {"$#{placeholder}", [{placeholder, string} | args]}
  end

  # Dynamics
  def build_formula({{_, :dynamic}, fun}, alias_prefix, aliases_count, args, opts)
      when is_function(fun, 4) do
    fun.(alias_prefix, aliases_count, args, opts)
  end

  def build_formula({:unsafe, {{_, :dynamic}, fun}}, alias_prefix, aliases_count, args, opts)
      when is_function(fun, 4) do
    fun.(alias_prefix, aliases_count, args, opts)
  end

  # Subquery
  def build_formula(%Endo.Query{} = subquery, alias_prefix, _aliases_count, args, opts) do
    {sql, args} =
      Endo.Query.SQLBuilder.build_sql(
        subquery,
        "s" <> alias_prefix,
        false,
        args,
        opts
      )

    {"(#{sql})", args}
  end

  def build_formula({:unsafe, %Endo.Query{} = subquery}, alias_prefix, aliases_count, args, opts) do
    build_formula(subquery, alias_prefix, aliases_count, args, opts)
  end

  # Dynamic injections (excluding subqueries)
  def build_formula({:unsafe, value}, _alias_prefix, _aliases_count, args, _opts)
      when not is_struct(value, Endo.Query) do
    placeholder = next_placeholder_index(args)
    {"$#{placeholder}", [{placeholder, value} | args]}
  end

  # Fields
  def build_formula({:field, [{:bind, n}, name]}, alias_prefix, aliases_count, args, opts) do
    n = if n >= 0, do: n, else: aliases_count + n
    name = name |> safe!() |> quote_ident()
    field = if !opts[:no_alias] do
      "#{alias_prefix}#{n}.#{name}"
    else
      name
    end
    {field, args}
  end

  def build_formula({{_, :in}, [lhs, rhs]}, alias_prefix, aliases_count, args, opts) do
    {lhs, args} = build_formula(lhs, alias_prefix, aliases_count, args, opts)
    {rhs, args} = build_formula(rhs, alias_prefix, aliases_count, args, opts)
    {"(#{lhs} = ANY(#{rhs}))", args}
  end

  # Binary operators. Precedence should be resolved in the query struct construction phase.
  def build_formula({{_, operator}, [lhs, rhs]}, alias_prefix, aliases_count, args, opts)
      when operator in @binary_operators do
    operator = @binary_operator_mapping[operator]
    {lhs, args} = build_formula(lhs, alias_prefix, aliases_count, args, opts)
    {rhs, args} = build_formula(rhs, alias_prefix, aliases_count, args, opts)
    {"(#{lhs} #{operator} #{rhs})", args}
  end

  # Percentile
  def build_formula(
        {{_, :percentile_cont}, [percentage, expr]},
        alias_prefix,
        aliases_count,
        args,
        opts
      ) do
    {percentage, args} = build_formula(percentage, alias_prefix, aliases_count, args, opts)
    {expr, args} = build_formula(expr, alias_prefix, aliases_count, args, opts)
    formula = "(percentile_cont (#{percentage}) WITHIN GROUP (ORDER BY #{expr}))"
    {formula, args}
  end

  # Median
  def build_formula({{_, :median}, [expr]}, alias_prefix, aliases_count, args, opts) do
    {expr, args} = build_formula(expr, alias_prefix, aliases_count, args, opts)
    formula = "(percentile_cont (0.5) WITHIN GROUP (ORDER BY #{expr}))"
    {formula, args}
  end

  # Count Distinct
  def build_formula({{_, :count_distinct}, [expr]}, alias_prefix, aliases_count, args, opts) do
    {expr, args} = build_formula(expr, alias_prefix, aliases_count, args, opts)
    formula = "count(DISTINCT #{expr})"
    {formula, args}
  end

  # is_nil
  def build_formula({{_, :is_nil}, [expr]}, alias_prefix, aliases_count, args, opts) do
    {expr, args} = build_formula(expr, alias_prefix, aliases_count, args, opts)
    formula = "(#{expr} IS NULL)"
    {formula, args}
  end

  # NOT
  def build_formula({{_, :not}, [expr]}, alias_prefix, aliases_count, args, opts) do
    {expr, args} = build_formula(expr, alias_prefix, aliases_count, args, opts)
    formula = "(NOT #{expr})"
    {formula, args}
  end

  # identity function which returns the arg as is
  def build_formula({{_, :identity}, [child]}, alias_prefix, aliases_count, args, opts) do
    build_formula(child, alias_prefix, aliases_count, args, opts)
  end

  # Other functions, including NOT(bool).
  def build_formula({{_, func}, children}, alias_prefix, aliases_count, args, opts)
      when is_atom(func) do
    {children, args} =
      Enum.reduce(children, {[], args}, fn child, {acc, args} ->
        {child, args} = build_formula(child, alias_prefix, aliases_count, args, opts)
        {[child | acc], args}
      end)

    children = Enum.reverse(children)
    formula = "#{func}(#{Enum.join(children, ", ")})"
    {formula, args}
  end

  def build_formula(
        {{agg_type, fragment}, fragment_args},
        alias_prefix,
        aliases_count,
        args,
        opts
      )
      when agg_type in [:agg, :non_agg] do
    # Risk SQL inject for flexibility.

    {fragment_clause, remaining_fragment_args, args} =
      fragment
      |> safe!()
      |> String.graphemes()
      |> Enum.reduce({[], fragment_args, args}, fn
        "?", {acc, [fragment_arg | rest_fragment_args], args} ->
          {subformula, args} =
            build_formula(fragment_arg, alias_prefix, aliases_count, args, opts)

          {[acc, subformula], rest_fragment_args, args}

        "?", {_acc, [], _args} ->
          raise ArgumentError,
                "The number of question marks should be equal to the number of arguments."

        char, {acc, fragment_args, args} ->
          {[acc, char], fragment_args, args}
      end)

    case remaining_fragment_args do
      [_ | _] ->
        raise ArgumentError,
              "The number of question marks should be equal to the number of arguments."

      [] ->
        {IO.iodata_to_binary(fragment_clause), args}
    end
  end

  defp next_placeholder_index([]), do: 1
  defp next_placeholder_index([{n, _} | _]), do: n + 1
end
