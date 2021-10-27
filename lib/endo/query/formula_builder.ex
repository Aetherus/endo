defmodule Endo.Query.FormulaBuilder do
  import Endo.Utils

  @binary_operator_mapping %{
    "+": :+,
    "-": :-,
    "*": :*,
    "/": :/,
    "==": :=,
    "!=": :<>,
    "=~": :"~*",
    "<": :<,
    "<=": :<=,
    ">": :>,
    ">=": :>=,
    "<>": :||,  # String concatenation
    and: :AND,
    or: :OR,
    in: :IN,
    like: :LIKE,
    ilike: :ILIKE
  }

  @binary_operators Map.keys(@binary_operator_mapping)

  # Number and Boolean Literals
  def build_formula(literal, _alias_prefix, _aliases_count, args) when is_number(literal) or is_boolean(literal) do
    {to_string(literal), args}
  end

  # String Literals should be considered unsafe even if it's hard-coded.
  def build_formula(string, _alias_prefix, _aliases_count, args) when is_binary(string) do
    placeholder = next_placeholder_index(args)
    {"$#{placeholder}", [{placeholder, string} | args]}
  end

  # Dynamics
  def build_formula({{_, :dynamic}, fun}, alias_prefix, aliases_count, args) when is_function(fun, 3) do
    fun.(alias_prefix, aliases_count, args)
  end

  def build_formula({:unsafe, {{_, :dynamic}, fun}}, alias_prefix, aliases_count, args) when is_function(fun, 3) do
    fun.(alias_prefix, aliases_count, args)
  end

  # Dynamic injections (excluding subqueries)
  def build_formula({:unsafe, value}, _alias_prefix, _aliases_count, args) when not is_struct(value, Endo.Query) do
    placeholder = next_placeholder_index(args)
    {"$#{placeholder}", [{placeholder, value} | args]}
  end

  # Fields
  def build_formula({:field, [{:bind, n}, name]}, alias_prefix, aliases_count, args) do
    n = if n >= 0, do: n, else: aliases_count + n
    name = name |> safe!() |> quote_ident()
    field = "#{alias_prefix}#{n}.#{name}"
    {field, args}
  end

  # Binary operators. Precedence should be resolved in the query struct construction phase.
  def build_formula({{_, operator}, [lhs, rhs]}, alias_prefix, aliases_count, args) when operator in @binary_operators do
    operator = @binary_operator_mapping[operator]
    {lhs, args} = build_formula(lhs, alias_prefix, aliases_count, args)
    {rhs, args} = build_formula(rhs, alias_prefix, aliases_count, args)
    {"(#{lhs} #{operator} #{rhs})", args}
  end

  # Percentile
  def build_formula({{_, :percentile_cont}, [percentage, expr]}, alias_prefix, aliases_count, args) do
    {percentage, args} = build_formula(percentage, alias_prefix, aliases_count, args)
    {expr, args} = build_formula(expr, alias_prefix, aliases_count, args)
    formula = "(percentile_cont (#{percentage}) WITHIN GROUP (ORDER BY #{expr}))"
    {formula, args}
  end

  # Median
  def build_formula({{_, :median}, [expr]}, alias_prefix, aliases_count, args) do
    {expr, args} = build_formula(expr, alias_prefix, aliases_count, args)
    formula = "(percentile_cont (0.5) WITHIN GROUP (ORDER BY #{expr}))"
    {formula, args}
  end

  # Count Distinct
  def build_formula({{_, :count_distinct}, [expr]}, alias_prefix, aliases_count, args) do
    {expr, args} = build_formula(expr, alias_prefix, aliases_count, args)
    formula = "count(DISTINCT #{expr})"
    {formula, args}
  end

  # is_nil
  def build_formula({{_, :is_nil}, [expr]}, alias_prefix, aliases_count, args) do
    {expr, args} = build_formula(expr, alias_prefix, aliases_count, args)
    formula = "(#{expr} IS NULL)"
    {formula, args}
  end

  # NOT
  def build_formula({{_, :not}, [expr]}, alias_prefix, aliases_count, args) do
    {expr, args} = build_formula(expr, alias_prefix, aliases_count, args)
    formula = "(NOT #{expr})"
    {formula, args}
  end

  # e.g. extract("year" FROM date)
  def build_formula({{_, :extract}, [unit, expr]}, alias_prefix, aliases_count, args) do
    unit = unit |> safe!() |> quote_ident()
    {expr, args} = build_formula(expr, alias_prefix, aliases_count, args)
    formula = "extract(#{unit} FROM #{expr})"
    {formula, args}
  end

  # Other functions, including NOT(bool).
  def build_formula({{_, func}, children}, alias_prefix, aliases_count, args) when is_atom(func) do
    {children, args} = Enum.reduce(children, {[], args}, fn child, {acc, args} ->
      {child, args} = build_formula(child, alias_prefix, aliases_count, args)
      {[child | acc], args}
    end)
    children = Enum.reverse(children)
    formula = "#{func}(#{Enum.join(children, ", ")})"
    {formula, args}
  end

  def build_formula({{agg_type, fragment}, fragment_args}, alias_prefix, aliases_count, args) when agg_type in [:agg, :non_agg] do
    # Risk SQL inject for flexibility.

    {fragment_clause, remaining_fragment_args, args} =
      fragment
      |> safe!()
      |> String.graphemes()
      |> Enum.reduce({[], fragment_args, args}, fn
        "?", {acc, [fragment_arg | rest_fragment_args], args} ->
          {subformula, args} = build_formula(fragment_arg, alias_prefix, aliases_count, args)
          {[acc, subformula], rest_fragment_args, args}
        "?", {_acc, [], _args} ->
          raise ArgumentError, "The number of question marks should be equal to the number of arguments."
        char, {acc, fragment_args, args} ->
          {[acc, char], fragment_args, args}
      end)

    case remaining_fragment_args do
      [_ | _] ->
        raise ArgumentError, "The number of question marks should be equal to the number of arguments."
      [] ->
        {IO.iodata_to_binary(fragment_clause), args}
    end
  end

  defp next_placeholder_index([]), do: 1
  defp next_placeholder_index([{n, _} | _]), do: n + 1
end
