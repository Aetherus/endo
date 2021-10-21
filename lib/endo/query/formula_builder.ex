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
  def build_formula(literal, _alias_prefix, args) when is_number(literal) or is_boolean(literal) do
    {to_string(literal), args}
  end

  # String Literals should be considered unsafe even if it's hard-coded.
  def build_formula(string, _alias_prefix, args) when is_binary(string) do
    placeholder = next_placeholder(args)
    {placeholder, [{placeholder, string} | args]}
  end

  # Dynamic injections (excluding subqueries)
  def build_formula({:unsafe, value}, _alias_prefix, args) when not is_struct(value, Endo.Query) do
    placeholder = next_placeholder(args)
    {placeholder, [{placeholder, value} | args]}
  end

  # Fields
  def build_formula({:field, [{:bind, n}, name]}, alias_prefix, args) do
    name = name |> safe!() |> quote_ident()
    field = "#{alias_prefix}#{n}.#{name}"
    {field, args}
  end

  # Binary operators. Precedence should be resolved in the query struct construction phase.
  def build_formula({{_, operator}, [lhs, rhs]}, alias_prefix, args) when operator in @binary_operators do
    operator = @binary_operator_mapping[operator]
    {lhs, args} = build_formula(lhs, alias_prefix, args)
    {rhs, args} = build_formula(rhs, alias_prefix, args)
    {"(#{lhs} #{operator} #{rhs})", args}
  end

  # Percentile
  def build_formula({{_, :percentile_cont}, [percentage, expr]}, alias_prefix, args) do
    {percentage, args} = build_formula(percentage, alias_prefix, args)
    {expr, args} = build_formula(expr, alias_prefix, args)
    formula = "(percentile_cont (#{percentage}) WITHIN GROUP (ORDER BY #{expr}))"
    {formula, args}
  end

  # Median
  def build_formula({{_, :median}, [expr]}, alias_prefix, args) do
    {expr, args} = build_formula(expr, alias_prefix, args)
    formula = "(percentile_cont (0.5) WITHIN GROUP (ORDER BY #{expr}))"
    {formula, args}
  end

  # Count Distinct
  def build_formula({{_, :count_distinct}, [expr]}, alias_prefix, args) do
    {expr, args} = build_formula(expr, alias_prefix, args)
    formula = "count(distinct #{expr})"
    {formula, args}
  end

    # Count Distinct
    def build_formula({{_, :is_nil}, [expr]}, alias_prefix, args) do
      {expr, args} = build_formula(expr, alias_prefix, args)
      formula = "#{expr} IS NULL"
      {formula, args}
    end

  # Other functions, including NOT(bool).
  def build_formula({{_, func}, children}, alias_prefix, args) when is_atom(func) do
    {children, args} = Enum.reduce(children, {[], args}, fn child, {acc, args} ->
      {child, args} = build_formula(child, alias_prefix, args)
      {[child | acc], args}
    end)
    children = Enum.reverse(children)
    formula = "#{func}(#{Enum.join(children, ", ")})"
    {formula, args}
  end

  defp next_placeholder([]), do: "$1"
  defp next_placeholder([{"$" <> n, _} | _]) do
    n = String.to_integer(n)
    "$#{n + 1}"
  end

  defp safe!({:unsafe, value}), do: value
  defp safe!(value), do: value
end
