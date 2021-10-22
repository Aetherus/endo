defmodule Endo.Utils do
  def quote_ident(string) when is_binary(string) do
    escaped =
      string
      |> String.replace("\"", "\"\"", global: true)
    ~s["#{escaped}"]
  end

  def safe!({:unsafe, value}), do: value
  def safe!(value), do: value
end
