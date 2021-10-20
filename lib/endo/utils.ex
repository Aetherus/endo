defmodule Endo.Utils do
  def quote_ident(string) when is_binary(string) do
    escaped =
      string
      |> String.replace("\"", "\"\"", global: true)
    ~s["#{escaped}"]
  end
end
