defmodule Endo.Sigils do
  @spec sigil_Q(String.t(), any()) :: String.t()
  def sigil_Q(sql, _) do
    sql
    |> String.replace(~r/\r?\n/, " ", global: true)
    |> String.replace(~r/\s{2,}/, " ", global: true)
    |> String.replace(~r/(?<=\()\s/, "", global: true)
    |> String.replace(~r/\s(?=\))/, "", global: true)
    |> String.trim()
  end
end
