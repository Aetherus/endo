defmodule Endo.TestCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      import Endo.CompileTimeAssertions
      import Endo.Sigils
    end
  end
end
