defmodule Endo.TestCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      import Endo.CompileTimeAssertions
    end
  end
end
