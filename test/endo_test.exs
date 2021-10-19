defmodule EndoTest do
  use ExUnit.Case
  doctest Endo

  test "greets the world" do
    assert Endo.hello() == :world
  end
end
