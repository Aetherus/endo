# QueryBuilder

A query builder to provide a more dynamic and more flexible query building experience than Ecto.

## Pros

- Ecto-like syntax (not exactly)
- Schemaless
- Composable
- String-based column definition that maximized dynamicity without the danger of exploding the atom table

## Cons

- PostgreSQL-specific (for now)
- No CRUD support. You have to use something else (e.g. `Ecto.Adapters.SQL.query/4`) to consume the SQL statement. 
- Not as elegant as Ecto (though I tried).
- No prepared statement caching.

## Basic

```elixir
from("Superstore Orders")
|> select([q], q["Name"], q["Sales"])
|> to_sql(schema: "data")
```

Generates SQL

```sql
SELECT
  "t0"."Product Name",
  "t0"."Sales"
FROM
  "data"."Superstore Orders" AS "t0"
```

## Where

```elixir
regions = ["East", "West"]

from("Superstore Orders")
|> select([q], q["Product Name"], q["Sales"])
|> where([q], q["Region"] in ^regions)
|> to_sql(schema: "data")
```

Generates SQL

```sql
SELECT
  "t0"."Product Name",
  "t0"."Sales"
FROM
  "data"."Superstore Orders" AS "t0"
WHERE "t0"."Region" = $1
```


## Order By, Offset & Limit

```elixir
from("Superstore Orders")
|> select([q], q["Product Name"], q["Sales"])
|> order_by([q],
  desc: q["Profit"],
  asc: q["Region"]
)
|> limit(10)
|> offset(20)
|> to_sql(schema: "data")
```

Generates SQL

```sql
SELECT 
  "t0"."Product Name",
  "t0"."Sales"
FROM
  "data"."Superstore Orders" AS "t0"
ORDER BY
  "t0"."Profit" DESC,
  "t0"."Region" ASC
LIMIT 10
OFFSET 20
```

## Aggregation

```elixir
from("Superstore Orders")
|> select([q],
  q["Product Name"],
  agg("percentile_cont (?) WITHIN GROUP (ORDER BY ?)", 0.95, q["Profit"])
)
|> group_by([q], q["Product Name"])
|> having([q], agg("avg(?)", q["Sales"]) > 10_000)
|> to_sql(schema: "data")
```

Generates SQL

```sql
SELECT
  "t0"."Product Name",
  percentile_cont ($1) WITHIN GROUP (ORDER BY "t0"."Profit")
FROM
  "data"."Superstore Orders" AS "t0"
GROUP BY
  "t0"."Product Name"
HAVING avg("t0"."Sales") > $1
```

## Joining

```elixir
from("Superstore Orders")
|> join([d], :inner, "Superstore People", [p], on:  d["Manager ID"] == p["ID"])
|> select([d, p], [
  d["Product Name"],
  p["Name"]
])
|> to_sql(schema: "data")
```

Generates SQL

```sql
SELECT
  "t0"."Product Name",
  "t1"."Name"
FROM
  "data"."Superstore Orders" AS "t0"
INNER JOIN
  "data"."Superstore People" AS "t1" ON "t0"."Manager ID" = "t1"."ID"
```

## Subquery

```elixir
sub = from("Superstore Orders")
      |> select([q], q["Manager ID"])
      |> group_by([q], q["Manager ID"])
      |> having([q], agg("sum(?)", q["Profit"]) < 0)
      |> subquery()

from("Superstore People")
|> where([q], q["ID"] in ^sub)
|> select([q], q["Name"])
|> to_sql(schema: "data")
```

Generates SQL

```sql
SELECT
  "t0"."Name"
FROM
  "data"."Superstore People" AS "t0"
WHERE
  "t0"."ID" in (
    SELECT "s0"."Manager ID"
    FROM "data"."Superstore Orders" AS "s0"
    GROUP BY "s0"."Manager ID"
    HAVING sum("s0"."Profit") < $1
  )
```