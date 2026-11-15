# Power BI Dashboard

## Existing report

The repository contains
[`dashbord_ecommerce.pbix`](../powerbi/dashbord_ecommerce.pbix). The filename
is preserved as it exists in the project. Static inspection of its report
metadata found visuals referencing these analytics tables:

- `analytics.agg_monthly_sales`
- `analytics.agg_orders_by_status`
- `analytics.agg_top_products`

The PBIX contains an embedded data model. The report metadata establishes
which model tables and fields its visuals reference, but not the model's last
refresh time, current PostgreSQL connection mode, or whether the embedded data
matches the current warehouse.

## KPIs and visuals

| Report visual | Fields referenced in the report |
|---|---|
| **Total Revenue (Excluding Freight)** card | `SUM(agg_monthly_sales.revenue)` |
| **Total Orders** card | `SUM(agg_monthly_sales.nb_orders)` |
| **Monthly Orders Trend** line chart | `order_month` and `SUM(nb_orders)` |
| **Monthly Revenue Trend** line chart | `order_month` and `SUM(revenue)` |
| **Monthly Freight Cost Trend** combo chart | `order_month` and `SUM(freight)` |
| **Orders by Status** donut chart | `order_status` and `SUM(nb_orders)` |
| **Top 10 Product Categories by Revenue** bar chart | `product_category_name_english`, `Rank_Product`, and `SUM(revenue)` |

The monthly aggregate defines `nb_orders` as a distinct count within each
month, while the status aggregate defines it as a distinct count within each
status. Summing those columns across a wider grouping is equivalent to a
global distinct-order count only when each order belongs to exactly one month
or one status in that visual's grouping. The relevant consistency should be
checked against the current warehouse data before interpreting grand totals.

`Rank_Product` is referenced by the top-category visual. Its calculation is
part of the PBIX model, not one of the three dbt aggregate SQL models.

## Data lineage

The report's referenced aggregate tables are dbt marts in PostgreSQL schema
`analytics`:

```text
fact_order_items
  ├─ agg_monthly_sales
  ├─ agg_orders_by_status
  └─ agg_top_products -> dim_products
```

`agg_monthly_sales` is based on the item-grain fact and groups by purchase
month. `agg_orders_by_status` groups fact rows by `order_status`.
`agg_top_products` joins `fact_order_items` to `dim_products` on `product_key`
and aggregates item counts and sales measures by product.

The Kafka-to-MinIO event path is separate from these dbt marts in the code
examined. The dashboard should not be assumed to include streaming snapshots
unless its model is explicitly refreshed or reconfigured to do so.

## Screenshots and refresh

The supplied dashboard capture is included below:

![Power BI dashboard screenshot](images/Power%20BI%20dashboard.png)

The warehouse schema diagram is available separately at
[`images/data_warehouse_schema.png`](images/data_warehouse_schema.png).

Before publishing KPI values, refresh the PBIX from the intended PostgreSQL
analytics source and compare its totals with the current dbt tables. This
documentation does not open or refresh the report.
