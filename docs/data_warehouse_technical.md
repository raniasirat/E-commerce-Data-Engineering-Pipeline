# Documentation technique du Data Warehouse e-commerce Olist

## 1. Présentation

Ce Data Warehouse fournit une base d’analyse cohérente des commandes, des articles vendus, des clients, des produits et des vendeurs de la marketplace Olist. Le projet exploite le jeu de données public Brazilian E-Commerce de Olist.

PostgreSQL héberge les tables sources et les objets analytiques. La base configurée dans les artefacts dbt inspectés est `ecommerce_dw`. dbt déclare les sources, décrit les transformations SQL et matérialise les vues et tables analytiques. Le projet dbt est configuré sous le nom `ecommerce_dbt` dans [`dbt_project.yml`](../ecommerce_dbt/dbt_project.yml).

## 2. Architecture du Data Warehouse

Les relations référencées dans le manifeste dbt se répartissent ainsi :

- Base : `ecommerce_dw`.
- Schéma source : `staging`, déclaré dans [`sources.yml`](../ecommerce_dbt/models/staging/sources.yml).
- Schéma des modèles dbt : `analytics`, observé dans le manifeste et le catalogue [`catalog.json`](../ecommerce_dbt/target/catalog.json).

Le flux logique est :

```text
Fichiers Olist traités (Parquet)
              |
              v
PostgreSQL : ecommerce_dw.staging
              |
              v
dbt : modèles staging (vues)
              |
              v
dbt : modèles intermediate (vues)
              |
              v
dbt : dimensions, fait et agrégations (tables)
       ecommerce_dw.analytics
```

Le modèle analytique est un schéma en étoile : `fact_order_items` se trouve au centre et référence les dimensions clients, produits, vendeurs et dates. Son grain est **une ligne par article de commande**, identifié par le couple `(order_id, order_item_id)`.

## 3. Modèle dimensionnel

Les modèles ci-dessous sont définis dans [`models/marts/`](../ecommerce_dbt/models/marts/).

| Table | Clé primaire logique | Rôle et attributs principaux |
|---|---|---|
| [`fact_order_items`](../ecommerce_dbt/models/marts/fact_order_items.sql) | `(order_id, order_item_id)` — unicité contrôlée par test dbt | Une ligne par article. Contient les identifiants de commande et les clés de dimensions, le statut, les horodatages de commande, `price`, `freight_value` et `total_item_amount`. |
| [`dim_customers`](../ecommerce_dbt/models/marts/dim_customers.sql) | `customer_key` | Clé technique générée par `ROW_NUMBER()`; conserve `customer_id`, `customer_unique_id`, préfixe postal, ville et état. |
| [`dim_products`](../ecommerce_dbt/models/marts/dim_products.sql) | `product_key` | Clé technique générée par `ROW_NUMBER()`; conserve `product_id`, catégories source et anglaise, et les attributs numériques du produit présents dans la source. |
| [`dim_sellers`](../ecommerce_dbt/models/marts/dim_sellers.sql) | `seller_key` | Clé technique générée par `ROW_NUMBER()`; conserve `seller_id`, préfixe postal, ville et état. |
| [`dim_date`](../ecommerce_dbt/models/marts/dim_date.sql) | `date_key` (`YYYYMMDD`) | Dimension calendrier avec `full_date`, année, trimestre, mois, jour, noms du mois et du jour, et indicateur de week-end. |

### Relations et dates

`fact_order_items` référence :

- `customer_key` vers `dim_customers.customer_key`;
- `product_key` vers `dim_products.product_key`;
- `seller_key` vers `dim_sellers.seller_key`;
- cinq rôles de `dim_date` : `purchase_date_key`, `approved_date_key`, `carrier_delivery_date_key`, `customer_delivery_date_key` et `estimated_delivery_date_key`.

Ces cinq rôles correspondent respectivement aux dates de commande, d’approbation, de remise au transporteur, de livraison au client et de livraison estimée. Les clés des dates facultatives restent nulles lorsque l’horodatage source est absent.

Les mesures du fait sont `price`, `freight_value` et `total_item_amount`, calculé dans le modèle comme `price + freight_value`. `order_id` reste disponible comme identifiant de commande au grain article. Les paiements ne sont pas joints à ce fait : plusieurs paiements peuvent correspondre à une commande et multiplier les lignes d’articles.

Les clés techniques et relations sont contrôlées par les tests déclarés dans [`schema.yml`](../ecommerce_dbt/models/marts/schema.yml). Les modèles SQL matérialisent des tables; les tests dbt vérifient les clés, mais ne déclarent pas des contraintes SQL `PRIMARY KEY` ou `FOREIGN KEY` dans le DDL.

## 4. Sources et données staging

La source dbt `ecommerce_staging` référence les neuf tables suivantes dans `ecommerce_dw.staging`. Les volumes par table ci-dessous proviennent des Parquet traités sous [`data/processed/`](../data/processed/); leur somme, 1 289 091 lignes, correspond au total vérifié directement dans PostgreSQL.

| Table source | Lignes dans le Parquet traité |
|---|---:|
| `customers` | 99 441 |
| `geolocation` | 738 332 |
| `orders` | 99 441 |
| `order_items` | 112 650 |
| `order_payments` | 103 886 |
| `order_reviews` | 99 224 |
| `products` | 32 951 |
| `product_category_translation` | 71 |
| `sellers` | 3 095 |
| **Total calculé** | **1 289 091** |

Le total des neuf tables staging a été vérifié directement dans PostgreSQL : **1 289 091 lignes**, identique à la somme du tableau ci-dessus. La géolocalisation compte 738 332 lignes dans le Parquet traité; son CSV brut en contenait 1 000 163 avant dédoublonnage, selon le [rapport de profilage](./profiling_report.md).

## 5. Transformations dbt

Les 19 modèles se répartissent en trois couches.

### Staging — 9 vues

[`stg_customers`](../ecommerce_dbt/models/staging/stg_customers.sql), [`stg_geolocation`](../ecommerce_dbt/models/staging/stg_geolocation.sql), [`stg_orders`](../ecommerce_dbt/models/staging/stg_orders.sql), [`stg_order_items`](../ecommerce_dbt/models/staging/stg_order_items.sql), [`stg_order_payments`](../ecommerce_dbt/models/staging/stg_order_payments.sql), [`stg_order_reviews`](../ecommerce_dbt/models/staging/stg_order_reviews.sql), [`stg_products`](../ecommerce_dbt/models/staging/stg_products.sql), [`stg_product_category_translation`](../ecommerce_dbt/models/staging/stg_product_category_translation.sql) et [`stg_sellers`](../ecommerce_dbt/models/staging/stg_sellers.sql) lisent les sources avec `source('ecommerce_staging', ...)`. Ils appliquent notamment `TRIM`, la normalisation de certains états et statuts, et des filtres sur les clés nulles spécifiés dans les modèles.

### Intermediate — 2 vues

- [`int_order_items_enriched`](../ecommerce_dbt/models/intermediate/int_order_items_enriched.sql) enrichit les articles par une jointure gauche avec les commandes sur `order_id`.
- [`int_products_enriched`](../ecommerce_dbt/models/intermediate/int_products_enriched.sql) ajoute la catégorie anglaise avec une jointure gauche sur `product_category_name`, en retenant le nom source si la traduction est absente ou vide.

### Marts — 8 tables

Les marts contiennent les quatre dimensions ([`dim_customers`](../ecommerce_dbt/models/marts/dim_customers.sql), [`dim_products`](../ecommerce_dbt/models/marts/dim_products.sql), [`dim_sellers`](../ecommerce_dbt/models/marts/dim_sellers.sql), [`dim_date`](../ecommerce_dbt/models/marts/dim_date.sql)), le fait [`fact_order_items`](../ecommerce_dbt/models/marts/fact_order_items.sql) et trois agrégations :

- [`agg_monthly_sales`](../ecommerce_dbt/models/marts/agg_monthly_sales.sql) regroupe par mois de commande et calcule le nombre distinct de commandes, le nombre de lignes d’articles et les sommes de `price`, `freight_value` et `total_item_amount`.
- [`agg_orders_by_status`](../ecommerce_dbt/models/marts/agg_orders_by_status.sql) regroupe par `order_status` et calcule le nombre distinct de commandes et le nombre d’articles.
- [`agg_top_products`](../ecommerce_dbt/models/marts/agg_top_products.sql) joint le fait à `dim_products` via `product_key`, regroupe les mesures par produit et trie par `total_revenue` décroissant.

## 6. Qualité des données

Les clés métier contrôlées dans les données traitées comprennent `orders.order_id`, `customers.customer_id`, `products.product_id`, `sellers.seller_id`, `(order_items.order_id, order_items.order_item_id)`, `(order_payments.order_id, order_payments.payment_sequential)`, `(order_reviews.review_id, order_reviews.order_id)` et `product_category_translation.product_category_name`. Le profilage et les contrôles sur les Parquet traités n’ont relevé aucun doublon sur ces clés. La géolocalisation est dédoublonnée sur la ligne complète et n’a pas de clé primaire déclarée dans la configuration de nettoyage.

Les relations sources vérifiées sur les fichiers traités n’ont pas de clés orphelines pour les jointures suivantes : commande-client, article-commande, article-produit, article-vendeur, paiement-commande et avis-commande. Dans les marts, les tests `relationships` vérifient aussi les clés client, produit, vendeur et date du fait par rapport à leurs dimensions.

Le [journal dbt](../ecommerce_dbt/logs/dbt.log) contient une exécution de `dbt test` achevée avec **59 réussites, 0 avertissement et 0 erreur**. Les trois tests SQL personnalisés de [`tests/`](../ecommerce_dbt/tests/) contrôlent :

1. [`assert_fact_order_items_unique.sql`](../ecommerce_dbt/tests/assert_fact_order_items_unique.sql) contrôle l’unicité du couple `(order_id, order_item_id)` dans `fact_order_items`;
2. [`assert_fact_amounts_non_negative.sql`](../ecommerce_dbt/tests/assert_fact_amounts_non_negative.sql) contrôle l’absence de prix ou de frais de livraison négatifs;
3. [`assert_fact_total_item_amount_consistent.sql`](../ecommerce_dbt/tests/assert_fact_total_item_amount_consistent.sql) contrôle l’égalité entre `total_item_amount` et `price + freight_value`.

Le journal et les définitions de tests font partie du projet; cette documentation ne lance pas de nouvelle exécution.

## 7. Vérification des agrégations

Les agrégats recalculés à partir des Parquet traités et de la jointure articles-commandes correspondent aux totaux attendus :

| Indicateur | Valeur |
|---|---:|
| Mois de commande distincts | 24 |
| Articles | 112 650 |
| Commandes contenant des articles | 98 666 |
| Somme de `price` (`revenue`) | 13 591 643,70 |
| Somme de `freight_value` (`freight`) | 2 251 909,54 |
| Somme de `total_item_amount` (`total_revenue`) | 15 843 553,24 |

Les sommes mensuelles de `agg_monthly_sales` correspondent aux sommes calculées sur les lignes du fait : `revenue` à `SUM(price)`, `freight` à `SUM(freight_value)` et `total_revenue` à `SUM(total_item_amount)`. Le nombre de mois, d’articles et de commandes indiqué ci-dessus a été recalculé sur les fichiers locaux traités.

## 8. Documentation dbt

Depuis le répertoire `ecommerce_dbt`, générer la documentation avec :

```shell
dbt docs generate
```

La consulter localement avec :

```shell
dbt docs serve --port 8080
```

Le serveur de documentation est alors accessible à `http://localhost:8080`.

## 9. Commandes utiles

À exécuter depuis le répertoire du projet dbt :

```shell
dbt debug
dbt parse
dbt run
dbt test
dbt docs generate
```

`dbt debug` vérifie la connexion et la configuration active; `dbt parse` valide la structure et les références du projet; `dbt run` matérialise les modèles; `dbt test` exécute les contrôles déclarés; `dbt docs generate` produit le catalogue de documentation.

## 10. Conclusion

Le projet organise les données Olist dans un schéma en étoile centré sur une ligne par article de commande, avec des dimensions réutilisables et cinq rôles de date. Les trois agrégations facilitent l’analyse des ventes par mois, par statut et par produit. Les contrôles dbt documentés et les vérifications sur les Parquet traités apportent une base de qualité mesurable.

Le total de 1 289 091 lignes dans les neuf tables staging a été vérifié directement dans PostgreSQL et correspond à la somme des volumes par table présentés dans ce document.
