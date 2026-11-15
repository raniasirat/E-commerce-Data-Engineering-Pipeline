# Référence Kafka — commandes Olist

## But

Cette référence décrit les notions Kafka utilisées par le projet et le contrat JSON
des messages publiés dans le topic `orders`.

Le producteur prévu lira le jeu de données historique Olist. Chaque message
représente un instantané (`order.snapshot`) d'une commande, et non un événement
temps réel émis au moment où la commande a été passée.

## Concepts Kafka

- **Broker** : serveur Kafka qui reçoit, stocke et distribue les messages.
  Dans ce projet, le broker est le service `kafka` de Docker Compose.
- **Topic** : flux nommé de messages. Le topic métier principal est `orders`.
- **Partition** : subdivision ordonnée d'un topic. L'ordre des messages est
  garanti à l'intérieur d'une partition. La configuration actuelle de `orders`
  utilise une partition.
- **Offset** : position d'un message dans une partition. Un consumer s'en sert
  pour suivre sa progression.
- **Producer** : application qui publie des messages dans un topic.
- **Consumer** : application qui lit les messages d'un topic. Un groupe de
  consumers permet de partager la lecture et de suivre les offsets du groupe.

## Batch et streaming

- **Batch** : traite un ensemble fini de fichiers ou d'enregistrements. Le
  pipeline Spark existant lit les CSV Olist et écrit des fichiers Parquet.
- **Streaming** : traite un flux d'événements au fil de leur arrivée. Kafka peut
  transporter les événements; Spark Structured Streaming pourra les lire et les
  traiter continuellement.

Le producteur de cette semaine relira des commandes d'un jeu de données batch,
puis publiera chacune sous forme d'un message Kafka.

## Contrat de l'événement `orders`

Le schéma machine-lisible est `../schemas/order-event.schema.json`.

- `schema_version` : version du contrat; valeur actuelle `1.0`.
- `event_id` : identifiant stable de cet instantané. Le producteur le construit
  sous la forme `olist:order.snapshot:v1:<order_id>` afin qu'une republication
  de la même ligne conserve le même identifiant.
- `event_type` : type de message; valeur `order.snapshot`.
- `event_timestamp` : instant UTC où le producteur publie l'événement, au format
  ISO 8601/RFC 3339. Ce n'est pas l'heure métier de la commande.
- `order_id`, `customer_id`, `order_status` : valeurs provenant du CSV
  `olist_orders_dataset.csv`.
- `order_purchase_timestamp` : valeur source de la commande, conservée comme
  chaîne. Le CSV ne fournit pas de fuseau horaire; ne pas lui ajouter `Z`
  arbitrairement.
- `order_approved_at`, `order_delivered_carrier_date`,
  `order_delivered_customer_date`, `order_estimated_delivery_date` : valeurs
  source conservées comme chaînes, ou `null` si elles sont vides.
- `order_amount` : somme des `payment_value` du CSV
  `olist_order_payments_dataset.csv` pour ce `order_id`. Valeur `null` si aucune
  ligne de paiement n'est disponible; ne pas remplacer l'absence par zéro.
- `currency` : `BRL`, devise du jeu de données Olist.

Les changements incompatibles au contrat nécessitent une nouvelle version et
une mise à jour du schéma JSON.

## Exemple d'événement

Exemple synthétique, avec les dates de commande au format texte source :

```json
{
  "schema_version": "1.0",
  "event_id": "olist:order.snapshot:v1:00000000-0000-4000-8000-000000000001",
  "event_type": "order.snapshot",
  "event_timestamp": "2026-11-14T20:00:00Z",
  "order_id": "00000000-0000-4000-8000-000000000001",
  "customer_id": "00000000-0000-4000-8000-000000000002",
  "order_status": "delivered",
  "order_purchase_timestamp": "2017-10-02 10:56:33",
  "order_approved_at": "2017-10-02 11:07:15",
  "order_delivered_carrier_date": "2017-10-04 19:55:00",
  "order_delivered_customer_date": "2017-10-10 21:25:13",
  "order_estimated_delivery_date": "2017-10-18 00:00:00",
  "order_amount": 141.9,
  "currency": "BRL"
}
```

## Validation

Le fichier `../schemas/order-event.schema.json` définit les champs requis, leurs
types et les valeurs fixes du contrat. Les dates source qui peuvent manquer
doivent être représentées par `null`. La validation des messages par le
producteur et le consumer sera ajoutée avec leurs étapes respectives.