// Comex Resilient Data & Fiduciary Engine V2 — MongoDB 7 Playground (VS Code / mongosh)
// Authenticated Connection String (MongoDB Compass / mongosh):
// mongodb://comex_mongo_user:comex_mongo_pass@localhost:27018/comex_raw_lake?authSource=admin

use("comex_raw_lake");

// 1. Listar coleções e contagens na Raw Landing Zone (Bronze)
const collections = [
  "raw_commercial_invoices",
  "raw_bacen_ptax",
  "raw_swift_statements",
  "raw_mdic_declarations",
];

collections.forEach((colName) => {
  const count = db.getCollection(colName).countDocuments({});
  print(`${colName}: ${count} BSON document(s)`);
});

// 2. Verificar índices únicos de idempotência
collections.forEach((colName) => {
  print(`--- Indexes on ${colName} ---`);
  printjson(db.getCollection(colName).getIndexes());
});

// 3. Inspecionar as 5 últimas Commercial Invoices brutas recebidas pela API / Pipeline
db.getCollection("raw_commercial_invoices")
  .find({}, { _id: 1, idempotency_key: 1, payload_hash: 1, source_system: 1, landed_at: 1 })
  .sort({ landed_at: -1 })
  .limit(5);

// 4. Inspecionar extratos SWIFT e cotações BACEN Olinda PTAX aterrissadas
db.getCollection("raw_swift_statements")
  .find({})
  .sort({ landed_at: -1 })
  .limit(5);

db.getCollection("raw_bacen_ptax")
  .find({}, { _id: 1, endpoint: 1, payload_hash: 1, landed_at: 1 })
  .sort({ landed_at: -1 })
  .limit(5);
