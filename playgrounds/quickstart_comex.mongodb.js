// ====================================================================
// 🚢 PLAYGROUND COMEX DATA ENGINE V2 - RAW LANDING ZONE (MongoDB 7)
// Conexão: mongodb://localhost:27018 ou mongodb://localhost:27017
// Dica: Abra este arquivo no VS Code e clique no ícone "▶ Run" no topo direito!
// ====================================================================

// 1. Seleciona o banco da Raw Landing Zone do Comex V2
use('comex_raw_lake');

// 2. Estatísticas das Coleções Brutas
const colecoes = db.getCollectionNames();

// 3. Inspeção das Faturas Comerciais Brutas
const totalInvoices = db.raw_commercial_invoices.countDocuments();
const faturasAmostra = db.raw_commercial_invoices.find(
  {},
  { payload_hash: 1, ingested_at: 1, "raw_payload.document_number": 1, "raw_payload.total_amount": 1, "raw_payload.currency_code": 1 }
).sort({ ingested_at: -1 }).limit(5).toArray();

// 4. Forense: Buscar documento bruto a partir de um mongo_raw_id obtido na Quarentena do DBeaver!
// (Substitua a string abaixo por um mongo_raw_id de comex_quarantine_hitl para inspecionar o JSON original)
const exemploRawId = db.raw_commercial_invoices.findOne({}, { _id: 1 });
const documentoForense = exemploRawId ? db.raw_commercial_invoices.findOne({ _id: exemploRawId._id }) : null;

// 5. Painel de Retorno Exibido no VS Code
({
  status: "✅ Raw Lake MongoDB Conectado!",
  database: "comex_raw_lake",
  colecoes_existentes: colecoes,
  total_invoices_brutas: totalInvoices,
  amostra_invoices: faturasAmostra,
  documento_inspecionado: documentoForense
});
