// ====================================================================
// 🚢 PLAYGROUND COMEX DATA ENGINE V2 - RAW LANDING ZONE
// Dica: Abra este arquivo no VS Code e pressione:
//       [ Ctrl + Alt + S ] ou clique no ícone "▶ Run" no canto superior direito!
// ====================================================================

// 1. Seleciona o banco da Raw Landing Zone HTAP
use('htap_raw_lake');

// 2. Garante o Índice Único de Idempotência
// Evita duplicação acidental de Webhooks ou Extrações repetidas
db.raw_landing_zone.createIndex(
  { "idempotency_key": 1 }, 
  { unique: true, name: "idx_unique_idempotency" }
);

// 3. Índice para busca e ordenação rápida por Status e Timestamp
db.raw_landing_zone.createIndex(
  { "status": 1, "ingested_at": -1 }, 
  { name: "idx_status_ingestion" }
);

// 4. Payloads simulados do Comex Data Engine V2
const samplePayloads = [
  {
    idempotency_key: "DI-2026-BR-009841",
    source: "SISCOMEX_DUIMP",
    source_type: "CUSTOMS_DECLARATION",
    ingested_at: new Date(),
    status: "PENDING_PROCESSING",
    raw_payload: {
      numero_declaracao: "26/009841-0",
      importador_cnpj: "12.345.678/0001-90",
      porto_desembarque: "BRSSZ - Santos",
      itens: [
        { ncm: "8471.30.12", descricao: "Laptops Industriais", valor_moeda: 15400.00, moeda: "USD" },
        { ncm: "8504.40.10", descricao: "Fontes de Alimentação", valor_moeda: 3200.50, moeda: "USD" }
      ],
      tributos_estimados: { ii: 2450.00, ipi: 1200.00, pis: 350.20, cofins: 1610.90 }
    }
  },
  {
    idempotency_key: "PTAX-USD-20261109",
    source: "BACEN_PTAX_API",
    source_type: "EXCHANGE_RATE",
    ingested_at: new Date(),
    status: "PROCESSED_TO_LEDGER",
    raw_payload: {
      moeda: "USD",
      tipo_boletim: "Fechamento",
      taxa_compra: 5.8234,
      taxa_venda: 5.8240,
      data_cotacao: "2026-11-09T13:00:00Z"
    }
  }
];

// Ingestão idempotente (upsert)
samplePayloads.forEach(doc => {
  db.raw_landing_zone.updateOne(
    { idempotency_key: doc.idempotency_key },
    { $setOnInsert: doc },
    { upsert: true }
  );
});

// 5. Consulta Analítica e Operacional de Validação
const pendentes = db.raw_landing_zone.find(
  { status: "PENDING_PROCESSING" },
  { idempotency_key: 1, source: 1, ingested_at: 1, "raw_payload.numero_declaracao": 1 }
).toArray();

const resumoFontes = db.raw_landing_zone.aggregate([
  {
    $group: {
      _id: "$source",
      total_payloads: { $sum: 1 },
      ultimo_registro: { $max: "$ingested_at" }
    }
  },
  { $sort: { total_payloads: -1 } }
]).toArray();

// 6. Objeto de Retorno Exibido no Painel Dividido do VS Code
({
  mensagem: "✅ Playground Comex executado com sucesso!",
  total_documentos_landing: db.raw_landing_zone.countDocuments(),
  pendentes_processamento: pendentes,
  resumo_por_fonte: resumoFontes
});
