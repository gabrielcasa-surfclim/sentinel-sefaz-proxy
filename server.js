require("dotenv").config();
const express = require("express");
const cors = require("cors");
const https = require("https");
const { URL } = require("url");
const pako = require("pako");
const { createClient } = require("@supabase/supabase-js");
const { SignedXml } = require("xml-crypto");
const cron = require("node-cron");

const app = express();
const PORT = process.env.PORT || 3333;

app.use(cors());
app.use(express.json({ limit: "5mb" }));

// ══════════════════════════════════════════════════════
// SUPABASE
// ══════════════════════════════════════════════════════
let _admin = null;
function getSupabaseAdmin() {
  if (!_admin) {
    _admin = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
  }
  return _admin;
}

// ══════════════════════════════════════════════════════
// SEFAZ CONFIG
// ══════════════════════════════════════════════════════
const SEFAZ_URLS = {
  dist_dfe: {
    producao: "https://www1.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx",
    homologacao: "https://hom1.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx",
  },
  recepcao_evento: {
    producao: "https://www1.nfe.fazenda.gov.br/NFeRecepcaoEvento4/NFeRecepcaoEvento4.asmx",
    homologacao: "https://hom1.nfe.fazenda.gov.br/NFeRecepcaoEvento4/NFeRecepcaoEvento4.asmx",
  },
};

const UF_CODE = {
  AC: "12", AL: "27", AP: "16", AM: "13", BA: "29", CE: "23", DF: "53",
  ES: "32", GO: "52", MA: "21", MT: "51", MS: "50", MG: "31", PA: "15",
  PB: "25", PR: "41", PE: "26", PI: "22", RJ: "33", RN: "24", RS: "43",
  RO: "11", RR: "14", SC: "42", SP: "35", SE: "28", TO: "17",
};

const MANIFESTACAO = {
  ciencia:       { codigo: "210210", descricao: "Ciencia da Operacao" },
  confirmada:    { codigo: "210200", descricao: "Confirmacao da Operacao" },
  desconhecida:  { codigo: "210220", descricao: "Desconhecimento da Operacao" },
  nao_realizada: { codigo: "210240", descricao: "Operacao nao Realizada" },
};

// ══════════════════════════════════════════════════════
// STORAGE HELPERS — XML sempre no Storage, nunca truncado
// ══════════════════════════════════════════════════════
const STORAGE_BUCKET = "nfe-xmls";

async function salvarXmlStorage(supabase, empresaId, chNFe, xmlContent) {
  const path = `${empresaId}/nfe/${chNFe}.xml`;
  const { error } = await supabase.storage
    .from(STORAGE_BUCKET)
    .upload(path, Buffer.from(xmlContent, "utf-8"), {
      contentType: "application/xml",
      upsert: true,
    });
  if (error) throw new Error(`Storage upload falhou: ${error.message}`);
  return path;
}

function xmlParaColuna(xmlContent) {
  return xmlContent.length <= 40000 ? xmlContent : null;
}

// ══════════════════════════════════════════════════════
// FUNÇÕES AUXILIARES
// ══════════════════════════════════════════════════════
function sefazRequest(url, soapBody, certPem, keyPem, timeoutMs = 30000, soapAction = null) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    let contentType = "application/soap+xml; charset=utf-8";
    if (soapAction) {
      contentType = `application/soap+xml; charset=utf-8; action="${soapAction}"`;
    }
    const headers = {
      "Content-Type": contentType,
      "Content-Length": Buffer.byteLength(soapBody, "utf-8"),
    };
    if (soapAction) {
      headers["SOAPAction"] = `"${soapAction}"`;
    }
    const options = {
      hostname: parsed.hostname, port: 443, path: parsed.pathname, method: "POST",
      headers, cert: certPem, key: keyPem, rejectUnauthorized: true, timeout: timeoutMs,
    };
    console.log(`[sefazRequest] URL: ${url}`);
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => resolve({ status: res.statusCode, body: data }));
    });
    req.on("error", (err) => reject(err));
    req.on("timeout", () => { req.destroy(); reject(new Error("Timeout SEFAZ")); });
    req.write(soapBody);
    req.end();
  });
}

function xmlTag(xml, tag) {
  const m = xml.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i"));
  return m ? m[1].trim() : "";
}

function parseRetEvento(respXml) {
  const retEvento = respXml.match(/<retEvento[\s\S]*?<\/retEvento>/i);
  if (!retEvento) {
    return {
      cStat: xmlTag(respXml, "cStat"),
      xMotivo: xmlTag(respXml, "xMotivo"),
      nProt: xmlTag(respXml, "nProt") || null,
      chNFe: xmlTag(respXml, "chNFe") || null,
    };
  }
  const ret = retEvento[0];
  return {
    cStat: xmlTag(ret, "cStat"),
    xMotivo: xmlTag(ret, "xMotivo"),
    nProt: xmlTag(ret, "nProt") || null,
    chNFe: xmlTag(ret, "chNFe") || null,
    tpEvento: xmlTag(ret, "tpEvento") || null,
    xEvento: xmlTag(ret, "xEvento") || null,
    nSeqEvento: xmlTag(ret, "nSeqEvento") || null,
    dhRegEvento: xmlTag(ret, "dhRegEvento") || null,
  };
}

function pad15(nsu) {
  return (nsu || "0").replace(/\D/g, "").padStart(15, "0");
}

function dhEvento() {
  const now = new Date();
  const off = -3;
  const local = new Date(now.getTime() + off * 3600000);
  const iso = local.toISOString().replace("Z", "").slice(0, 19);
  return `${iso}-03:00`;
}

function decompressGzip(b64) {
  const compressed = Buffer.from(b64, "base64");
  const decompressed = pako.inflate(compressed);
  return Buffer.from(decompressed).toString("utf-8");
}

// ══════════════════════════════════════════════════════
// ASSINATURA DIGITAL XML (XMLDSig)
// ══════════════════════════════════════════════════════
function extrairCertBase64(certPem) {
  return certPem
    .replace(/-----BEGIN CERTIFICATE-----/g, "")
    .replace(/-----END CERTIFICATE-----/g, "")
    .replace(/\s/g, "");
}

function assinarEvento(eventoXml, certPem, keyPem) {
  const certBase64 = extrairCertBase64(certPem);
  const sig = new SignedXml({
    privateKey: keyPem,
    canonicalizationAlgorithm: "http://www.w3.org/TR/2001/REC-xml-c14n-20010315",
    signatureAlgorithm: "http://www.w3.org/2000/09/xmldsig#rsa-sha1",
    getKeyInfoContent: function () {
      return `<X509Data><X509Certificate>${certBase64}</X509Certificate></X509Data>`;
    },
  });
  sig.addReference({
    xpath: "//*[local-name(.)='infEvento']",
    transforms: [
      "http://www.w3.org/2000/09/xmldsig#enveloped-signature",
      "http://www.w3.org/TR/2001/REC-xml-c14n-20010315",
    ],
    digestAlgorithm: "http://www.w3.org/2000/09/xmldsig#sha1",
  });
  sig.computeSignature(eventoXml, {
    location: {
      reference: "//*[local-name(.)='infEvento']",
      action: "after",
    },
  });
  return sig.getSignedXml();
}

// ══════════════════════════════════════════════════════
// BUILD SOAP — DISTRIBUIÇÃO DF-e
// ══════════════════════════════════════════════════════
function buildDistDFeSoap(cnpj, ufCode, tpAmb, ultNSU) {
  const nsu = pad15(ultNSU);
  const body = `<distDFeInt xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.01"><tpAmb>${tpAmb}</tpAmb><cUFAutor>${ufCode}</cUFAutor><CNPJ>${cnpj}</CNPJ><distNSU><ultNSU>${nsu}</ultNSU></distNSU></distDFeInt>`;
  return `<?xml version="1.0" encoding="utf-8"?><soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope"><soap12:Body><nfeDistDFeInteresse xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe"><nfeDadosMsg>${body}</nfeDadosMsg></nfeDistDFeInteresse></soap12:Body></soap12:Envelope>`;
}

// ══════════════════════════════════════════════════════
// BUILD SOAP — MANIFESTAÇÃO (com assinatura digital)
// ══════════════════════════════════════════════════════
function buildManifestacaoSoap(chNFe, cnpj, tpAmb, codigoEvento, descEvento, justificativa, certPem, keyPem) {
  const dh = dhEvento();
  const nSeqEvento = "1";
  const seqPadded = nSeqEvento.padStart(2, "0");
  const eventoId = `ID${codigoEvento}${chNFe}${seqPadded}`;
  console.log(`[buildManifestacao] eventoId: ${eventoId} (length: ${eventoId.length})`);
  let detEvento = `<detEvento versao="1.00"><descEvento>${descEvento}</descEvento>`;
  if (justificativa) detEvento += `<xJust>${justificativa}</xJust>`;
  detEvento += `</detEvento>`;
  const eventoXml = `<evento xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.00"><infEvento Id="${eventoId}"><cOrgao>91</cOrgao><tpAmb>${tpAmb}</tpAmb><CNPJ>${cnpj}</CNPJ><chNFe>${chNFe}</chNFe><dhEvento>${dh}</dhEvento><tpEvento>${codigoEvento}</tpEvento><nSeqEvento>${nSeqEvento}</nSeqEvento><verEvento>1.00</verEvento>${detEvento}</infEvento></evento>`;
  console.log(`[buildManifestacao] Evento antes de assinar:\n${eventoXml}`);
  const eventoAssinado = assinarEvento(eventoXml, certPem, keyPem);
  console.log(`[buildManifestacao] Evento assinado:\n${eventoAssinado}`);
  const envioLote = `<envEvento xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.00"><idLote>1</idLote>${eventoAssinado}</envEvento>`;
  return `<?xml version="1.0" encoding="utf-8"?><soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope"><soap12:Body><nfeDadosMsg xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4">${envioLote}</nfeDadosMsg></soap12:Body></soap12:Envelope>`;
}

// ══════════════════════════════════════════════════════
// HELPER: Extrair parcelas (duplicatas) do XML da NF-e
// ══════════════════════════════════════════════════════
function extrairParcelas(xmlContent) {
  const parcelas = [];
  const dupMatches = xmlContent.match(/<dup>([\s\S]*?)<\/dup>/gi) || [];
  dupMatches.forEach((dup, index) => {
    const nDup = xmlTag(dup, "nDup") || String(index + 1);
    let dVenc = xmlTag(dup, "dVenc");
    const vDup = parseFloat(xmlTag(dup, "vDup"));

    // Normalizar dVenc: YYYYMMDD → YYYY-MM-DD
    if (dVenc && dVenc.length === 8 && !dVenc.includes("-")) {
      dVenc = `${dVenc.slice(0, 4)}-${dVenc.slice(4, 6)}-${dVenc.slice(6, 8)}`;
    }

    // parcela_numero: tenta extrair número do nDup (ex: "001"→1, "19A"→19, "A"→index)
    const numerico = parseInt(nDup.replace(/\D/g, ""), 10);
    const parcelaNumero = isNaN(numerico) ? index + 1 : numerico;

    if (dVenc && vDup > 0) {
      parcelas.push({
        nDup,
        vencimento: dVenc,
        valor: vDup,
        index: index + 1,
        parcelaNumero,
      });
    }
  });
  return parcelas;
}

// ══════════════════════════════════════════════════════
// HELPER: Buscar ou criar entidade pelo CNPJ do emitente
// ══════════════════════════════════════════════════════
async function buscarOuCriarEntidade(supabase, empresaId, cnpjEmitente, nomeEmitente) {
  if (!cnpjEmitente) return null;
  const cnpjLimpo = cnpjEmitente.replace(/\D/g, "");
  if (!cnpjLimpo) return null;

  // Busca existente pelo CNPJ
  const { data: existente } = await supabase
    .from("entidades")
    .select("id")
    .eq("empresa_id", empresaId)
    .eq("cnpj_cpf", cnpjLimpo)
    .maybeSingle();

  if (existente) return existente.id;

  // Cria nova entidade automaticamente
  const { data: nova, error } = await supabase
    .from("entidades")
    .insert({
      empresa_id: empresaId,
      razao_social: nomeEmitente || cnpjLimpo,
      cnpj_cpf: cnpjLimpo,
      tipo: "fornecedor",
      ativo: true,
      prazo_padrao_dias: 30,
    })
    .select("id")
    .single();

  if (error) {
    console.error(`[buscarOuCriarEntidade] Erro ao criar entidade ${cnpjLimpo}: ${error.message}`);
    return null;
  }

  console.log(`[buscarOuCriarEntidade] Entidade criada: ${nomeEmitente || cnpjLimpo}`);
  return nova.id;
}

// ══════════════════════════════════════════════════════
// HELPER: Salvar log e controle de sync
// ══════════════════════════════════════════════════════
async function salvarSyncLog(supabase, empresaId, status, notasEncontradas, notasNovas, nsuInicio, nsuFim, mensagem) {
  try {
    const { data: existing } = await supabase
      .from("controle_sync").select("id")
      .eq("empresa_id", empresaId).eq("tipo", "sefaz").maybeSingle();
    if (existing) {
      await supabase.from("controle_sync").update({
        ultima_execucao: new Date().toISOString(),
        ultimo_nsu: nsuFim,
        status,
        total_notas_sync: notasNovas,
        erro_ultima_sync: status === "erro" ? mensagem : null,
        updated_at: new Date().toISOString(),
      }).eq("id", existing.id);
    } else {
      await supabase.from("controle_sync").insert({
        empresa_id: empresaId,
        tipo: "sefaz",
        ultima_execucao: new Date().toISOString(),
        ultimo_nsu: nsuFim,
        status,
        total_notas_sync: notasNovas,
        erro_ultima_sync: status === "erro" ? mensagem : null,
      });
    }
    await supabase.from("log_sync_sefaz").insert({
      empresa_id: empresaId,
      tipo: "sync",
      status,
      notas_encontradas: notasEncontradas,
      notas_novas: notasNovas,
      nsu_inicio: nsuInicio,
      nsu_fim: nsuFim,
      mensagem,
    });
  } catch (e) {
    console.error(`[salvarSyncLog] Erro: ${e.message}`);
  }
}

// ══════════════════════════════════════════════════════
// HELPER: Criar contas a pagar das parcelas
// ══════════════════════════════════════════════════════
async function criarContasPagar(supabase, empresaId, nfId, numero, nomeEmitente, dataEmissao, valorTotal, chNFe, xmlContent, errors, cnpjEmitente) {
  try {
    // Busca ou cria entidade pelo CNPJ
    const entidadeId = await buscarOuCriarEntidade(supabase, empresaId, cnpjEmitente, nomeEmitente);

    const parcelas = extrairParcelas(xmlContent);
    const dataEmissaoFormatada = dataEmissao ? dataEmissao.split("T")[0] : null;

    if (parcelas.length > 0) {
      const contasInsert = parcelas.map((p) => ({
        empresa_id: empresaId,
        tipo: "pagar",
        status: "previsto",
        origem: "sefaz",
        descricao: `NF ${numero || ""} - ${nomeEmitente || ""} (${p.nDup}/${parcelas.length})`.trim(),
        valor_original: p.valor,
        data_vencimento: p.vencimento,
        data_emissao: dataEmissaoFormatada,
        nota_fiscal_id: nfId,
        entidade_id: entidadeId,
        parcela_numero: p.parcelaNumero,
        parcela_total: parcelas.length,
        documento_ref: chNFe,
      }));

      const { error: contasErr } = await supabase.from("contas").insert(contasInsert);
      if (contasErr) {
        console.error(`[criarContas] Erro: ${contasErr.message}`);
        errors.push(`Erro contas NF ${chNFe.slice(-10)}: ${contasErr.message}`);
      } else {
        console.log(`[criarContas] NF ${numero}: ${parcelas.length} conta(s) criada(s) | entidade: ${entidadeId || "não vinculada"}`);
      }
    } else {
      // Sem duplicatas: NF à vista — vencimento = hoje (usuário ajusta)
      const hoje = new Date().toISOString().split("T")[0];
      await supabase.from("contas").insert({
        empresa_id: empresaId,
        tipo: "pagar",
        status: "previsto",
        origem: "sefaz",
        descricao: `NF ${numero || ""} - ${nomeEmitente || ""}`.trim(),
        valor_original: valorTotal || 0,
        data_vencimento: hoje,
        data_emissao: dataEmissaoFormatada,
        nota_fiscal_id: nfId,
        entidade_id: entidadeId,
        parcela_numero: 1,
        parcela_total: 1,
        documento_ref: chNFe,
      });
      console.log(`[criarContas] NF ${numero}: 1 conta criada (sem duplicatas no XML) | entidade: ${entidadeId || "não vinculada"}`);
    }
  } catch (e) {
    console.error(`[criarContas] Erro: ${e.message}`);
    errors.push(`Erro inesperado contas NF ${chNFe.slice(-10)}: ${e.message}`);
  }
}

// ══════════════════════════════════════════════════════
// AUTH MIDDLEWARE
// ══════════════════════════════════════════════════════
function authMiddleware(req, res, next) {
  const token = req.headers["x-api-secret"];
  if (!token || token !== process.env.API_SECRET) {
    return res.status(401).json({ success: false, error: "Unauthorized" });
  }
  next();
}

// ══════════════════════════════════════════════════════
// HEALTH CHECK
// ══════════════════════════════════════════════════════
app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    service: "sentinel-sefaz-proxy",
    timestamp: new Date().toISOString(),
    ambiente: process.env.SEFAZ_AMBIENTE || "producao",
  });
});

// ══════════════════════════════════════════════════════
// CONSULTA POR CHAVE — POST /api/consulta-chave
// ══════════════════════════════════════════════════════
app.post("/api/consulta-chave", authMiddleware, async (req, res) => {
  const { empresa_id, chave_acesso, ambiente = "producao" } = req.body;
  if (!empresa_id) return res.status(400).json({ success: false, error: "empresa_id obrigatório" });
  if (!chave_acesso || chave_acesso.replace(/\D/g, "").length !== 44)
    return res.status(400).json({ success: false, error: "chave_acesso inválida (44 dígitos)" });

  const tpAmb = ambiente === "homologacao" ? "2" : "1";
  const sefazUrl = SEFAZ_URLS.dist_dfe[ambiente] || SEFAZ_URLS.dist_dfe.producao;
  const supabase = getSupabaseAdmin();

  const { data: empresa } = await supabase
    .from("empresas").select("id, cnpj, uf, sefaz_ativo")
    .eq("id", empresa_id).single();
  if (!empresa) return res.status(404).json({ success: false, error: "Empresa não encontrada" });

  const cnpj = (empresa.cnpj || "").replace(/\D/g, "");
  const uf = (empresa.uf || "").toUpperCase().trim();
  const ufCode = UF_CODE[uf];
  if (!ufCode) return res.status(400).json({ success: false, error: "UF inválida" });

  const { data: cert } = await supabase
    .from("certificados_digitais").select("cert_pem, key_pem")
    .eq("empresa_id", empresa_id).eq("ativo", true)
    .order("created_at", { ascending: false }).limit(1).maybeSingle();
  if (!cert || !cert.cert_pem || !cert.key_pem)
    return res.status(404).json({ success: false, error: "Certificado não configurado" });

  const chave = chave_acesso.replace(/\D/g, "");
  const body = `<distDFeInt xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.01"><tpAmb>${tpAmb}</tpAmb><cUFAutor>${ufCode}</cUFAutor><CNPJ>${cnpj}</CNPJ><consChNFe><chNFe>${chave}</chNFe></consChNFe></distDFeInt>`;
  const soap = `<?xml version="1.0" encoding="utf-8"?><soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope"><soap12:Body><nfeDistDFeInteresse xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe"><nfeDadosMsg>${body}</nfeDadosMsg></nfeDistDFeInteresse></soap12:Body></soap12:Envelope>`;

  let respText;
  try {
    console.log(`[consulta-chave] Chave: ${chave} | CNPJ: ${cnpj}`);
    const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem);
    respText = resp.body;
    console.log(`[consulta-chave] HTTP ${resp.status}`);
  } catch (e) {
    return res.status(502).json({ success: false, error: `Erro conexão SEFAZ: ${e.message}` });
  }

  const cStat = xmlTag(respText, "cStat");
  const xMotivo = xmlTag(respText, "xMotivo");
  console.log(`[consulta-chave] cStat: ${cStat} | ${xMotivo}`);

  if (cStat !== "138") {
    return res.json({
      success: false,
      data: { cStat, xMotivo, chave_acesso: chave, xml: null, is_complete: false, reason: xMotivo },
    });
  }

  const docZipMatch = respText.match(/<docZip[^>]*>([^<]+)<\/docZip>/i);
  let xmlCompleto = null;
  let isComplete = false;
  let reason = "Nenhum docZip retornado pela SEFAZ";

  if (docZipMatch) {
    try {
      xmlCompleto = decompressGzip(docZipMatch[1].trim());
      isComplete = xmlCompleto.includes("<nfeProc") || xmlCompleto.includes("<procNFe");
      if (!isComplete) {
        reason = "SEFAZ retornou apenas o resumo (resNFe). Manifeste Ciência da Operação e aguarde alguns minutos.";
      }
      console.log(`[consulta-chave] XML descomprimido: ${xmlCompleto.length} chars | isComplete: ${isComplete}`);
    } catch (e) {
      console.error(`[consulta-chave] Erro ao descomprimir docZip: ${e.message}`);
      reason = `Erro ao descomprimir XML: ${e.message}`;
    }
  }

  return res.json({
    success: cStat === "138",
    data: { cStat, xMotivo, chave_acesso: chave, xml: xmlCompleto, is_complete: isComplete, reason: isComplete ? null : reason },
  });
});

// ══════════════════════════════════════════════════════
// HELPER: Processar um docZip e persistir NF
// ══════════════════════════════════════════════════════
async function processarDocZip(supabase, docZip, empresaId, cnpj, tpAmb, certPem, keyPem, errors, sefazAmbiente) {
  const nsuMatch = docZip.match(/NSU="(\d+)"/i);
  const schemaMatch = docZip.match(/schema="([^"]+)"/i);
  const b64 = docZip.replace(/<\/?docZip[^>]*>/gi, "").trim();
  if (!b64) return null;

  const xmlContent = decompressGzip(b64);
  const schema = schemaMatch ? schemaMatch[1] : null;
  const nsuVal = nsuMatch ? nsuMatch[1] : null;

  const isResEvento = schema?.includes("resEvento") || xmlContent.includes("<resEvento");
  const isProcEvento = schema?.includes("procEventoNFe") || xmlContent.includes("<procEventoNFe");
  const isResNFe = schema?.includes("resNFe") || xmlContent.includes("<resNFe");
  const isNFeProc = schema?.includes("procNFe") || xmlContent.includes("<nfeProc");

  if (isResEvento) {
    console.log(`[processarDoc] NSU ${nsuVal}: resEvento ignorado`);
    return null;
  }

  if (isProcEvento) {
    console.log(`[processarDoc] NSU ${nsuVal}: procEventoNFe ignorado`);
    const chNFeEvt = xmlTag(xmlContent, "chNFe");
    if (chNFeEvt) {
      await supabase.from("notas_fiscais").update({ status_manifestacao: "ciencia" })
        .eq("empresa_id", empresaId).eq("chave_acesso", chNFeEvt);
    }
    return null;
  }

  const chNFe = xmlTag(xmlContent, "chNFe");
  if (!chNFe) return null;

  const { data: existing } = await supabase
    .from("notas_fiscais").select("id, tipo_documento")
    .eq("empresa_id", empresaId).eq("chave_acesso", chNFe).maybeSingle();

  // ── Upgrade: resumo → completo ────────────────────────────────────────────
  if (existing && isNFeProc && existing.tipo_documento !== "completo") {
    const emitBlock = xmlContent.match(/<emit>([\s\S]*?)<\/emit>/)?.[1] || "";
    const upCnpj = xmlTag(emitBlock, "CNPJ") || xmlTag(emitBlock, "CPF") || null;
    const upNome = xmlTag(emitBlock, "xNome") || null;
    const upNumero = xmlTag(xmlContent, "nNF") || null;
    const upSerie = xmlTag(xmlContent, "serie") || null;
    const upData = xmlTag(xmlContent, "dhEmi") || null;
    const totBlock = xmlContent.match(/<ICMSTot>([\s\S]*?)<\/ICMSTot>/)?.[1] || "";
    const upValor = parseFloat(xmlTag(totBlock, "vNF")) || parseFloat(xmlTag(xmlContent, "vNF")) || null;

    let storagePath = null;
    try {
      storagePath = await salvarXmlStorage(supabase, empresaId, chNFe, xmlContent);
    } catch (e) {
      console.error(`[processarDoc] Storage upload falhou (upgrade): ${e.message}`);
      errors.push(`Storage falhou NF ${chNFe.slice(-10)}: ${e.message}`);
    }

    await supabase.from("notas_fiscais").update({
      tipo_documento: "completo",
      xml_completo: xmlParaColuna(xmlContent),
      xml_storage_path: storagePath,
      numero_nf: upNumero,
      serie: upSerie,
      data_emissao: upData,
      valor_total_nf: upValor,
      emit_cnpj: upCnpj,
      emit_razao_social: upNome,
      processado: true,
    }).eq("id", existing.id);

    console.log(`[processarDoc] NF ${upNumero || chNFe.slice(-10)}: atualizada resumo → completo`);

    const { data: contasExist } = await supabase
      .from("contas").select("id").eq("nota_fiscal_id", existing.id).limit(1);
    if (!contasExist || contasExist.length === 0) {
      await criarContasPagar(supabase, empresaId, existing.id, upNumero, upNome, upData, upValor, chNFe, xmlContent, errors, upCnpj);
    }
    return { action: "upgrade", nfId: existing.id };
  }

  if (existing) return null;

  // ── Insert: nova NF ───────────────────────────────────────────────────────
  let numero, serie, dataEmissao, valorTotal, cnpjEmitente, nomeEmitente, tipoDoc;

  if (isNFeProc) {
    const emitBlock = xmlContent.match(/<emit>([\s\S]*?)<\/emit>/)?.[1] || "";
    cnpjEmitente = xmlTag(emitBlock, "CNPJ") || xmlTag(emitBlock, "CPF") || null;
    nomeEmitente = xmlTag(emitBlock, "xNome") || null;
    numero = xmlTag(xmlContent, "nNF") || null;
    serie = xmlTag(xmlContent, "serie") || null;
    dataEmissao = xmlTag(xmlContent, "dhEmi") || null;
    const totBlock = xmlContent.match(/<ICMSTot>([\s\S]*?)<\/ICMSTot>/)?.[1] || "";
    valorTotal = parseFloat(xmlTag(totBlock, "vNF")) || parseFloat(xmlTag(xmlContent, "vNF")) || null;
    tipoDoc = "completo";
  } else if (isResNFe) {
    cnpjEmitente = xmlTag(xmlContent, "CNPJ") || null;
    nomeEmitente = xmlTag(xmlContent, "xNome") || null;
    if (chNFe && chNFe.length === 44) {
      serie = String(parseInt(chNFe.substring(22, 25), 10));
      numero = String(parseInt(chNFe.substring(25, 34), 10));
    } else { serie = null; numero = null; }
    dataEmissao = xmlTag(xmlContent, "dhEmi") || null;
    valorTotal = parseFloat(xmlTag(xmlContent, "vNF")) || null;
    tipoDoc = "resumo";
  } else {
    cnpjEmitente = xmlTag(xmlContent, "CNPJ") || null;
    nomeEmitente = xmlTag(xmlContent, "xNome") || null;
    numero = xmlTag(xmlContent, "nNF") || null;
    serie = xmlTag(xmlContent, "serie") || null;
    dataEmissao = xmlTag(xmlContent, "dhEmi") || null;
    valorTotal = parseFloat(xmlTag(xmlContent, "vNF")) || null;
    tipoDoc = "outro";
  }

  let storagePath = null;
  try {
    storagePath = await salvarXmlStorage(supabase, empresaId, chNFe, xmlContent);
  } catch (e) {
    console.error(`[processarDoc] Storage upload falhou (insert): ${e.message}`);
    errors.push(`Storage falhou NF ${chNFe.slice(-10)}: ${e.message}`);
  }

  const { data: insertedNF, error: insertErr } = await supabase.from("notas_fiscais").insert({
    empresa_id: empresaId,
    chave_acesso: chNFe,
    numero_nf: numero,
    serie,
    data_emissao: dataEmissao,
    valor_total_nf: valorTotal,
    emit_cnpj: cnpjEmitente,
    emit_razao_social: nomeEmitente,
    status_sefaz: "recebida",
    nsu: nsuVal,
    tipo_documento: tipoDoc,
    xml_completo: xmlParaColuna(xmlContent),
    xml_storage_path: storagePath,
    origem: "sefaz",
    processado: isNFeProc,
  }).select("id").single();

  if (insertErr) {
    errors.push(`Erro ao inserir NF ${chNFe.slice(-10)}: ${insertErr.message}`);
    return null;
  }

  // Auto-manifestar Ciência para resumos
  if (isResNFe && chNFe && certPem && keyPem) {
    try {
      const manifestSoap = buildManifestacaoSoap(
        chNFe, cnpj, tpAmb, "210210", "Ciencia da Operacao",
        undefined, certPem, keyPem
      );
      const manifestUrl = SEFAZ_URLS.recepcao_evento[sefazAmbiente] || SEFAZ_URLS.recepcao_evento.producao;
      const soapAction = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento";
      const manifestResp = await sefazRequest(manifestUrl, manifestSoap, certPem, keyPem, 30000, soapAction);
      const manifestEvento = parseRetEvento(manifestResp.body);
      const manifestOk = manifestEvento.cStat === "135" || manifestEvento.cStat === "136" || manifestEvento.cStat === "573";
      if (manifestOk) {
        await supabase.from("notas_fiscais").update({ status_manifestacao: "ciencia" }).eq("id", insertedNF.id);
        console.log(`[processarDoc] NF ${chNFe.slice(-10)}: Ciência registrada (${manifestEvento.cStat})`);
      } else {
        console.warn(`[processarDoc] NF ${chNFe.slice(-10)}: Ciência falhou: ${manifestEvento.cStat} - ${manifestEvento.xMotivo}`);
      }
      await new Promise(r => setTimeout(r, 1000));
    } catch (e) {
      console.error(`[processarDoc] Erro manifestação: ${e.message}`);
    }
  }

  // Auto-gerar contas a pagar para procNFe
  if (isNFeProc) {
    await criarContasPagar(supabase, empresaId, insertedNF.id, numero, nomeEmitente, dataEmissao, valorTotal, chNFe, xmlContent, errors, cnpjEmitente);
  }

  return { action: "insert", nfId: insertedNF.id };
}

// ══════════════════════════════════════════════════════
// SYNC SEFAZ — POST /api/sync-sefaz
// ══════════════════════════════════════════════════════
app.post("/api/sync-sefaz", authMiddleware, async (req, res) => {
  const { empresa_id, ambiente = "producao", reset_nsu } = req.body;
  if (!empresa_id) return res.status(400).json({ success: false, error: "empresa_id obrigatório" });

  const tpAmb = ambiente === "homologacao" ? "2" : "1";
  const sefazUrl = SEFAZ_URLS.dist_dfe[ambiente] || SEFAZ_URLS.dist_dfe.producao;
  const supabase = getSupabaseAdmin();

  const { data: empresa } = await supabase
    .from("empresas").select("id, cnpj, uf, sefaz_ativo, sefaz_ultimo_nsu")
    .eq("id", empresa_id).single();
  if (!empresa) return res.status(404).json({ success: false, error: "Empresa não encontrada" });
  if (!empresa.sefaz_ativo) return res.status(400).json({ success: false, error: "SEFAZ não ativa" });

  const cnpj = (empresa.cnpj || "").replace(/\D/g, "");
  const uf = (empresa.uf || "").toUpperCase().trim();
  const ufCode = UF_CODE[uf];
  if (!ufCode) return res.status(400).json({ success: false, error: "UF inválida" });

  const { data: cert } = await supabase
    .from("certificados_digitais").select("cert_pem, key_pem")
    .eq("empresa_id", empresa_id).eq("ativo", true)
    .order("created_at", { ascending: false }).limit(1).maybeSingle();
  if (!cert || !cert.cert_pem || !cert.key_pem)
    return res.status(404).json({ success: false, error: "Certificado não configurado" });

  let ultNSU = reset_nsu ? "0" : (empresa.sefaz_ultimo_nsu || "0");
  const nsuInicio = ultNSU;
  let maxNSU = "999999999999999";
  let totalDocs = 0;
  let notasNovas = 0;
  let loops = 0;
  const maxLoops = req.body.max_loops || 5;
  const errors = [];

  while (ultNSU < maxNSU && loops < maxLoops) {
    loops++;
    const soap = buildDistDFeSoap(cnpj, ufCode, tpAmb, ultNSU);

    let respText;
    try {
      const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem);
      respText = resp.body;
    } catch (e) {
      errors.push(`Erro conexão: ${e.message}`);
      await salvarSyncLog(supabase, empresa_id, "erro", totalDocs, notasNovas, nsuInicio, ultNSU, `Erro conexão SEFAZ: ${e.message}`);
      return res.status(502).json({ success: false, error: `Erro conexão SEFAZ: ${e.message}`, data: { notas_encontradas: totalDocs, notas_novas: notasNovas, ultimo_nsu: ultNSU, loops, errors } });
    }

    const cStat = xmlTag(respText, "cStat");
    const xMotivo = xmlTag(respText, "xMotivo");
    const newUltNSU = xmlTag(respText, "ultNSU");
    const newMaxNSU = xmlTag(respText, "maxNSU");

    console.log(`[sync] Loop ${loops}: cStat=${cStat} ultNSU=${newUltNSU} maxNSU=${newMaxNSU}`);

    if (cStat === "137" || cStat === "138") {
      if (newUltNSU) ultNSU = newUltNSU;
      if (newMaxNSU) maxNSU = newMaxNSU;

      const docMatches = respText.match(/<docZip[\s\S]*?<\/docZip>/gi) || [];
      totalDocs += docMatches.length;

      for (const docZip of docMatches) {
        try {
          const result = await processarDocZip(supabase, docZip, empresa_id, cnpj, tpAmb, cert.cert_pem, cert.key_pem, errors, ambiente);
          if (result) notasNovas++;
        } catch (docErr) {
          console.error(`[sync] Erro processando doc: ${docErr.message}`);
          errors.push(docErr.message);
        }
      }

      await supabase.from("empresas").update({ sefaz_ultimo_nsu: ultNSU }).eq("id", empresa_id);
      if (cStat === "137" || ultNSU >= maxNSU) break;

    } else if (cStat === "656") {
      await salvarSyncLog(supabase, empresa_id, "erro", totalDocs, notasNovas, nsuInicio, ultNSU, "Consumo indevido. Aguarde 1h.");
      return res.json({ success: true, data: { notas_encontradas: totalDocs, notas_novas: notasNovas, ultimo_nsu: ultNSU, max_nsu: maxNSU, loops, errors, consumo_indevido: true, nenhum_documento: false } });
    } else {
      await salvarSyncLog(supabase, empresa_id, "erro", totalDocs, notasNovas, nsuInicio, ultNSU, `SEFAZ ${cStat}: ${xMotivo}`);
      return res.json({ success: false, error: `SEFAZ ${cStat}: ${xMotivo}`, data: { notas_encontradas: totalDocs, notas_novas: notasNovas, ultimo_nsu: ultNSU, loops, errors } });
    }
  }

  const nenhum = totalDocs === 0 && notasNovas === 0;
  const status = errors.length > 0 ? "parcial" : "sucesso";
  await salvarSyncLog(supabase, empresa_id, status, totalDocs, notasNovas, nsuInicio, ultNSU, nenhum ? "Nenhum documento novo" : `${notasNovas} nota(s) nova(s) importada(s)`);

  // Auto-manifestação de resumos pendentes
  let manifestadas = 0;
  let xmlsCompletos = 0;
  try {
    const { data: nfsSemManifesto } = await supabase
      .from("notas_fiscais")
      .select("id, chave_acesso")
      .eq("empresa_id", empresa_id)
      .eq("tipo_documento", "resumo")
      .or("status_manifestacao.is.null,status_manifestacao.eq.pendente")
      .not("chave_acesso", "is", null)
      .limit(5);

    if (nfsSemManifesto && nfsSemManifesto.length > 0) {
      const sefazUrlEvento = SEFAZ_URLS.recepcao_evento[ambiente] || SEFAZ_URLS.recepcao_evento.producao;
      const soapAction = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento";

      for (const nf of nfsSemManifesto) {
        try {
          const soapManif = buildManifestacaoSoap(nf.chave_acesso, cnpj, tpAmb, MANIFESTACAO.ciencia.codigo, MANIFESTACAO.ciencia.descricao, undefined, cert.cert_pem, cert.key_pem);
          const respManif = await sefazRequest(sefazUrlEvento, soapManif, cert.cert_pem, cert.key_pem, 30000, soapAction);
          const evento = parseRetEvento(respManif.body);
          const sucManif = evento.cStat === "135" || evento.cStat === "136" || evento.cStat === "573";
          if (sucManif) {
            await supabase.from("notas_fiscais").update({ status_manifestacao: "ciencia" }).eq("id", nf.id);
            manifestadas++;
          }
          await new Promise(r => setTimeout(r, 3000));
        } catch (e) {
          console.error(`[sync] Erro manifestação: ${e.message}`);
        }
      }

      if (manifestadas > 0) {
        await new Promise(r => setTimeout(r, 15000));
        try {
          const soapRetry = buildDistDFeSoap(cnpj, ufCode, tpAmb, nsuInicio);
          const respRetry = await sefazRequest(sefazUrl, soapRetry, cert.cert_pem, cert.key_pem);
          const cStatRetry = xmlTag(respRetry.body, "cStat");
          if (cStatRetry === "137" || cStatRetry === "138") {
            const docMatchesRetry = respRetry.body.match(/<docZip[\s\S]*?<\/docZip>/gi) || [];
            for (const docZip of docMatchesRetry) {
              try {
                const result = await processarDocZip(supabase, docZip, empresa_id, cnpj, tpAmb, cert.cert_pem, cert.key_pem, errors, ambiente);
                if (result) xmlsCompletos++;
              } catch (e) {
                console.error(`[sync] Erro doc retry: ${e.message}`);
              }
            }
          }
        } catch (e) {
          console.error(`[sync] Erro re-sync: ${e.message}`);
        }
      }
    }
  } catch (e) {
    console.error(`[sync] Erro geral manifestação: ${e.message}`);
  }

  return res.json({ success: true, data: { notas_encontradas: totalDocs, notas_novas: notasNovas, ultimo_nsu: ultNSU, max_nsu: maxNSU, loops, errors, nenhum_documento: nenhum, parcial: errors.length > 0, consumo_indevido: false, manifestadas, xmls_completos: xmlsCompletos } });
});

// ══════════════════════════════════════════════════════
// MANIFESTAR SEFAZ — POST /api/manifestar-sefaz
// ══════════════════════════════════════════════════════
app.post("/api/manifestar-sefaz", authMiddleware, async (req, res) => {
  const { empresa_id, nota_fiscal_id, tipo_manifestacao, justificativa, ambiente = "producao" } = req.body;
  if (!empresa_id) return res.status(400).json({ success: false, error: "empresa_id obrigatório" });
  if (!nota_fiscal_id) return res.status(400).json({ success: false, error: "nota_fiscal_id obrigatório" });
  if (!tipo_manifestacao) return res.status(400).json({ success: false, error: "tipo_manifestacao obrigatório" });

  const manifestacao = MANIFESTACAO[tipo_manifestacao];
  if (!manifestacao) return res.status(400).json({ success: false, error: "tipo_manifestacao inválido" });
  if (tipo_manifestacao === "nao_realizada" && (!justificativa || justificativa.trim().length < 15))
    return res.status(400).json({ success: false, error: "Justificativa obrigatória (min 15 chars)" });

  const tpAmb = ambiente === "homologacao" ? "2" : "1";
  const sefazUrl = SEFAZ_URLS.recepcao_evento[ambiente] || SEFAZ_URLS.recepcao_evento.producao;
  const supabase = getSupabaseAdmin();

  const { data: empresa } = await supabase
    .from("empresas").select("id, cnpj, sefaz_ativo")
    .eq("id", empresa_id).single();
  if (!empresa) return res.status(404).json({ success: false, error: "Empresa não encontrada" });
  if (!empresa.sefaz_ativo) return res.status(400).json({ success: false, error: "SEFAZ não ativa" });
  const cnpj = empresa.cnpj.replace(/\D/g, "");

  const { data: nf } = await supabase
    .from("notas_fiscais").select("id, chave_acesso")
    .eq("id", nota_fiscal_id).eq("empresa_id", empresa_id).single();
  if (!nf) return res.status(404).json({ success: false, error: "NF não encontrada" });
  if (!nf.chave_acesso) return res.status(400).json({ success: false, error: "NF sem chave de acesso" });

  const { data: cert } = await supabase
    .from("certificados_digitais").select("cert_pem, key_pem, data_validade")
    .eq("empresa_id", empresa_id).eq("ativo", true)
    .order("created_at", { ascending: false }).limit(1).maybeSingle();
  if (!cert || !cert.cert_pem || !cert.key_pem)
    return res.status(404).json({ success: false, error: "Certificado não configurado" });
  if (cert.data_validade && new Date(cert.data_validade) < new Date())
    return res.status(400).json({ success: false, error: "Certificado expirado" });

  let soap;
  try {
    soap = buildManifestacaoSoap(nf.chave_acesso, cnpj, tpAmb, manifestacao.codigo, manifestacao.descricao, tipo_manifestacao === "nao_realizada" ? justificativa.trim() : undefined, cert.cert_pem, cert.key_pem);
  } catch (e) {
    return res.status(500).json({ success: false, error: `Erro assinatura: ${e.message}` });
  }

  let respText;
  try {
    const soapAction = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento";
    const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem, 30000, soapAction);
    respText = resp.body;
  } catch (e) {
    return res.status(502).json({ success: false, error: `Erro conexão SEFAZ: ${e.message}` });
  }

  const cStatLote = xmlTag(respText, "cStat");
  const evento = parseRetEvento(respText);
  const sucesso = evento.cStat === "135" || evento.cStat === "136" || evento.cStat === "573";

  console.log(`[manifestar] Lote cStat: ${cStatLote} | Evento cStat: ${evento.cStat} | ${evento.xMotivo}`);

  if (sucesso) {
    await supabase.from("notas_fiscais").update({ status_manifestacao: tipo_manifestacao }).eq("id", nota_fiscal_id);
    return res.json({ success: true, data: { cStat: evento.cStat, xMotivo: evento.xMotivo, nProt: evento.nProt, dhRegEvento: evento.dhRegEvento, status_manifestacao: tipo_manifestacao, nota_fiscal_id } });
  }

  const msgErros = { "573": "Manifestação já registrada.", "217": "NF-e não consta na SEFAZ.", "656": "Consumo indevido." };
  return res.status(422).json({ success: false, error: msgErros[evento.cStat] || `SEFAZ erro ${evento.cStat}: ${evento.xMotivo}`, cStat: evento.cStat, xMotivo: evento.xMotivo });
});

// ══════════════════════════════════════════════════════
// MANIFESTAR POR CHAVE — POST /api/manifestar-por-chave
// ══════════════════════════════════════════════════════
app.post("/api/manifestar-por-chave", authMiddleware, async (req, res) => {
  const { empresa_id, chave_acesso, tipo_manifestacao = "ciencia", justificativa, ambiente = "producao" } = req.body;
  if (!empresa_id) return res.status(400).json({ success: false, error: "empresa_id obrigatório" });
  if (!chave_acesso || chave_acesso.replace(/\D/g, "").length !== 44)
    return res.status(400).json({ success: false, error: "chave_acesso inválida (44 dígitos)" });

  const manifestacao = MANIFESTACAO[tipo_manifestacao];
  if (!manifestacao) return res.status(400).json({ success: false, error: "tipo_manifestacao inválido" });
  if (tipo_manifestacao === "nao_realizada" && (!justificativa || justificativa.trim().length < 15))
    return res.status(400).json({ success: false, error: "Justificativa obrigatória (min 15 chars)" });

  const tpAmb = ambiente === "homologacao" ? "2" : "1";
  const sefazUrl = SEFAZ_URLS.recepcao_evento[ambiente] || SEFAZ_URLS.recepcao_evento.producao;
  const supabase = getSupabaseAdmin();

  const { data: empresa } = await supabase
    .from("empresas").select("id, cnpj, sefaz_ativo")
    .eq("id", empresa_id).single();
  if (!empresa) return res.status(404).json({ success: false, error: "Empresa não encontrada" });
  const cnpj = (empresa.cnpj || "").replace(/\D/g, "");

  const { data: cert } = await supabase
    .from("certificados_digitais").select("cert_pem, key_pem")
    .eq("empresa_id", empresa_id).eq("ativo", true)
    .order("created_at", { ascending: false }).limit(1).maybeSingle();
  if (!cert || !cert.cert_pem || !cert.key_pem)
    return res.status(404).json({ success: false, error: "Certificado não configurado" });

  const chave = chave_acesso.replace(/\D/g, "");

  let soap;
  try {
    soap = buildManifestacaoSoap(chave, cnpj, tpAmb, manifestacao.codigo, manifestacao.descricao, tipo_manifestacao === "nao_realizada" ? justificativa.trim() : undefined, cert.cert_pem, cert.key_pem);
  } catch (e) {
    return res.status(500).json({ success: false, error: `Erro assinatura: ${e.message}` });
  }

  let respText;
  try {
    const soapAction = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento";
    const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem, 30000, soapAction);
    respText = resp.body;
  } catch (e) {
    return res.status(502).json({ success: false, error: `Erro conexão SEFAZ: ${e.message}` });
  }

  const cStatLote = xmlTag(respText, "cStat");
  const evento = parseRetEvento(respText);
  const sucesso = evento.cStat === "135" || evento.cStat === "136";

  console.log(`[manifestar-chave] Lote cStat: ${cStatLote} | Evento cStat: ${evento.cStat} | ${evento.xMotivo}`);

  return res.json({
    success: sucesso,
    data: { cStat: evento.cStat, xMotivo: evento.xMotivo, nProt: evento.nProt, dhRegEvento: evento.dhRegEvento, chave_acesso: chave, tipo_manifestacao },
  });
});

// ══════════════════════════════════════════════════════
// AUTO-SYNC CRON — Sincroniza todas as empresas ativas
// ══════════════════════════════════════════════════════
async function autoSyncTodasEmpresas() {
  console.log(`[cron] Iniciando sync automático: ${new Date().toISOString()}`);
  const supabase = getSupabaseAdmin();

  try {
    const { data: empresas } = await supabase
      .from("empresas")
      .select("id, cnpj, uf, sefaz_ativo, sefaz_ultimo_nsu")
      .eq("sefaz_ativo", true);

    if (!empresas || empresas.length === 0) {
      console.log("[cron] Nenhuma empresa com SEFAZ ativo");
      return;
    }

    for (const empresa of empresas) {
      try {
        console.log(`[cron] Sincronizando empresa ${empresa.cnpj}...`);

        const { data: cert } = await supabase
          .from("certificados_digitais").select("cert_pem, key_pem, data_validade")
          .eq("empresa_id", empresa.id).eq("ativo", true)
          .order("created_at", { ascending: false }).limit(1).maybeSingle();

        if (!cert || !cert.cert_pem || !cert.key_pem) {
          console.log(`[cron] Empresa ${empresa.cnpj}: sem certificado ativo`);
          continue;
        }
        if (cert.data_validade && new Date(cert.data_validade) < new Date()) {
          console.log(`[cron] Empresa ${empresa.cnpj}: certificado expirado`);
          continue;
        }

        const cnpj = (empresa.cnpj || "").replace(/\D/g, "");
        const ufCode = UF_CODE[empresa.uf] || "42";
        const tpAmb = "1";
        const sefazUrl = SEFAZ_URLS.dist_dfe.producao;

        let ultNSU = empresa.sefaz_ultimo_nsu || "0";
        const nsuInicio = ultNSU;
        let maxNSU = "999999999999999";
        let totalDocs = 0;
        let notasNovas = 0;
        let loops = 0;
        const maxLoops = 10;
        const errors = [];

        while (ultNSU < maxNSU && loops < maxLoops) {
          loops++;
          const soap = buildDistDFeSoap(cnpj, ufCode, tpAmb, ultNSU);

          let respText;
          try {
            const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem);
            respText = resp.body;
          } catch (e) {
            console.error(`[cron] Empresa ${empresa.cnpj}: erro conexão: ${e.message}`);
            break;
          }

          const cStat = xmlTag(respText, "cStat");
          const newUltNSU = xmlTag(respText, "ultNSU");
          const newMaxNSU = xmlTag(respText, "maxNSU");

          console.log(`[cron] ${empresa.cnpj} loop ${loops}: cStat=${cStat} ultNSU=${newUltNSU}`);

          if (cStat === "137" || cStat === "138") {
            if (newUltNSU) ultNSU = newUltNSU;
            if (newMaxNSU) maxNSU = newMaxNSU;

            const docMatches = respText.match(/<docZip[\s\S]*?<\/docZip>/gi) || [];
            totalDocs += docMatches.length;

            for (const docZip of docMatches) {
              try {
                const result = await processarDocZip(supabase, docZip, empresa.id, cnpj, tpAmb, cert.cert_pem, cert.key_pem, errors, "producao");
                if (result) notasNovas++;
              } catch (docErr) {
                console.error(`[cron] Erro doc: ${docErr.message}`);
              }
            }

            await supabase.from("empresas").update({ sefaz_ultimo_nsu: ultNSU }).eq("id", empresa.id);
            if (cStat === "137" || ultNSU >= maxNSU) break;

          } else if (cStat === "656") {
            console.log(`[cron] Empresa ${empresa.cnpj}: consumo indevido, pulando`);
            break;
          } else {
            console.log(`[cron] Empresa ${empresa.cnpj}: SEFAZ ${cStat}`);
            break;
          }

          await new Promise(r => setTimeout(r, 2000));
        }

        await salvarSyncLog(supabase, empresa.id, "sucesso", totalDocs, notasNovas, nsuInicio, ultNSU, `Cron auto-sync: ${notasNovas} notas novas`);
        console.log(`[cron] Empresa ${empresa.cnpj}: ${totalDocs} docs, ${notasNovas} novas`);

        // Manifestar resumos pendentes
        let cronManifestadas = 0;
        try {
          const { data: resumosPendentes } = await supabase
            .from("notas_fiscais")
            .select("id, chave_acesso")
            .eq("empresa_id", empresa.id)
            .eq("tipo_documento", "resumo")
            .or("status_manifestacao.is.null,status_manifestacao.eq.pendente")
            .limit(5);

          if (resumosPendentes && resumosPendentes.length > 0) {
            console.log(`[cron] ${empresa.cnpj}: ${resumosPendentes.length} resumos pendentes`);
            for (const nfResumo of resumosPendentes) {
              if (!nfResumo.chave_acesso) continue;
              try {
                const manifestSoap = buildManifestacaoSoap(nfResumo.chave_acesso, cnpj, tpAmb, "210210", "Ciencia da Operacao", undefined, cert.cert_pem, cert.key_pem);
                const manifestUrl = SEFAZ_URLS.recepcao_evento.producao;
                const soapAction = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento";
                const manifestResp = await sefazRequest(manifestUrl, manifestSoap, cert.cert_pem, cert.key_pem, 30000, soapAction);
                const manifestEvento = parseRetEvento(manifestResp.body);
                const manifestOk = manifestEvento.cStat === "135" || manifestEvento.cStat === "136" || manifestEvento.cStat === "573";
                if (manifestOk) {
                  await supabase.from("notas_fiscais").update({ status_manifestacao: "ciencia" }).eq("id", nfResumo.id);
                  cronManifestadas++;
                }
                await new Promise(r => setTimeout(r, 3000));
              } catch (e) {
                console.error(`[cron] Erro ciência pendente: ${e.message}`);
              }
            }

            if (cronManifestadas > 0) {
              console.log(`[cron] ${empresa.cnpj}: aguardando 15s para XMLs completos...`);
              await new Promise(r => setTimeout(r, 15000));
              try {
                const soapRetry = buildDistDFeSoap(cnpj, ufCode, tpAmb, nsuInicio);
                const respRetry = await sefazRequest(sefazUrl, soapRetry, cert.cert_pem, cert.key_pem);
                const cStatRetry = xmlTag(respRetry.body, "cStat");
                if (cStatRetry === "137" || cStatRetry === "138") {
                  const docMatchesRetry = respRetry.body.match(/<docZip[\s\S]*?<\/docZip>/gi) || [];
                  for (const docZip of docMatchesRetry) {
                    try {
                      await processarDocZip(supabase, docZip, empresa.id, cnpj, tpAmb, cert.cert_pem, cert.key_pem, errors, "producao");
                    } catch (e) {
                      console.error(`[cron] Erro doc retry: ${e.message}`);
                    }
                  }
                }
              } catch (e) {
                console.error(`[cron] Erro re-sync: ${e.message}`);
              }
            }
          }
        } catch (e) {
          console.error(`[cron] Erro resumos pendentes: ${e.message}`);
        }

        await new Promise(r => setTimeout(r, 5000));
      } catch (empresaErr) {
        console.error(`[cron] Erro empresa ${empresa.cnpj}: ${empresaErr.message}`);
      }
    }

    console.log(`[cron] Sync automático concluído: ${new Date().toISOString()}`);
  } catch (e) {
    console.error(`[cron] Erro geral: ${e.message}`);
  }
}

// Sync automático: todo dia às 02:00 BRT
cron.schedule("0 2 * * *", () => {
  console.log("[cron] Disparando sync automático 02:00 BRT...");
  autoSyncTodasEmpresas();
}, { timezone: "America/Sao_Paulo" });

// ══════════════════════════════════════════════════════
// START
// ══════════════════════════════════════════════════════
app.listen(PORT, () => {
  console.log(`🛰️  Sentinel SEFAZ Proxy rodando na porta ${PORT}`);
  console.log(`   Ambiente: ${process.env.SEFAZ_AMBIENTE || "producao"}`);
  console.log(`   Storage bucket: ${STORAGE_BUCKET}`);
  console.log(`   Auto-sync: todo dia às 02:00 BRT`);
});
