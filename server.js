require("dotenv").config();
const express = require("express");
const cors = require("cors");
const https = require("https");
const { URL } = require("url");
const pako = require("pako");
const { createClient } = require("@supabase/supabase-js");
const { SignedXml } = require("xml-crypto");

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
// ASSINATURA DIGITAL XML (XMLDSig) - SEFAZ NF-e
// ══════════════════════════════════════════════════════
//
// A SEFAZ exige que o <evento> contenha uma assinatura digital XML
// (enveloped signature) referenciando o <infEvento> pelo atributo Id.
//
// Algoritmos exigidos pela SEFAZ:
//   - Canonicalização: C14N 1.0 (http://www.w3.org/TR/2001/REC-xml-c14n-20010315)
//   - Digest: SHA-256 (a partir da NT 2019.001)
//   - Assinatura: RSA-SHA1 (ainda aceito) ou RSA-SHA256
//   - Transforms: enveloped-signature + C14N 1.0
//
// O bloco <Signature> deve ficar DENTRO do <evento>, após </infEvento>.
// O certificado X509 do destinatário deve estar em <KeyInfo>.
//
function extrairCertBase64(certPem) {
  return certPem
    .replace(/-----BEGIN CERTIFICATE-----/g, "")
    .replace(/-----END CERTIFICATE-----/g, "")
    .replace(/\s/g, "");
}

function assinarEvento(eventoXml, certPem, keyPem) {
  // Extrai o certificado em base64 limpo (sem headers PEM)
  const certBase64 = extrairCertBase64(certPem);

  // Cria o SignedXml usando a API v6+ do xml-crypto
  // - privateKey: chave privada PEM para assinar
  // - canonicalizationAlgorithm: C14N 1.0 (exigido pela SEFAZ)
  // - signatureAlgorithm: RSA-SHA1 (aceito pela SEFAZ)
  // - getKeyInfoContent: função que retorna o conteúdo do <KeyInfo>
  //   com o certificado X509 do destinatário
  const sig = new SignedXml({
    privateKey: keyPem,
    canonicalizationAlgorithm: "http://www.w3.org/TR/2001/REC-xml-c14n-20010315",
    signatureAlgorithm: "http://www.w3.org/2000/09/xmldsig#rsa-sha1",
    getKeyInfoContent: function () {
      return `<X509Data><X509Certificate>${certBase64}</X509Certificate></X509Data>`;
    },
  });

  // Adiciona a referência ao infEvento
  // A SEFAZ exige: enveloped-signature + C14N 1.0, digest SHA-1
  // O xml-crypto automaticamente detecta o atributo Id e gera URI="#ID..."
  sig.addReference({
    xpath: "//*[local-name(.)='infEvento']",
    transforms: [
      "http://www.w3.org/2000/09/xmldsig#enveloped-signature",
      "http://www.w3.org/TR/2001/REC-xml-c14n-20010315",
    ],
    digestAlgorithm: "http://www.w3.org/2000/09/xmldsig#sha1",
  });

  // Computa a assinatura — coloca o <Signature> após o </infEvento>, dentro do <evento>
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

  // Monta o detEvento (compacto, sem espaços extras)
  let detEvento = `<detEvento versao="1.00"><descEvento>${descEvento}</descEvento>`;
  if (justificativa) {
    detEvento += `<xJust>${justificativa}</xJust>`;
  }
  detEvento += `</detEvento>`;

  // Monta o XML do <evento> SEM assinatura (compacto!)
  // IMPORTANTE: o <infEvento> NÃO tem namespace próprio — herda do <evento>
  // O XML deve ser compacto (sem whitespace) para canonicalização correta
  const eventoXml = `<evento xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.00"><infEvento Id="${eventoId}"><cOrgao>91</cOrgao><tpAmb>${tpAmb}</tpAmb><CNPJ>${cnpj}</CNPJ><chNFe>${chNFe}</chNFe><dhEvento>${dh}</dhEvento><tpEvento>${codigoEvento}</tpEvento><nSeqEvento>${nSeqEvento}</nSeqEvento><verEvento>1.00</verEvento>${detEvento}</infEvento></evento>`;

  console.log(`[buildManifestacao] Evento antes de assinar:\n${eventoXml}`);

  // Assina o evento (insere <Signature> após </infEvento>, dentro de <evento>)
  const eventoAssinado = assinarEvento(eventoXml, certPem, keyPem);

  console.log(`[buildManifestacao] Evento assinado:\n${eventoAssinado}`);

  // Monta o envEvento com o evento assinado (compacto)
  const envioLote = `<envEvento xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.00"><idLote>1</idLote>${eventoAssinado}</envEvento>`;

  // Envelope SOAP 1.2 — <nfeDadosMsg> direto no Body, sem wrapper <nfeRecepcaoEvento>
  return `<?xml version="1.0" encoding="utf-8"?><soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope"><soap12:Body><nfeDadosMsg xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4">${envioLote}</nfeDadosMsg></soap12:Body></soap12:Envelope>`;
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

  const retMatch = respText.match(/<retDistDFeInt[\s\S]*?<\/retDistDFeInt>/i);
  return res.json({
    success: cStat === "138",
    data: { cStat, xMotivo, chave_acesso: chave, retXml: retMatch ? retMatch[0].slice(0, 2000) : null },
  });
});

// ══════════════════════════════════════════════════════
// SYNC SEFAZ — POST /api/sync-sefaz
// ══════════════════════════════════════════════════════
app.post("/api/sync-sefaz", authMiddleware, async (req, res) => {
  const { empresa_id, ambiente = "producao" } = req.body;
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

  let ultNSU = empresa.sefaz_ultimo_nsu || "0";
  let maxNSU = "999999999999999";
  let totalDocs = 0;
  let loops = 0;
  const maxLoops = 5;

  while (ultNSU < maxNSU && loops < maxLoops) {
    loops++;
    const soap = buildDistDFeSoap(cnpj, ufCode, tpAmb, ultNSU);

    let respText;
    try {
      const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem);
      respText = resp.body;
    } catch (e) {
      return res.status(502).json({ success: false, error: `Erro conexão SEFAZ: ${e.message}`, parcial: { totalDocs, ultNSU } });
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
          const nsuMatch = docZip.match(/NSU="(\d+)"/i);
          const schemaMatch = docZip.match(/schema="([^"]+)"/i);
          const b64 = docZip.replace(/<\/?docZip[^>]*>/gi, "").trim();
          if (!b64) continue;

          const xmlContent = decompressGzip(b64);
          const chNFe = xmlTag(xmlContent, "chNFe");
          if (!chNFe) continue;

          const existing = await supabase
            .from("notas_fiscais").select("id")
            .eq("empresa_id", empresa_id).eq("chave_acesso", chNFe).maybeSingle();

          if (!existing?.data) {
            const nsuVal = nsuMatch ? nsuMatch[1] : null;
            const schema = schemaMatch ? schemaMatch[1] : null;
            const isResumo = schema && schema.includes("resNFe");

            await supabase.from("notas_fiscais").insert({
              empresa_id,
              chave_acesso: chNFe,
              numero: xmlTag(xmlContent, "nNF") || null,
              serie: xmlTag(xmlContent, "serie") || null,
              data_emissao: xmlTag(xmlContent, "dhEmi") || null,
              valor_total: parseFloat(xmlTag(xmlContent, "vNF")) || null,
              cnpj_emitente: xmlTag(xmlContent, "CNPJ") || null,
              nome_emitente: xmlTag(xmlContent, "xNome") || null,
              status_sefaz: "recebida",
              nsu: nsuVal,
              tipo_documento: isResumo ? "resumo" : "completo",
              xml_completo: xmlContent.slice(0, 50000),
            });
          }
        } catch (docErr) {
          console.error(`[sync] Erro processando doc: ${docErr.message}`);
        }
      }

      await supabase.from("empresas").update({ sefaz_ultimo_nsu: ultNSU }).eq("id", empresa_id);

      if (ultNSU >= maxNSU) break;
    } else if (cStat === "656") {
      return res.json({ success: true, data: { message: "Consumo indevido. Aguarde.", cStat, xMotivo, totalDocs, ultNSU } });
    } else {
      return res.json({ success: false, data: { cStat, xMotivo, totalDocs, ultNSU } });
    }
  }

  return res.json({ success: true, data: { totalDocs, ultNSU, maxNSU, loops } });
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
    soap = buildManifestacaoSoap(
      nf.chave_acesso, cnpj, tpAmb, manifestacao.codigo, manifestacao.descricao,
      tipo_manifestacao === "nao_realizada" ? justificativa.trim() : undefined,
      cert.cert_pem, cert.key_pem
    );
  } catch (e) {
    console.error(`[manifestar] Erro ao construir SOAP: ${e.message}`);
    return res.status(500).json({ success: false, error: `Erro assinatura: ${e.message}` });
  }

  let respText;
  try {
    console.log(`[manifestar] ${tipo_manifestacao} | NF: ${nf.chave_acesso.slice(-10)}`);
    const soapAction = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento";
    const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem, 30000, soapAction);
    respText = resp.body;
  } catch (e) {
    return res.status(502).json({ success: false, error: `Erro conexão SEFAZ: ${e.message}` });
  }

  const cStat = xmlTag(respText, "cStat");
  const xMotivo = xmlTag(respText, "xMotivo");
  const nProt = xmlTag(respText, "nProt");
  const sucesso = cStat === "135" || cStat === "136";

  console.log(`[manifestar] cStat: ${cStat} | sucesso: ${sucesso}`);

  if (sucesso) {
    await supabase.from("notas_fiscais").update({ status_manifestacao: tipo_manifestacao }).eq("id", nota_fiscal_id);
    return res.json({ success: true, data: { cStat, xMotivo, nProt, status_manifestacao: tipo_manifestacao, nota_fiscal_id } });
  }

  const msgErros = { "573": "Manifestação já registrada.", "217": "NF-e não consta na SEFAZ.", "656": "Consumo indevido." };
  return res.status(422).json({ success: false, error: msgErros[cStat] || `SEFAZ erro ${cStat}: ${xMotivo}`, cStat, xMotivo });
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
    soap = buildManifestacaoSoap(
      chave, cnpj, tpAmb, manifestacao.codigo, manifestacao.descricao,
      tipo_manifestacao === "nao_realizada" ? justificativa.trim() : undefined,
      cert.cert_pem, cert.key_pem
    );
  } catch (e) {
    console.error(`[manifestar-chave] Erro ao construir SOAP/assinar: ${e.message}`);
    console.error(e.stack);
    return res.status(500).json({ success: false, error: `Erro assinatura: ${e.message}` });
  }

  let respText;
  try {
    console.log(`[manifestar-chave] ${tipo_manifestacao} | Chave: ${chave} | CNPJ: ${cnpj}`);
    console.log(`[manifestar-chave] URL: ${sefazUrl}`);
    console.log(`[manifestar-chave] XML enviado:\n${soap}`);
    const soapAction = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento";
    const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem, 30000, soapAction);
    respText = resp.body;
    console.log(`[manifestar-chave] HTTP ${resp.status}`);
    console.log(`[manifestar-chave] Resposta COMPLETA:\n${respText}`);
  } catch (e) {
    console.error(`[manifestar-chave] ERRO: ${e.message}`);
    return res.status(502).json({ success: false, error: `Erro conexão SEFAZ: ${e.message}` });
  }

  const cStat = xmlTag(respText, "cStat");
  const xMotivo = xmlTag(respText, "xMotivo");
  const nProt = xmlTag(respText, "nProt");
  const sucesso = cStat === "135" || cStat === "136";

  console.log(`[manifestar-chave] cStat: ${cStat} | ${xMotivo} | sucesso: ${sucesso}`);

  return res.json({
    success: sucesso,
    data: {
      cStat, xMotivo,
      nProt: nProt || null,
      chave_acesso: chave,
      tipo_manifestacao,
      soap_enviado: soap,
      resposta_completa: respText,
    },
  });
});

// ══════════════════════════════════════════════════════
// START
// ══════════════════════════════════════════════════════
app.listen(PORT, () => {
  console.log(`🛰️  Sentinel SEFAZ Proxy rodando na porta ${PORT}`);
  console.log(`   Ambiente: ${process.env.SEFAZ_AMBIENTE || "producao"}`);
});
