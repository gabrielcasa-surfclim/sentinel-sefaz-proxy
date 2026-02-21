require("dotenv").config();
const express = require("express");
const cors = require("cors");
const https = require("https");
const { URL } = require("url");
const pako = require("pako");
const { createClient } = require("@supabase/supabase-js");

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
// SEFAZ CLIENT
// ══════════════════════════════════════════════════════
const SEFAZ_URLS = {
  dist_dfe: {
    producao: "https://www1.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx",
    homologacao: "https://hom1.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx",
  },
  recepcao_evento: {
    producao: "https://www.nfe.fazenda.gov.br/NFeRecepcaoEvento4/NFeRecepcaoEvento4.asmx",
    homologacao: "https://homologacao.nfe.fazenda.gov.br/NFeRecepcaoEvento4/NFeRecepcaoEvento4.asmx",
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
    const options = {
      hostname: parsed.hostname, port: 443, path: parsed.pathname, method: "POST",
      headers, cert: certPem, key: keyPem, rejectUnauthorized: true, timeout: timeoutMs,
    };
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => resolve({ status: res.statusCode, body: data }));
    });
    req.on("error", (err) => reject(err));
    req.on("timeout", () => { req.destroy(); reject(new Error("Timeout na conexão com a SEFAZ")); });
    req.write(soapBody);
    req.end();
  });
}

function escapeXml(str) {
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
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
  const sign = "-";
  const local = new Date(now.getTime() + off * 3600000);
  const iso = local.toISOString().replace("Z", "").slice(0, 19);
  return `${iso}-03:00`;
}

function decompressGzip(b64) {
  const compressed = Buffer.from(b64, "base64");
  const decompressed = pako.inflate(compressed);
  return Buffer.from(decompressed).toString("utf-8");
}

function buildDistDFeSoap(cnpj, ufCode, tpAmb, ultNSU) {
  const nsu = pad15(ultNSU);
  const body = `<distDFeInt xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.01">
  <tpAmb>${tpAmb}</tpAmb>
  <cUFAutor>${ufCode}</cUFAutor>
  <CNPJ>${cnpj}</CNPJ>
  <distNSU>
    <ultNSU>${nsu}</ultNSU>
  </distNSU>
</distDFeInt>`;

  return `<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <nfeDistDFeInteresse xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe">
      <nfeDadosMsg>${body}</nfeDadosMsg>
    </nfeDistDFeInteresse>
  </soap12:Body>
</soap12:Envelope>`;
}

function buildManifestacaoSoap(chNFe, cnpj, tpAmb, codigoEvento, descEvento, justificativa) {
  const dh = dhEvento();
  const nSeqEvento = "1";
  const detEvento = justificativa
    ? `<detEvento versao="1.00"><descEvento>${descEvento}</descEvento><justificativa>${justificativa}</justificativa></detEvento>`
    : `<detEvento versao="1.00"><descEvento>${descEvento}</descEvento></detEvento>`;
  const eventoXml = `<evento xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.00"><infEvento Id="ID${codigoEvento}${chNFe}${nSeqEvento.padStart(2,"0")}"><cOrgao>91</cOrgao><tpAmb>${tpAmb}</tpAmb><CNPJ>${cnpj}</CNPJ><chNFe>${chNFe}</chNFe><dhEvento>${dh}</dhEvento><tpEvento>${codigoEvento}</tpEvento><nSeqEvento>${nSeqEvento}</nSeqEvento><verEvento>1.00</verEvento>${detEvento}</infEvento></evento>`;

  const envioLote = `<envEvento xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.00"><idLote>1</idLote>${eventoXml}</envEvento>`;

  return `<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <nfeRecepcaoEvento xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4">
      <nfeDadosMsg>${envioLote}</nfeDadosMsg>
    </nfeRecepcaoEvento>
  </soap12:Body>
</soap12:Envelope>`;
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
  res.json({ status: "ok", service: "sentinel-sefaz-proxy", timestamp: new Date().toISOString(), ambiente: process.env.SEFAZ_AMBIENTE || "producao" });
});

// ══════════════════════════════════════════════════════
// CONSULTA POR CHAVE — POST /api/consulta-chave
// ══════════════════════════════════════════════════════
app.post("/api/consulta-chave", authMiddleware, async (req, res) => {
  const { empresa_id, chave_acesso, ambiente = "producao" } = req.body;
  if (!empresa_id) return res.status(400).json({ success: false, error: "empresa_id obrigatório" });
  if (!chave_acesso || chave_acesso.replace(/\D/g, "").length !== 44) return res.status(400).json({ success: false, error: "chave_acesso inválida (44 dígitos)" });

  const tpAmb = ambiente === "homologacao" ? "2" : "1";
  const sefazUrl = SEFAZ_URLS.dist_dfe[ambiente] || SEFAZ_URLS.dist_dfe.producao;
  const supabase = getSupabaseAdmin();

  const { data: empresa } = await supabase.from("empresas").select("id, cnpj, uf, sefaz_ativo").eq("id", empresa_id).single();
  if (!empresa) return res.status(404).json({ success: false, error: "Empresa não encontrada" });

  const cnpj = (empresa.cnpj || "").replace(/\D/g, "");
  const uf = (empresa.uf || "").toUpperCase().trim();
  const ufCode = UF_CODE[uf];
  if (!ufCode) return res.status(400).json({ success: false, error: "UF inválida" });

  const { data: cert } = await supabase.from("certificados_digitais").select("cert_pem, key_pem").eq("empresa_id", empresa_id).eq("ativo", true).order("created_at", { ascending: false }).limit(1).maybeSingle();
  if (!cert || !cert.cert_pem || !cert.key_pem) return res.status(404).json({ success: false, error: "Certificado não configurado" });

  const chave = chave_acesso.replace(/\D/g, "");

  const body = `<distDFeInt xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.01">
  <tpAmb>${tpAmb}</tpAmb>
  <cUFAutor>${ufCode}</cUFAutor>
  <CNPJ>${cnpj}</CNPJ>
  <consChNFe>
    <chNFe>${chave}</chNFe>
  </consChNFe>
</distDFeInt>`;

  const soap = `<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <nfeDistDFeInteresse xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe">
      <nfeDadosMsg>${body}</nfeDadosMsg>
    </nfeDistDFeInteresse>
  </soap12:Body>
</soap12:Envelope>`;

  let respText;
  try {
    console.log(`[consulta-chave] Chave: ${chave} | CNPJ: ${cnpj}`);
    const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem, 30000, "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento");
    respText = resp.body;
    console.log(`[consulta-chave] HTTP ${resp.status}`);
  } catch (e) {
    return res.status(502).json({ success: false, error: `Erro conexão SEFAZ: ${e.message}` });
  }

  const cStat = xmlTag(respText, "cStat");
  const xMotivo = xmlTag(respText, "xMotivo");

  console.log(`[consulta-chave] cStat: ${cStat} | ${xMotivo}`);

  // Retornar resposta raw para debug
  const retMatch = respText.match(/<retDistDFeInt[\s\S]*?<\/retDistDFeInt>/i);
  return res.json({
    success: cStat === "138",
    data: { cStat, xMotivo, chave_acesso: chave, retXml: retMatch ? retMatch[0].slice(0, 2000) : null }
  });
});

// ══════════════════════════════════════════════════════
// SYNC SEFAZ — POST /api/sync-sefaz
// ══════════════════════════════════════════════════════
app.post("/api/sync-sefaz", authMiddleware, async (req, res) => {
  const { empresa_id, ambiente = "producao", max_loops = 10 } = req.body;
  if (!empresa_id) return res.status(400).json({ success: false, error: "empresa_id é obrigatório" });

  const tpAmb = ambiente === "homologacao" ? "2" : "1";
  const sefazUrl = SEFAZ_URLS.dist_dfe[ambiente] || SEFAZ_URLS.dist_dfe.producao;
  const supabase = getSupabaseAdmin();

  // Buscar empresa
  const { data: empresa, error: empErr } = await supabase.from("empresas").select("id, cnpj, razao_social, uf, sefaz_ativo").eq("id", empresa_id).single();
  if (empErr || !empresa) return res.status(404).json({ success: false, error: "Empresa não encontrada" });
  if (!empresa.sefaz_ativo) return res.status(400).json({ success: false, error: "Integração SEFAZ não está ativa" });

  const cnpj = (empresa.cnpj || "").replace(/\D/g, "");
  const uf = (empresa.uf || "").toUpperCase().trim();
  if (!cnpj || cnpj.length !== 14) return res.status(400).json({ success: false, error: "CNPJ inválido ou não cadastrado" });
  if (!uf || !UF_CODE[uf]) return res.status(400).json({ success: false, error: "UF inválida ou não cadastrada" });

  const ufCode = UF_CODE[uf];

  // Buscar certificado PEM
  const { data: cert, error: certErr } = await supabase.from("certificados_digitais").select("id, cert_pem, key_pem, data_validade, nome_arquivo").eq("empresa_id", empresa_id).eq("ativo", true).order("created_at", { ascending: false }).limit(1).maybeSingle();
  if (certErr || !cert) return res.status(404).json({ success: false, error: "Nenhum certificado digital configurado" });
  if (!cert.cert_pem || !cert.key_pem) return res.status(400).json({ success: false, error: "Certificado sem PEM extraído. Reenvie o certificado." });
  if (cert.data_validade && new Date(cert.data_validade) < new Date()) return res.status(400).json({ success: false, error: "Certificado digital expirado" });

  // Buscar último NSU
  const { data: ctrlSync } = await supabase.from("controle_sync").select("id, ultimo_nsu").eq("empresa_id", empresa_id).eq("tipo", "sefaz").maybeSingle();
  let ultNSU = ctrlSync?.ultimo_nsu ?? "0";
  const ctrlSyncId = ctrlSync?.id ?? null;

  // Loop de consulta DistDFe
  let notasEncontradas = 0, notasNovas = 0, loops = 0, maxNSU = ultNSU;
  const errors = [];
  let shouldStop = false, consumoIndevido = false, nenhumDocumento = false;

  while (loops < max_loops && !shouldStop) {
    loops++;
    const soap = buildDistDFeSoap(cnpj, ufCode, tpAmb, ultNSU);

    let respText;
    try {
      console.log(`[sync] Loop ${loops} | NSU: ${pad15(ultNSU)} | ${empresa.razao_social}`);
      const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem, 30000, "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento");
      respText = resp.body;
      console.log(`[sync] SEFAZ HTTP ${resp.status}`);
    } catch (e) {
      console.error(`[sync] Erro conexão: ${e.message}`);
      errors.push(`Erro de conexão com a SEFAZ: ${e.message}`);
      break;
    }

    const retMatch = respText.match(/<retDistDFeInt[\s\S]*?<\/retDistDFeInt>/i);
    if (!retMatch) { errors.push("Resposta SOAP inválida"); break; }
    const retXml = retMatch[0];

    const cStat = xmlTag(retXml, "cStat");
    const xMotivo = xmlTag(retXml, "xMotivo");
    const maxNsuResp = xmlTag(retXml, "maxNSU");
    const ultNsuResp = xmlTag(retXml, "ultNSU");

    console.log(`[sync] cStat: ${cStat} | ${xMotivo}`);
    if (maxNsuResp) maxNSU = maxNsuResp;

    if (cStat === "137") { nenhumDocumento = true; shouldStop = true; break; }
    if (cStat === "656") { errors.push("SEFAZ bloqueou consultas (Consumo Indevido). Aguarde 1h."); consumoIndevido = true; shouldStop = true; break; }
    if (cStat !== "138") { errors.push(`SEFAZ cStat ${cStat}: ${xMotivo}`); break; }

    // Processar docZips
    const docZipMatches = [...retXml.matchAll(/<docZip\s+[^>]*schema="([^"]+)"[^>]*>([^<]+)<\/docZip>/gi)];

    for (const match of docZipMatches) {
      const schema = match[1];
      const b64 = match[2].trim();

      let xmlDoc;
      try { xmlDoc = decompressGzip(b64); } catch (e) { errors.push(`Falha descomprimir (${schema})`); continue; }
      notasEncontradas++;

      try {
        if (schema.startsWith("resNFe")) {
          const chNFe = xmlTag(xmlDoc, "chNFe");
          const emitCnpj = xmlTag(xmlDoc, "CNPJ") || xmlTag(xmlDoc, "CPF");
          const xNome = xmlTag(xmlDoc, "xNome");
          const dhEmi = xmlTag(xmlDoc, "dhEmi");
          const vNF = xmlTag(xmlDoc, "vNF");
          if (!chNFe) continue;

          const { data: existing } = await supabase.from("notas_fiscais").select("id").eq("chave_acesso", chNFe).eq("empresa_id", empresa_id).maybeSingle();
          if (!existing) {
            let entidadeId = null;
            if (emitCnpj) {
              const { data: ent } = await supabase.from("entidades").select("id").eq("empresa_id", empresa_id).eq("cnpj_cpf", emitCnpj).maybeSingle();
              if (ent) { entidadeId = ent.id; }
              else if (xNome) { const { data: ne } = await supabase.from("entidades").insert({ empresa_id, razao_social: xNome, cnpj_cpf: emitCnpj, tipo: "fornecedor", ativo: true }).select("id").single(); if (ne) entidadeId = ne.id; }
            }
            await supabase.from("notas_fiscais").insert({ empresa_id, chave_acesso: chNFe, emit_cnpj: emitCnpj || null, emit_razao_social: xNome || null, data_emissao: dhEmi || null, valor_total_nf: vNF ? parseFloat(vNF) : null, entidade_id: entidadeId, origem: "sefaz", tipo_documento: "resumo", processado: false, status_manifestacao: "pendente" });
            notasNovas++;
            console.log(`[sync] + resumo: ${chNFe.slice(-10)} | ${xNome} | R$ ${vNF}`);
          }

        } else if (schema.startsWith("procNFe")) {
          const chNFe = xmlTag(xmlDoc, "chNFe") || (() => { const m = xmlDoc.match(/chNFe="([^"]{44})"/); return m ? m[1] : ""; })();
          const emitCnpj = xmlTag(xmlDoc, "CNPJ");
          const xNome = xmlTag(xmlDoc, "xNome");
          const dhEmi = xmlTag(xmlDoc, "dhEmi");
          const nNF = xmlTag(xmlDoc, "nNF");
          const serie = xmlTag(xmlDoc, "serie");
          const vNF = xmlTag(xmlDoc, "vNF");
          if (!chNFe) continue;

          const xmlPath = `${empresa_id}/nfe/${chNFe}.xml`;
          await supabase.storage.from("nfe-xmls").upload(xmlPath, Buffer.from(xmlDoc), { contentType: "application/xml", upsert: true });

          const { data: existingNF } = await supabase.from("notas_fiscais").select("id").eq("chave_acesso", chNFe).eq("empresa_id", empresa_id).maybeSingle();

          let entidadeId = null;
          if (emitCnpj) {
            const { data: ent } = await supabase.from("entidades").select("id").eq("empresa_id", empresa_id).eq("cnpj_cpf", emitCnpj).maybeSingle();
            if (ent) { entidadeId = ent.id; }
            else if (xNome) { const { data: ne } = await supabase.from("entidades").insert({ empresa_id, razao_social: xNome, cnpj_cpf: emitCnpj, tipo: "fornecedor", ativo: true }).select("id").single(); if (ne) entidadeId = ne.id; }
          }

          let nfId = existingNF?.id ?? null;
          if (existingNF) {
            await supabase.from("notas_fiscais").update({ numero_nf: nNF || null, serie: serie || null, valor_total_nf: vNF ? parseFloat(vNF) : null, xml_storage_path: xmlPath, tipo_documento: "nfe", processado: true, entidade_id: entidadeId }).eq("id", existingNF.id);
          } else {
            const { data: ins } = await supabase.from("notas_fiscais").insert({ empresa_id, chave_acesso: chNFe, emit_cnpj: emitCnpj || null, emit_razao_social: xNome || null, data_emissao: dhEmi || null, numero_nf: nNF || null, serie: serie || null, valor_total_nf: vNF ? parseFloat(vNF) : null, entidade_id: entidadeId, origem: "sefaz", tipo_documento: "nfe", xml_storage_path: xmlPath, processado: true, status_manifestacao: "pendente" }).select("id").single();
            nfId = ins?.id ?? null;
            notasNovas++;
          }

          if (nfId) {
            const detMatches = [...xmlDoc.matchAll(/<det\b[^>]*>([\s\S]*?)<\/det>/gi)];
            const itens = detMatches.map((d) => {
              const prod = d[1];
              return { empresa_id, nota_fiscal_id: nfId, descricao: xmlTag(prod, "xProd"), ncm: xmlTag(prod, "NCM") || null, cfop: xmlTag(prod, "CFOP") || null, quantidade: parseFloat(xmlTag(prod, "qCom") || "0") || null, valor_unitario: parseFloat(xmlTag(prod, "vUnCom") || "0") || null, valor_total: parseFloat(xmlTag(prod, "vProd") || "0") || null };
            }).filter((i) => i.descricao);
            if (itens.length > 0) {
              await supabase.from("itens_nf").delete().eq("nota_fiscal_id", nfId);
              await supabase.from("itens_nf").insert(itens);
            }
          }
          console.log(`[sync] + NF-e: ${chNFe.slice(-10)} | NF ${nNF} | ${xNome} | R$ ${vNF}`);
        }
      } catch (e) {
        errors.push(`Erro processar (${schema}): ${e.message}`);
      }
    }

    if (ultNsuResp) ultNSU = ultNsuResp;
    if (!maxNsuResp || pad15(ultNSU) >= pad15(maxNSU)) shouldStop = true;
  }

  // Status final
  const temErros = errors.length > 0;
  const logStatus = !temErros ? "sucesso" : (temErros && notasEncontradas > 0) ? "erro_parcial" : "erro";
  let mensagemLog = nenhumDocumento && notasNovas === 0 ? "Nenhuma NF-e nova encontrada na SEFAZ." : consumoIndevido ? errors[0] : temErros ? errors[0] : `Sincronização concluída. ${notasNovas} nota(s) nova(s) importada(s).`;

  console.log(`[sync] ${logStatus} | Encontradas: ${notasEncontradas} | Novas: ${notasNovas}`);

  // Atualizar controle_sync
  const ctrlData = { empresa_id, tipo: "sefaz", ultimo_nsu: pad15(ultNSU), ultima_execucao: new Date().toISOString(), status: !temErros ? "idle" : "erro", total_notas_sync: notasEncontradas, erro_ultima_sync: temErros ? mensagemLog : null, updated_at: new Date().toISOString() };
  if (ctrlSyncId) { await supabase.from("controle_sync").update(ctrlData).eq("id", ctrlSyncId); }
  else { await supabase.from("controle_sync").insert(ctrlData); }

  // Log
  await supabase.from("log_sync_sefaz").insert({ empresa_id, tipo: "dist_dfe", status: logStatus, nsu_inicio: "0", nsu_fim: pad15(ultNSU), notas_encontradas: notasEncontradas, notas_novas: notasNovas, mensagem: mensagemLog, detalhes: { loops, ambiente, max_nsu: maxNSU, errors, consumo_indevido: consumoIndevido, nenhum_documento: nenhumDocumento } });

  // Alerta
  if (notasNovas > 0) {
    await supabase.from("alertas").insert({ empresa_id, tipo: "nfe_nova", severidade: "info", titulo: `${notasNovas} nova(s) NF-e recebida(s) da SEFAZ`, mensagem: `${notasNovas} nota(s) fiscal(is) nova(s) importadas via proxy SEFAZ.`, lido: false });
  }

  return res.json({ success: true, data: { notas_encontradas: notasEncontradas, notas_novas: notasNovas, ultimo_nsu: pad15(ultNSU), max_nsu: pad15(maxNSU), loops, errors, nenhum_documento: nenhumDocumento, consumo_indevido: consumoIndevido, parcial: temErros && notasEncontradas > 0 } });
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
  if (!manifestacao) return res.status(400).json({ success: false, error: `tipo_manifestacao inválido` });
  if (tipo_manifestacao === "nao_realizada" && (!justificativa || justificativa.trim().length < 15)) return res.status(400).json({ success: false, error: "Justificativa obrigatória (min 15 chars)" });

  const tpAmb = ambiente === "homologacao" ? "2" : "1";
  const sefazUrl = SEFAZ_URLS.recepcao_evento[ambiente] || SEFAZ_URLS.recepcao_evento.producao;
  const supabase = getSupabaseAdmin();

  const { data: empresa } = await supabase.from("empresas").select("id, cnpj, sefaz_ativo").eq("id", empresa_id).single();
  if (!empresa) return res.status(404).json({ success: false, error: "Empresa não encontrada" });
  if (!empresa.sefaz_ativo) return res.status(400).json({ success: false, error: "SEFAZ não ativa" });
  const cnpj = empresa.cnpj.replace(/\D/g, "");

  const { data: nf } = await supabase.from("notas_fiscais").select("id, chave_acesso").eq("id", nota_fiscal_id).eq("empresa_id", empresa_id).single();
  if (!nf) return res.status(404).json({ success: false, error: "NF não encontrada" });
  if (!nf.chave_acesso) return res.status(400).json({ success: false, error: "NF sem chave de acesso" });

  const { data: cert } = await supabase.from("certificados_digitais").select("cert_pem, key_pem, data_validade").eq("empresa_id", empresa_id).eq("ativo", true).order("created_at", { ascending: false }).limit(1).maybeSingle();
  if (!cert || !cert.cert_pem || !cert.key_pem) return res.status(404).json({ success: false, error: "Certificado não configurado" });
  if (cert.data_validade && new Date(cert.data_validade) < new Date()) return res.status(400).json({ success: false, error: "Certificado expirado" });

  const soap = buildManifestacaoSoap(nf.chave_acesso, cnpj, tpAmb, manifestacao.codigo, manifestacao.descricao, tipo_manifestacao === "nao_realizada" ? justificativa.trim() : undefined);

  let respText;
  try {
    console.log(`[manifestar] ${tipo_manifestacao} | NF: ${nf.chave_acesso.slice(-10)}`);
    const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem, 30000, "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento");
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
// (Permite manifestar sem ter a NF-e no banco - útil para ativar DistDFe)
// ══════════════════════════════════════════════════════
app.post("/api/manifestar-por-chave", authMiddleware, async (req, res) => {
  const { empresa_id, chave_acesso, tipo_manifestacao = "ciencia", justificativa, ambiente = "producao" } = req.body;
  if (!empresa_id) return res.status(400).json({ success: false, error: "empresa_id obrigatório" });
  if (!chave_acesso || chave_acesso.replace(/\D/g, "").length !== 44) return res.status(400).json({ success: false, error: "chave_acesso inválida (44 dígitos)" });

  const manifestacao = MANIFESTACAO[tipo_manifestacao];
  if (!manifestacao) return res.status(400).json({ success: false, error: "tipo_manifestacao inválido" });
  if (tipo_manifestacao === "nao_realizada" && (!justificativa || justificativa.trim().length < 15)) return res.status(400).json({ success: false, error: "Justificativa obrigatória (min 15 chars)" });

  const tpAmb = ambiente === "homologacao" ? "2" : "1";
  const sefazUrl = SEFAZ_URLS.recepcao_evento[ambiente] || SEFAZ_URLS.recepcao_evento.producao;
  const supabase = getSupabaseAdmin();

  const { data: empresa } = await supabase.from("empresas").select("id, cnpj, sefaz_ativo").eq("id", empresa_id).single();
  if (!empresa) return res.status(404).json({ success: false, error: "Empresa não encontrada" });
  const cnpj = (empresa.cnpj || "").replace(/\D/g, "");

  const { data: cert } = await supabase.from("certificados_digitais").select("cert_pem, key_pem").eq("empresa_id", empresa_id).eq("ativo", true).order("created_at", { ascending: false }).limit(1).maybeSingle();
  if (!cert || !cert.cert_pem || !cert.key_pem) return res.status(404).json({ success: false, error: "Certificado não configurado" });

  const chave = chave_acesso.replace(/\D/g, "");
  const soap = buildManifestacaoSoap(chave, cnpj, tpAmb, manifestacao.codigo, manifestacao.descricao, tipo_manifestacao === "nao_realizada" ? justificativa.trim() : undefined);

  let respText;
  try {
    console.log(`[manifestar-chave] ${tipo_manifestacao} | Chave: ${chave} | CNPJ: ${cnpj}`);
    const resp = await sefazRequest(sefazUrl, soap, cert.cert_pem, cert.key_pem, 30000, "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento");
    respText = resp.body;
    console.log(`[manifestar-chave] HTTP ${resp.status}`);
  } catch (e) {
    return res.status(502).json({ success: false, error: `Erro conexão SEFAZ: ${e.message}` });
  }

  const cStat = xmlTag(respText, "cStat");
  const xMotivo = xmlTag(respText, "xMotivo");
  const nProt = xmlTag(respText, "nProt");
  const sucesso = cStat === "135" || cStat === "136";

  console.log(`[manifestar-chave] cStat: ${cStat} | ${xMotivo} | sucesso: ${sucesso}`);
  console.log(`[manifestar-chave] Resposta raw: ${respText.slice(0, 500)}`);

  return res.json({
    success: sucesso,
    data: { cStat, xMotivo, nProt: nProt || null, chave_acesso: chave, tipo_manifestacao, resposta_raw: respText.slice(0, 1000) }
  });
});

// ══════════════════════════════════════════════════════
// START
// ══════════════════════════════════════════════════════
app.listen(PORT, () => {
  console.log(`🛰️  Sentinel SEFAZ Proxy rodando na porta ${PORT}`);
  console.log(`   Ambiente: ${process.env.SEFAZ_AMBIENTE || "producao"}`);
});
