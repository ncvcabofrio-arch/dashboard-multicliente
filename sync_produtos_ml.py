"""
Robo: importa os ANUNCIOS ATIVOS das contas conectadas -> tabela anuncios.

- NAO cria produto. Produto e criado na mao pelo painel (a partir de um anuncio).
- Le a fila sync_jobs: pega jobs tipo='produtos' pendentes (o botao "Importar
  anuncios" do painel). Com FORCAR_TODAS=1 processa todas as contas.
- Para cada conta: varre anuncios ativos (scan), pega detalhes/variacoes e faz
  UPSERT em anuncios (1 linha por MLB/variacao). NAO mexe em produto_id (o vinculo
  feito na mao fica preservado).

Secrets (GitHub Actions): SUPABASE_URL, SUPABASE_KEY (service_role),
                          ML_CLIENT_ID, ML_CLIENT_SECRET
"""
import os
import time
import requests
from datetime import datetime, timezone
from ml_auth_multi import sb, get_token

FORCAR_TODAS = os.environ.get("FORCAR_TODAS", "0") == "1"


def _agora():
    return datetime.now(timezone.utc).isoformat()


def ids_ativos(token, seller_id):
    """Todos os ids de anuncios ativos (modo scan aguenta qualquer volume)."""
    ids, scroll = [], None
    while True:
        params = {"search_type": "scan", "limit": 100, "status": "active"}
        if scroll:
            params["scroll_id"] = scroll
        r = requests.get(
            f"https://api.mercadolibre.com/users/{seller_id}/items/search",
            params=params, headers={"Authorization": "Bearer " + token}, timeout=60)
        if r.status_code != 200:
            print(f"  aviso: items/search status {r.status_code} {r.text[:150]}")
            break
        d = r.json()
        res = d.get("results", []) or []
        if not res:
            break
        ids += res
        scroll = d.get("scroll_id")
        if not scroll:
            break
        time.sleep(0.3)
    return ids


def detalhes(token, ids):
    """Detalhes dos anuncios (multiget de 20 em 20)."""
    out = []
    campos = "id,title,price,available_quantity,status,seller_custom_field,attributes,variations"
    for i in range(0, len(ids), 20):
        lote = ids[i:i + 20]
        r = requests.get(
            "https://api.mercadolibre.com/items",
            params={"ids": ",".join(lote), "attributes": campos},
            headers={"Authorization": "Bearer " + token}, timeout=60)
        if r.status_code != 200:
            time.sleep(1)
            continue
        for w in (r.json() or []):
            b = w.get("body") or {}
            if b.get("id"):
                out.append(b)
        time.sleep(0.3)
    return out


def _sku_item(b):
    scf = b.get("seller_custom_field")
    if scf:
        return str(scf).strip()
    for a in (b.get("attributes") or []):
        if a.get("id") == "SELLER_SKU" and a.get("value_name"):
            return str(a["value_name"]).strip()
    return None


def _sku_variacao(v):
    sku = v.get("seller_sku") or v.get("seller_custom_field")
    if not sku:
        for a in (v.get("attributes") or []):
            if a.get("id") == "SELLER_SKU" and a.get("value_name"):
                sku = a["value_name"]
                break
    return str(sku).strip() if sku else None


def linhas_anuncio(org_id, b):
    """1 linha por anuncio/variacao. produto_id NAO vai aqui (fica preservado)."""
    linhas = []
    variacoes = b.get("variations") or []
    if variacoes:
        for v in variacoes:
            linhas.append({
                "org_id": org_id, "mlb": b.get("id"),
                "variacao_id": str(v.get("id") or ""),
                "sku": _sku_variacao(v),
                "titulo": b.get("title"),
                "preco": v.get("price") if v.get("price") is not None else b.get("price"),
                "estoque": v.get("available_quantity"),
                "status_ml": b.get("status"), "atualizado_em": _agora(),
            })
    else:
        linhas.append({
            "org_id": org_id, "mlb": b.get("id"), "variacao_id": "",
            "sku": _sku_item(b), "titulo": b.get("title"),
            "preco": b.get("price"), "estoque": b.get("available_quantity"),
            "status_ml": b.get("status"), "atualizado_em": _agora(),
        })
    return linhas


def upsert_anuncios(linhas):
    # on_conflict (org_id, mlb, variacao_id): atualiza SO as colunas enviadas.
    # produto_id nao vai no payload -> vinculo feito na mao fica preservado.
    for i in range(0, len(linhas), 200):
        sb.table("anuncios").upsert(linhas[i:i + 200],
                                    on_conflict="org_id,mlb,variacao_id").execute()


def processar_conta(conta):
    tok = get_token(conta["id"])
    ids = ids_ativos(tok, conta["seller_id"])
    print(f"{conta.get('nickname') or conta['seller_id']}: {len(ids)} anuncios ativos")
    dets = detalhes(tok, ids)
    linhas = []
    for b in dets:
        linhas += linhas_anuncio(conta["org_id"], b)
    # dedup defensivo por (mlb, variacao_id)
    dedup = {}
    for l in linhas:
        dedup[(l["org_id"], l["mlb"], l["variacao_id"])] = l
    linhas = list(dedup.values())
    if linhas:
        upsert_anuncios(linhas)
    print(f"  -> {len(linhas)} anuncios importados")
    return len(linhas)


def conta_por_id(cid):
    return (sb.table("contas").select("id,org_id,seller_id,nickname")
            .eq("id", cid).maybe_single().execute().data)


def contas_ativas():
    return (sb.table("contas").select("id,org_id,seller_id,nickname")
            .eq("canal", "mercadolivre").eq("status", "ativa").execute().data or [])


def main():
    jobs = (sb.table("sync_jobs").select("*")
            .eq("tipo", "produtos").eq("status", "pendente")
            .order("criado_em").execute().data or [])
    if jobs:
        for job in jobs:
            sb.table("sync_jobs").update({"status": "rodando", "progresso": "importando anuncios",
                                          "atualizado_em": _agora()}).eq("id", job["id"]).execute()
            try:
                conta = conta_por_id(job["conta_id"])
                if not conta:
                    raise RuntimeError("conta do job nao encontrada")
                n = processar_conta(conta)
                sb.table("sync_jobs").update({"status": "ok", "progresso": f"{n} anuncios",
                                              "atualizado_em": _agora()}).eq("id", job["id"]).execute()
            except Exception as e:
                print("ERRO no job", job["id"], ":", e)
                sb.table("sync_jobs").update({"status": "erro", "erro": str(e)[:400],
                                              "atualizado_em": _agora()}).eq("id", job["id"]).execute()
        return

    if FORCAR_TODAS:
        total = 0
        for c in contas_ativas():
            try:
                total += processar_conta(c)
            except Exception as e:
                print("ERRO conta", c["id"], ":", e)
        print(f"Total: {total} anuncios")
    else:
        print("Nada a fazer (sem job pendente). Use FORCAR_TODAS=1 pra varrer todas.")


if __name__ == "__main__":
    main()
