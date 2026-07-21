"""
Robo: importa os ANUNCIOS ATIVOS das contas conectadas -> tabela produtos.

- Le a fila sync_jobs: pega os jobs tipo='produtos' pendentes e processa.
  (Se rodar sem job pendente, com FORCAR_TODAS=1, processa todas as contas.)
- Para cada conta: varre os anuncios ativos (modo scan), pega detalhes,
  monta 1 linha por SKU (inclui variacoes) e faz UPSERT em produtos.
- NAO toca em custo/fornecedor (sao preenchidos na mao no painel).

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


def linhas_produto(org_id, b):
    """1 linha por SKU. Se tiver variacoes, uma por variacao."""
    linhas = []
    variacoes = b.get("variations") or []
    if variacoes:
        for v in variacoes:
            sku = _sku_variacao(v)
            if not sku:
                continue
            linhas.append({
                "org_id": org_id, "sku": sku, "mlb": b.get("id"),
                "nome": b.get("title"),
                "preco": v.get("price") if v.get("price") is not None else b.get("price"),
                "estoque": v.get("available_quantity"),
                "status": b.get("status"), "atualizado_em": _agora(),
            })
    else:
        sku = _sku_item(b)
        if sku:
            linhas.append({
                "org_id": org_id, "sku": sku, "mlb": b.get("id"),
                "nome": b.get("title"), "preco": b.get("price"),
                "estoque": b.get("available_quantity"),
                "status": b.get("status"), "atualizado_em": _agora(),
            })
    return linhas


def upsert_produtos(linhas):
    # on_conflict org_id,sku -> atualiza SO as colunas enviadas
    # (custo/fornecedor nao vao no payload, entao ficam preservados)
    for i in range(0, len(linhas), 200):
        sb.table("produtos").upsert(linhas[i:i + 200], on_conflict="org_id,sku").execute()


def processar_conta(conta):
    tok = get_token(conta["id"])
    ids = ids_ativos(tok, conta["seller_id"])
    print(f"{conta.get('nickname') or conta['seller_id']}: {len(ids)} anuncios ativos")
    dets = detalhes(tok, ids)
    linhas = []
    for b in dets:
        linhas += linhas_produto(conta["org_id"], b)
    if linhas:
        upsert_produtos(linhas)
    print(f"  -> {len(linhas)} SKUs importados")
    return len(linhas)


def conta_por_id(cid):
    return (sb.table("contas").select("id,org_id,seller_id,nickname")
            .eq("id", cid).maybe_single().execute().data)


def contas_ativas():
    return (sb.table("contas").select("id,org_id,seller_id,nickname")
            .eq("canal", "mercadolivre").eq("status", "ativa").execute().data or [])


def main():
    # 1) jobs pendentes de produtos
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
                sb.table("sync_jobs").update({"status": "ok", "progresso": f"{n} SKUs",
                                              "atualizado_em": _agora()}).eq("id", job["id"]).execute()
            except Exception as e:
                print("ERRO no job", job["id"], ":", e)
                sb.table("sync_jobs").update({"status": "erro", "erro": str(e)[:400],
                                              "atualizado_em": _agora()}).eq("id", job["id"]).execute()
        return

    # 2) sem job: so processa tudo se forçado (agendamento diario)
    if FORCAR_TODAS:
        total = 0
        for c in contas_ativas():
            try:
                total += processar_conta(c)
            except Exception as e:
                print("ERRO conta", c["id"], ":", e)
        print(f"Total: {total} SKUs")
    else:
        print("Nada a fazer (sem job pendente). Use FORCAR_TODAS=1 pra varrer todas.")


if __name__ == "__main__":
    main()
