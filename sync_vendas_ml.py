"""
Robo: baixa os PEDIDOS (vendas) das contas conectadas -> tabela vendas.

- Le a fila sync_jobs, jobs tipo='vendas'. O parametro {"meses": N} diz quanto
  tempo pra tras baixar (o botao do painel manda 12).
- Busca por JANELAS de 30 dias (evita o limite de paginacao da API) e pagina
  de 50 em 50 dentro de cada janela.
- 1 linha por ITEM do pedido. O frete e rateado entre os itens pelo valor.
- No fim: vincula as vendas aos produtos por SKU e congela o custo.

Secrets: SUPABASE_URL, SUPABASE_KEY (service_role), ML_CLIENT_ID, ML_CLIENT_SECRET
"""
import os
import time
import requests
from datetime import datetime, timezone, timedelta
from ml_auth_multi import sb, get_token

MESES_PADRAO = int(os.environ.get("MESES_PADRAO", "12"))
FORCAR_TODAS = os.environ.get("FORCAR_TODAS", "0") == "1"


def _agora():
    return datetime.now(timezone.utc).isoformat()


def janelas(meses):
    """Janelas de 30 dias, da mais antiga pra mais nova."""
    fim = datetime.now(timezone.utc)
    passos, atual = [], fim
    for _ in range(max(1, meses)):
        ini = atual - timedelta(days=30)
        passos.append((ini, atual))
        atual = ini
    return list(reversed(passos))


def buscar_pedidos(token, seller_id, ini, fim):
    """Todos os pedidos da janela (paginado)."""
    out, offset, total = [], 0, None
    while total is None or offset < total:
        r = requests.get(
            "https://api.mercadolibre.com/orders/search",
            params={
                "seller": seller_id,
                "order.date_created.from": ini.strftime("%Y-%m-%dT%H:%M:%S.000-00:00"),
                "order.date_created.to":   fim.strftime("%Y-%m-%dT%H:%M:%S.000-00:00"),
                "sort": "date_asc", "offset": offset, "limit": 50,
            },
            headers={"Authorization": "Bearer " + token}, timeout=60)
        if r.status_code != 200:
            print(f"  aviso: orders/search {r.status_code} {r.text[:150]}")
            break
        d = r.json()
        total = (d.get("paging") or {}).get("total", 0)
        res = d.get("results", []) or []
        if not res:
            break
        out += res
        offset += len(res)
        time.sleep(0.35)
        if offset >= 9000:      # trava de seguranca do limite da API
            print("  aviso: janela muito grande, cortando em 9000")
            break
    return out


def _uf_cidade(o):
    # as vezes o proprio pedido ja traz o endereco
    try:
        addr = ((o.get("shipping") or {}).get("receiver_address") or {})
        uf = ((addr.get("state") or {}).get("name")) or ((addr.get("state") or {}).get("id"))
        cid = ((addr.get("city") or {}).get("name"))
        if uf:
            return uf, cid
    except Exception:
        pass
    return None, None


def uf_cidade_pedido(token, o, cache):
    """UF/cidade do comprador. Se o pedido nao traz, busca no ENVIO (shipment).
    cache: {shipment_id: (uf, cidade)} pra nao repetir chamada.
    """
    uf, cid = _uf_cidade(o)
    if uf:
        return uf, cid
    sid = (o.get("shipping") or {}).get("id")
    if not sid:
        return None, None
    if sid in cache:
        return cache[sid]
    try:
        r = requests.get("https://api.mercadolibre.com/shipments/" + str(sid),
                         headers={"Authorization": "Bearer " + token}, timeout=30)
        time.sleep(0.25)
        if r.status_code == 200:
            addr = (r.json().get("receiver_address") or {})
            uf = ((addr.get("state") or {}).get("name")) or ((addr.get("state") or {}).get("id"))
            cid = ((addr.get("city") or {}).get("name"))
            cache[sid] = (uf, cid)
            return uf, cid
        if r.status_code in (401, 403):
            # app sem permissao de Envios -> avisa 1x e desiste de buscar UF
            if not cache.get("_avisou"):
                print("  aviso: sem permissao pra ler Envios (UF ficara vazio). "
                      "Ative a permissao de Envios no app do ML.")
                cache["_avisou"] = True
    except Exception:
        pass
    cache[sid] = (None, None)
    return None, None


def _frete_total(o):
    tot = 0.0
    for p in (o.get("payments") or []):
        try:
            tot += float(p.get("shipping_cost") or 0)
        except Exception:
            pass
    if not tot:
        try:
            tot = float(((o.get("shipping") or {}).get("cost")) or 0)
        except Exception:
            tot = 0.0
    return tot


def linhas_do_pedido(org_id, conta_id, o, token=None, ship_cache=None):
    itens = o.get("order_items") or []
    if not itens:
        return []
    if token is not None and ship_cache is not None:
        uf, cidade = uf_cidade_pedido(token, o, ship_cache)
    else:
        uf, cidade = _uf_cidade(o)
    frete = _frete_total(o)
    # valor de cada linha, pra ratear o frete proporcionalmente
    valores = []
    for it in itens:
        q = int(it.get("quantity") or 0)
        pu = float(it.get("unit_price") or 0)
        valores.append(q * pu)
    soma = sum(valores) or 1.0

    linhas = []
    for seq, it in enumerate(itens):
        item = it.get("item") or {}
        q = int(it.get("quantity") or 0)
        pu = float(it.get("unit_price") or 0)
        fee = float(it.get("sale_fee") or 0)
        linhas.append({
            "org_id": org_id, "conta_id": conta_id,
            "order_id": str(o.get("id")), "seq": seq,
            "data": o.get("date_created"),
            "status": o.get("status"),
            "status_pag": (o.get("payments") or [{}])[0].get("status") if o.get("payments") else None,
            "mlb": item.get("id"),
            "variacao_id": str(item.get("variation_id") or ""),
            "sku": (item.get("seller_sku") or item.get("seller_custom_field") or None),
            "titulo": item.get("title"),
            "quantidade": q,
            "preco_unit": pu,
            "valor_total": round(q * pu, 2),
            "taxa_ml": round(fee * q, 2),
            "frete": round(frete * (valores[seq] / soma), 2),
            "uf": uf, "cidade": cidade,
        })
    return linhas


def upsert_vendas(linhas):
    for i in range(0, len(linhas), 200):
        sb.table("vendas").upsert(linhas[i:i + 200], on_conflict="org_id,order_id,seq").execute()


# ---------------------------------------------------------------------
# Completar anuncios que VENDERAM mas nao estao na base (encerrados/pausados).
# O robo de produtos importa so os ATIVOS; sem isso, vendas antigas ficam
# sem vinculo com produto.
# ---------------------------------------------------------------------
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


def _linhas_anuncio(org_id, b):
    linhas, variacoes = [], (b.get("variations") or [])
    if variacoes:
        for v in variacoes:
            linhas.append({"org_id": org_id, "mlb": b.get("id"), "variacao_id": str(v.get("id") or ""),
                           "sku": _sku_variacao(v), "titulo": b.get("title"),
                           "preco": v.get("price") if v.get("price") is not None else b.get("price"),
                           "estoque": v.get("available_quantity"), "status_ml": b.get("status"),
                           "atualizado_em": _agora()})
    else:
        linhas.append({"org_id": org_id, "mlb": b.get("id"), "variacao_id": "",
                       "sku": _sku_item(b), "titulo": b.get("title"), "preco": b.get("price"),
                       "estoque": b.get("available_quantity"), "status_ml": b.get("status"),
                       "atualizado_em": _agora()})
    return linhas


def completar_anuncios_das_vendas(conta, token):
    org = conta["org_id"]
    vend = sb.table("vendas").select("mlb").eq("org_id", org).execute().data or []
    anun = sb.table("anuncios").select("mlb").eq("org_id", org).execute().data or []
    faltam = sorted({r["mlb"] for r in vend if r.get("mlb")} - {r["mlb"] for r in anun if r.get("mlb")})
    if not faltam:
        return 0
    print(f"  completando {len(faltam)} anuncio(s) que venderam mas nao estavam na base...")
    campos = "id,title,price,available_quantity,status,seller_custom_field,attributes,variations"
    linhas = []
    for i in range(0, len(faltam), 20):
        r = requests.get("https://api.mercadolibre.com/items",
                         params={"ids": ",".join(faltam[i:i + 20]), "attributes": campos},
                         headers={"Authorization": "Bearer " + token}, timeout=60)
        if r.status_code != 200:
            time.sleep(1)
            continue
        for w in (r.json() or []):
            b = w.get("body") or {}
            if b.get("id"):
                linhas += _linhas_anuncio(org, b)
        time.sleep(0.3)
    dedup = {}
    for l in linhas:
        dedup[(l["org_id"], l["mlb"], l["variacao_id"])] = l
    linhas = list(dedup.values())
    for i in range(0, len(linhas), 200):
        sb.table("anuncios").upsert(linhas[i:i + 200], on_conflict="org_id,mlb,variacao_id").execute()
    return len(linhas)


def prog(job_id, txt):
    if not job_id:
        return
    sb.table("sync_jobs").update({"progresso": txt, "atualizado_em": _agora()}).eq("id", job_id).execute()


def processar_conta(conta, meses, job_id=None):
    tok = get_token(conta["id"])
    nome = conta.get("nickname") or conta["seller_id"]
    jans = janelas(meses)
    total_linhas, total_ped = 0, 0
    ship_cache = {}   # {shipment_id: (uf, cidade)} reaproveitado entre janelas
    for idx, (ini, fim) in enumerate(jans, start=1):
        pedidos = buscar_pedidos(tok, conta["seller_id"], ini, fim)
        linhas = []
        for o in pedidos:
            linhas += linhas_do_pedido(conta["org_id"], conta["id"], o, tok, ship_cache)
        # dedup defensivo por (order_id, seq)
        dedup = {}
        for l in linhas:
            dedup[(l["org_id"], l["order_id"], l["seq"])] = l
        linhas = list(dedup.values())
        if linhas:
            upsert_vendas(linhas)
        total_ped += len(pedidos)
        total_linhas += len(linhas)
        msg = f"{idx}/{len(jans)} periodos · {total_ped} pedidos"
        print(f"  {nome}: {msg}")
        prog(job_id, msg)
        # renova o token se a varredura for longa
        if idx % 4 == 0:
            tok = get_token(conta["id"])
    print(f"{nome}: {total_ped} pedidos -> {total_linhas} itens gravados")
    return total_linhas


def pos_processar():
    """Liga vendas a produtos por SKU e congela o custo."""
    for fn in ("vincular_vendas_por_sku", "backfill_custo_vendas"):
        try:
            sb.rpc(fn).execute()
            print("ok:", fn)
        except Exception as e:
            print("aviso:", fn, e)


def conta_por_id(cid):
    return (sb.table("contas").select("id,org_id,seller_id,nickname")
            .eq("id", cid).maybe_single().execute().data)


def contas_ativas():
    return (sb.table("contas").select("id,org_id,seller_id,nickname")
            .eq("canal", "mercadolivre").eq("status", "ativa").execute().data or [])


def main():
    # tipos que este robo atende:
    #   vendas            -> baixar pedidos (params.meses)
    #   anuncios_vendidos -> buscar no ML os anuncios que venderam e nao estao na base
    jobs = (sb.table("sync_jobs").select("*")
            .in_("tipo", ["vendas", "anuncios_vendidos"]).eq("status", "pendente")
            .order("criado_em").execute().data or [])

    if jobs:
        for job in jobs:
            sb.table("sync_jobs").update({"status": "rodando", "progresso": "iniciando",
                                          "atualizado_em": _agora()}).eq("id", job["id"]).execute()
            try:
                conta = conta_por_id(job["conta_id"])
                if not conta:
                    raise RuntimeError("conta do job nao encontrada")

                if job.get("tipo") == "anuncios_vendidos":
                    n = completar_anuncios_das_vendas(conta, get_token(conta["id"]))
                    sb.table("sync_jobs").update({"status": "ok",
                                                  "progresso": f"{n} anúncio(s) trazidos",
                                                  "atualizado_em": _agora()}).eq("id", job["id"]).execute()
                else:
                    meses = int((job.get("params") or {}).get("meses") or MESES_PADRAO)
                    n = processar_conta(conta, meses, job["id"])
                    pos_processar()
                    sb.table("sync_jobs").update({"status": "ok", "progresso": f"{n} itens de venda",
                                                  "atualizado_em": _agora()}).eq("id", job["id"]).execute()
            except Exception as e:
                print("ERRO no job", job["id"], ":", e)
                sb.table("sync_jobs").update({"status": "erro", "erro": str(e)[:400],
                                              "atualizado_em": _agora()}).eq("id", job["id"]).execute()
        return

    if FORCAR_TODAS:
        # atualizacao incremental: so o ultimo mes
        total = 0
        for c in contas_ativas():
            try:
                total += processar_conta(c, 1)
            except Exception as e:
                print("ERRO conta", c["id"], ":", e)
        pos_processar()
        print(f"Total: {total} itens de venda")
    else:
        print("Nada a fazer (sem job pendente). Use FORCAR_TODAS=1 pra atualizar o ultimo mes.")


if __name__ == "__main__":
    main()
