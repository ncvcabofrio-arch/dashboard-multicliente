"""
Helper de token do Mercado Livre (multi-conta).

Cada conta conectada guarda seu token em public.contas_tokens. Este modulo
entrega um access_token VALIDO para uma conta: se o atual ainda vale, usa;
se esta pertinho de expirar, renova pelo refresh_token e SALVA o novo.

Importante: o ML ROTACIONA o refresh_token a cada renovacao — por isso a gente
sempre grava o refresh_token novo que vem na resposta.

Variaveis de ambiente (secrets do GitHub Actions):
  SUPABASE_URL, SUPABASE_KEY (a service_role!), ML_CLIENT_ID, ML_CLIENT_SECRET
"""
import os
import requests
from datetime import datetime, timezone, timedelta
from supabase import create_client

SUPABASE_URL     = os.environ["SUPABASE_URL"]
SUPABASE_KEY     = os.environ["SUPABASE_KEY"]          # service_role
ML_CLIENT_ID     = os.environ["ML_CLIENT_ID"]
ML_CLIENT_SECRET = os.environ["ML_CLIENT_SECRET"]

sb = create_client(SUPABASE_URL, SUPABASE_KEY)


def _agora():
    return datetime.now(timezone.utc)


def get_token(conta_id):
    """Devolve um access_token valido para a conta (renova se preciso)."""
    row = (sb.table("contas_tokens").select("*")
           .eq("conta_id", conta_id).maybe_single().execute().data)
    if not row:
        raise RuntimeError(f"conta {conta_id} sem tokens (reconecte no painel)")

    # ainda vale por mais de 5 min? usa direto.
    exp = row.get("expires_at")
    if exp:
        try:
            expdt = datetime.fromisoformat(str(exp).replace("Z", "+00:00"))
            if expdt - _agora() > timedelta(minutes=5):
                return row["access_token"]
        except Exception:
            pass  # se nao der pra ler a data, renova por seguranca

    # renova pelo refresh_token
    r = requests.post(
        "https://api.mercadolibre.com/oauth/token",
        data={
            "grant_type":    "refresh_token",
            "client_id":     ML_CLIENT_ID,
            "client_secret": ML_CLIENT_SECRET,
            "refresh_token": row["refresh_token"],
        },
        headers={"Accept": "application/json"},
        timeout=30,
    )
    j = r.json() if r.content else {}
    if r.status_code != 200 or not j.get("access_token"):
        raise RuntimeError(f"falha ao renovar token da conta {conta_id}: "
                           f"{r.status_code} {r.text[:200]}")

    novo_exp = (_agora() + timedelta(seconds=int(j.get("expires_in", 21600)))).isoformat()
    sb.table("contas_tokens").update({
        "access_token":  j["access_token"],
        "refresh_token": j.get("refresh_token", row["refresh_token"]),  # rotaciona!
        "expires_at":    novo_exp,
        "atualizado_em": _agora().isoformat(),
    }).eq("conta_id", conta_id).execute()

    return j["access_token"]
