"""
carga_bigquery.py — CARGA no Banco em Nuvem (Silver -> BigQuery)
Cria o dataset (se necessário) e carrega a base tratada numa tabela do BigQuery.

Uso no notebook (recomendado — usa a conta autenticada na sessão do Colab):
    from google.cloud import bigquery
    from carga_bigquery import carregar
    client = bigquery.Client(project=PROJECT)
    carregar(client, "dados_tratados/pnad_covid.csv", PROJECT, "pnad_covid", "fato_pnad")
"""
from __future__ import annotations
import pandas as pd
from google.cloud import bigquery


def carregar(client: "bigquery.Client", csv_path: str, project: str,
             dataset: str = "pnad_covid", tabela: str = "fato_pnad") -> str:
    """Cria o dataset se não existir e carrega o CSV tratado no BigQuery.
    Retorna o id completo da tabela."""
    ds = bigquery.Dataset(f"{project}.{dataset}")
    ds.location = "US"
    client.create_dataset(ds, exists_ok=True)

    df = pd.read_csv(csv_path)
    tabela_id = f"{project}.{dataset}.{tabela}"
    job = client.load_table_from_dataframe(
        df, tabela_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    n = client.get_table(tabela_id).num_rows
    print(f"Carregado em nuvem: {tabela_id} ({n:,} linhas)")
    return tabela_id


def tornar_publico(client: "bigquery.Client", project: str, dataset: str = "pnad_covid") -> None:
    """(Opcional) libera leitura do dataset para qualquer conta Google autenticada."""
    ds = client.get_dataset(f"{project}.{dataset}")
    entries = list(ds.access_entries)
    entries.append(bigquery.AccessEntry(
        role="READER", entity_type="specialGroup", entity_id="allAuthenticatedUsers"))
    ds.access_entries = entries
    client.update_dataset(ds, ["access_entries"])
    print("Dataset liberado para leitura (allAuthenticatedUsers).")


if __name__ == "__main__":
    # Uso standalone exige GOOGLE_APPLICATION_CREDENTIALS apontando p/ a chave de service account
    import os
    project = os.environ["GCP_PROJECT"]
    client = bigquery.Client(project=project)
    carregar(client, "dados_tratados/pnad_covid.csv", project)
