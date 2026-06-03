"""
download_pnad.py — INGESTÃO (Bronze)
Baixa e descompacta os microdados mensais do PNAD-COVID19 (IBGE), arquivos públicos.
Recorte: setembro, outubro e novembro de 2020.
"""
from __future__ import annotations
import io, zipfile, pathlib, urllib.request

BASE = ("https://ftp.ibge.gov.br/Trabalho_e_Rendimento/"
        "Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/Microdados/Dados/")
ARQUIVOS_ZIP = ["PNAD_COVID_092020.zip", "PNAD_COVID_102020.zip", "PNAD_COVID_112020.zip"]
DEST = pathlib.Path("dados_brutos"); DEST.mkdir(exist_ok=True)


def baixar(zip_name: str) -> None:
    url = BASE + zip_name
    print(f"  -> {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=180) as resp:
        conteudo = resp.read()
    with zipfile.ZipFile(io.BytesIO(conteudo)) as z:
        for membro in z.namelist():
            if membro.lower().endswith(".csv"):
                z.extract(membro, DEST)
                print(f"     extraído: {membro}")


def main() -> None:
    print("Ingestão dos microdados PNAD-COVID19 (IBGE)")
    for z in ARQUIVOS_ZIP:
        try:
            baixar(z)
        except Exception as e:  # noqa: BLE001
            print(f"     [erro] {z}: {e}\n     Verifique o índice: {BASE}")
    print(f"CSVs em: {DEST.resolve()}")


if __name__ == "__main__":
    main()
