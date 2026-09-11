"""Cria posições de clusters por bairro a partir dos CEPs da base original.

Fluxo: registro -> CEP -> bairro/logradouro do CEP -> geocodificação OSM.
Nenhuma coordenada ou polígono do mapa atual é usado para posicionar clusters.
"""

import csv
import json
import unicodedata
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


RAIZ_PROJETO = Path(__file__).resolve().parents[2]
ARQUIVO_DADOS = RAIZ_PROJETO / "dados" / "brutos" / "cancer_mama_mulheres_rio_claro_2021_2025.csv"
CACHE_CEPS = RAIZ_PROJETO / "dados" / "cache" / "ceps_rio_claro.json"
CACHE_COORDENADAS = RAIZ_PROJETO / "dados" / "cache" / "coordenadas_bairros_photon.json"
SAIDA_CSV = RAIZ_PROJETO / "dados" / "processados" / "clusters_por_bairro_validacao.csv"
SAIDA_JSON = RAIZ_PROJETO / "dados" / "processados" / "clusters_por_bairro.json"

# Limites amplos do município, usados apenas para recusar homônimos fora de RC.
LAT_MIN, LAT_MAX = -22.51, -22.31
LON_MIN, LON_MAX = -47.66, -47.45


def normalizar_cep(valor):
    return "".join(c for c in str(valor) if c.isdigit()).zfill(8)


def chave(texto):
    texto = unicodedata.normalize("NFD", str(texto or ""))
    return "".join(c for c in texto if unicodedata.category(c) != "Mn").casefold().strip()


def dentro_de_rio_claro(latitude, longitude):
    return LAT_MIN <= latitude <= LAT_MAX and LON_MIN <= longitude <= LON_MAX


def termos_distintivos(bairro):
    genericos = {"bairro", "centro", "conjunto", "de", "do", "das", "dos", "habitacional", "interesse", "jardim", "loteamento", "nova", "novo", "parque", "residencial", "vila", "zona"}
    return [termo for termo in chave(bairro).replace("(", " ").replace(")", " ").split() if len(termo) > 3 and termo not in genericos]


def consultar_photon(consulta, bairro):
    url = "https://photon.komoot.io/api/?" + urlencode({"q": consulta, "limit": 5})
    requisicao = Request(url, headers={"User-Agent": "RioClaroCancerMamaMap/1.0"})
    with urlopen(requisicao, timeout=30) as resposta:
        resultados = json.load(resposta).get("features", [])
    for resultado in resultados:
        longitude, latitude = resultado["geometry"]["coordinates"][:2]
        if dentro_de_rio_claro(latitude, longitude):
            propriedades = resultado.get("properties", {})
            texto_osm = " ".join(str(propriedades.get(campo, "")) for campo in ("name", "street", "district", "city"))
            # Recusa resultados de outro bairro (por exemplo, uma busca por
            # Araucária que retornava Jardim Nova Rio Claro). Um registro sem
            # confirmação é preferível a um marcador em localidade errada.
            termos = termos_distintivos(bairro)
            if termos and not any(termo in chave(texto_osm) for termo in termos):
                continue
            return {
                "latitude": round(latitude, 6),
                "longitude": round(longitude, 6),
                "consulta": consulta,
                "resultado_osm": ", ".join(
                    str(propriedades[campo])
                    for campo in ("name", "street", "district", "city")
                    if propriedades.get(campo)
                ),
                "fonte": "OpenStreetMap/Photon",
            }
    return None


def obter_coordenada(bairro, ceps, detalhes_ceps, cache):
    identificador = chave(bairro)
    if identificador in cache:
        return cache[identificador]

    # O endereço é originado pelo CEP. Bairro vem primeiro, pois o cluster é
    # deliberadamente definido pela localidade, não por proximidade entre casos.
    consultas = []
    for cep in sorted(ceps)[:1]:
        logradouro = str(detalhes_ceps.get(cep, {}).get("logradouro", "")).strip()
        if logradouro:
            consultas.append(f"{logradouro}, {bairro}, Rio Claro, São Paulo, Brasil")
    consultas.append(f"{bairro}, Rio Claro, São Paulo, Brasil")

    for consulta in dict.fromkeys(consultas):
        resultado = consultar_photon(consulta, bairro)
        if resultado:
            cache[identificador] = resultado
            return resultado

    return {
        "latitude": None,
        "longitude": None,
        "consulta": consultas[0],
        "resultado_osm": "",
        "fonte": "Sem resultado — revisão necessária",
    }


def main():
    dados = pd.read_csv(ARQUIVO_DADOS, sep=";", dtype={"CEP": str})
    detalhes_ceps = json.loads(CACHE_CEPS.read_text(encoding="utf-8"))
    cache = json.loads(CACHE_COORDENADAS.read_text(encoding="utf-8")) if CACHE_COORDENADAS.exists() else {}

    por_bairro = defaultdict(lambda: {"ceps": set(), "registros": []})
    for _, registro in dados.iterrows():
        cep = normalizar_cep(registro["CEP"])
        bairro = str(detalhes_ceps.get(cep, {}).get("bairro", "")).strip()
        if bairro:
            por_bairro[bairro]["ceps"].add(cep)
            por_bairro[bairro]["registros"].append(registro)

    bairros_ordenados = sorted(por_bairro, key=chave)
    localizacoes = {}
    # A busca é somente de bairro/endereço derivado do CEP. Limitar a seis
    # requisições evita sobrecarregar o geocodificador e permite concluir a
    # tabela sem recorrer a uma posição arbitrária.
    with ThreadPoolExecutor(max_workers=6) as executor:
        futuros = {
            executor.submit(obter_coordenada, bairro, por_bairro[bairro]["ceps"], detalhes_ceps, cache): bairro
            for bairro in bairros_ordenados
        }
        for futuro in as_completed(futuros):
            localizacoes[futuros[futuro]] = futuro.result()

    linhas = []
    for bairro in bairros_ordenados:
        grupo = por_bairro[bairro]
        registros = grupo["registros"]
        localizacao = localizacoes[bairro]
        idades = [int(float(r["IDADE"])) for r in registros]
        valores = [float(r["VAL_TOT"]) for r in registros]
        anos = Counter(str(r["ANO_CMPT"])[:4] for r in registros)
        linhas.append({
            "bairro": bairro,
            "casos": len(registros),
            "latitude_cluster": localizacao["latitude"],
            "longitude_cluster": localizacao["longitude"],
            "idade_media": round(sum(idades) / len(idades), 1),
            "idade_minima": min(idades),
            "idade_maxima": max(idades),
            "casos_por_ano": json.dumps(dict(sorted(anos.items())), ensure_ascii=False),
            "valor_total_sus": round(sum(valores), 2),
            "ceps": ", ".join(sorted(grupo["ceps"])),
            "fonte_localizacao": localizacao["fonte"],
            "consulta_localizacao": localizacao["consulta"],
            "resultado_osm": localizacao["resultado_osm"],
        })

    CACHE_COORDENADAS.parent.mkdir(parents=True, exist_ok=True)
    CACHE_COORDENADAS.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    SAIDA_CSV.parent.mkdir(parents=True, exist_ok=True)
    with SAIDA_CSV.open("w", encoding="utf-8-sig", newline="") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=linhas[0].keys(), delimiter=";")
        escritor.writeheader()
        escritor.writerows(linhas)
    SAIDA_JSON.write_text(json.dumps(linhas, ensure_ascii=False, indent=2), encoding="utf-8")
    sem_coordenada = sum(linha["latitude_cluster"] is None for linha in linhas)
    print(f"Tabela de validação criada: {SAIDA_CSV}")
    print(f"Clusters geocodificados: {len(linhas) - sem_coordenada}/{len(linhas)}")
    print(f"Bairros sem coordenada: {sem_coordenada}")


if __name__ == "__main__":
    main()
