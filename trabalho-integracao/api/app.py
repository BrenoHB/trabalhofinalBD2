from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder

import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
import json

app = FastAPI(title="API Integração de Bancos de Dados")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------
# Conexões com os bancos
# --------------------------

pg_conn = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname="recomendador",
    user="admin",
    password="admin",
)
pg_conn.autocommit = True

mongo_client = MongoClient("mongodb://mongo:27017")
mongo_db = mongo_client["recomendador_docs"]

neo4j_driver = GraphDatabase.driver(
    "bolt://neo4j:7687",
    auth=("neo4j", "senha123"),
)

redis_client = redis.Redis(
    host="redis",
    port=6379,
    db=0,
    decode_responses=True,  # retorna strings, não bytes
)

# --------------------------
# Rotas básicas
# --------------------------


@app.get("/")
def root():
    return {"status": "ok", "message": "API de integração funcionando"}


@app.get("/health")
def health_check():
    """
    Verifica se os serviços respondem.
    """
    try:
        # Postgres
        with pg_conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()

        # Mongo
        mongo_db.command("ping")

        # Neo4j
        with neo4j_driver.session() as session:
            session.run("RETURN 1 AS n").single()

        # Redis
        redis_client.ping()

        return {"postgres": True, "mongo": True, "neo4j": True, "redis": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --------------------------
# Função auxiliar central
# --------------------------


def montar_dados_consolidados_cliente(cliente_id: int) -> dict:
    """
    Monta o pacote consolidado de dados de um cliente:
    - dados do cliente (Postgres)
    - compras (Postgres)
    - interesses e comportamento (Mongo)
    - amigos e indicações (Neo4j)
    """

    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM clientes WHERE id = %s;", (cliente_id,))
        cliente = cur.fetchone()
        if not cliente:
            raise HTTPException(status_code=404, detail=f"Cliente {cliente_id} não encontrado")

        cur.execute(
            """
            SELECT c.id, p.produto, p.tipo, p.valor, c.data
            FROM compras c
            JOIN produtos p ON p.id = c.id_produto
            WHERE c.id_cliente = %s
            ORDER BY c.data DESC;
            """,
            (cliente_id,),
        )
        compras = cur.fetchall()


    doc_interesses = mongo_db.clientes_interesses.find_one(
        {"cliente_id": cliente_id},
        {"_id": 0},
    ) or {}

    interesses = doc_interesses.get("interesses", [])
    tags_comportamento = doc_interesses.get("tags_comportamento", [])


    with neo4j_driver.session() as session:
        query_amigos = """
        MATCH (c:Cliente {id_cliente: $id})-[:AMIGO_DE]->(amigo:Cliente)
        RETURN amigo.id_cliente AS id_cliente, amigo.nome AS nome
        """
        result_amigos = session.run(query_amigos, id=cliente_id)
        amigos = [record.data() for record in result_amigos]

        query_indicacoes = """
        MATCH (c:Cliente {id_cliente: $id})-[r:INDICOU]->(amigo:Cliente)
        RETURN amigo.id_cliente AS id_cliente, amigo.nome AS nome, r.produto AS produto
        """
        result_indicacoes = session.run(query_indicacoes, id=cliente_id)
        indicacoes = [record.data() for record in result_indicacoes]


    dados = {
        "cliente": cliente,
        "compras": compras,
        "interesses": interesses,
        "tags_comportamento": tags_comportamento,
        "amigos": amigos,
        "indicacoes": indicacoes,
    }

    return dados


def chave_redis_cliente(cliente_id: int) -> str:
    return f"cliente:{cliente_id}"



@app.get("/clientes")
def listar_clientes():
    """
    Lista todos os clientes direto do PostgreSQL.
    (Útil para conferência dos dados fonte)
    """
    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM clientes ORDER BY id;")
        clientes = cur.fetchall()
    return {"clientes": clientes}


@app.get("/clientes/{cliente_id}")
def detalhar_cliente(cliente_id: int):
    """
    Detalha um cliente com suas compras (apenas Postgres).
    """
    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM clientes WHERE id = %s;", (cliente_id,))
        cliente = cur.fetchone()
        if not cliente:
            raise HTTPException(status_code=404, detail="Cliente não encontrado")

        cur.execute(
            """
            SELECT c.id, p.produto, p.tipo, p.valor, c.data
            FROM compras c
            JOIN produtos p ON p.id = c.id_produto
            WHERE c.id_cliente = %s
            ORDER BY c.data DESC;
            """,
            (cliente_id,),
        )
        compras = cur.fetchall()

    return {"cliente": cliente, "compras": compras}


# --------------------------
# Endpoint de recomendação (usa Redis como cache)
# --------------------------


@app.get("/recomendacoes/{cliente_id}")
def gerar_recomendacoes(cliente_id: int):
    """
    Endpoint que integra:
    - PostgreSQL: dados do cliente + compras
    - MongoDB: interesses e comportamento
    - Neo4j: amigos e indicações
    - Redis: cache do resultado e base consolidada por cliente
    """

    key = chave_redis_cliente(cliente_id)


    cached = redis_client.get(key)
    if cached:
        data = json.loads(cached)
        data["origem"] = "cache_redis"
        return data


    dados = montar_dados_consolidados_cliente(cliente_id)

    response = {
        "origem": "bancos",
        **dados,
    }


    data_to_cache = jsonable_encoder(dados)  
    redis_client.set(key, json.dumps(data_to_cache))

    return response





@app.post("/redis/rebuild")
def redis_rebuild():
    """
    Limpa a base Redis e reconstrói os dados consolidados
    de TODOS os clientes a partir dos demais bancos.

    Esta rota implementa:
    - limpeza do cache
    - replicação dos dados das 3 bases para a 4ª base (Redis)
    """
    try:

        redis_client.flushdb()


        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id FROM clientes ORDER BY id;")
            clientes = cur.fetchall()

        total = 0


        for row in clientes:
            cid = row["id"]
            dados = montar_dados_consolidados_cliente(cid)
            key = chave_redis_cliente(cid)
            redis_client.set(key, json.dumps(jsonable_encoder(dados)))
            total += 1

        return {
            "status": "ok",
            "mensagem": "Redis reconstruído com sucesso",
            "total_clientes": total,
        }
    except HTTPException:

        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




def _carregar_todos_clientes_redis():
    """
    Lê todos os clientes consolidados do Redis (cliente:*).
    Retorna uma lista de dicionários com a estrutura consolidada.
    """
    keys = redis_client.keys("cliente:*")
    dados = []
    for k in keys:
        raw = redis_client.get(k)
        if not raw:
            continue
        try:
            dados.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return dados


@app.get("/Rclientes")
def redis_listar_clientes():
    """
    Mostrar os dados de todos os clientes (somente Redis).
    """
    dados = _carregar_todos_clientes_redis()
    clientes = [d["cliente"] for d in dados if "cliente" in d]
    return {"origem": "redis", "clientes": clientes}


@app.get("/Rclientes-amigos")
def redis_clientes_amigos():
    """
    Mostrar os dados dos clientes e seus amigos (somente Redis).
    """
    dados = _carregar_todos_clientes_redis()

    resultado = []
    for d in dados:
        resultado.append(
            {
                "cliente": d.get("cliente"),
                "amigos": d.get("amigos", []),
            }
        )

    return {"origem": "redis", "clientes_amigos": resultado}


@app.get("/Rclientes-compras")
def redis_clientes_compras():
    """
    Mostrar os dados dos clientes e as compras realizadas por eles (somente Redis).
    """
    dados = _carregar_todos_clientes_redis()

    resultado = []
    for d in dados:
        resultado.append(
            {
                "cliente": d.get("cliente"),
                "compras": d.get("compras", []),
            }
        )

    return {"origem": "redis", "clientes_compras": resultado}


@app.get("/Ramigos-recomendacoes")
def redis_amigos_recomendacoes():
    """
    Listar os dados dos amigos dos clientes e as possíveis recomendações para eles (somente Redis).

    Aqui, as "recomendações" são consideradas como as relações 'indicacoes'
    armazenadas para cada cliente, que contêm amigo + produto indicado.
    """
    dados = _carregar_todos_clientes_redis()

    resultado = []
    for d in dados:
        cliente = d.get("cliente")
        indicacoes = d.get("indicacoes", [])
        amigos = d.get("amigos", [])

        resultado.append(
            {
                "cliente": cliente,
                "amigos": amigos,
                "indicacoes": indicacoes,
            }
        )

    return {"origem": "redis", "amigos_recomendacoes": resultado}
