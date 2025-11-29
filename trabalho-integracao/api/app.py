from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder  

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

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
    decode_responses=True,  
)



@app.get("/")
def root():
    return {"status": "ok", "message": "API de integração funcionando"}


@app.get("/health")
def health_check():
    """
    Verifica se os serviços respondem.
    (Simples, só para prova de conceito)
    """
    try:
    
        with pg_conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()

   
        mongo_db.command("ping")

        with neo4j_driver.session() as session:
            session.run("RETURN 1 AS n").single()

        redis_client.ping()

        return {"postgres": True, "mongo": True, "neo4j": True, "redis": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/clientes")
def listar_clientes():
    """
    Lista todos os clientes do PostgreSQL.
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



@app.get("/recomendacoes/{cliente_id}")
def gerar_recomendacoes(cliente_id: int):
    """
    Endpoint que integra:
    - PostgreSQL: dados do cliente + compras
    - MongoDB: interesses e comportamento
    - Neo4j: amigos e indicações
    - Redis: cache do resultado
    """

    cache_key = f"recomendacoes:{cliente_id}"

  
    cached = redis_client.get(cache_key)
    if cached:
        data = json.loads(cached)
        data["origem"] = "cache_redis"
        return data

   
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

  
    response = {
        "origem": "bancos",
        "cliente": cliente,
        "compras": compras,
        "interesses": interesses,
        "tags_comportamento": tags_comportamento,
        "amigos": amigos,
        "indicacoes": indicacoes,
    }

    data_to_cache = jsonable_encoder(response)
    redis_client.set(cache_key, json.dumps(data_to_cache))

    return response
