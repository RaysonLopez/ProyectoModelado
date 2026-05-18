import pandas as pd
import json
from pymongo import MongoClient
import pprint

# --- CONFIGURACIÓN DE CONEXIÓN ---
print("Conectando a MongoDB (Mongos Router) en localhost:27018...")
client = MongoClient('mongodb://localhost:27018/')
db = client['GA_Analytics_DB']
collection = db['sessions']

# --- FASE 1: INGESTA DE DATOS ---
def procesar_e_ingestar_datos():
    print("\n--- FASE 2: ETL (Extracción, Transformación, Carga) ---")
    json_cols = ['device', 'geoNetwork', 'totals', 'trafficSource']
    tamano_lote = 50000
    total_insertados = 0

    try:
        # 1. Primero limpiamos la colección (fuera del ciclo, para no borrar en cada lote)
        collection.delete_many({})
        print("Colección limpia. Iniciando lectura por lotes...")

        # 2. Leemos todo el archivo CSV, pero entregando lotes de 50k
        for chunk in pd.read_csv('train.csv', dtype={'fullVisitorId': 'str'}, chunksize=tamano_lote):
            
            # 3. Parseamos las columnas JSON en este lote específico
            for col in json_cols:
                chunk[col] = chunk[col].apply(lambda x: json.loads(x) if pd.notnull(x) else {})
                
            # 4. Reemplazamos NaN por None para compatibilidad con MongoDB
            chunk = chunk.replace({np.nan: None})
            
            # 5. Convertimos a diccionario e insertamos el lote
            records = chunk.to_dict(orient='records')
            collection.insert_many(records)
            
            # 6. Actualizamos y mostramos el progreso en vivo
            total_insertados += len(records)
            print(f"Lote insertado. Total subido hasta ahora: {total_insertados} sesiones...")

        print(f"¡Ingesta masiva completada exitosamente! {total_insertados} sesiones registradas en tu clúster distribuido.")

    except FileNotFoundError:
        print("Archivo train.csv no encontrado. Asegúrese de descargarlo.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

# --- FASE 2: OPERACIONES CRUD INICIALES ---
def operaciones_crud():
    print("\n--- OPERACIONES CRUD ---")
    print("Insertando Sesión VIP...")
    vip_session = {
        "fullVisitorId": "VIP_999999999",
        "visitId": 1234567890,
        "channelGrouping": "Referral",
        "device": {
            "deviceCategory": "mobile",
            "isMobile": True,
            "operatingSystem": "iOS",
            "browser": "Safari"
        },
        "geoNetwork": {
            "country": "United States",
            "continent": "Americas"
        },
        "totals": {
            "hits": "50",
            "pageviews": "45",
            "transactionRevenue": "150000000000",
            "visits": "1"
        },
        "trafficSource": {
            "isTrueDirect": True,
            "source": "vip-network.com"
        },
        "loyaltyScore": 10
    }
    collection.insert_one(vip_session)
    print("Sesión VIP insertada exitosamente.")

    print("Insertando 1000 sesiones fraudulentas (BOTS)...")
    fraud_sessions = []
    for i in range(1000):
        fraud_sessions.append({
            "fullVisitorId": f"BOT_XYZ_{i}",
            "device": {"browser": "UnknownBot", "isMobile": False, "operatingSystem": "Linux"},
            "geoNetwork": {"country": "Russia", "continent": "Europe"},
            "totals": {"hits": "1", "bounces": "1", "timeOnSite": "0", "visits": "1"},
            "trafficSource": {"source": "cheap-traffic-bot.xyz"},
            "isFraud": True
        })
    collection.insert_many(fraud_sessions)
    print("1000 sesiones fraudulentas inyectadas para pruebas.")

# --- FASE 3: PIPELINES DE AGREGACIÓN ---
def ejecutar_pipelines():
    print("\n--- CONSULTAS GERENCIALES (AGGREGATION PIPELINES) ---")

    print("\n1. Top 10 países por volumen de sesiones")
    pipeline_1 = [
        {"$group": {"_id": "$geoNetwork.country", "total_sessions": {"$sum": 1}}},
        {"$sort": {"total_sessions": -1}},
        {"$limit": 10}
    ]
    for doc in collection.aggregate(pipeline_1): print(doc)

    print("\n2. Conversión por dispositivo (Total Revenue)")
    pipeline_2 = [
        {"$match": {"totals.transactionRevenue": {"$exists": True, "$ne": None}}},
        {"$group": {
            "_id": "$device.deviceCategory",
            "total_revenue": {"$sum": {"$toLong": "$totals.transactionRevenue"}}
        }},
        {"$sort": {"total_revenue": -1}}
    ]
    for doc in collection.aggregate(pipeline_2): print(doc)

    print("\n3. Usuarios recurrentes de alto valor (>100k USD y Direct)")
    pipeline_3 = [
        {"$match": {"totals.transactionRevenue": {"$exists": True}}},
        {"$addFields": {"revenue_usd": {"$divide": [{"$toLong": "$totals.transactionRevenue"}, 1000000]}}},
        {"$match": {"revenue_usd": {"$gt": 100000}, "trafficSource.isTrueDirect": True}},
        {"$project": {"fullVisitorId": 1, "revenue_usd": 1, "geoNetwork.country": 1}},
        {"$limit": 5}
    ]
    for doc in collection.aggregate(pipeline_3): print(doc)

    print("\n4. Tráfico sospechoso (Rebotes sin tiempo)")
    pipeline_4 = [
        {"$match": {
            "totals.bounces": "1",
            "totals.hits": "1",
            "totals.timeOnSite": {"$in": ["0", None]}
        }},
        {"$group": {"_id": "$trafficSource.source", "bot_sessions": {"$sum": 1}}},
        {"$sort": {"bot_sessions": -1}},
        {"$limit": 5}
    ]
    for doc in collection.aggregate(pipeline_4): print(doc)

    print("\n5. Ingresos promedio por Canal")
    pipeline_5 = [
        {"$match": {"totals.transactionRevenue": {"$exists": True}}},
        {"$group": {
            "_id": "$channelGrouping",
            "avg_revenue": {"$avg": {"$toLong": "$totals.transactionRevenue"}},
            "total_transactions": {"$sum": 1}
        }},
        {"$sort": {"avg_revenue": -1}}
    ]
    for doc in collection.aggregate(pipeline_5): print(doc)

    print("\n6. Promedio de hits y pageviews por SO")
    pipeline_6 = [
        {"$match": {"device.operatingSystem": {"$exists": True, "$ne": "(not set)"}}},
        {"$group": {
            "_id": "$device.operatingSystem",
            "avg_hits": {"$avg": {"$toInt": "$totals.hits"}},
            "avg_pageviews": {"$avg": {"$toInt": "$totals.pageviews"}}
        }},
        {"$sort": {"avg_hits": -1}},
        {"$limit": 5}
    ]
    for doc in collection.aggregate(pipeline_6): print(doc)

    print("\n7. Anomalía de Tracking (Pageviews > Hits usando $expr)")
    pipeline_7 = [
        {"$match": {
            "$and": [
                {"totals.pageviews": {"$exists": True, "$ne": None}},
                {"totals.hits": {"$exists": True, "$ne": None}},
                {"$expr": {"$gt": [{"$toInt": "$totals.pageviews"}, {"$toInt": "$totals.hits"}]}}
            ]
        }},
        {"$project": {"fullVisitorId": 1, "totals.pageviews": 1, "totals.hits": 1}},
        {"$limit": 5}
    ]
    for doc in collection.aggregate(pipeline_7): print(doc)

    print("\n8. Top 5 fuentes que generan compras")
    pipeline_8 = [
        {"$match": {"totals.transactionRevenue": {"$exists": True}}},
        {"$group": {"_id": "$trafficSource.source", "successful_purchases": {"$sum": 1}}},
        {"$sort": {"successful_purchases": -1}},
        {"$limit": 5}
    ]
    for doc in collection.aggregate(pipeline_8): print(doc)

    print("\n9. Distribución de sesiones por continente (Sin nulos)")
    pipeline_9 = [
        {"$match": {"geoNetwork.continent": {"$exists": True, "$ne": "(not set)"}}},
        {"$group": {"_id": "$geoNetwork.continent", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    for doc in collection.aggregate(pipeline_9): print(doc)

    print("\n10. Revenue total en países de habla inglesa ($or)")
    pipeline_10 = [
        {"$match": {"$or": [
            {"geoNetwork.country": "United States"},
            {"geoNetwork.country": "Canada"},
            {"geoNetwork.country": "United Kingdom"}
        ]}},
        {"$group": {
            "_id": "$geoNetwork.country",
            "total_revenue": {"$sum": {"$toLong": "$totals.transactionRevenue"}}
        }},
        {"$sort": {"total_revenue": -1}}
    ]
    for doc in collection.aggregate(pipeline_10): print(doc)

# --- FASE 4: ACTUALIZACIONES Y LIMPIEZA ---
def actualizaciones_y_limpieza():
    print("\n--- ACTUALIZACIONES Y LIMPIEZA ---")
    
    print("1. Reclasificación VIP...")
    res_vip = collection.update_many(
        {"totals.transactionRevenue": {"$exists": True}, "$expr": {"$gt": [{"$toLong": "$totals.transactionRevenue"}, 100000000000]}},
        {"$set": {"isVIP": True}}
    )
    print(f"Sesiones convertidas a VIP: {res_vip.modified_count}")

    print("2. Normalización de 'United States' a 'USA'...")
    res_norm = collection.update_many(
        {"geoNetwork.country": "United States"},
        {"$set": {"geoNetwork.country": "USA"}}
    )
    print(f"Documentos normalizados: {res_norm.modified_count}")

    print("3. Incremento de loyaltyScore para tráfico directo...")
    res_loyalty = collection.update_many(
        {"trafficSource.isTrueDirect": True},
        {"$inc": {"loyaltyScore": 1}}
    )
    print(f"Scores incrementados: {res_loyalty.modified_count}")

    print("4. Limpieza de bots...")
    res_del = collection.delete_many({
        "device.browser": {"$regex": ".*(bot|spider).*", "$options": "i"}
    })
    print(f"Bots eliminados: {res_del.deleted_count}")


if __name__ == "__main__":
    procesar_e_ingestar_datos()
    operaciones_crud()
    ejecutar_pipelines()
    actualizaciones_y_limpieza()
    print("\nProceso finalizado exitosamente.")
