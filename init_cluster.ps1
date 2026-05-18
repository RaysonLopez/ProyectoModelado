Write-Host "Iniciando configuración del clúster de MongoDB..." -ForegroundColor Green

Write-Host "1. Inicializando Config Server Replica Set..." -ForegroundColor Cyan
docker exec configsvr mongosh --port 27019 --eval "rs.initiate({_id: 'configRS', configsvr: true, members: [{_id: 0, host: 'configsvr:27019'}]})"

Write-Host "Esperando 5 segundos para que el Config Server esté listo..."
Start-Sleep -Seconds 5

Write-Host "2. Inicializando Shard 1 Replica Set (mongo1, mongo2, mongo3)..." -ForegroundColor Cyan
docker exec mongo1 mongosh --port 27018 --eval "rs.initiate({_id: 'shard1RS', members: [{_id: 0, host: 'mongo1:27018', priority: 2}, {_id: 1, host: 'mongo2:27020', priority: 1}, {_id: 2, host: 'mongo3:27021', priority: 1}]})"

Write-Host "3. Inicializando Shard 2 Replica Set (mongo4, mongo5, mongo6)..." -ForegroundColor Cyan
docker exec mongo4 mongosh --port 27022 --eval "rs.initiate({_id: 'shard2RS', members: [{_id: 0, host: 'mongo4:27022', priority: 2}, {_id: 1, host: 'mongo5:27023', priority: 1}, {_id: 2, host: 'mongo6:27024', priority: 1}]})"

Write-Host "Esperando 10 segundos para la elección de primarios en los Shards..."
Start-Sleep -Seconds 10

Write-Host "4. Agregando Shards al Mongos Router..." -ForegroundColor Cyan
docker exec mongos mongosh --port 27017 --eval "sh.addShard('shard1RS/mongo1:27018,mongo2:27020,mongo3:27021')"
docker exec mongos mongosh --port 27017 --eval "sh.addShard('shard2RS/mongo4:27022,mongo5:27023,mongo6:27024')"

Write-Host "5. Habilitando Sharding para la base de datos GA_Analytics_DB y la colección sessions..." -ForegroundColor Cyan
docker exec mongos mongosh --port 27017 --eval "sh.enableSharding('GA_Analytics_DB')"
docker exec mongos mongosh --port 27017 --eval "db.getSiblingDB('GA_Analytics_DB').sessions.createIndex({ fullVisitorId: 'hashed' })"
docker exec mongos mongosh --port 27017 --eval "sh.shardCollection('GA_Analytics_DB.sessions', { fullVisitorId: 'hashed' })"

Write-Host "Configuración completada exitosamente. El clúster está listo." -ForegroundColor Green
