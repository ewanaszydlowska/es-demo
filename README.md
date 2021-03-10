Insert data:
curl -H "Content-Type: application/json" -XPOST "localhost:9200/employee/_bulk?pretty&refresh" --data-binary "@Employees50K.json"
