#!/bin/bash
es_host="192.168.175.12:9200"

titan_es="titan-es"
curl -XDELETE ${es_host}'/'${titan_es}
curl -XPUT ${es_host}'/'${titan_es} -d '{
    "settings" : {
        "index" : {
            "number_of_shards" : 60,
            "number_of_replicas" : 2
        }
    },
    "mappings": {
        "rel": {
           "_all" : {
              "enabled" : false
            },
            "properties": {
                "vertexId": {
                    "type": "string",
                    "index": "not_analyzed"
                }
            }
        },
        "ifcache": {
           "_all" : {
              "enabled" : false
            },
            "properties": {
                "isCached": {
                    "type": "boolean"
                }
            }
        }
    }
}'