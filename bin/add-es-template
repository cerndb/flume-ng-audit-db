curl -XPUT ela1:9200/_template/aud1 -d '
{
    "template" : "aud1-*",
    "mappings" : {
        "log" : {
            "properties": {
                "EVENT_TIMESTAMP": {
                	"type": "date",
                	"format": "strict_date_optional_time||epoch_millis"
                }
            }
        }
    }
}
'