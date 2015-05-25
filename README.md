es-prune
========

Prunes large documents in Elasticsearch. Will iterate through all documents in logstash index dated now()-PRUNE_OLDER_THAN_MONTHS and empty 
given fields (default message and body). Run daily from cron.
