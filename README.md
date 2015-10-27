# couch2elastic4sync

Since Elasticsearch rivers are [deprecated](https://www.elastic.co/blog/deprecating-rivers) this
is a simple node process that will follow a couchdb changes feed and updates elasticsearch.

You can also include an optional mapper.

```
npm install couch2elastic4sync -g
```

## Usage

(rc)[http://npm.im/rc] to set to variable. For example, create a .couch2elastic4sync file with the following


    database=http://localhost:5984/idx-edm-v5
    elasticsearch=http://elastic-1.com:9200/idx-edm-v5/listing

To load all the documents into elasticsearch, run

    couch2elastic4sync load

To keep a sync process going, run

    couch2elastic4sync


## License

MIT
