# couch2elastic4sync

Since Elasticsearch rivers are [deprecated](https://www.elastic.co/blog/deprecating-rivers) this
is a simple node process that will follow a couchdb changes feed and updates elasticsearch.

You can also include an optional mapper.

```sh
npm install couch2elastic4sync -g
```

## Usage

### Configuration file

[rc](http://npm.im/rc) is used to set to variables. For example, create a .couch2elastic4sync file with the following
```ini
database=http://localhost:5984/idx-edm-v5
elasticsearch=http://elastic-1.com:9200/idx-edm-v5/listing
```

or pass the config file path explicity
```ini
couch2elastic4sync --config=path/to/configfile
```

Alternatively, the config file can be in JSON
```json
{
  "database": "http://localhost:5984/idx-edm-v5",
  "elasticsearch": "http://elastic-1.com:9200/idx-edm-v5/listing"
}
```

You can also destructure some of the urls to help configuration management
```ini
couch=http://localhost:5984
database=idx-edm-v5
elasticsearch=http://elastic-1.com:9200
indexName=idx-edm-v5
indexType=listing
```

### Load documents

To load all the documents into elasticsearch, run
```sh
couch2elastic4sync load
```

### Keep documents in sync

To keep a sync process going, run
```sh
couch2elastic4sync
```

### Format and filter documents

A `mapper` function can be passed from the config to format documents before being put to ElasticSearch:
```ini
database=http://localhost:5984/idx-edm-v5
elasticsearch=http://elastic-1.com:9200/idx-edm-v5/listing
mapper=path/to/my-mapper.js
```

Where my-mapper.js could be something like
```
module.exports = function (doc) {
  // apply formatting here
  return doc
}
```

If the function returns empty, the document is filtered-out

## License

MIT
