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

### Filter documents

You can filter documents that should be loaded or kept in sync by setting a filter function in the config file.
Example:
```json
{
  "database": "http://localhost:5984/idx-edm-v5",
  "elasticsearch": "http://elastic-1.com:9200/idx-edm-v5/listing",
  "filter": "(doc) => doc.type === 'some-listing-type'"
}
```
Just like you would expect a [CouchDB filter function](http://docs.couchdb.org/en/1.6.1/couchapp/ddocs.html#filter-functions) to behave:
> [returning] true means that doc passes the filter rules, false means that it does not.

**NB**: Make sure that your function can be parsed by [`eval`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/eval#eval_as_a_string_defining_function_requires_(_and_)_as_prefix_and_suffix)

## License

MIT
