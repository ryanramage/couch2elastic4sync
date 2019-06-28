#!/usr/bin/env node
var path = require('path')
var md5 = require('md5')
var url = require('url')
var mkdirp = require('mkdirp')
var bunyan = require('bunyan')
var jsonist = require('jsonist')
var selectn = require('selectn')

var config = require('rc')('couch2elastic4sync', {
  addRaw: false,
  rawField: 'raw',
  endOnCatchup: false,
  removeMeta: true,
  urlTemplate: false,
  load: {
    swallowErrors: false
  },
  concurrency: 5,
  checkpointSize: 20,
  retry: {
    times: 10,
    interval: 200
  }
})
if (!config.elasticsearch) {
  console.log('No elasticsearch search.')
  process.exit(1)
}

if (config.mapper && typeof config.mapper === 'string') {
  config.mapper = require(path.resolve(config.mapper))
}

// allow config.couch to be the root url of the couch and config.database to be just the db name
// config.couch = http://sofa.place.com:5984
// config.database = idx-edm-v5
// join them together to make the complete url
if (config.couch) config.database = config.couch + '/' + config.database

// allow elasticsearch to be the root url of elasticsearch, and indexName be just the index
// and indexType be the type
// config.elasticsearch = http://elastic.place.com:9200
// config.indexName = idx-edm-v5
// config.indexType = listing
// join them together to make the complete url
if (config.indexName && config.indexType) config.elasticsearch = config.elasticsearch + '/' + config.indexName + '/' + config.indexType

var index_name = url.parse(config.elasticsearch).pathname.split('/')[1]
config.seq_url = url.resolve(config.elasticsearch, '/' + index_name + '/_mapping/seq')

var log = getLogFile(config)
if (config._[0] === 'id') {
  var id = config._[1]
  var one = require('../lib/one')(config, log, id, function onDone (err) {
    if (err) log.error('An error occured', err)
  })
  one.pipe(process.stdout)
} else if (config._[0] === 'cleanup') {
  require('../lib/cleanup')(config, log, function onDone (err) {
    if (err) log.error('An error occured', err)
    console.log('complete')
  })
} else if (config._[0] === 'load') {
  var load = require('../lib/load')(config, log, function onDone (err) {
    if (err) log.error('An error occured', err)
  })
  load.pipe(process.stdout)
} else {
  getSince(config, index_name, function (err, since) {
    if (err) {
      log.error('an error occured', err)
      log.info('since: now')
      since = null
    } else {
      log.info('since:', since)
    }
    log.info('endOnCatchup:', config.endOnCatchup)
    if (config.bunyan_base_path) {
      log.info('logging to:', getLogPath(config))
    } else if (config.bunyan_log_stderr) {
      log.info('logging to stderr')
    }
    require('../lib')(config, log, since)
  })
}

function getLogPath (config) {
  var filename = md5(config.elasticsearch) + '.log'
  return path.resolve(config.bunyan_base_path, filename)
}

function getLogFile (config) {
  var _b_opts = {
    name: 'couch2elastic4sync',
    stream: process.stdout
  }
  if (config.bunyan_log_stderr) {
    _b_opts.stream = process.stderr
  } else if (config.bunyan_base_path) {
    mkdirp.sync(config.bunyan_base_path)
    var filename = md5(config.elasticsearch) + '.log'
    var where = path.resolve(config.bunyan_base_path, filename)
    _b_opts.stream = null
    _b_opts.streams = [{
      path: where
    }]
  }

  var log = bunyan.createLogger(_b_opts)
  return log
}

function getSince (config, index_name, cb) {
  if (config.since) return cb(null, config.since)
  jsonist.get(config.seq_url, function (err, data) {
    if (err) return cb(err)
    if (!data[index_name]) return cb('index name does not match')
    var seq = selectn('mappings.seq._meta.seq', data[index_name])
    if (!seq) return cb('no seq number in elasticsearch at ' + config.seq_url)
    return cb(null, seq)
  })
}
