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
  bunyan_base_path: '/tmp/couch2elastic4sync',
  endOnCatchup: false,
  removeMeta: true,
  urlTemplate: false,
  load: {
    swallowErrors: false
  },
  checkpointSize: 20
})
if (!config.elasticsearch) {
  console.log('No elasticsearch search.')
  process.exit(1)
}

if (config.mapper && typeof config.mapper === 'string') {
  config.mapper = require(path.resolve(config.mapper))
}
config.seq_url = url.resolve(config.elasticsearch, '/' + url.parse(config.elasticsearch).pathname.split('/')[1] + '/_mapping/seq')

var log = getLogFile(config)

if (config._[0] === 'load') {
  var load = require('../lib/load')(config, log)
  load.pipe(process.stdout)
} else {
  getSince(config, function (err, since) {
    if (err) {
      console.log('an error occured', err)
      console.log('since: now')
      since = null
    } else {
      console.log('since:', since)
    }
    console.log('endOnCatchup:', config.endOnCatchup)
    console.log('logging to:', getLogPath(config))
    require('../lib')(config, log, since)
  })
}

function getLogPath (config) {
  var filename = md5(config.elasticsearch) + '.log'
  return path.resolve(config.bunyan_base_path, filename)
}

function getLogFile (config) {
  mkdirp.sync(config.bunyan_base_path)
  var filename = md5(config.elasticsearch) + '.log'
  var where = path.resolve(config.bunyan_base_path, filename)

  var log = bunyan.createLogger({
    name: 'couch2elastic4sync',
    streams: [{
      path: where
    }]
  })
  return log
}


function getSince (config, cb) {
  if (config.since) return cb(null, config.since)
  jsonist.get(config.seq_url, function (err, data) {
    if (err) return cb(err)
    var seq = selectn('idx-edm-v5.mappings.seq._meta.seq', data)
    if (!seq) return cb('no seq number in elasticsearch at ' + config.seq_url)
    return cb(null, seq)
  })
}
