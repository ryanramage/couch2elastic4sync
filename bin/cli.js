#!/usr/bin/env node
var path = require('path')
var config = require('rc')('couch2elastic4sync', {
  type: 'listing'
})
if (!config.elasticsearch) {
  console.log('No elasticsearch search.')
  process.exit(1)
}

if (config.mapper && typeof config.mapper === 'string') {
  config.mapper = require(path.resolve(config.mapper))
}

if (config._[0] === 'load') {
  var load = require('../lib/load')(config.database, config.elasticsearch, config.mapper)
  load.pipe(process.stdout)
} else {
  require('../lib')(config.database, config.elasticsearch, config.mapper)
}

