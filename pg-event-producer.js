'use strict'
var http = require('http')
var keepAliveAgent = new http.Agent({ keepAlive: true });

var SPEEDUP = process.env.SPEEDUP || 1
var ONEMINUTE = 60*1000/SPEEDUP
var TWOMINUTES = 2*ONEMINUTE
var TENMINUTES = 10*ONEMINUTE
var ONEHOUR = 60*ONEMINUTE
var ONEDAY = 24*ONEHOUR
var ONEYEAR = 365*ONEDAY

var INTERNAL_SCHEME = process.env.INTERNAL_SCHEME || 'http'

function eventProducer(pool) {
  this.pool = pool
  this.consumers = []
}

eventProducer.prototype.init = function(callback) {
  var self = this
  this.createTablesThen(function () {
    self.getListeners(self, function () {
      self.getListenerTimer = setInterval(self.getListeners, ONEMINUTE, self)
      self.discardListenerTimer = setInterval(self.discardListenersOlderThan, TWOMINUTES, TENMINUTES, self)
      self.discardEventTimer = setInterval(self.discardEventsOlderThan, ONEDAY, ONEYEAR, self)
      callback()
    })
  })  
}

eventProducer.prototype.finalize = function() {
  console.log('pg-event-producer finalizing')
  this.getListenerTimer.unref()
  this.discardListenerTimer.unref()
  this.discardEventTimer.unref()
}

eventProducer.prototype.discardListenersOlderThan = function(interval, self) {
  var time = Date.now() - interval
  var pool = self.pool
  pool.query(`DELETE FROM consumers WHERE registrationtime < ${time}`, function (err, pgResult) {
    if (err) 
      console.log('discardListenersOlderThan:', `unable to delete old consumers ${err}`)
    else
      console.log('discardListenersOlderThan:', `trimmed consumers older than ${time}`)
  })
}

eventProducer.prototype.getListeners = function(self, callback) {
  var query = 'SELECT ipaddress FROM consumers'
  var pool = self.pool
  pool.query(query, function (err, pgResult) {
    if (err)
      console.log(`unable to retrieve ipaddresses from consumers`)
    else
      self.setConsumers(pgResult.rows.map(row => row.ipaddress))
    if (callback) 
      callback()
  })
}

eventProducer.prototype.discardEventsOlderThan = function(interval, self) {
  var time = Date.now() - interval
  var pool = self.pool
  pool.query(`DELETE FROM events WHERE eventtime < ${time} RETURNING index`, function (err, pgResult) {
    if (err)
      console.log('discardEventsOlderThan:', `unable to delete old events ${err}`)
    else
      console.log(`discardEventsOlderThan: ${interval} found: ${pgResult.rows.map(row => row.index)}`)
  })
}

eventProducer.prototype.createTablesThen = function(callback) {
  var query = 'CREATE TABLE IF NOT EXISTS events (index bigserial, topic text, eventtime bigint, data jsonb)'
  var pool = this.pool
  pool.query(query, function(err, pgResult) {
    if(err)
      console.error('error creating events table', err)
    else {
      query = 'CREATE TABLE IF NOT EXISTS consumers (ipaddress text primary key, registrationtime bigint)'
      pool.query(query, function(err, pgResult) {
        if(err)
          console.error('error creating consumers table', err)
        else
          callback()
      })
    }
  })
}

eventProducer.prototype.setConsumers = function(consumers) {
  console.log('setConsumers:', 'consumers:', consumers)
  this.consumers = consumers
}

eventProducer.prototype.tellConsumers = function(req, event, callback) {
  var count = 0
  var total = this.consumers.length
  if (total > 0) {
    var responded = false
    for (var i = 0; i < total; i++) {
      let listener = this.consumers[i]
      sendEventThen(req, event, listener, function(err) {
        if (err)
          console.log(`failed to send event ${event.index} to ${listener} err: ${err}`)
        else
          console.log(`sent event ${event.index} to ${listener} index: ${event.index}`)
        if (++count == total && !responded) {
          responded = true
          callback()
        }
      })
    }
    setTimeout(function() { // don't wait more than 50ms for listeners 
      if (!responded) {
        responded = true 
        callback()
      }
    }, 50)
  } else
    callback()
}

function sendEventThen(serverReq, event, host, callback) {
  var postData = JSON.stringify(event)
  var headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(postData)
  }
  if (serverReq.headers.authorization)
    headers.authorization = serverReq.headers.authorization 
  var hostParts = host.split(':')
  var options = {
    protocol: `${INTERNAL_SCHEME}:`,
    hostname: hostParts[0],
    path: '/events',
    method: 'POST',
    headers: headers,
    agent: keepAliveAgent
  }
  if (hostParts.length > 1)
    options.port = hostParts[1]
  var client_req = http.request(options, function (client_res) {
    client_res.setEncoding('utf8')
    var body = ''
    client_res.on('data', chunk => body += chunk)
    client_res.on('end', function() {
      if (client_res.statusCode == 200)  
        callback(null)
      else 
        callback(`unable to send event to: ${host} statusCode: ${client_res.statusCode}`)
    })
  })
  client_req.on('error', function (err) {
    callback(err)
  })
  client_req.setTimeout(2000, function() {
    client_req.abort()
  })
  client_req.write(postData)
  client_req.end()
}

eventProducer.prototype.queryAndStoreEvent = function(req, query, queryArgs, eventTopic, eventData, callback) {
  // We use a transaction here, since its PG and we can. In fact it would be OK to create the event record first and then do the update.
  // If the update failed we would have created an unnecessary event record, which is not ideal, but probably harmless.
  // The converse—creating an update without an event record—could be harmful.
  var pool = this.pool
  var self = this
  pool.connect(function(err, client, release) {
    if (err)
      callback(err)
    else
      // console.log('query:', query)
      client.query('BEGIN', function(err) {
        if(err) {
          client.query('ROLLBACK', release)
          callback(err)
        } else
          client.query(query, queryArgs, function(err, pgResult) {
            if(err) {
              client.query('ROLLBACK', release)
              if (err.code == 23505)
                callback(409)
              else { 
                console.log(err)
                callback(err)
              }
            } else {
              var time = Date.now()
              var event = eventData(pgResult)
              if (event) {
                var equery = `INSERT INTO events (topic, eventtime, data) 
                              values ($1, $2, $3)
                              RETURNING *`
                client.query(equery, [eventTopic, time, JSON.stringify(event)], function(err, pgEventResult) {
                  if(err) {
                    client.query('ROLLBACK', release)
                    callback(err)
                  } else
                    if (pgEventResult.rowcount == 0) {
                      client.query('ROLLBACK', release)
                      callback('no event created')
                    } else {
                      client.query('COMMIT', release)
                      self.tellConsumers(req, pgEventResult.rows[0], function(){
                        callback(null, pgResult, pgEventResult)
                      })
                    }
                })
              } else {
                client.query('COMMIT', release)
                callback(null, pgResult)            
              }
            }
          })
      })
  })
}

exports.eventProducer=eventProducer