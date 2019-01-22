const test = require('tape')
const ram = require('random-access-memory')
const hypercore = require('hypercore')

const createStream = require('..')

function create (numRecords, recordSize, cb) {
  let core = hypercore(ram)
  let records = []
  for (let i = 0; i < numRecords; i++) {
    let record = Buffer.allocUnsafe(recordSize).fill(Math.floor(Math.random() * 10))
    records.push(record)
  }
  core.append(records, err => {
    if (err) return cb(err)
    return cb(null, core, records)
  })
}

test('stream entire feed', t => {
  t.plan(1 + 10)
  create(10, 100, (err, core, records) => {
    t.error(err, 'create hypercore ok')
    let combined = Buffer.concat(records, 1000)

    let stream = createStream(core)

    let offset = 0
    stream.on('data', data => {
      t.same(data, combined.slice(offset, offset + data.length), 'chunks are the same') 
      offset += data.length
    })
    stream.on('error', err => {
      t.error(err)
    })
  })
})

test('stream with byteOffset, no length, no bounds', t => {
  t.plan(1 + 8)
  create(10, 100, (err, core, records) => {
    t.error(err, 'create hypercore ok')
    let combined = Buffer.concat(records.slice(2), 800)

    let stream = createStream(core)
    stream.setRange({
      byteOffset: 200
    })

    let offset = 0
    stream.on('data', data => {
      t.same(data, combined.slice(offset, offset + data.length), 'chunks are the same') 
      offset += data.length
    })
    stream.on('error', err => {
      t.error(err)
    })
  })
})

test('stream with byteOffset, length, no bounds', t => {
  t.plan(1 + 2)
  create(10, 100, (err, core, records) => {
    t.error(err, 'create hypercore ok')
    let combined = Buffer.concat(records.slice(5), 500)

    let stream = createStream(core)
    stream.setRange({
      byteOffset: 500,
      length: 50
    })

    let offset = 0
    stream.on('data', data => {
      t.same(data.length, 50)
      t.same(data, combined.slice(offset, offset + data.length), 'chunks are the same') 
      offset += data.length
    })
    stream.on('error', err => {
      t.error(err)
    })
  })
})

test('stream with byteOffset, length, start bound', t => {
  t.plan(1 + 2)
  create(10, 100, (err, core, records) => {
    t.error(err, 'create hypercore ok')
    let combined = Buffer.concat(records.slice(5), 500)

    let stream = createStream(core)
    stream.setRange({
      byteOffset: 500,
      length: 50,
      start: 4
    })

    let offset = 0
    stream.on('data', data => {
      t.same(data.length, 50)
      t.same(data, combined.slice(offset, offset + data.length), 'chunks are the same') 
      offset += data.length
    })
    stream.on('error', err => {
      t.error(err)
    })
  })
})

test('stream with byteOffset, length, start and end bounds', t => {
  t.plan(1 + 2)
  create(10, 100, (err, core, records) => {
    t.error(err, 'create hypercore ok')
    let combined = Buffer.concat(records.slice(5), 500)

    let stream = createStream(core)
    stream.setRange({
      byteOffset: 500,
      length: 50,
      start: 7,
      end: 8
    })

    let offset = 0
    stream.on('data', data => {
      t.same(data.length, 50)
      t.same(data, combined.slice(offset, offset + data.length), 'chunks are the same') 
      offset += data.length
    })
    stream.on('error', err => {
      t.error(err)
    })
  })
})

test('cannot call setRange after streaming\'s started', t => {
  t.plan(2)
  create(10, 100, (err, core, records) => {
    t.error(err, 'create hypercore ok')
    let stream = createStream(core)
    stream.setRange({
      start: 2
    })
    stream.once('data', data => {
      try {
        stream.setRange({
          start: 4
        })
        t.fail('called setRange after streaming')
      } catch (err) {
        t.true(err)
      }
    })
  })
})

