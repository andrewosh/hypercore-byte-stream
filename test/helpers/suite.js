const test = require('tape')

module.exports = function (tag, create) {
  test(`${tag}: stream entire feed`, t => {
    t.plan(1 + 10)
    create(10, 100, (err, core, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records, 1000)

      stream.start({
        feed: core
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

  test(`${tag}: stream with byteOffset, no length, no bounds`, t => {
    t.plan(1 + 8)
    create(10, 100, (err, core, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records.slice(2), 800)

      stream.start({
        feed: core,
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

  test(`${tag}: stream with byteOffset, length, no bounds`, t => {
    t.plan(1 + 2)
    create(10, 100, (err, core, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records.slice(5), 500)

      stream.start({
        feed: core,
        byteOffset: 500,
        byteLength: 50
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

  test(`${tag}: stream with byteOffset, length, start bound`, t => {
    t.plan(1 + 2)
    create(10, 100, (err, core, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records.slice(5), 500)

      stream.start({
        feed: core,
        byteOffset: 500,
        byteLength: 50,
        blockOffset: 4
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

  test(`${tag}: stream with byteOffset, length, start and end bounds`, t => {
    t.plan(1 + 2)
    create(10, 100, (err, core, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records.slice(5), 500)

      stream.start({
        feed: core,
        byteOffset: 500,
        byteLength: 50,
        blockOffset: 7,
        blockLength: 1
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

  test(`${tag}: stream with byteOffset, zero length, start and end bounds`, t => {
    create(10, 100, (err, core, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records.slice(5), 500)

      stream.start({
        feed: core,
        byteOffset: 500,
        byteLength: 0,
        blockOffset: 7,
        blockLength: 1
      })

      let offset = 0
      stream.on('data', data => {
        t.fail('data should not be emitted')
      })
      stream.on('end',() => {
        t.end()
      })
      stream.on('error', err => {
        t.error(err)
      })
    })
  })

  test.only(`${tag}: stream with byteOffset, length larger than hypercore size`, t => {
    create(10, 100, (err, core, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records.slice(5), 500)

      stream.start({
        feed: core,
        byteOffset: 500,
        byteLength: 700,
      })

      setTimeout(() => {
        core.append(Buffer.allocUnsafe(1000).fill(8))
      }, 1000)

      let offset = 0
      stream.on('data', data => {
        t.same(data, combined.slice(offset, offset + data.length), 'chunks are the same') 
        offset += data.length
      })
      stream.on('end',() => {
        t.end()
      })
      stream.on('error', err => {
        t.error(err)
      })
    })
  })

  test(`${tag}: reads will be resumed after start`, t => {
    t.plan(2)
    create(10, 100, (err, core, stream, records) => {
      t.error(err, 'create stream ok')

      stream.once('data', data => {
        t.true(data)
      })

      setTimeout(() => {
        stream.start({
          feed: core
        })
      }, 100)
    })
  })


  test(`${tag}: cannot call start after streaming\'s started`, t => {
    t.plan(2)
    create(10, 100, (err, core, stream, records) => {
      t.error(err, 'create stream ok')

      stream.start({
        feed: core,
        blockOffset: 2
      })

      stream.once('data', data => {
        try {
          stream.start({
            start: 4
          })
          t.fail('called start after streaming')
        } catch (err) {
          t.true(err)
        }
      })
      stream.on('error', err => {
        t.error(err)
      })
    })
  })

  test(`${tag}: destroy during read leads to cleanup`, t => {
    t.plan(1 + 5 + 3)
    create(10, 100, (err, core, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records, 1000)

      stream.start({
        feed: core
      })

      let offset = 0
      let count = 0
      stream.on('data', data => {
        t.same(data, combined.slice(offset, offset + data.length), 'chunks are the same') 
        offset += data.length
        if (count++ === 5) {
          stream.destroy()
        }
      })
      stream.on('error', err => {
        t.error(err)
      })
      stream.on('close', () => {
        t.true(stream._ended)
        t.false(stream._range)
      })
    })
  })
}
