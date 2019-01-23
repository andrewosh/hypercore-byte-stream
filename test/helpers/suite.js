const test = require('tape')

module.exports = function (tag, create) {
  test(`${tag}: stream entire feed`, t => {
    t.plan(1 + 10)
    create(10, 100, (err, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records, 1000)

      stream.setRange()

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

  test.only(`${tag}: stream with byteOffset, no length, no bounds`, t => {
    t.plan(1 + 8)
    create(10, 100, (err, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records.slice(2), 800)

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

  test(`${tag}: stream with byteOffset, length, no bounds`, t => {
    t.plan(1 + 2)
    create(10, 100, (err, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records.slice(5), 500)

      stream.setRange({
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
    create(10, 100, (err, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records.slice(5), 500)

      stream.setRange({
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
    create(10, 100, (err, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records.slice(5), 500)

      stream.setRange({
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

  test(`${tag}: reads will be resumed after setRange`, t => {
    t.plan(2)
    create(10, 100, (err, stream, records) => {
      t.error(err, 'create stream ok')

      stream.once('data', data => {
        t.true(data)
      })

      setTimeout(() => {
        stream.setRange()
      }, 100)
    })
  })


  test(`${tag}: cannot call setRange after streaming\'s started`, t => {
    t.plan(2)
    create(10, 100, (err, stream, records) => {
      t.error(err, 'create stream ok')

      stream.setRange({
        blockOffset: 2
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
      stream.on('error', err => {
        t.error(err)
      })
    })
  })

  test(`${tag}: destroy during read leads to cleanup`, t => {
    t.plan(1 + 5 + 3)
    create(10, 100, (err, stream, records) => {
      t.error(err, 'create stream ok')
      let combined = Buffer.concat(records, 1000)

      stream.setRange()

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
