const assert = require('assert')
const { Readable } = require('readable-stream')
const HypercoreByteCursor = require('hypercore-byte-cursor')

module.exports = (feed, opts) => new HypercoreByteStream(feed, opts)

class HypercoreByteStream extends Readable {
  constructor (opts) {
    super(opts)
    opts = opts || {}

    this.feed = null
    this.cursor = null

    this.bytesRead = 0
    this._byteLength = -1
    this._opened = false
    this._resume = false
    this._ended = false

    if (opts.feed) {
      this.start(opts)
    }
  }

  start ({ feed, blockOffset, blockLength, byteOffset, byteLength} = {}) {
    assert(!this.feed, 'Can only provide options once (in the constructor, or asynchronously).')

    assert(feed, 'Must provide a feed')
    assert(!this._opened, 'Cannot call start multiple after streaming has started.')
    assert(!byteLength || byteLength >= -1, 'length must be a positive integer or -1')

    this.feed = feed
    feed.on('close', this._cleanup.bind(this))
    feed.on('end', this._cleanup.bind(this))

    this._byteLength = (byteLength !== undefined) ? byteLength : -1
    this.cursor = new HypercoreByteCursor(this.feed, this._onSeek.bind(this), {
      blockOffset,
      blockLength,
      byteOffset
    })

    if (this._resume) {
      return this._read(0)
    }
  }

  _onSeek({ start, end, position }, cb) {
    let missing = 1
    let self = this

    let downloadRange = self.feed.download({
      start,
      end,
      linear: true
    }, ondownload)

    if (self._byteLength > -1) {
      self.feed.seek(self._byteOffset + self._byteLength - 1, downloadRange, onend)
    }

    function onend (err, index) {
      if (err || !self._range) return cb(err)
      if (self._ended || self.destroyed) return cb(err)
      missing++

      self.feed.undownload(downloadRange)

      downloadRange = self.feed.download({
        start: downloadRange.start,
        end: index,
        linear: true
      }, ondownload)
    }

    function ondownload (err) {
      if (--missing) return
      if (err && !self._ended && !self._downloaded && err.code !== 'ECANCELED') return cb(err)
      return cb(null)
    }
  }

  _open (size) {
    let self = this
    this._opened =  true
    this.cursor.seek(0)
    return this._read(size)
  }

  _cleanup () {
    if (this._opened) {
      this.cursor.close()
      this._ended = true
    }
  }

  _destroy (err, cb) {
    this._cleanup()
    return cb(err)
  }

  _read (size) {
    if (!this.cursor) {
      this._resume = true
      return
    } else if (this.cursor) {
      this._resume = false
    }

    if (this._ended) return this.push(null)
    if (!this._opened) {
      return this._open(size)
    }

    this.cursor.next((err, data) => {
      if (err || this.destroyed) return this.destroy(err)
      if (!data) {
        this._cleanup()
        return this.push(null)
      }
      if (this._byteLength > -1) {
        if (this._byteLength < data.length) data = data.slice(0, this._byteLength)
        this._byteLength -= data.length
      }
      this.bytesRead += data.length
      this.push(data)
    })
  }
}
