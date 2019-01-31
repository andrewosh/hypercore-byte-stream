const assert = require('assert')
const { Readable } = require('readable-stream')

module.exports = (feed, opts) => new HypercoreByteStream(feed, opts)

class HypercoreByteStream extends Readable {
  constructor (opts) {
    super(opts)
    opts = opts || {}

    this.feed = null
    this.bytesRead = 0
    this.pending = true

    this._range = null
    this._downloadRange = null
    this._offset = 0
    this._opened = false
    this._resume = false
    this._ended = false
    this._downloaded = false

    if (opts.feed) {
      this.start(opts)
    }
  }

  start ({ feed, blockOffset, blockLength, byteOffset, byteLength} = {}) {
    assert(!this.feed, 'Can only provide options once (in the constructor, or asynchronously).')

    assert(feed, 'Must provide a feed')
    assert(!this._opened, 'Cannot call start multiple after streaming has started.')
    assert(!blockOffset || blockOffset >= 0, 'start must be >= 0')
    assert(!blockLength || blockLength >= 0, 'end must be >= 0')
    assert(!byteLength || byteLength >= -1, 'length must be a positive integer or -1')

    this.feed = feed
    this._range = {
      start: blockOffset || 0,
      end: (blockOffset && blockLength) ? blockOffset + blockLength : -1,
      byteOffset: byteOffset || 0,
      length: (byteLength !== undefined) ? byteLength : -1
    }

    if (this._resume) {
      return this._read(0)
    }
  }

  _open (size) {
    let self = this
    let missing = 1

    this._opened =  true
    this.feed.ready(err => {
      if (err || this.destroyed) return this.destroy(err)
      this.open = true
      this.feed.seek(this._range.byteOffset, this._range, onstart)
    })

    function onend (err, index) {
      if (err || !self._range) return
      if (self._ended || self.destroyed) return
      missing++

      self.feed.undownload(self._downloadRange)

      self._downloadRange = self.feed.download({
        start: self._range.start,
        end: index,
        linear: true
      }, ondownload)

      self._range = { 
        ...self._range,
        ...self._downloadRange
      }

      self.pending = false
      self._read(size)
    }

    function onstart (err, index, off) {
      if (err) return cb(err)
      if (self._ended || self.destroyed) return

      self._range.start = index
      self._offset = off

      self._downloadRange = self.feed.download({
        ...self._range,
        linear: true
      }, ondownload)

      self._range = {
        ...self._range,
        ...self._downloadRange
      }

      if (self._range.length > -1) {
        self.feed.seek(self._range.byteOffset + self._range.length - 1, self._range, onend)
      } else {
        self.pending = false
        self._read(size)
      }
    }

    function ondownload (err) {
      if (--missing) return
      if (err && !self._ended && !self._downloaded && err.code !== 'ECANCELED') self.destroy(err)
      else self._downloaded = true
    }
  }

  _cleanup () {
    if (this._range && this._opened) {
      this.feed.undownload(this._range)
      this._range = null
      this._ended = true
    }
  }

  _destroy (err, cb) {
    this._cleanup()
    return cb(err)
  }

  _read (size) {
    if (!this._range) {
      this._resume = true
      return
    } else if (this._resume) {
      this._resume = false
    }

    if (this._ended) return this.push(null)
    if (!this._opened) {
      return this._open(size)
    }

    if (this._range.end !== -1 && this._range.start > this._range.end || this._range.length === 0) {
      return this.push(null)
    }

    this.feed.get(this._range.start++, { wait: !this._downloaded }, (err, data) => {
      if (err || this.destroyed) return this.destroy(err)
      if (this._offset) data = data.slice(offset)
      this._offset = 0
      if (this._range.length > -1) {
        if (this._range.length < data.length) data = data.slice(0, this._range.length)
        this._range.length -= data.length
      }
      if (!data) {
        this._cleanup()
      }
      this.bytesRead += data.length
      this.push(data)
    })
  }
}
