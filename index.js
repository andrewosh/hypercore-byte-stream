const assert = require('assert')
const { Readable } = require('stream')

module.exports = (feed, opts) => new HypercoreByteStream(feed, opts)

class HypercoreByteStream extends Readable {
  constructor (feed, opts) {
    super(opts)
    opts = opts || {}

    this.feed = feed
    this.inclusive = (opts.inclusive != undefined) ? opts.inclusive : false
    this._range = null
    this._offset = 0
    this._opened = false
    this._ended = false
    this._downloaded = false

    feed.on('close', this._cleanup.bind(this))
    feed.on('end', this._cleanup.bind(this))

    this.setRange({})
  }

  setRange ({ start, end, byteOffset, length}) {
    assert(!this._opened, 'Cannot call setRange multiple after streaming has started.')
    assert(!start || start >= 0, 'start must be >= 0')
    assert(!end || end >= 0, 'end must be >= 0')
    assert(!length || length >= -1, 'length must be a positive integer or -1')
    if (start && end) {
      assert(start <= end, 'start must be <= end')
    }
    this._range = {
      start: start || 0,
      end: end || +Infinity,
      byteOffset: byteOffset || 0,
      length: (length !== undefined) ? length : -1
    }
  }

  _open (cb) {
    let self = this
    let missing = 1
    let downloaded = false

    this._opened =  true
    this.feed.ready(err => {
      if (err) return this.destroy(err)
      this.open = true
      this.feed.seek(this._range.byteOffset, this._range, onstart)
    })

    function onend (err, index) {
      if (err || !self._downloadRange) return
      if (self._ended || self.destroyed) return

      missing++
      self.feed.undownload(self._downloadRange)
      self._range = { 
        ...self._range,
        ...self.feed.download({
          start: self._range.start,
          end: index,
          linear: true
        }, ondownload)
      }
    }

    function onstart (err, index, off) {
      if (err) return cb(err)
      if (self._ended || self.feed.destroyed) return

      self._range.start = index
      self._offset = off

      self._range = {
        ...self._range,
        ...self.feed.download({
          ...self._range,
          linear: true
        }, ondownload)
      }

      if (self._range.length > -1) {
        self.feed.seek(self._range.byteOffset + self._range.length, self._range, onend)
      }

      return cb(null)
    }

    function ondownload (err) {
      if (--missing) return
      if (err && !self._ended && !self._downloaded) feed.destroy(err)
      else self._downloaded = true
    }
  }

  _cleanup () {
    if (self._range && self._opened) {
      self.feed.undownload(self._range)
      self._range = null
      self._ended = true
    }
  }

  _destroy (err, cb) {
    self._cleanup()
    return cb(err)
  }

  _read (size) {
    if (this._ended) return this.push(null)
    if (!this._opened) {
      this._open(err => {
        if (err) return this.destroy(err)
        return this._read(size)
      })
      return
    }

    if (this._range.start === this._range.end || this._range.length === 0) {
      return this.push(null)
    }

    this.feed.get(this._range.start++, { wait: !this._downloaded }, (err, data) => {
      if (err) return this.destroy(err)
      if (this._offset) data = data.slice(offset)
      this._offset = 0
      if (this._range.length > -1) {
        if (this._range.length < data.length) data = data.slice(0, this._range.length)
        this._range.length -= data.length
      }
      if (!data) {
        this._cleanup()
      }
      this.push(data)
    })
  }
}
