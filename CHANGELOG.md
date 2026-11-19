# Revision history for lossy-chan

## 0.2.0.0 -- 2026-11-19

* Change type of `new` to `Word8 -> IO (InChan a, OutChan a)`.

  This change was made to reflect the fact that the channel ring buffer uses
  a `SmallArray` and therefore the buffer length should not be much bigger than
  128. This is now reflected in the argument type.

* Fix issue where `NoInChanError` was raised early.

  In previous versions, `receive` and `tryReceive` would raise `NoInChanError`
  if no live code path could call `out`. This was due to an error where the
  value used for liveness testing was stored in the `InChan` separately from
  the channel ring buffer and was only used in `out`. Hence, if no live code
  path could call `out`, the value used for liveness would be garbage collected
  and `receive` and `tryReceive` would erroneously raise `NoInChanError` if
  they caught up with the input channel.

## 0.1.1.0 -- 2026-11-12

* Support creating a new output channel from the input channel.

## 0.1.0.0 -- 2026-11-11

* First version. Released on an unsuspecting world.
