{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE UnboxedTuples #-}

module Control.Concurrent.LossyChan (
  InChan,
  OutChan,
  new,
  out,
  send,
  receive,
  tryReceive,
) where

import Control.Concurrent.STM.TVar (TVar, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Monad (when)
import Data.Word (Word16, Word32, Word8)
import GHC.Conc (STM (..), atomically, retry)
import GHC.Conc.Sync (orElse)
import GHC.Exts (Int#, RealWorld, SmallArray#, State#, TVar#, Word32#, indexSmallArray#, isTrue#, newSmallArray#, newTVar#, readTVar#, unsafeFreezeSmallArray#, word2Int#, word32ToWord#, writeSmallArray#, writeTVar#, (+#), (==#))
import GHC.IO (IO (..))
import GHC.Word (Word32 (..))

{- |
The position in a channel ring buffer.
-}
data ChanPos = MkChanPos
  { full :: {-# UNPACK #-} !Word8
  {- ^
  This value tracks whether the underlying channel ring buffer is full, i.e.,
  whether every position is initialised. This differs from @`loop` > 0@, as
  the @`loop`@ count is intended to overflow.

  The value `0` represents `False` and every other value represents `True`.
  -}
  , loop :: {-# UNPACK #-} !Word16
  {- ^
  This value tracks the current loop around the channel ring buffer, which is
  used to determine whether or not a channel position refers to a live value.

  The `maxBound` of this type determines the resilience of the `OutChan`.
  If a receiver stops at loop @N@ and does not resume until the sender has
  written @`maxBound` - 1@ or @`maxBound`@ values and the loop counter has
  wrapped around all the way back to loop @N - 1 @ or @N@, then the receiver
  will treat its current position as live, which may cause it to skip some of
  the messages in the buffer.
  -}
  , next :: {-# UNPACK #-} !Word32
  {- ^
  This value tracks the next position that should be read (for receivers) or
  written to (for senders).
  -}
  }
  deriving (Eq, Ord, Show)

{- |
The initial position in a channel ring buffer.
-}
zero :: ChanPos
zero = MkChanPos{full = 0, loop = 0, next = 0}
{-# INLINE zero #-}

{- |
Advance to the next channel position.
-}
advance :: Word32 -> ChanPos -> ChanPos
advance size MkChanPos{..}
  | next < size - 1 = MkChanPos{next = next + 1, ..}
  | otherwise = MkChanPos{full = 1, loop = loop + 1, next = 0}
{-# INLINE advance #-}

{- |
Get the oldest live position given the current input position.
-}
minLive :: ChanPos -> ChanPos
minLive MkChanPos{..}
  | full == 0 = MkChanPos{next = 0, ..}
  | otherwise = MkChanPos{loop = loop - 1, ..}
{-# INLINE minLive #-}

{- |
Get the latest of the current output position or the oldest live position.
-}
live :: ChanPos -> ChanPos -> ChanPos
live icp ocp = max (minLive icp) ocp
{-# INLINE live #-}

{- |
The channel ring buffer that is used in `InChan`.

The channel ring buffer is an unboxed `SmallArray#` of unboxed `TVar#`. These
types are explicitly unboxed, because we cannot rely on an @UNPACK@ pragma to
unpack the @TVar@ inside of a @SmallArray@.
-}
data ChanBuf a = MkChanBuf
  { len# :: Word32#
  -- ^ The length of the channel ring buffer.
  , arr# :: SmallArray# (TVar# RealWorld a)
  -- ^ The array that backs the channel ring buffer.
  }

{- |
Used to fill uninitialised memory in the `ChanBuf`.

This should never be encountered by a library user.
-}
uninitialised :: a
uninitialised = error "Control.Concurrent.LossyChan: uninitialised memory"

{- |
Create a new channel buffer.
-}
newChanBuf :: Word32 -> IO (ChanBuf a)
newChanBuf (W32# len#) = IO $ \s1# ->
  case word32ToInt# len# of
    ilen# ->
      case newTVar# uninitialised s1# of
        (# s2#, var0# #) ->
          case newSmallArray# (word32ToInt# len#) var0# s2# of
            (# s3#, mutarr# #) ->
              let go :: Int# -> State# RealWorld -> State# RealWorld
                  go i# = \s4# ->
                    if isTrue# (i# ==# ilen#)
                      then s4#
                      else case newTVar# uninitialised s4# of
                        (# s5#, vari# #) ->
                          case writeSmallArray# mutarr# i# vari# s5# of
                            s6# -> go (i# +# 1#) s6#
               in case unsafeFreezeSmallArray# mutarr# (go 1# s3#) of
                    (# s7#, arr# #) ->
                      (# s7#, MkChanBuf len# arr# #)

{- |
Read the value from the given position in a `ChanBuf`.
-}
readChanBufT :: ChanBuf a -> Word32 -> STM a
readChanBufT MkChanBuf{..} (W32# i#) =
  STM $ \s1# ->
    case indexSmallArray# arr# (word32ToInt# i#) of
      (# vari# #) ->
        readTVar# vari# s1#
{-# INLINE readChanBufT #-}

{- |
Write the given value to a `ChanBuf` at the given position.
-}
writeChanBufT :: ChanBuf a -> Word32 -> a -> STM ()
writeChanBufT MkChanBuf{..} (W32# i#) a =
  STM $ \s1# ->
    case indexSmallArray# arr# (word32ToInt# i#) of
      (# vari# #) ->
        case writeTVar# vari# a s1# of
          s2# ->
            (# s2#, () #)
{-# INLINE writeChanBufT #-}

{- |
An input channel.
-}
data InChan a = MkInChan
  { buffer :: {-# UNPACK #-} !(ChanBuf a)
  -- ^ The underlying channel ring buffer.
  , icpVar :: {-# UNPACK #-} !(TVar ChanPos)
  -- ^ A `TVar` that holds the current input channel position.
  }

{- |
@`new` len@ creates an input channel that is backed by a ring buffer with length @len@.

The ring buffer is represented as a @SmallArray@, so @len@ should be smaller than 128.
-}
new :: Word32 -> IO (InChan a)
new len = do
  when (len <= 0) $ error "Control.Concurrent.LossyChan.new: invalid length"
  buffer <- newChanBuf len
  icpVar <- newTVarIO zero
  pure MkInChan{..}

{- |
@`send` i x@ sends the value @x@ over the input channel @i@.
-}
send :: InChan a -> a -> IO ()
send MkInChan{buffer = buffer@MkChanBuf{..}, ..} a =
  atomically $ do
    icp@MkChanPos{..} <- readTVar icpVar
    writeChanBufT buffer next a
    writeTVar icpVar (advance (W32# len#) icp)

{- |
An output channel.
-}
data OutChan a = MkOutChan
  { inChan :: {-# UNPACK #-} !(InChan a)
  , outPosVar :: {-# UNPACK #-} !(TVar ChanPos)
  }

{- |
@`out` i@ creates an output channel for input channel @i@.
-}
out :: InChan a -> IO (OutChan a)
out inChan@(MkInChan{..}) = do
  ip <- readTVarIO icpVar
  outPosVar <- newTVarIO (minLive ip)
  pure MkOutChan{..}

{- |
@`receiveT` o@ receives a value from an output channel and retries if none are available.
-}
receiveT :: OutChan a -> STM a
receiveT MkOutChan{inChan = MkInChan{buffer = buffer@MkChanBuf{..}, ..}, ..} = do
  icp <- readTVar icpVar
  ocp <- live icp <$> readTVar outPosVar
  if icp <= ocp
    then retry
    else do
      writeTVar outPosVar (advance (W32# len#) ocp)
      readChanBufT buffer (next ocp)

{- |
@`receive` o@ receives a value from an output channel and blocks if none is available.
-}
receive :: OutChan a -> IO a
receive o =
  atomically $
    receiveT o

{- |
@`tryReceive` o@ receives `Just` a value from an output channel and returns `Nothing` if none are available.
-}
tryReceive :: OutChan a -> IO (Maybe a)
tryReceive o =
  atomically $
    (Just <$> receiveT o)
      `orElse` (pure Nothing)

{- |
The channel ring buffer is indexed by a `Word32`,
which we must convert to an `Int` to index the underlying `SmallArray#`.
-}
word32ToInt# :: Word32# -> Int#
word32ToInt# = \i# -> word2Int# (word32ToWord# i#)
{-# INLINE word32ToInt# #-}
