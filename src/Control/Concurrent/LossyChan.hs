{-# LANGUAGE CPP #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

module Control.Concurrent.LossyChan (
  InChan,
  OutChan,
  new,
  dup,
  out,
  send,
  receive,
  tryReceive,
  NoInChanError (..),
) where

import Control.Exception (Exception (..), mask_)
import Control.Monad (when)
import Data.Functor (void)
import Data.IORef (IORef, mkWeakIORef, newIORef, readIORef)
import Data.Word (Word32, Word8)
import GHC.Conc (STM (..), TVar (..), atomically, newTVar, newTVarIO, orElse, readTVar, retry, throwSTM, writeTVar)
import GHC.Exts (Int#, RealWorld, SmallArray#, State#, TVar#, Word8#, indexSmallArray#, isTrue#, mkWeak#, newSmallArray#, newTVar#, readTVar#, unsafeFreezeSmallArray#, word2Int#, word8ToWord#, writeSmallArray#, writeTVar#, (+#), (==#))
import GHC.IO (IO (..))
import GHC.Word (Word8 (..))

#ifdef LOSSY_CHAN_SMALL_LOOP_COUNTER
-- If the small-loop-counter flag is set, use Word8.
import Data.Word (Word8)

type LoopCounter = Word8
#else
-- Otherwise, use Word8.
type LoopCounter = Word32
#endif

{- |
The position in a channel ring buffer.
-}
data ChanPos = MkChanPos
  { loop :: {-# UNPACK #-} !LoopCounter
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
  , next :: {-# UNPACK #-} !Word8
  {- ^
  This value tracks the next position that should be read (for receivers) or
  written to (for senders).
  -}
  , done :: {-# UNPACK #-} !Word8
  {- ^
  This value tracks whether or not the `InChan` is connected.

  If the value is @0@, the `InChan` is live. Otherwise, it was garbage collected.
  -}
  }
  deriving (Show)

{- |
The initial position in a channel ring buffer.
-}
zero :: ChanPos
zero = MkChanPos{done = 0, loop = 0, next = 0}
{-# INLINE zero #-}

{- |
Check whether two `ChanPos` are at the same position.
-}
eqPos :: ChanPos -> ChanPos -> Bool
eqPos icPos ocPos =
  loop icPos == loop ocPos && next icPos == next ocPos

{- |
Check whether the input channel `ChanPos` is done.
-}
isDone :: ChanPos -> Bool
isDone icPos = done icPos >= 1

{- |
Advance to the next channel position.
-}
advance :: Word8# -> ChanPos -> ChanPos
advance len# MkChanPos{..}
  | next < W8# len# - 1 = MkChanPos{next = next + 1, ..}
  | otherwise = MkChanPos{loop = loop + 1, next = 0, ..}
{-# INLINE advance #-}

{- |
Get the latest of the current output position or the oldest live position.
-}
live :: ChanPos -> ChanPos -> ChanPos
live icPos ocPos
  -- The output channel is on the same loop as the input channel, and
  -- the position is behind (or just caught up with) the input channel.
  | loop ocPos == loop icPos && next ocPos <= next icPos = ocPos
  -- The output channel is one loop behind the input channel, and the
  -- position is aheaad of the input channel.
  | loop ocPos == loop icPos - 1 && next ocPos >= next icPos = ocPos
  -- The output channel is several loops behind the input channel.
  | otherwise = ocPos{loop = loop icPos - 1, next = next icPos}
{-# INLINE live #-}

data Entry a
  = Empty
  | Entry {seen :: {-# UNPACK #-} !Word, val :: !a}

{- |
The channel ring buffer that is used in `InChan`.

The channel ring buffer is an unboxed `SmallArray#` of unboxed `TVar#`. These
types are explicitly unboxed, because we cannot rely on an @UNPACK@ pragma to
unpack the @TVar@ inside of a @SmallArray@.
-}
data ChanBuf a = MkChanBuf
  { len# :: Word8#
  -- ^ The length of the channel ring buffer.
  , arr# :: SmallArray# (TVar# RealWorld (Entry a))
  -- ^ The array that backs the channel ring buffer.
  }

{- |
Create a new channel buffer.
-}
newChanBuf :: Word8 -> IO (ChanBuf a)
newChanBuf (W8# len#) = IO $ \s1# ->
  case word8ToInt# len# of
    ilen# ->
      case newTVar# Empty s1# of
        (# s2#, var0# #) ->
          case newSmallArray# (word8ToInt# len#) var0# s2# of
            (# s3#, mutarr# #) ->
              let go :: Int# -> State# RealWorld -> State# RealWorld
                  go i# = \s4# ->
                    if isTrue# (i# ==# ilen#)
                      then s4#
                      else case newTVar# Empty s4# of
                        (# s5#, vari# #) ->
                          case writeSmallArray# mutarr# i# vari# s5# of
                            s6# -> go (i# +# 1#) s6#
               in case unsafeFreezeSmallArray# mutarr# (go 1# s3#) of
                    (# s7#, arr# #) ->
                      (# s7#, MkChanBuf len# arr# #)

{- |
Read the value from the given position in a `ChanBuf`.
-}
takeChanBuf :: ChanBuf a -> Word -> Word8 -> STM (Maybe a)
takeChanBuf MkChanBuf{..} ocCount (W8# i#) =
  STM $ \s1# ->
    case indexSmallArray# arr# (word8ToInt# i#) of
      (# vari# #) ->
        case readTVar# vari# s1# of
          (# s2#, Empty #) ->
            (# s2#, Nothing #)
          (# s2#, Entry seen val #)
            | seen + 1 >= ocCount ->
                case writeTVar# vari# Empty s2# of
                  s3# -> (# s3#, Just val #)
            | otherwise ->
                case writeTVar# vari# Entry{seen = seen + 1, ..} s2# of
                  s3# -> (# s3#, Just val #)

{- |
Read the value from the given position in a `ChanBuf`.
-}
isEmptyChanBuf :: ChanBuf a -> Word8 -> STM Bool
isEmptyChanBuf MkChanBuf{..} (W8# i#) =
  STM $ \s1# ->
    case indexSmallArray# arr# (word8ToInt# i#) of
      (# vari# #) ->
        case readTVar# vari# s1# of
          (# s2#, Empty #) ->
            (# s2#, True #)
          (# s2#, Entry{} #) ->
            (# s2#, False #)

{- |
Write the given value to a `ChanBuf` at the given position.
-}
writeChanBuf :: ChanBuf a -> Word8 -> Entry a -> STM ()
writeChanBuf MkChanBuf{..} (W8# i#) a =
  STM $ \s1# ->
    case indexSmallArray# arr# (word8ToInt# i#) of
      (# vari# #) ->
        case writeTVar# vari# a s1# of
          s2# ->
            (# s2#, () #)

{- |
Check the `ChanBuf` `Entry` at the given position. If the `seen` count is
greater than or equal to the new (decremented) output channel count, free
the entry.

This function returns `Just` `True` if the entry was freed, `Just` `False`
if the entry was not freed, and `Nothing` if the entry was uninitialised.
-}
tryFreeChanBufAt :: ChanBuf a -> Word -> Word8 -> STM (Maybe Bool)
tryFreeChanBufAt MkChanBuf{..} ocCount (W8# i#) =
  STM $ \s1# ->
    case indexSmallArray# arr# (word8ToInt# i#) of
      (# vari# #) ->
        case readTVar# vari# s1# of
          (# s2#, Empty #) ->
            (# s2#, Nothing #)
          (# s2#, Entry seen _val #)
            | seen >= ocCount ->
                case writeTVar# vari# Empty s2# of
                  s3# -> (# s3#, Just True #)
            | otherwise ->
                (# s2#, Just False #)

{- |
The channel state.
-}
data ChanState a = MkChanState
  { buffer :: {-# UNPACK #-} !(ChanBuf a)
  -- ^ The underlying channel ring buffer.
  , icPosVar :: {-# UNPACK #-} !(TVar ChanPos)
  -- ^ A `TVar` that holds the current input channel position.
  , ocCountVar :: {-# UNPACK #-} !(TVar Word)
  -- ^ A `TVar` that holds the current output channel count.
  }

{- |
An input channel.
-}
data InChan a = MkInChan
  { icStateRef :: {-# UNPACK #-} !(IORef (ChanState a))
  {- ^
  An `IORef` to the underlying channel state.

  This `IORef` is /never/ written and exists only to attach a finalizer.
  -}
  }

{- |
An output channel.

An output channel has a finalizer that frees unreachable values in the ring buffer.
The worst-case complexity of this operation is /O(N)/ where /N/ is the length of the ring buffer.
-}
data OutChan a = MkOutChan
  { ocState :: {-# UNPACK #-} !(ChanState a)
  {- ^
  The underlying channel state.

  This is shared between all input and output channels.
  -}
  , ocPosVar :: {-# UNPACK #-} !(TVar ChanPos)
  -- ^ A variable that holds this output channel's position.
  }

{- |
Free an input channel.
-}
freeInChan :: ChanState a -> IO ()
freeInChan MkChanState{..} =
  atomically $ do
    icPos <- readTVar icPosVar
    writeTVar icPosVar icPos{done = 1}

{- |
Add a finalizer to an `IORef`.

Adapted from @mkWeakTVar in @stm@.
-}
addFinalizerIORef :: IORef a -> IO () -> IO ()
addFinalizerIORef ref finalizer = void (mkWeakIORef ref finalizer)

{- |
Free an output channel.

__Warning:__
This is the only STM operation that walks the channel buffer. Therefore, if
there's a lot of competition, this STM operation may repeatedly retry, which
would prevent the buffer entries from being garbage collected until the point
where the input channel position passed this output channel's position.
-}
freeOutChan :: OutChan a -> IO ()
freeOutChan MkOutChan{ocState = MkChanState{buffer = buffer@MkChanBuf{..}, ..}, ..} =
  atomically $ do
    -- Decrement the output channel counter.
    ocCount <- subtract 1 <$> readTVar ocCountVar
    writeTVar ocCountVar ocCount

    -- Walk the buffer to free values that have been seen by every remaining OutChan.
    icPos <- readTVar icPosVar
    ocPos <- live icPos <$> readTVar ocPosVar
    let go pos
          | icPos `eqPos` pos = pure ()
          | otherwise = do
              mbSuccess <- tryFreeChanBufAt buffer ocCount (next pos)
              case mbSuccess of
                Nothing
                  | loop icPos == 0 && next icPos <= next pos -> go zero
                  | otherwise -> pure ()
                Just True -> go (advance len# pos)
                Just False -> pure ()
    go ocPos

{- |
Add a finalizer to a `TVar`.

Adapted from @mkWeakTVar in @stm@.
-}
addFinalizerTVar :: TVar a -> IO () -> IO ()
addFinalizerTVar t@(TVar t#) (IO finalizer) = IO $ \s1# ->
  case mkWeak# t# t finalizer s1# of
    (# s2, _ #) ->
      (# s2, () #)

{- |
@`new` len@ creates an input channel that is backed by a ring buffer with length @len@.

The ring buffer is represented as a @SmallArray@, so @len@ should be smaller than 128.
-}
new :: Word8 -> IO (InChan a, OutChan a)
new len = do
  -- Create the input channel.
  buffer <- newChanBuf len
  icPosVar <- newTVarIO zero
  ocCountVar <- newTVarIO 1
  let icState = MkChanState{..}
  icStateRef <- newIORef icState
  addFinalizerIORef icStateRef (freeInChan icState)
  let ic = MkInChan{..}

  -- Create the output channel.
  ocPosVar <- newTVarIO zero
  let ocState = icState
  let oc = MkOutChan{..}
  addFinalizerTVar ocPosVar (freeOutChan oc)

  pure (ic, oc)

{- |
@`send` i x@ sends the value @x@ over the input channel @i@.

__Warning:__ If there are no output channels, the value is discarded.
-}
send :: InChan a -> a -> IO ()
send MkInChan{..} a = do
  MkChanState{buffer = buffer@MkChanBuf{..}, ..} <- readIORef icStateRef
  atomically $ do
    ocCount <- readTVar ocCountVar
    when (ocCount > 0) $ do
      icPos@MkChanPos{..} <- readTVar icPosVar
      writeChanBuf buffer next Entry{seen = 0, val = a}
      writeTVar icPosVar (advance len# icPos)

{- |
@`dup` o@ duplicates the output channel @o@.

The new output channel starts receiving messages from the same position as the duplicated channel.
-}
dup :: OutChan a -> IO (OutChan a)
dup oc@MkOutChan{ocState = ocState@MkChanState{..}, ..} =
  -- This operation is masked to ensure that the finalizer is attached.
  mask_ $ do
    -- These two operations are performed atomically. Otherwise, if we're very
    -- unlucky with reorderings, we may get this interleaving between dup and
    -- receive on the same output channel:
    --
    -- ocCount <- readTVar ocCountVar          [receive]
    -- incrOcCountVar ocCountVar               [dup, (1)]
    -- newTVar =<< readTVar ocPosVar           [dup, (2)]
    -- writeTVar ocPosVar (advance ocPos)      [receive]
    -- takeChanBuf buffer ocCount (next ocPos) [receive]
    --
    -- The takeChanBuf operation may then decide, based on an outdated output
    -- channel count, to replace the Entry with Empty, which causes the new
    -- output channel position to point to a forgotten value.
    ocPosVar' <- atomically $ do
      -- (1) Increment the output channel counter.
      writeTVar ocCountVar . (+ 1) =<< readTVar ocCountVar

      -- (2) Duplicate the output channel position variable.
      newTVar =<< readTVar ocPosVar

    addFinalizerTVar ocPosVar' (freeOutChan oc)
    pure MkOutChan{ocPosVar = ocPosVar', ..}

{- |
@`out` i@ creates a new output channel from the input channel @i@.

The new output channel starts receiving messages from the oldest position that still holds a live value.
This is non-determinstic, as it depends on garbage collection.

This worst-case complexity of this operation is /O(N)/ where /N/ is the length of the ring buffer.
-}
out :: InChan a -> IO (OutChan a)
out MkInChan{..} = do
  icState@MkChanState{..} <- readIORef icStateRef
  -- This operation is masked to ensure that the finalizer is attached.
  mask_ $ do
    -- See `dup` for an explanation for the use of `atomically`.
    ocPosVar <- atomically $ do
      -- (1) Increment the output channel counter.
      writeTVar ocCountVar . (+ 1) =<< readTVar ocCountVar

      -- (2) Set the output channel position variable.
      icPos <- readTVar icPosVar
      newTVar icPos{loop = loop icPos - 1}

    -- Create the output channel.
    let ocState = icState
    let oc = MkOutChan{..}
    addFinalizerTVar ocPosVar (freeOutChan oc)

    -- Advance to the next non-Empty position.
    advanceToNonEmpty oc

    -- Return the output channel.
    pure oc

{- |
Find the next non-`Empty` position in the channel buffer.
-}
advanceToNonEmpty :: OutChan a -> IO ()
advanceToNonEmpty MkOutChan{ocState = MkChanState{buffer = buffer@MkChanBuf{..}, ..}, ..} = do
  let go = do
        continue <-
          atomically $ do
            icPos <- readTVar icPosVar
            ocPos <- live icPos <$> readTVar ocPosVar
            if icPos `eqPos` ocPos
              then pure False
              else do
                isEmpty <- isEmptyChanBuf buffer (next ocPos)
                if isEmpty
                  then writeTVar ocPosVar (advance len# ocPos) >> pure True
                  else pure False
        when continue go
  go

--

{- |
The error thrown by `receive` and `tryReceive` when the corresponding `InChan`
has been garbage collected and the channel buffer is empty.
-}
data NoInChanError = ErrNoInChan
  deriving (Show)

instance Exception NoInChanError

{- |
@`receiveT` o@ receives a value from an output channel and
retries if none are available.

__Warning:__
If the corresponding input channel was garbage collected and
the buffer is empty, this function throws an `NoInChanError`.
-}
receiveT :: OutChan a -> STM a
receiveT MkOutChan{ocState = MkChanState{buffer = buffer@MkChanBuf{..}, ..}, ..} = do
  icPos <- readTVar icPosVar
  ocPos <- live icPos <$> readTVar ocPosVar
  receiveT_ icPos ocPos
 where
  receiveT_ icPos ocPos
    | icPos `eqPos` ocPos = do
        -- If the input channel is done...
        if isDone icPos
          -- ...throw an NoInChanError.
          then throwSTM ErrNoInChan
          -- Otherwise, we retry
          else retry
    | otherwise = do
        ocCount <- readTVar ocCountVar
        mbVal <- takeChanBuf buffer ocCount (next ocPos)
        case mbVal of
          Nothing -> retry
          Just val -> writeTVar ocPosVar (advance len# ocPos) >> pure val

{- |
@`receive` o@ receives a value from an output channel and
blocks if none is available.

__Warning:__
If the corresponding input channel was garbage collected and
the buffer is empty, this function throws an `NoInChanError`.
-}
receive :: OutChan a -> IO a
receive o =
  atomically (receiveT o)

{- |
@`tryReceive` o@ receives `Just` a value from an output channel
and returns `Nothing` if none are available.

__Warning:__
If the corresponding input channel was garbage collected and
the buffer is empty, this function throws an `NoInChanError`.
-}
tryReceive :: OutChan a -> IO (Maybe a)
tryReceive o = do
  atomically ((Just <$> receiveT o) `orElse` (pure Nothing))

{- |
The channel ring buffer is indexed by a `Word8`,
which we must convert to an `Int` to index the underlying `SmallArray#`.
-}
word8ToInt# :: Word8# -> Int#
word8ToInt# = \i# -> word2Int# (word8ToWord# i#)
{-# INLINE word8ToInt# #-}
