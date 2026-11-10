{-# LANGUAGE CPP #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

module Control.Concurrent.LossyChan (
  InChan,
  OutChan,
  new,
  dup,
  send,
  receive,
  tryReceive,
  NoInChanError (..),
) where

import Control.Exception (Exception (..), mask)
import GHC.Conc (STM (..), TVar (..), atomically, newTVar, newTVarIO, orElse, readTVar, retry, throwSTM, writeTVar)
import GHC.Exts (Int#, MutVar#, RealWorld, SmallArray#, State#, TVar#, Weak#, Word32#, deRefWeak#, indexSmallArray#, isTrue#, mkWeak#, newMutVar#, newSmallArray#, newTVar#, readTVar#, unsafeFreezeSmallArray#, word2Int#, word32ToWord#, writeSmallArray#, writeTVar#, (+#), (==#))
import GHC.IO (IO (..))
import GHC.Word (Word32 (..))

#ifdef LOSSY_CHAN_SMALL_LOOP_COUNTER
-- If the small-loop-counter flag is set, use Word8.
import Data.Word (Word8)

type LoopCounter = Word8
#else
-- Otherwise, use Word32.
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
  , next :: {-# UNPACK #-} !Word32
  {- ^
  This value tracks the next position that should be read (for receivers) or
  written to (for senders).
  -}
  }
  deriving (Eq, Show)

{- |
The initial position in a channel ring buffer.
-}
zero :: ChanPos
zero = MkChanPos{loop = 0, next = 0}
{-# INLINE zero #-}

{- |
Advance to the next channel position.
-}
advance :: Word32# -> ChanPos -> ChanPos
advance len# MkChanPos{..}
  | next < W32# len# - 1 = MkChanPos{next = next + 1, ..}
  | otherwise = MkChanPos{loop = loop + 1, next = 0}
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
  | otherwise = icPos{loop = loop icPos - 1}
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
  { len# :: Word32#
  -- ^ The length of the channel ring buffer.
  , arr# :: SmallArray# (TVar# RealWorld (Entry a))
  -- ^ The array that backs the channel ring buffer.
  }

{- |
Create a new channel buffer.
-}
newChanBuf :: Word32 -> IO (ChanBuf a)
newChanBuf (W32# len#) = IO $ \s1# ->
  case word32ToInt# len# of
    ilen# ->
      case newTVar# Empty s1# of
        (# s2#, var0# #) ->
          case newSmallArray# (word32ToInt# len#) var0# s2# of
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
takeChanBuf :: ChanBuf a -> Word -> Word32 -> STM (Maybe a)
takeChanBuf MkChanBuf{..} ocCount (W32# i#) =
  STM $ \s1# ->
    case indexSmallArray# arr# (word32ToInt# i#) of
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
Write the given value to a `ChanBuf` at the given position.
-}
writeChanBuf :: ChanBuf a -> Word32 -> Entry a -> STM ()
writeChanBuf MkChanBuf{..} (W32# i#) a =
  STM $ \s1# ->
    case indexSmallArray# arr# (word32ToInt# i#) of
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
tryFreeChanBufAt :: ChanBuf a -> Word -> Word32 -> STM (Maybe Bool)
tryFreeChanBufAt MkChanBuf{..} ocCount (W32# i#) =
  STM $ \s1# ->
    case indexSmallArray# arr# (word32ToInt# i#) of
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
  { icState :: {-# UNPACK #-} !(ChanState a)
  {- ^
  The underlying channel state.

  This is shared between all input and output channels.
  -}
  , icLive :: {-# UNPACK #-} !Live
  {- ^
  When this value is dropped, the corresponding `IsLive`
  values in the output channels can be used to detect it.
  -}
  }

{- |
An output channel.
-}
data OutChan a = MkOutChan
  { ocState :: !(ChanState a)
  {- ^
  The underlying channel state.

  This is shared between all input and output channels.
  -}
  , ocIsLive :: !IsLive
  -- ^ A value used test whether the corresponding input channel is live.
  , ocPosVar :: !(TVar ChanPos)
  -- ^ A variable that holds this output channel's position.
  }

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
    let go cp
          | icPos == cp = pure ()
          | otherwise = do
              mbSuccess <- tryFreeChanBufAt buffer ocCount (next cp)
              case mbSuccess of
                Nothing
                  | loop icPos == 0 && next icPos <= next cp -> go zero
                  | otherwise -> pure ()
                Just True -> go (advance len# cp)
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
new :: Word32 -> IO (InChan a, OutChan a)
new len = do
  -- Create the input channel.
  buffer <- newChanBuf len
  icPosVar <- newTVarIO zero
  ocCountVar <- newTVarIO 1
  icLive <- mkLive
  let icState = MkChanState{..}
  let ic = MkInChan{..}

  -- Create the output channel.
  ocPosVar <- newTVarIO zero
  let ocState = icState
  ocIsLive <- mkIsLive icLive
  let oc = MkOutChan{..}
  addFinalizerTVar ocPosVar (freeOutChan oc)

  pure (ic, oc)

{- |
@`send` i x@ sends the value @x@ over the input channel @i@.
-}
send :: InChan a -> a -> IO ()
send MkInChan{icState = MkChanState{buffer = buffer@MkChanBuf{..}, ..}} a =
  atomically $ do
    icPos@MkChanPos{..} <- readTVar icPosVar
    writeChanBuf buffer next Entry{seen = 0, val = a}
    writeTVar icPosVar (advance len# icPos)

{- |
@`dup` o@ duplicates the output channel @o@.
-}
dup :: OutChan a -> IO (OutChan a)
dup oc@MkOutChan{ocState = ocState@MkChanState{..}, ..} =
  -- This operation is masked to ensure that the finalizer is attached.
  mask $ \_restore -> do
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
    | icPos == ocPos = do
        -- If the input channel is still live...
        icLive <- isLive ocIsLive
        if icLive
          -- ...we retry.
          then retry
          -- Otherwise, throw an NoInChanError.
          else throwSTM ErrNoInChan
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
This type is used to test for liveness.

The target should hold on to a value of this type, the tester can use
the corresponding `IsLive` to test whether the target is still live.
-}
data Live = MkLive (MutVar# RealWorld ())

{- |
Create a value of type `Live`.
-}
mkLive :: IO Live
mkLive =
  IO $ \s1# ->
    case newMutVar# () s1# of
      (# s2#, mv# #) ->
        (# s2#, MkLive mv# #)

{- |
This type is used to test for liveness.

See `Live`.
-}
data IsLive = MkIsLive (Weak# (MutVar# RealWorld ()))

{- |
Create an `IsLive` value that tests whether the given `Live` value is live.
-}
mkIsLive :: Live -> IO IsLive
mkIsLive (MkLive mv#) =
  IO $ \s1# ->
    case mkWeak# mv# mv# (\s3# -> (# s3#, () #)) s1# of
      (# s2#, wmv# #) ->
        (# s2#, MkIsLive wmv# #)

{- |
Test whether the `Live` value that corresponds to the given `IsLive`
value is still live.
-}
isLive :: IsLive -> STM Bool
isLive (MkIsLive wmv#) =
  STM $ \s1# ->
    case deRefWeak# wmv# s1# of
      (# s2#, flag#, _ #) ->
        case flag# of
          0# -> (# s2#, False #)
          _ -> (# s2#, True #)

{- |
The channel ring buffer is indexed by a `Word32`,
which we must convert to an `Int` to index the underlying `SmallArray#`.
-}
word32ToInt# :: Word32# -> Int#
word32ToInt# = \i# -> word2Int# (word32ToWord# i#)
{-# INLINE word32ToInt# #-}
