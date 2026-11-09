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
) where

import Control.Exception (mask)
import GHC.Conc (STM (..), TVar (..), atomically, newTVar, newTVarIO, orElse, readTVar, retry, writeTVar)
import GHC.Exts (Int#, RealWorld, SmallArray#, State#, TVar#, Word32#, indexSmallArray#, isTrue#, mkWeak#, newSmallArray#, newTVar#, readTVar#, unsafeFreezeSmallArray#, word2Int#, word32ToWord#, writeSmallArray#, writeTVar#, (+#), (==#))
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
live icp ocp
  | loop icp == loop ocp || next icp <= next ocp = ocp
  | otherwise = icp{loop = loop icp - 1}
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
takeChanBuf MkChanBuf{..} occ (W32# i#) =
  STM $ \s1# ->
    case indexSmallArray# arr# (word32ToInt# i#) of
      (# vari# #) ->
        case readTVar# vari# s1# of
          (# s2#, Empty #) ->
            (# s2#, Nothing #)
          (# s2#, Entry seen val #)
            | seen + 1 >= occ ->
                case writeTVar# vari# Empty s2# of
                  s3# -> (# s3#, Just val #)
            | otherwise ->
                case writeTVar# vari# Entry{seen = seen + 1, ..} s2# of
                  s3# -> (# s3#, Just val #)
{-# INLINE takeChanBuf #-}

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
{-# INLINE writeChanBuf #-}

{- |
The channel state.
-}
data ChanState a = MkChanState
  { buffer :: {-# UNPACK #-} !(ChanBuf a)
  -- ^ The underlying channel ring buffer.
  , icpVar :: {-# UNPACK #-} !(TVar ChanPos)
  -- ^ A `TVar` that holds the current input channel position.
  , occVar :: {-# UNPACK #-} !(TVar Word)
  -- ^ A `TVar` that holds the current output channel count.
  }

{- |
An input channel.
-}
data InChan a = MkInChan
  { istate :: {-# UNPACK #-} !(ChanState a)
  }

{- |
An output channel.
-}
data OutChan a = MkOutChan
  { ostate :: !(ChanState a)
  , ocpVar :: !(TVar ChanPos)
  }

{- |
@`new` len@ creates an input channel that is backed by a ring buffer with length @len@.

The ring buffer is represented as a @SmallArray@, so @len@ should be smaller than 128.
-}
new :: Word32 -> IO (InChan a, OutChan a)
new len = do
  -- Create the input channel.
  buffer <- newChanBuf len
  icpVar <- newTVarIO zero
  occVar <- newTVarIO 1
  let state = MkChanState{..}
  let inChan = MkInChan{istate = state}

  -- This section is masked to ensure that the finalizer is attached.
  outChan <- mask $ \_restore -> do
    -- Create the output channel.
    ocpVar <- newTVarIO zero
    let outChan = MkOutChan{ostate = state, ..}
    addFinalizerTVar ocpVar (freeOutChan outChan)
    pure outChan

  pure (inChan, outChan)

{- |
@`send` i x@ sends the value @x@ over the input channel @i@.
-}
send :: InChan a -> a -> IO ()
send MkInChan{istate = MkChanState{buffer = buffer@MkChanBuf{..}, ..}} a =
  atomically $ do
    icp@MkChanPos{..} <- readTVar icpVar
    writeChanBuf buffer next Entry{seen = 0, val = a}
    writeTVar icpVar (advance len# icp)

{- |
Free an output channel.

__Warning:__
This is the only STM operation that walks the channel buffer. Therefore, if
there's a lot of competition, this STM operation may repeatedly retry, which
would prevent the buffer entries from being garbage collected until the point
where the input channel position passed this output channel's position.
-}
freeOutChan :: OutChan a -> IO ()
freeOutChan MkOutChan{ostate = MkChanState{buffer = buffer@MkChanBuf{..}, ..}, ..} =
  atomically $ do
    -- Decrement the output channel counter.
    occ <- subtract 1 <$> readTVar occVar
    writeTVar occVar occ

    -- Walk the buffer to free values that have been seen by every remaining OutChan.
    icp <- readTVar icpVar
    ocp <- live icp <$> readTVar ocpVar
    let go cp
          | icp == cp = pure ()
          | otherwise = do
              mbSuccess <- tryFreeChanBufAt buffer occ (next cp)
              case mbSuccess of
                Nothing
                  | loop icp == 0 && next icp <= next cp -> go zero
                  | otherwise -> pure ()
                Just True -> go (advance len# cp)
                Just False -> pure ()
    go ocp

{- |
Check the `ChanBuf` `Entry` at the given position. If the `seen` count is
greater than or equal to the new (decremented) output channel count, free
the entry.

This function returns `Just` `True` if the entry was freed, `Just` `False`
if the entry was not freed, and `Nothing` if the entry was uninitialised.
-}
tryFreeChanBufAt :: ChanBuf a -> Word -> Word32 -> STM (Maybe Bool)
tryFreeChanBufAt MkChanBuf{..} occ (W32# i#) =
  STM $ \s1# ->
    case indexSmallArray# arr# (word32ToInt# i#) of
      (# vari# #) ->
        case readTVar# vari# s1# of
          (# s2#, Empty #) ->
            (# s2#, Nothing #)
          (# s2#, Entry seen _val #)
            | seen >= occ ->
                case writeTVar# vari# Empty s2# of
                  s3# -> (# s3#, Just True #)
            | otherwise ->
                (# s2#, Just False #)

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
@`dup` o@ duplicates the output channel @o@.
-}
dup :: OutChan a -> IO (OutChan a)
dup outChan@MkOutChan{ostate = ostate@MkChanState{..}, ..} =
  -- This operation is masked to ensure that the finalizer is attached.
  mask $ \_restore -> do
    -- These two operations are performed atomically. Otherwise, if we're very
    -- unlucky with reorderings, we may get this interleaving between dup and
    -- receive on the same output channel:
    --
    -- occ <- readTVar occVar            [receive]
    -- incrementOccVar occVar            [dup, (1)]
    -- newTVar =<< readTVar ocpVar       [dup, (2)]
    -- writeTVar ocpVar (advance ocp)    [receive]
    -- takeChanBuf buffer occ (next ocp) [receive]
    --
    -- The takeChanBuf operation may then decide, based on an outdated output
    -- channel count, to replace the Entry with Empty, which causes the new
    -- output channel position to point to a forgotten value.
    ocpVar' <- atomically $ do
      -- (1) Increment the output channel counter.
      writeTVar occVar . (+ 1) =<< readTVar occVar

      -- (2) Duplicate the output channel position variable.
      newTVar =<< readTVar ocpVar

    addFinalizerTVar ocpVar' (freeOutChan outChan)
    pure MkOutChan{ocpVar = ocpVar', ..}

{- |
@`receiveT` o@ receives a value from an output channel and retries if none are available.
-}
receiveT :: OutChan a -> STM a
receiveT MkOutChan{ostate = MkChanState{buffer = buffer@MkChanBuf{..}, ..}, ..} = do
  icp <- readTVar icpVar
  ocp <- live icp <$> readTVar ocpVar
  receiveT_ icp ocp
 where
  -- During the first loop, the loop counter is zero and the segment
  -- between channel positions icp and zero is uninitialised. During
  -- subsequent loops, that segment is live. However, when the loop
  -- counter wraps around, we will encounter the situation where the
  -- loop counter is zero, but the segment between channel positions
  -- icp and zero is live. To handle this situation, if loop is zero,
  -- we perform one read at icp. If this memory is uninitialised, then
  -- we skip to zero. This incurs one extra read on the first loop and
  -- every maxBound loops after that.
  receiveT_ icp ocp
    | icp == ocp = retry
    | otherwise = do
        occ <- readTVar occVar
        mbVal <- takeChanBuf buffer occ (next ocp)
        case mbVal of
          Nothing
            | loop icp == 0 && next icp <= next ocp -> receiveT_ icp zero
            | otherwise -> uninitialised ocp
          Just val -> writeTVar ocpVar (advance len# ocp) >> pure val
   where
    uninitialised :: a
    uninitialised =
      error . concat $
        [ "Control.Concurrent.LossyChan: uninitialised memory"
        , " at output position " <> show ocp
        , " (with input position " <> show icp <> ")"
        ]

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
