{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
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
import Data.Word (Word8)
import GHC.Conc (STM (..), atomically, retry)
import GHC.Exts (Int (..), Int#, RealWorld, SmallArray#, TVar#, indexSmallArray#, isTrue#, newSmallArray#, newTVar#, readTVar#, unsafeFreezeSmallArray#, writeSmallArray#, writeTVar#, (+#), (==#))
import GHC.IO (IO (..))

{- |
The position in a channel ring buffer.
-}
data ChanPos = MkChanPos
  { full :: {-# UNPACK #-} !Bool
  -- ^ Whether the ring buffer is full.
  , loop :: {-# UNPACK #-} !Word8
  {- ^ The current loop that is being written. The `maxBound` of the type of
  the loop counter determines the resilience of the readers. If a reader
  stops at loop @N@ and the writer manages to overflow all the way back
  to loop @N - 1@ or @N@, then the reader may skip some extra messages.
  -}
  , next :: {-# UNPACK #-} !Int
  -- ^ The next position that should be read or written to.
  }
  deriving (Eq, Ord, Show)

{- |
The initial position for an empty channel ring buffer.
-}
zero :: ChanPos
zero = MkChanPos{full = False, loop = 0, next = 0}
{-# INLINE zero #-}

{- |
Get the oldest live position given the current input position.
-}
minLive :: ChanPos -> ChanPos
minLive MkChanPos{..}
  | not full = MkChanPos{next = 0, ..}
  | otherwise = MkChanPos{loop = loop - 1, ..}
{-# INLINE minLive #-}

{- |
Advance to the next position.
-}
advance :: Int -> ChanPos -> ChanPos
advance size MkChanPos{..}
  | next < size - 1 = MkChanPos{next = next + 1, ..}
  | otherwise = MkChanPos{full = True, loop = loop + 1, next = 0}
{-# INLINE advance #-}

{- |
Get the first live position.
-}
live :: ChanPos -> ChanPos -> ChanPos
live icp ocp = max (minLive icp) ocp
{-# INLINE live #-}

data ChanBuf a = MkChanBuf
  { len# :: Int#
  , arr# :: SmallArray# (TVar# RealWorld a)
  }

uninitialised :: a
uninitialised = error "Control.Concurrent.LossyChan: uninitialised memory"

newChanBuf :: Int -> IO (ChanBuf a)
newChanBuf (I# len#) = IO $ \s1# ->
  case newTVar# uninitialised s1# of
    (# s2#, var0# #) ->
      case newSmallArray# len# var0# s2# of
        (# s3#, mutarr# #) ->
          let go i# = \s4# ->
                if isTrue# (i# ==# len#)
                  then s4#
                  else case newTVar# uninitialised s4# of
                    (# s5#, vari# #) ->
                      case writeSmallArray# mutarr# i# vari# s5# of
                        s6# -> go (i# +# 1#) s6#
           in case unsafeFreezeSmallArray# mutarr# (go 1# s3#) of
                (# s7#, arr# #) ->
                  (# s7#, MkChanBuf len# arr# #)

readChanBuf :: ChanBuf a -> Int -> STM a
readChanBuf MkChanBuf{..} (I# i#) =
  STM $ \s1# ->
    case indexSmallArray# arr# i# of
      (# vari# #) ->
        readTVar# vari# s1#

writeChanBuf :: ChanBuf a -> Int -> a -> STM ()
writeChanBuf MkChanBuf{..} (I# i#) a =
  STM $ \s1# ->
    case indexSmallArray# arr# i# of
      (# vari# #) ->
        case writeTVar# vari# a s1# of
          s2# ->
            (# s2#, () #)

data InChan a = MkInChan
  { buffer :: {-# UNPACK #-} !(ChanBuf a)
  , icpVar :: {-# UNPACK #-} !(TVar ChanPos)
  }

new :: Int -> IO (InChan a)
new len = do
  when (len <= 0) $ error "Control.Concurrent.LossyChan.new: invalid length"
  buffer <- newChanBuf len
  icpVar <- newTVarIO zero
  pure MkInChan{..}

send :: InChan a -> a -> IO ()
send MkInChan{buffer = buffer@MkChanBuf{..}, ..} a =
  atomically $ do
    icp@MkChanPos{..} <- readTVar icpVar
    writeChanBuf buffer next a
    writeTVar icpVar (advance (I# len#) icp)

data OutChan a = MkOutChan
  { inChan :: {-# UNPACK #-} !(InChan a)
  , outPosVar :: {-# UNPACK #-} !(TVar ChanPos)
  }

out :: InChan a -> IO (OutChan a)
out inChan@(MkInChan{..}) = do
  ip <- readTVarIO icpVar
  outPosVar <- newTVarIO (minLive ip)
  pure MkOutChan{..}

livePos :: OutChan a -> STM (Maybe ChanPos)
livePos MkOutChan{inChan = MkInChan{..}, ..} = do
  icp <- readTVar icpVar
  ocp <- live icp <$> readTVar outPosVar
  pure $ if icp <= ocp then Nothing else Just ocp

tryReceive :: OutChan a -> IO (Maybe a)
tryReceive outChan@MkOutChan{inChan = MkInChan{buffer = buffer@MkChanBuf{..}}, ..} = do
  atomically $
    livePos outChan >>= \case
      Nothing -> pure Nothing
      Just ocp -> do
        writeTVar outPosVar (advance (I# len#) ocp)
        Just <$> readChanBuf buffer (next ocp)

receive :: OutChan a -> IO a
receive outChan@MkOutChan{inChan = MkInChan{buffer = buffer@MkChanBuf{..}}, ..} = do
  atomically $
    livePos outChan >>= \case
      Nothing -> retry
      Just ocp -> do
        writeTVar outPosVar (advance (I# len#) ocp)
        readChanBuf buffer (next ocp)
