module Control.Concurrent.LossyChan (
  InChan,
  OutChan,
  new,
  out,
  send,
  receive,
) where

import Control.Concurrent.STM.TVar (TVar, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Monad.STM (atomically, retry)
import Data.Foldable (for_)
import Data.Primitive (newSmallArray)
import Data.Primitive.SmallArray (SmallArray, indexSmallArray, unsafeFreezeSmallArray, writeSmallArray)
import Data.Word (Word8)

{- |
The position in a channel ring buffer.
-}
data ChanPos = MkChanPos
  { full :: {-# UNPACK #-} !Bool
    -- ^ Whether the ring buffer is full.
  , loop :: {-# UNPACK #-} !Word8
    -- ^ The current loop that is being written. The `maxBound` of the type of
    --   the loop counter determines the resilience of the readers. If a reader
    --   stops at loop @N@ and the writer manages to overflow all the way back
    --   to loop @N - 1@ or @N@, then the reader may skip some extra messages.
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

data InChan a = MkInChan
  { size :: !Int
  , buffer :: !(SmallArray (TVar a))
  , icpVar :: !(TVar ChanPos)
  }

uninitialised :: a
uninitialised = error "Control.Concurrent.LossyChan: uninitialised memory"

new :: Int -> IO (InChan a)
new size = do
  mutableBuffer <- newSmallArray size uninitialised
  for_ [0 .. size - 1] $ \i ->
    writeSmallArray mutableBuffer i =<< newTVarIO uninitialised
  buffer <- unsafeFreezeSmallArray mutableBuffer
  icpVar <- newTVarIO zero
  pure MkInChan{..}

send :: InChan a -> a -> IO ()
send MkInChan{..} a =
  atomically $ do
    icp@MkChanPos{..} <- readTVar icpVar
    writeTVar (indexSmallArray buffer next) a
    writeTVar icpVar (advance size icp)

data OutChan a = MkOutChan
  { inChan :: {-# UNPACK #-} !(InChan a)
  , outPosVar :: !(TVar ChanPos)
  }

out :: InChan a -> IO (OutChan a)
out inChan@(MkInChan{..}) = do
  ip <- readTVarIO icpVar
  outPosVar <- newTVarIO (minLive ip)
  pure MkOutChan{..}

receive :: OutChan a -> IO a
receive MkOutChan{inChan = MkInChan{..}, ..} = do
  atomically $ do
    icp <- readTVar icpVar
    ocp <- live icp <$> readTVar outPosVar
    if ocp == zero && icp == zero
      then retry
      else do
        writeTVar outPosVar (advance size ocp)
        readTVar (indexSmallArray buffer (next ocp))
