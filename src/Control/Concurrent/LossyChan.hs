module Control.Concurrent.LossyChan (
  InChan,
  OutChan,
  new,
  out,
  send,
  receive,
) where

import Control.Concurrent.STM.TArray
import Control.Concurrent.STM.TVar (TVar, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Monad.STM (atomically)
import Data.Array.MArray (MArray (..), newArray_, readArray, writeArray)

data ChanPos = MkChanPos
  { full :: {-# UNPACK #-} !Bool
  , loop :: {-# UNPACK #-} !Word
  , next :: {-# UNPACK #-} !Int
  }
  deriving (Eq, Ord, Show)

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
advance :: (Int, Int) -> ChanPos -> ChanPos
advance (l, u) MkChanPos{..}
  | next >= u = MkChanPos{full = True, loop = loop + 1, next = l}
  | otherwise = MkChanPos{next = next + 1, ..}
{-# INLINE advance #-}

{- |
Get the first live position.
-}
live :: ChanPos -> ChanPos -> ChanPos
live icp ocp = max (minLive icp) ocp
{-# INLINE live #-}

data InChan a = MkInChan
  { buffer :: !(TArray Int a)
  , icpVar :: !(TVar ChanPos)
  }

new :: Int -> IO (InChan a)
new n = MkInChan <$> newArray_ (0, n) <*> newTVarIO zero

send :: InChan a -> a -> IO ()
send MkInChan{..} a =
  atomically $ do
    lxu <- getBounds buffer
    icp@MkChanPos{..} <- readTVar icpVar
    writeArray buffer next a
    writeTVar icpVar (advance lxu icp)

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
    lxu <- getBounds buffer
    writeTVar outPosVar (advance lxu ocp)
    readArray buffer (next ocp)
