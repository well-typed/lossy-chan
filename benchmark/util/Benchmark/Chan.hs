{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Benchmark.Chan (
  Chan (..),
  lossyChan,
  baseChan,
  stmTChan,
  unagiChan,
) where

import Control.Concurrent.Chan qualified as Base
import Control.Concurrent.Chan.Unagi qualified as UC
import Control.Concurrent.LossyChan qualified as LC
import Control.Concurrent.STM.TChan qualified as STM
import Control.Monad.STM (atomically)
import Data.Kind (Type)
import Data.Word (Word32)

{- |
An abstraction over channel libraries.
-}
data Chan (i :: Type -> Type) (o :: Type -> Type)
  = MkChan
  { new :: forall a. IO (i a, o a)
  , dup :: forall a. (i a, o a) -> IO (o a)
  , send :: forall a. i a -> a -> IO ()
  , recv :: forall a. o a -> IO a
  }

{- |
The `Chan` instance for @lossy-chan@.
-}
lossyChan :: Word32 -> Chan LC.InChan LC.OutChan
lossyChan len =
  MkChan
    { new = LC.new len
    , dup = LC.dup . snd
    , send = LC.send
    , recv = LC.receive
    }
{-# INLINE lossyChan #-}

{- |
The `Chan` instance for "Control.Concurrent.Chan" from @base@.
-}
baseChan :: Chan Base.Chan Base.Chan
baseChan =
  MkChan
    { new = do i <- Base.newChan; pure (i, i)
    , dup = Base.dupChan . fst
    , send = Base.writeChan
    , recv = Base.readChan
    }
{-# INLINE baseChan #-}

{- |
The `Chan` instance for "Control.Concurrent.STM.TChan" from @stm@.
-}
stmTChan :: Chan STM.TChan STM.TChan
stmTChan =
  MkChan
    { new =
        atomically $ do
          i <- STM.newBroadcastTChan
          o <- STM.dupTChan i
          pure (i, o)
    , dup =
        \(i, _) ->
          atomically $
            STM.dupTChan i
    , send =
        \i x ->
          atomically $
            STM.writeTChan i x
    , recv =
        \o ->
          atomically $
            STM.readTChan o
    }
{-# INLINE stmTChan #-}

{- |
The `Chan` instance for "Control.Concurrent.Chan.Unagi" from @unagi-chan@.
-}
unagiChan :: Chan UC.InChan UC.OutChan
unagiChan =
  MkChan
    { new = UC.newChan
    , dup = UC.dupChan . fst
    , send = UC.writeChan
    , recv = UC.readChan
    }
{-# INLINE unagiChan #-}
