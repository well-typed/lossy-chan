{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Benchmark.Simple (
  SimpleBenchOpts (..),
  runSimpleBench,
) where

import Benchmark.Chan
import Control.Concurrent.Async (async, wait)
import Control.Concurrent.LossyChan (NoInChanError)
import Control.Exception (BlockedIndefinitelyOnSTM, Handler (..), catches)
import Control.Monad (void)
import Data.Foldable (for_)
import Data.Traversable (for)

{- |
Options for a simple benchmark.
-}
data SimpleBenchOpts
  = MkSimpleBenchOpts
  { messages :: !Int
  , senders :: !Int
  , receivers :: !Int
  }

{- |
Run a simple benchmark.
-}
runSimpleBench :: Chan i o -> SimpleBenchOpts -> IO ()
runSimpleBench MkChan{..} =
  \MkSimpleBenchOpts{..} -> do
    (i, o) <- new
    receiverAsyncs <-
      for [1 .. receivers] $ \r -> do
        o' <- if r == receivers then pure o else dup (i, o)
        async $
          handles $
            for_ [1 .. messages] $ \_m -> do
              void (recv o')
    _senderAsync <-
      for [1 .. senders] $ \s -> do
        async $
          for_ [1 .. messages] $ \m -> do
            void (send i (s, m))
    for_ receiverAsyncs wait
{-# INLINE runSimpleBench #-}


handles :: IO () -> IO ()
handles = (`catches` handlers)
 where
  handlers :: [Handler ()]
  handlers =
    [ Handler (\(_e :: BlockedIndefinitelyOnSTM) -> pure ())
    , Handler (\(_e :: NoInChanError) -> pure ())
    ]
{-# INLINE handles #-}
