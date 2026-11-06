{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main (main) where

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan.Unagi qualified as UC
import Control.Concurrent.LossyChan qualified as LC
import Criterion.Main (Benchmark, Benchmarkable, bench, bgroup, defaultMain, whnfIO)
import Data.Foldable (for_)
import Data.Kind (Type)
import Data.Word (Word32)

main :: IO ()
main = do
  defaultMain . concat $
    [ bgroup
        "unagi-chan"
        [ benchChanWith unagiChan s r
        | s <- [1 .. 4]
        , let r = s
        ]
        : [ bgroup ("lossy-chan-" <> show n) $
              [ benchChanWith (lossyChan n) s r
              | s <- [1 .. 4]
              , let r = s
              ]
          | n <- [2, 8, 32, 128]
          ]
    ]

data BenchOpts
  = MkBenchOpts
  { messages :: Int
  , senders :: Int
  , receivers :: Int
  }

benchChanWith :: Chan i o -> Int -> Int -> Benchmark
benchChanWith c s r =
  bench (show s <> " senders, " <> show r <> " receivers") . runChanWith c $
    MkBenchOpts
      { messages = 10000
      , senders = s
      , receivers = r
      }

runChanWith :: Chan i o -> BenchOpts -> Benchmarkable
runChanWith MkChan{..} MkBenchOpts{..} =
  whnfIO $ do
    (i, o) <- new
    for_ [1 .. senders] $ \sender ->
      forkIO $
        for_ [1 .. messages] $ \messageId ->
          send i (sender, messageId)
    for_ [1 .. receivers] $ \_receiver -> do
      o' <- dup (i, o)
      forkIO $ do
        let go (_sender, messageId)
              | messageId == messages = pure ()
              | otherwise = go =<< recv o'
        go =<< recv o'

data Chan (i :: Type -> Type) (o :: Type -> Type)
  = MkChan
  { new :: forall a. IO (i a, o a)
  , dup :: forall a. (i a, o a) -> IO (o a)
  , send :: forall a. i a -> a -> IO ()
  , recv :: forall a. o a -> IO a
  }

lossyChan :: Word32 -> Chan LC.InChan LC.OutChan
lossyChan len =
  MkChan
    { new = LC.new len
    , dup = LC.dup . snd
    , send = LC.send
    , recv = LC.receive
    }
{-# INLINE lossyChan #-}

unagiChan :: Chan UC.InChan UC.OutChan
unagiChan =
  MkChan
    { new = UC.newChan
    , dup = UC.dupChan . fst
    , send = UC.writeChan
    , recv = UC.readChan
    }
{-# INLINE unagiChan #-}
