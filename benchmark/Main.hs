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
import Prelude hiding (snd)

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
    i <- new
    for_ [1 .. senders] $ \sender ->
      forkIO $
        for_ [1 .. messages] $ \messageId ->
          snd i (sender, messageId)
    for_ [1 .. receivers] $ \_receiver ->
      forkIO $ do
        o <- dup i
        let go (_sender, messageId)
              | messageId == messages = pure ()
              | otherwise = go =<< rcv o
        go =<< rcv o

data Chan (i :: Type -> Type) (o :: Type -> Type)
  = MkChan
  { new :: forall a. IO (i a)
  , dup :: forall a. i a -> IO (o a)
  , snd :: forall a. i a -> a -> IO ()
  , rcv :: forall a. o a -> IO a
  }

lossyChan :: Word32 -> Chan LC.InChan LC.OutChan
lossyChan len =
  MkChan
    { new = LC.new len
    , dup = LC.out
    , snd = LC.send
    , rcv = LC.receive
    }
{-# INLINE lossyChan #-}

unagiChan :: Chan UC.InChan UC.OutChan
unagiChan =
  MkChan
    { new = fst <$> UC.newChan
    , dup = UC.dupChan
    , snd = UC.writeChan
    , rcv = UC.readChan
    }
{-# INLINE unagiChan #-}
