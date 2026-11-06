{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main (main) where

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan.Unagi qualified as UC
import Control.Concurrent.LossyChan qualified as LC
import Criterion.Main (Benchmark, bench, defaultMain, whnfIO)
import Data.Foldable (for_)
import Data.Kind (Type)
import Prelude hiding (snd)
import Data.Word (Word32)

main :: IO ()
main = do
  defaultMain . concat $
    [ [ benchmark lossyChan opts
      , benchmark unagiChan opts
      ]
    | opts <-
        concat
          [ [sNrN n, s1rN n, sNr1 n] | n <- [1 .. 10]
          ]
    ]
 where
  sNrN = \n ->
    MkBenchmarkOptions
      { optionTag = "s" <> show n <> "r" <> show n
      , bufferSize = 128
      , senders = n
      , receivers = n
      , messagesPerSender = 1000
      }
  s1rN = \n ->
    MkBenchmarkOptions
      { optionTag = "s1r" <> show n
      , bufferSize = 128
      , senders = 1
      , receivers = n
      , messagesPerSender = 1000
      }
  sNr1 = \n ->
    MkBenchmarkOptions
      { optionTag = "s" <> show n <> "r1"
      , bufferSize = 128
      , senders = n
      , receivers = 1
      , messagesPerSender = 1000
      }

data BenchmarkOptions
  = MkBenchmarkOptions
  { optionTag :: String
  , bufferSize :: Word32
  , senders :: Int
  , receivers :: Int
  , messagesPerSender :: Int
  }

benchmark :: Chan i o -> BenchmarkOptions -> Benchmark
benchmark MkChan{..} MkBenchmarkOptions{..} =
  bench (tag <> "-" <> optionTag) . whnfIO $ do
    i <- new bufferSize
    for_ [1 .. senders] $ \sender ->
      forkIO $
        for_ [1 .. messagesPerSender] $ \messageId ->
          snd i (sender, messageId)
    for_ [1 .. receivers] $ \_receiver ->
      forkIO $ do
        o <- dup i
        let go (_sender, messageId)
              | messageId == messagesPerSender = pure ()
              | otherwise = go =<< rcv o
        go =<< rcv o

data Chan (i :: Type -> Type) (o :: Type -> Type)
  = MkChan
  { tag :: String
  , new :: forall a. Word32 -> IO (i a)
  , dup :: forall a. i a -> IO (o a)
  , snd :: forall a. i a -> a -> IO ()
  , rcv :: forall a. o a -> IO a
  }

lossyChan :: Chan LC.InChan LC.OutChan
lossyChan =
  MkChan
    { tag = "lossy-chan"
    , new = LC.new
    , dup = LC.out
    , snd = LC.send
    , rcv = LC.receive
    }
{-# INLINE lossyChan #-}

unagiChan :: Chan UC.InChan UC.OutChan
unagiChan =
  MkChan
    { tag = "unagi-chan"
    , new = \_ -> fst <$> UC.newChan
    , dup = UC.dupChan
    , snd = UC.writeChan
    , rcv = UC.readChan
    }
{-# INLINE unagiChan #-}
