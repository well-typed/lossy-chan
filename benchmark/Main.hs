module Main (main) where

import Control.Concurrent (forkIO)
import Control.Concurrent.LossyChan qualified as LC
import Criterion.Main (Benchmark, bench, defaultMain, whnfIO)
import Data.Foldable (for_)

main :: IO ()
main =
  defaultMain
    [ benchmark
        BenchmarkOptions
          { bufferSize = 128
          , senders = 1
          , receivers = 1
          , messagesPerSender = 1000
          }
    ]

data BenchmarkOptions = BenchmarkOptions
  { bufferSize :: Int
  , senders :: Int
  , receivers :: Int
  , messagesPerSender :: Int
  }

benchmark :: BenchmarkOptions -> Benchmark
benchmark BenchmarkOptions{..} =
  bench ("lossy-chan") . whnfIO $ do
    i <- LC.new bufferSize
    for_ [1 .. senders] $ \sender ->
      forkIO $
        for_ [1 .. messagesPerSender] $ \messageId ->
          LC.send i (sender, messageId)
    for_ [1 .. receivers] $ \_receiver ->
      forkIO $ do
        o <- LC.out i
        let go (_sender, messageId)
              | messageId == messagesPerSender = pure ()
              | otherwise = go =<< LC.receive o
        go =<< LC.receive o
