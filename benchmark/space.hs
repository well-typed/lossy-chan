{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main (main) where

import Benchmark.Chan
import Benchmark.Simple
import Data.Foldable (for_)
import Weigh (Column (..), Format (..), Weigh, io, mainWith, setColumns, setFormat, wgroup)

main :: IO ()
main = do
  mainWith $ do
    setColumns [Case, Allocated, Max]
    setFormat Markdown
    wgroup "base" $
      for_ [1 .. 4] $ \s ->
        weighChanWith baseChan s s
    wgroup "stm" $
      for_ [1 .. 4] $ \s ->
        weighChanWith stmTChan s s
    wgroup "unagi-chan" $
      for_ [1 .. 4] $ \s ->
        weighChanWith unagiChan s s
    for_ [2, 4, 8, 16, 32, 64, 128] $ \n ->
      wgroup ("lossy-chan-" <> show n) $
        for_ [1 .. 4] $ \s ->
          weighChanWith (lossyChan n) s s

weighChanWith :: Chan i o -> Int -> Int -> Weigh ()
weighChanWith chan senders receivers = do
  let label = show senders <> " senders, " <> show receivers <> " receivers"
  let action = runSimpleBench chan
  let opts = MkSimpleBenchOpts{messages = 100000, ..}
  io label action opts
