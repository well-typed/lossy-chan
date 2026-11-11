{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main (main) where

import Benchmark.Chan
import Benchmark.Simple
import Criterion.Main (Benchmark, bench, bgroup, defaultMain, whnfIO)

main :: IO ()
main = do
  defaultMain . concat $
    [ [ bgroup
          "base"
          [ benchChanWith baseChan s r
          | s <- [1 .. 4]
          , let r = s
          ]
      , bgroup
          "stm"
          [ benchChanWith stmTChan s r
          | s <- [1 .. 4]
          , let r = s
          ]
      , bgroup
          "unagi-chan"
          [ benchChanWith unagiChan s r
          | s <- [1 .. 4]
          , let r = s
          ]
      ]
        <> [ bgroup ("lossy-chan-" <> show n) $
               [ benchChanWith (lossyChan n) s r
               | s <- [1 .. 4]
               , let r = s
               ]
           | n <- [2, 8, 32, 128]
           ]
    ]

benchChanWith :: Chan i o -> Int -> Int -> Benchmark
benchChanWith chan senders receivers =
  bench (show senders <> " senders, " <> show receivers <> " receivers") $
    whnfIO . runSimpleBench chan $
      MkSimpleBenchOpts{messages = 10000, ..}
