module Main (main) where

import Control.Concurrent.LossyChan (InChan)
import Control.Concurrent.LossyChan qualified as LC
import Data.Foldable (for_)
import Data.Word (Word16, Word32)
import Test.Tasty (defaultMain, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))
import System.Timeout (timeout)

main :: IO ()
main =
  defaultMain $
    testGroup "Tests" $
      let n = 4
       in [ testCase "send i 1 >> receive o ==> 1" $ do
              i <- LC.new n :: IO (InChan Word32)
              o <- LC.out i
              v <- LC.send i 1 >> LC.receive o
              v @?= 1
          , testCase "send i 1 >> receive o >> receive o ==> block" $ do
              i <- LC.new n :: IO (InChan Word32)
              o <- LC.out i
              v <- LC.send i 1 >> LC.receive o
              v @?= 1
              w <- timeout 1000 (LC.receive o)
              w @?= Nothing
          , testCase "for_ [0..n+1] (send i) >> receive o ==> 2" $ do
              i <- LC.new n :: IO (InChan Word32)
              o <- LC.out i
              v <- for_ [0 .. n + 1] (LC.send i) >> LC.receive o
              v @?= 2
          , testCase "for_ [0..n*2] (send i) >> receive o ==> n + 1" $ do
              i <- LC.new n :: IO (InChan Word32)
              o <- LC.out i
              v <- for_ [0 .. n * 2] (LC.send i) >> LC.receive o
              v @?= n + 1
          , testCase "for_ [0..n*L+1] (send i) >> receive o ==> n * (L - 1) + 1" $ do
              i <- LC.new n :: IO (InChan Word32)
              o <- LC.out i
              v <- for_ [0 .. n * maxLoop] (LC.send i) >> LC.receive o
              v @?= n * (maxLoop - 1) + 1
          , testCase "send i 1 >> traverse receive [o1,o2] ==> [1,1]" $ do
              i <- LC.new n :: IO (InChan Word32)
              o1 <- LC.out i
              o2 <- LC.out i
              vs <- LC.send i 1 >> traverse LC.receive [o1, o2]
              vs @?= [1, 1]
          ]

maxLoop :: Word32
maxLoop = fromIntegral (maxBound :: Word16)
